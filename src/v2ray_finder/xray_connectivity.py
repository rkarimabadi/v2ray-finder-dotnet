"""Layer 3 — RealConnectivityChecker: end-to-end proxy connectivity test.

This module ties together all three layers of the xray integration:

1. :class:`~v2ray_finder.xray_runner.XrayBinaryManager` — start/stop xray
2. :class:`~v2ray_finder.xray_config_adapter.ConfigAdapter` — build config
3. This module — send an HTTP probe *through* the running proxy and
   measure real latency to ``connectivitycheck.gstatic.com/generate_204``

Why generate_204?
-----------------
Google’s generate_204 endpoint returns an **empty HTTP 204** response
with no body.  It is:

* Stable — Google has served it since Android 2.x for captive-portal
  detection and has no incentive to remove it.
* Minimal — zero-byte body means latency measures pure round-trip time
  through the proxy, not download time.
* Globally reachable — accessible from virtually every country/network
  (CDN-backed, multiple IPs).
* Unambiguous — any status other than 204 indicates the proxy is
  mis-configured or the server is not passing traffic correctly.

Design decisions
----------------
* xray is started and stopped **per server** to guarantee isolation;
  a crashed or mis-configured server cannot affect the next check.
* Port allocation uses :func:`find_free_port` (bind to port 0, read
  back the OS-assigned port) to avoid collisions in concurrent use.
* The SOCKS5 proxy is consumed via ``aiohttp`` with
  ``aiohttp_socks.ProxyConnector`` — the only new dependency added by
  this module.  It is listed as an optional extra (``xray``).
* The public API mirrors :class:`~v2ray_finder.health_checker.HealthChecker`
  so callers can swap one for the other with minimal changes.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import time
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

_GOOGLE_204_URL = "http://connectivitycheck.gstatic.com/generate_204"
_FALLBACK_204_URL = "http://www.gstatic.com/generate_204"


# ---------------------------------------------------------------------------
# Port allocation helper
# ---------------------------------------------------------------------------


def find_free_port() -> int:
    """Return an unused TCP port on localhost.

    Binds to port 0 (OS assigns a free port), reads the port back, then
    closes the socket.  There is a small TOCTOU window but it is
    acceptable for local use.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass
class RealHealthResult:
    """Result of a real end-to-end connectivity check via xray."""

    config: str
    protocol: str
    reachable: bool = False
    latency_ms: Optional[float] = None
    google_204_ok: bool = False
    error: Optional[str] = None
    xray_version: str = "unknown"
    socks_port: Optional[int] = None
    check_methods: List[str] = field(default_factory=list)

    @property
    def quality_score(self) -> float:
        """0–100 score based on real latency through the proxy."""
        if not self.reachable:
            return 0.0
        if self.latency_ms is None:
            return 10.0
        if self.latency_ms <= 100:
            return 100.0
        if self.latency_ms <= 300:
            return 100.0 - (self.latency_ms - 100) * 0.25
        if self.latency_ms <= 1000:
            return 50.0 - (self.latency_ms - 300) / 700 * 40
        return max(0.0, 10.0 - (self.latency_ms - 1000) / 5000 * 10)


# ---------------------------------------------------------------------------
# RealConnectivityChecker
# ---------------------------------------------------------------------------


class RealConnectivityChecker:
    """End-to-end proxy connectivity checker using xray-core.

    Parameters
    ----------
    binary_path:
        Explicit path to the xray binary.  ``None`` → auto-discover.
    auto_download:
        Download xray automatically if not found.
    timeout:
        Seconds to wait for the HTTP probe through the proxy.
    startup_timeout:
        Seconds to wait for xray to finish starting up.
    concurrent_limit:
        Max number of simultaneous xray instances during batch checks.
    """

    def __init__(
        self,
        binary_path: Optional[str] = None,
        auto_download: bool = True,
        timeout: float = 10.0,
        startup_timeout: float = 5.0,
        concurrent_limit: int = 10,
    ) -> None:
        self.timeout = timeout
        self.startup_timeout = startup_timeout
        self.concurrent_limit = concurrent_limit

        # Import here so the module can be imported even when xray_runner
        # is not yet initialised
        from v2ray_finder.xray_runner import XrayBinaryManager

        self._manager = XrayBinaryManager(
            binary_path=binary_path,
            auto_download=auto_download,
            startup_timeout=startup_timeout,
        )
        from v2ray_finder.xray_config_adapter import ConfigAdapter

        self._adapter = ConfigAdapter(log_level="none")

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    def is_xray_available(self) -> bool:
        """Return True if xray binary is findable."""
        return self._manager.is_available()

    def get_xray_version(self) -> str:
        """Return the xray version string, e.g. ``'Xray 24.9.30'``."""
        return self._manager.get_version()

    # ------------------------------------------------------------------
    # Single-server check
    # ------------------------------------------------------------------

    async def check_real_connectivity(
        self,
        socks_port: int,
        protocol: str,
    ) -> Tuple[bool, Optional[float], bool, Optional[str]]:
        """Send an HTTP probe through an already-running SOCKS5 proxy.

        Parameters
        ----------
        socks_port:
            Local port of the running xray SOCKS5 inbound.
        protocol:
            Protocol name for logging.

        Returns
        -------
        (reachable, latency_ms, google_204_ok, error_msg)
        """
        try:
            from aiohttp_socks import ProxyConnector
        except ImportError as exc:
            raise ImportError(
                "aiohttp_socks is required for real connectivity checks. "
                "Install it with: pip install aiohttp-socks"
            ) from exc

        import aiohttp

        connector = ProxyConnector.from_url(
            f"socks5://127.0.0.1:{socks_port}",
            ssl=False,
        )
        start = time.perf_counter()
        try:
            async with aiohttp.ClientSession(
                connector=connector,
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as session:
                async with session.get(
                    _GOOGLE_204_URL, allow_redirects=False
                ) as resp:
                    latency_ms = (time.perf_counter() - start) * 1000
                    if resp.status == 204:
                        return True, latency_ms, True, None
                    return (
                        True,
                        latency_ms,
                        False,
                        f"Unexpected status {resp.status} (expected 204)",
                    )
        except asyncio.TimeoutError:
            return False, None, False, f"Probe timed out after {self.timeout}s"
        except Exception as exc:
            return False, None, False, str(exc)

    async def check_server_real(
        self,
        config: str,
        protocol: Optional[str] = None,
    ) -> RealHealthResult:
        """Full three-layer check for a single server.

        1. Builds an xray config via ConfigAdapter.
        2. Starts xray via XrayBinaryManager.
        3. Sends an HTTP probe through the SOCKS5 port.
        4. Stops xray and returns the result.

        Parameters
        ----------
        config:
            Raw vmess://, vless://, trojan://, or ss:// string.
        protocol:
            Protocol name; auto-detected from the config prefix when
            *None*.
        """
        if protocol is None:
            protocol = config.split("://")[0] if "://" in config else "unknown"

        from v2ray_finder.xray_config_adapter import UnsupportedProtocolError

        socks_port = find_free_port()
        version = self._manager.get_version()

        try:
            with self._adapter.build_config_file(config, socks_port) as cfg_path:
                async with self._manager.run(cfg_path, socks_port):
                    reachable, latency_ms, g204_ok, error = (
                        await self.check_real_connectivity(socks_port, protocol)
                    )
        except UnsupportedProtocolError as exc:
            return RealHealthResult(
                config=config,
                protocol=protocol,
                reachable=False,
                error=f"Unsupported protocol: {exc}",
                xray_version=version,
                socks_port=socks_port,
                check_methods=[],
            )
        except RuntimeError as exc:
            # xray failed to start
            return RealHealthResult(
                config=config,
                protocol=protocol,
                reachable=False,
                error=str(exc),
                xray_version=version,
                socks_port=socks_port,
                check_methods=["xray_start"],
            )
        except Exception as exc:
            return RealHealthResult(
                config=config,
                protocol=protocol,
                reachable=False,
                error=f"Unexpected error: {exc}",
                xray_version=version,
                socks_port=socks_port,
                check_methods=["xray_start"],
            )

        return RealHealthResult(
            config=config,
            protocol=protocol,
            reachable=reachable,
            latency_ms=latency_ms,
            google_204_ok=g204_ok,
            error=error,
            xray_version=version,
            socks_port=socks_port,
            check_methods=["xray_start", "socks5_probe", "google_204"],
        )

    # ------------------------------------------------------------------
    # Batch check
    # ------------------------------------------------------------------

    async def check_servers_real_batch(
        self,
        servers: List[Tuple[str, str]],
    ) -> List[RealHealthResult]:
        """Check multiple servers concurrently.

        Parameters
        ----------
        servers:
            List of ``(config, protocol)`` tuples.

        Returns
        -------
        List of :class:`RealHealthResult` in the same order as input.
        """
        semaphore = asyncio.Semaphore(self.concurrent_limit)

        async def _check(config: str, protocol: str) -> RealHealthResult:
            async with semaphore:
                return await self.check_server_real(config, protocol)

        tasks = [_check(cfg, proto) for cfg, proto in servers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        out: List[RealHealthResult] = []
        for i, result in enumerate(results):
            if isinstance(result, RealHealthResult):
                out.append(result)
            else:
                cfg, proto = servers[i]
                out.append(
                    RealHealthResult(
                        config=cfg,
                        protocol=proto,
                        reachable=False,
                        error=str(result),
                    )
                )
        return out

    def check_servers_real(
        self,
        servers: List[Tuple[str, str]],
    ) -> List[RealHealthResult]:
        """Synchronous wrapper around :meth:`check_servers_real_batch`."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            return loop.run_until_complete(self.check_servers_real_batch(servers))
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(self.check_servers_real_batch(servers))
            finally:
                loop.close()
