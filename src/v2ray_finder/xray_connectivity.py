"""Layer 3 — RealConnectivityChecker: end-to-end proxy connectivity test.

This module ties together all three layers of the xray integration:

1. :class:`~v2ray_finder.xray_runner.XrayBinaryManager` — start/stop xray
2. :class:`~v2ray_finder.xray_config_adapter.ConfigAdapter` — build config
3. This module — send an HTTP probe *through* the running proxy and
   measure real latency to ``connectivitycheck.gstatic.com/generate_204``

Why generate_204?
-----------------
Google's generate_204 endpoint returns an **empty HTTP 204** response
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

Nice-to-have features (v0.5.1)
-------------------------------
* **Rate limiting / backoff** — consecutive failures trigger exponential
  backoff with full jitter (base 0.5 s, cap 8 s) so a flapping network
  or a batch of broken servers does not hammer the host machine.
* **Progress bar** — ``tqdm`` is used when installed and
  ``show_progress=True``; falls back gracefully to periodic
  ``logger.info`` log lines when tqdm is absent.
* **Result cache** — successful (and briefly, failed) real-check results
  are stored in an in-memory dict keyed by a SHA-256 hash of the config
  string.  Re-checking the same server within the TTL window returns the
  cached result immediately without launching xray again.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import socket
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_GOOGLE_204_URL = "http://connectivitycheck.gstatic.com/generate_204"
_FALLBACK_204_URL = "http://www.gstatic.com/generate_204"

# ---------------------------------------------------------------------------
# Backoff constants
# ---------------------------------------------------------------------------
_BACKOFF_BASE = 0.5   # seconds
_BACKOFF_CAP = 8.0    # seconds
_BACKOFF_RESET_AFTER = 1  # consecutive successes needed to reset


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
    from_cache: bool = False  # True when result was served from cache

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
# Simple in-process result cache
# ---------------------------------------------------------------------------


class _ResultCache:
    """Lightweight in-memory cache for :class:`RealHealthResult` objects.

    Keys are SHA-256 hashes of the raw config string so that two
    identical configs always map to the same bucket regardless of
    whitespace or ordering.
    """

    def __init__(self) -> None:
        # key -> (result, expiry_epoch_float)
        self._store: Dict[str, Tuple[RealHealthResult, float]] = {}
        self._hits = 0
        self._misses = 0

    # ------------------------------------------------------------------
    @staticmethod
    def _key(config: str) -> str:
        return hashlib.sha256(config.strip().encode()).hexdigest()

    # ------------------------------------------------------------------
    def get(self, config: str) -> Optional[RealHealthResult]:
        key = self._key(config)
        entry = self._store.get(key)
        if entry is None:
            self._misses += 1
            return None
        result, expiry = entry
        if time.monotonic() > expiry:
            del self._store[key]
            self._misses += 1
            return None
        self._hits += 1
        return result

    def set(self, config: str, result: RealHealthResult, ttl: float) -> None:
        key = self._key(config)
        self._store[key] = (result, time.monotonic() + ttl)

    def clear(self) -> None:
        self._store.clear()
        self._hits = 0
        self._misses = 0

    @property
    def stats(self) -> Dict:
        total = self._hits + self._misses
        return {
            "hits": self._hits,
            "misses": self._misses,
            "size": len(self._store),
            "hit_rate": round(self._hits / total * 100, 1) if total else 0.0,
        }


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
    cache_enabled:
        Cache real-check results so the same server is not re-probed
        within the TTL window.  Default ``True``.
    cache_ttl:
        Seconds to keep a **successful** result cached.  Failed results
        are cached for a much shorter window (60 s) so transient errors
        recover quickly.  Default ``600`` (10 min).
    show_progress:
        Show a ``tqdm`` progress bar during batch checks.  Falls back to
        periodic ``logger.info`` log lines when tqdm is not installed.
        Default ``False``.
    """

    def __init__(
        self,
        binary_path: Optional[str] = None,
        auto_download: bool = True,
        timeout: float = 10.0,
        startup_timeout: float = 5.0,
        concurrent_limit: int = 10,
        cache_enabled: bool = True,
        cache_ttl: float = 600.0,
        show_progress: bool = False,
    ) -> None:
        self.timeout = timeout
        self.startup_timeout = startup_timeout
        self.concurrent_limit = concurrent_limit
        self.cache_enabled = cache_enabled
        self.cache_ttl = cache_ttl
        self.show_progress = show_progress

        self._cache = _ResultCache()

        from v2ray_finder.xray_runner import XrayBinaryManager

        self._manager = XrayBinaryManager(
            binary_path=binary_path,
            auto_download=auto_download,
            startup_timeout=startup_timeout,
        )
        from v2ray_finder.xray_config_adapter import ConfigAdapter

        self._adapter = ConfigAdapter(log_level="none")

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    def clear_result_cache(self) -> None:
        """Invalidate all cached real-check results."""
        self._cache.clear()
        logger.info("Real-check result cache cleared")

    @property
    def cache_stats(self) -> Dict:
        """Return cache hit/miss statistics."""
        return self._cache.stats

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

        Checks the result cache first; only launches xray when no valid
        cached entry exists.

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

        # --- cache lookup ---
        if self.cache_enabled:
            cached = self._cache.get(config)
            if cached is not None:
                logger.debug("Cache hit for %s", config[:60])
                # Return a copy flagged as from_cache so callers can tell
                return RealHealthResult(
                    config=cached.config,
                    protocol=cached.protocol,
                    reachable=cached.reachable,
                    latency_ms=cached.latency_ms,
                    google_204_ok=cached.google_204_ok,
                    error=cached.error,
                    xray_version=cached.xray_version,
                    socks_port=cached.socks_port,
                    check_methods=list(cached.check_methods),
                    from_cache=True,
                )

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
            result = RealHealthResult(
                config=config,
                protocol=protocol,
                reachable=False,
                error=f"Unsupported protocol: {exc}",
                xray_version=version,
                socks_port=socks_port,
                check_methods=[],
            )
            if self.cache_enabled:
                self._cache.set(config, result, ttl=60.0)
            return result
        except RuntimeError as exc:
            result = RealHealthResult(
                config=config,
                protocol=protocol,
                reachable=False,
                error=str(exc),
                xray_version=version,
                socks_port=socks_port,
                check_methods=["xray_start"],
            )
            if self.cache_enabled:
                self._cache.set(config, result, ttl=60.0)
            return result
        except Exception as exc:
            result = RealHealthResult(
                config=config,
                protocol=protocol,
                reachable=False,
                error=f"Unexpected error: {exc}",
                xray_version=version,
                socks_port=socks_port,
                check_methods=["xray_start"],
            )
            if self.cache_enabled:
                self._cache.set(config, result, ttl=60.0)
            return result

        result = RealHealthResult(
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

        # --- cache store ---
        if self.cache_enabled:
            ttl = self.cache_ttl if reachable else 60.0
            self._cache.set(config, result, ttl=ttl)

        return result

    # ------------------------------------------------------------------
    # Batch check  (rate-limiting backoff + progress bar)
    # ------------------------------------------------------------------

    async def check_servers_real_batch(
        self,
        servers: List[Tuple[str, str]],
    ) -> List[RealHealthResult]:
        """Check multiple servers concurrently with backoff and progress.

        **Rate-limiting / backoff**
        Each task slot uses a shared *failure counter*.  When consecutive
        failures accumulate the slot sleeps for::

            sleep = min(cap, base * 2^failures) * random(0, 1)  # full jitter

        A single success resets the counter so well-behaved servers are
        never penalised.

        **Progress bar**
        If ``tqdm`` is installed and ``show_progress=True`` a live bar is
        displayed.  Otherwise a ``logger.info`` line is emitted every 10 %
        of completion.

        Parameters
        ----------
        servers:
            List of ``(config, protocol)`` tuples.

        Returns
        -------
        List of :class:`RealHealthResult` in the same order as input.
        """
        total = len(servers)
        if total == 0:
            return []

        semaphore = asyncio.Semaphore(self.concurrent_limit)
        # Shared mutable state — protected by semaphore slots (one writer
        # per slot, asyncio is single-threaded)
        consecutive_failures = 0
        completed = 0
        log_step = max(1, total // 10)  # log every ~10 %

        # --- tqdm setup ---
        try:
            if self.show_progress:
                from tqdm.asyncio import tqdm as atqdm
                progress = atqdm(total=total, desc="xray checks", unit="srv")
            else:
                progress = None
        except ImportError:
            progress = None

        async def _check(
            config: str, protocol: str, idx: int
        ) -> RealHealthResult:
            nonlocal consecutive_failures, completed

            async with semaphore:
                # --- backoff before the check ---
                if consecutive_failures > 0:
                    sleep_s = min(
                        _BACKOFF_CAP,
                        _BACKOFF_BASE * (2 ** consecutive_failures),
                    ) * random.random()  # full jitter
                    logger.debug(
                        "Backoff %.2fs after %d consecutive failures",
                        sleep_s,
                        consecutive_failures,
                    )
                    await asyncio.sleep(sleep_s)

                result = await self.check_server_real(config, protocol)

                # --- update failure counter ---
                if result.reachable:
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1

                # --- progress reporting ---
                completed += 1
                if progress is not None:
                    progress.update(1)
                elif completed % log_step == 0 or completed == total:
                    logger.info(
                        "xray batch: %d/%d checked (%.0f%%)",
                        completed,
                        total,
                        completed / total * 100,
                    )

                return result

        tasks = [
            _check(cfg, proto, i) for i, (cfg, proto) in enumerate(servers)
        ]
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

        if progress is not None:
            progress.close()

        # Log cache efficiency at the end of a batch
        if self.cache_enabled:
            cs = self._cache.stats
            logger.info(
                "xray result cache: %d hits / %d misses (%.0f%% hit rate, %d entries)",
                cs["hits"],
                cs["misses"],
                cs["hit_rate"],
                cs["size"],
            )

        out: List[RealHealthResult] = []
        for i, result in enumerate(raw_results):
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
