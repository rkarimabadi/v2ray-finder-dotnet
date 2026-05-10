"""Probe real internet connectivity through a running xray SOCKS5 proxy.

This is the *real* health check: we spin up xray with the server's config,
then send an HTTP request through the SOCKS5 proxy to Google's generate_204
endpoint.  A 204 response means the proxy is alive and has internet access.

All heavy lifting is done by :class:`RealConnectivityChecker`.
For batch use, call :func:`check_real_connectivity_batch`.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import struct
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .xray_config_adapter import config_to_xray
from .xray_runner import XrayRunner

logger = logging.getLogger(__name__)

_GOOGLE_204_HOST = "clients3.google.com"
_GOOGLE_204_PATH = "/generate_204"
_GOOGLE_204_PORT = 80


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def find_free_port() -> int:
    """Return an available TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class RealHealthResult:
    """Result of a real-connectivity probe through xray."""

    config: str
    protocol: str
    reachable: bool = False
    google_204_ok: bool = False
    latency_ms: Optional[float] = None
    error: Optional[str] = None
    from_cache: bool = False
    xray_version: Optional[str] = None
    socks_port: Optional[int] = None
    check_methods: List[str] = field(default_factory=list)

    @property
    def quality_score(self) -> float:
        """Score 0-100 based on reachability and latency.

        100  → reachable and latency <= 100 ms
        0    → not reachable
        Linear interpolation between 100 ms (score 100) and 2000 ms (score 0)
        for intermediate latencies.
        """
        if not self.reachable or self.latency_ms is None:
            return 0.0
        latency = self.latency_ms
        if latency <= 100.0:
            return 100.0
        if latency >= 2000.0:
            return max(0.0, 100.0 - (latency - 100.0) / 19.0)
        # Linear: 100 ms → 100, 2000 ms → 0
        return max(0.0, 100.0 - (latency - 100.0) * 100.0 / 1900.0)


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

class _ResultCache:
    """In-memory cache for connectivity check results with per-entry TTL."""

    def __init__(self) -> None:
        self._cache: Dict[str, Tuple[RealHealthResult, float, float]] = {}
        self._hits: int = 0
        self._misses: int = 0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _key(config: str) -> str:
        return config.strip()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(self, config: str) -> Optional[RealHealthResult]:
        """Return cached result or None if absent / expired."""
        key = self._key(config)
        entry = self._cache.get(key)
        if entry is None:
            self._misses += 1
            return None
        result, timestamp, ttl = entry
        if time.monotonic() - timestamp > ttl:
            del self._cache[key]
            self._misses += 1
            return None
        self._hits += 1
        return result

    def set(self, config: str, result: RealHealthResult, ttl: float = 300.0) -> None:
        """Store *result* under *config* key with the given TTL (seconds)."""
        self._cache[self._key(config)] = (result, time.monotonic(), ttl)

    def clear(self) -> None:
        """Remove all cached entries and reset counters."""
        self._cache.clear()
        self._hits = 0
        self._misses = 0

    def __len__(self) -> int:
        return len(self._cache)

    @property
    def stats(self) -> Dict[str, Any]:
        """Return hit/miss statistics."""
        total = self._hits + self._misses
        hit_rate = round(self._hits / total * 100, 1) if total else 0.0
        return {
            "hits": self._hits,
            "misses": self._misses,
            "size": len(self._cache),
            "hit_rate": hit_rate,
        }


# ---------------------------------------------------------------------------
# Checker
# ---------------------------------------------------------------------------

class RealConnectivityChecker:
    """Check real internet connectivity through xray proxies.

    Supports both sync and async (asyncio) usage::

        checker = RealConnectivityChecker()

        # Sync
        result = checker.check_server_real(uri)

        # Async
        result = await checker.check_server_real(uri)

        # Batch (async)
        results = await checker.check_servers_real_batch([(uri, protocol)])
    """

    def __init__(
        self,
        timeout: float = 10.0,
        startup_timeout: float = 5.0,
        max_workers: int = 5,
        concurrent_limit: int = 5,
        local_port_base: int = 10900,
        binary_path: Optional[str] = None,
        auto_download: bool = True,
        cache_enabled: bool = True,
        cache_ttl: float = 300.0,
        show_progress: bool = False,
    ) -> None:
        self.timeout = timeout
        self.startup_timeout = startup_timeout
        self.max_workers = max_workers
        self.concurrent_limit = concurrent_limit
        self.local_port_base = local_port_base
        self.binary_path = binary_path
        self.auto_download = auto_download
        self.cache_enabled = cache_enabled
        self.cache_ttl = cache_ttl
        self.show_progress = show_progress

        self._cache = _ResultCache()
        self._port_counter = local_port_base

        # Optional manager/adapter set by tests or subclasses
        self._manager: Any = None
        self._adapter: Any = None

    # ------------------------------------------------------------------
    # Port helpers
    # ------------------------------------------------------------------

    def _next_port(self) -> int:
        port = self._port_counter
        self._port_counter += 1
        return port

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    @property
    def cache_stats(self) -> Dict[str, Any]:
        """Return cache hit/miss statistics."""
        return self._cache.stats

    def clear_result_cache(self) -> None:
        """Clear the result cache."""
        self._cache.clear()

    # Backward-compat alias
    def clear_cache(self) -> None:
        self.clear_result_cache()

    # ------------------------------------------------------------------
    # xray availability check
    # ------------------------------------------------------------------

    def is_xray_available(self) -> bool:
        """Return True if the xray binary can be located."""
        try:
            runner = XrayRunner(
                local_port=10808,
                binary_path=self.binary_path,
                auto_download=False,
            )
            return runner.binary_path is not None
        except Exception:
            return False

    # ------------------------------------------------------------------
    # async real connectivity check
    # ------------------------------------------------------------------

    async def check_real_connectivity(
        self,
        config: str,
        socks_port: int,
    ) -> Tuple[bool, Optional[float], bool, Optional[str]]:
        """Low-level async probe: returns (reachable, latency_ms, g204_ok, error)."""
        loop = asyncio.get_event_loop()
        ok, status, latency = await loop.run_in_executor(
            None,
            lambda: _socks5_http_get(
                socks_host="127.0.0.1",
                socks_port=socks_port,
                target_host=_GOOGLE_204_HOST,
                target_port=_GOOGLE_204_PORT,
                path=_GOOGLE_204_PATH,
                timeout=self.timeout,
            ),
        )
        g204 = ok and status == 204
        return ok, latency if ok else None, g204, None

    async def check_server_real(
        self,
        config: str,
        protocol: Optional[str] = None,
    ) -> RealHealthResult:
        """Async check of a single server config."""
        if protocol is None:
            protocol = config.split("://")[0] if "://" in config else "unknown"

        # Cache hit
        if self.cache_enabled:
            cached = self._cache.get(config)
            if cached is not None:
                result = RealHealthResult(
                    config=cached.config,
                    protocol=cached.protocol,
                    reachable=cached.reachable,
                    google_204_ok=cached.google_204_ok,
                    latency_ms=cached.latency_ms,
                    error=cached.error,
                    from_cache=True,
                    xray_version=cached.xray_version,
                    socks_port=cached.socks_port,
                    check_methods=list(cached.check_methods),
                )
                return result

        # Use injected manager/adapter (for tests) or fall back to check_one
        if self._manager is not None and self._adapter is not None:
            socks_port = find_free_port()
            xray_version: Optional[str] = None
            try:
                with self._adapter.build_config_file(config, socks_port=socks_port):
                    async with self._manager.run(config, socks_port=socks_port):
                        xray_version = self._manager.get_version()
                        reachable, latency, g204, err = await self.check_real_connectivity(
                            config, socks_port
                        )
            except Exception as exc:
                reachable, latency, g204, err = False, None, False, str(exc)

            result = RealHealthResult(
                config=config,
                protocol=protocol,
                reachable=reachable,
                google_204_ok=g204,
                latency_ms=latency,
                error=err,
                from_cache=False,
                xray_version=xray_version,
                socks_port=socks_port,
                check_methods=["xray_start", "socks5_probe", "google_204"],
            )
        else:
            # Fallback: run sync check_one in executor
            loop = asyncio.get_event_loop()
            port = self._next_port()
            sync_result = await loop.run_in_executor(
                None,
                lambda: check_one(
                    config,
                    local_port=port,
                    timeout=self.timeout,
                    binary_path=self.binary_path,
                    auto_download=self.auto_download,
                ),
            )
            result = RealHealthResult(
                config=sync_result.config,
                protocol=sync_result.protocol,
                reachable=sync_result.reachable,
                google_204_ok=sync_result.google_204_ok,
                latency_ms=sync_result.latency_ms,
                error=sync_result.error,
                from_cache=False,
                socks_port=port,
                check_methods=["socks5_probe", "google_204"],
            )

        # Cache result (failed results get a short TTL)
        if self.cache_enabled:
            ttl = self.cache_ttl if result.reachable else 60.0
            self._cache.set(config, result, ttl=ttl)

        return result

    async def check_servers_real_batch(
        self,
        servers: List[Tuple[str, str]],
    ) -> List[RealHealthResult]:
        """Async batch check with semaphore-based concurrency control.

        Args:
            servers: List of (config_uri, protocol) tuples.

        Returns:
            List of :class:`RealHealthResult`, sorted best-first.
        """
        if not servers:
            return []

        semaphore = asyncio.Semaphore(self.concurrent_limit)
        consecutive_failures = [0]

        async def _checked(config: str, protocol: str) -> RealHealthResult:
            async with semaphore:
                try:
                    result = await self.check_server_real(config, protocol=protocol)
                    if not result.reachable:
                        consecutive_failures[0] += 1
                        if consecutive_failures[0] > 1:
                            await asyncio.sleep(0.1 * consecutive_failures[0])
                    else:
                        consecutive_failures[0] = 0
                    return result
                except Exception as exc:
                    consecutive_failures[0] += 1
                    return RealHealthResult(
                        config=config,
                        protocol=protocol,
                        reachable=False,
                        error=str(exc),
                    )

        tasks = [_checked(cfg, proto) for cfg, proto in servers]
        results = await asyncio.gather(*tasks)
        return list(results)

    # ------------------------------------------------------------------
    # Sync wrappers (backward compat)
    # ------------------------------------------------------------------

    def check_server_real_sync(self, uri: str, use_cache: bool = True) -> RealHealthResult:
        """Synchronous wrapper around check_server_real."""
        if use_cache and self.cache_enabled:
            cached = self._cache.get(uri)
            if cached is not None:
                return cached

        result = check_one(
            uri,
            local_port=self._next_port(),
            timeout=self.timeout,
            binary_path=self.binary_path,
            auto_download=self.auto_download,
        )
        rhr = RealHealthResult(
            config=result.config,
            protocol=result.protocol,
            reachable=result.reachable,
            google_204_ok=result.google_204_ok,
            latency_ms=result.latency_ms,
            error=result.error,
        )
        if use_cache and self.cache_enabled:
            ttl = self.cache_ttl if rhr.reachable else 60.0
            self._cache.set(uri, rhr, ttl=ttl)
        return rhr


# ---------------------------------------------------------------------------
# Low-level SOCKS5 probe
# ---------------------------------------------------------------------------

def _socks5_http_get(
    socks_host: str,
    socks_port: int,
    target_host: str,
    target_port: int,
    path: str,
    timeout: float = 8.0,
) -> Tuple[bool, int, float]:
    """Send an HTTP GET through a SOCKS5 proxy without auth.

    Returns (success, http_status_code, latency_ms).
    """
    t0 = time.monotonic()
    try:
        sock = socket.create_connection((socks_host, socks_port), timeout=timeout)
        sock.settimeout(timeout)

        sock.sendall(b"\x05\x01\x00")
        resp = sock.recv(2)
        if len(resp) < 2 or resp[1] != 0x00:
            sock.close()
            return False, 0, (time.monotonic() - t0) * 1000

        host_bytes = target_host.encode()
        request = (
            b"\x05\x01\x00\x03"
            + bytes([len(host_bytes)])
            + host_bytes
            + struct.pack(">H", target_port)
        )
        sock.sendall(request)
        conn_resp = sock.recv(10)
        if len(conn_resp) < 2 or conn_resp[1] != 0x00:
            sock.close()
            return False, 0, (time.monotonic() - t0) * 1000

        http_req = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {target_host}\r\n"
            "Connection: close\r\n"
            "User-Agent: v2ray-finder-probe/1.0\r\n"
            "\r\n"
        ).encode()
        sock.sendall(http_req)

        raw = b""
        while True:
            chunk = sock.recv(256)
            if not chunk:
                break
            raw += chunk
            if b"\r\n" in raw:
                break
        sock.close()

        latency = (time.monotonic() - t0) * 1000
        first_line = raw.split(b"\r\n")[0].decode(errors="replace")
        parts = first_line.split()
        if len(parts) >= 2:
            status = int(parts[1])
            return True, status, latency
        return False, 0, latency

    except Exception as exc:
        logger.debug("SOCKS5 probe failed: %s", exc)
        return False, 0, (time.monotonic() - t0) * 1000


# ---------------------------------------------------------------------------
# Standalone check_one (used by sync path and legacy callers)
# ---------------------------------------------------------------------------

@dataclass
class _LegacyResult:
    """Minimal result for check_one — consumed internally only."""
    config: str
    protocol: str
    reachable: bool = False
    google_204_ok: bool = False
    latency_ms: Optional[float] = None
    error: Optional[str] = None


def check_one(
    uri: str,
    local_port: int = 10808,
    timeout: float = 10.0,
    binary_path: Optional[str] = None,
    auto_download: bool = True,
) -> _LegacyResult:
    """Spin up xray for *uri*, probe Google 204, return result."""
    if "://" not in uri:
        return _LegacyResult(config=uri, protocol="unknown", error="Not a valid URI")

    protocol = uri.split("://")[0].lower()
    xray_cfg = config_to_xray(uri, local_port=local_port)
    if xray_cfg is None:
        return _LegacyResult(
            config=uri, protocol=protocol,
            error="Could not convert URI to xray config",
        )

    runner = XrayRunner(
        local_port=local_port,
        binary_path=binary_path,
        auto_download=auto_download,
    )
    try:
        runner.start(xray_cfg)
    except RuntimeError as exc:
        return _LegacyResult(
            config=uri, protocol=protocol,
            error=f"xray start failed: {exc}",
        )

    try:
        ok, status, latency = _socks5_http_get(
            socks_host="127.0.0.1",
            socks_port=local_port,
            target_host=_GOOGLE_204_HOST,
            target_port=_GOOGLE_204_PORT,
            path=_GOOGLE_204_PATH,
            timeout=timeout,
        )
        g204 = ok and status == 204
        return _LegacyResult(
            config=uri, protocol=protocol,
            reachable=ok, google_204_ok=g204,
            latency_ms=latency,
        )
    finally:
        runner.stop()


# ---------------------------------------------------------------------------
# Batch helper (public, backward compat)
# ---------------------------------------------------------------------------

def check_real_connectivity_batch(
    uris: List[str],
    max_workers: int = 5,
    local_port_base: int = 10900,
    timeout: float = 10.0,
    binary_path: Optional[str] = None,
    auto_download: bool = True,
) -> List[RealHealthResult]:
    """Run real-connectivity checks on a list of URIs concurrently."""
    import threading
    results: List[RealHealthResult] = []
    port_lock = threading.Lock()
    port_counter = [local_port_base]

    def _get_port() -> int:
        with port_lock:
            p = port_counter[0]
            port_counter[0] += 1
            return p

    def _worker(uri: str) -> RealHealthResult:
        port = _get_port()
        r = check_one(uri, local_port=port, timeout=timeout,
                      binary_path=binary_path, auto_download=auto_download)
        return RealHealthResult(
            config=r.config, protocol=r.protocol,
            reachable=r.reachable, google_204_ok=r.google_204_ok,
            latency_ms=r.latency_ms, error=r.error,
        )

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_worker, u): u for u in uris}
        for fut in as_completed(futures):
            try:
                results.append(fut.result())
            except Exception as exc:
                uri = futures[fut]
                results.append(RealHealthResult(
                    config=uri,
                    protocol=uri.split("://")[0] if "://" in uri else "unknown",
                    error=str(exc),
                ))

    results.sort(key=lambda r: (not r.google_204_ok, not r.reachable,
                                r.latency_ms if r.latency_ms is not None else 9999))
    return results


def real_health_to_dict(r: RealHealthResult) -> Dict:
    """Serialise to plain dict (CLI / JSON output)."""
    return {
        "config": r.config,
        "protocol": r.protocol,
        "reachable": r.reachable,
        "google_204_ok": r.google_204_ok,
        "latency_ms": r.latency_ms,
        "error": r.error,
        "from_cache": r.from_cache,
        "xray_version": r.xray_version,
        "quality_score": r.quality_score,
    }
