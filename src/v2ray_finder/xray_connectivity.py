"""Probe real internet connectivity through a running xray SOCKS5 proxy.

This is the *real* health check: we spin up xray with the server's config,
then send an HTTP request through the SOCKS5 proxy to Google's generate_204
endpoint.  A 204 response means the proxy is alive and has internet access.

All heavy lifting is done by :class:`XrayConnectivityChecker`.
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
from typing import Dict, List, Optional, Tuple

from .xray_config_adapter import config_to_xray
from .xray_runner import XrayRunner

logger = logging.getLogger(__name__)

_GOOGLE_204_HOST = "clients3.google.com"
_GOOGLE_204_PATH = "/generate_204"
_GOOGLE_204_PORT = 80


@dataclass
class RealHealthResult:
    """Result of a real-connectivity probe through xray."""

    config: str
    protocol: str
    reachable: bool = False
    google_204_ok: bool = False
    latency_ms: float = 0.0
    error: Optional[str] = None


class _ResultCache:
    """Simple in-memory cache for connectivity check results."""

    def __init__(self, ttl_seconds: float = 300.0) -> None:
        self._ttl = ttl_seconds
        self._cache: Dict[str, Tuple[RealHealthResult, float]] = {}

    def get(self, key: str) -> Optional[RealHealthResult]:
        entry = self._cache.get(key)
        if entry is None:
            return None
        result, timestamp = entry
        if time.monotonic() - timestamp > self._ttl:
            del self._cache[key]
            return None
        return result

    def set(self, key: str, result: RealHealthResult) -> None:
        self._cache[key] = (result, time.monotonic())

    def clear(self) -> None:
        self._cache.clear()

    def __len__(self) -> int:
        return len(self._cache)


class RealConnectivityChecker:
    """Check real internet connectivity through xray proxies.

    Supports both sync and async (asyncio) usage::

        checker = RealConnectivityChecker()

        # Sync
        result = checker.check_server_real(uri)

        # Async
        result = await checker.check_server_real_async(uri)

        # Batch (async)
        results = await checker.check_servers_real_batch([uri1, uri2])
    """

    def __init__(
        self,
        timeout: float = 10.0,
        max_workers: int = 5,
        local_port_base: int = 10900,
        binary_path: Optional[str] = None,
        auto_download: bool = True,
        cache_ttl: float = 300.0,
    ) -> None:
        self.timeout = timeout
        self.max_workers = max_workers
        self.local_port_base = local_port_base
        self.binary_path = binary_path
        self.auto_download = auto_download
        self._cache = _ResultCache(ttl_seconds=cache_ttl)
        self._port_counter = local_port_base

    def _next_port(self) -> int:
        port = self._port_counter
        self._port_counter += 1
        return port

    def check_server_real(self, uri: str, use_cache: bool = True) -> RealHealthResult:
        """Synchronously check real connectivity for a single URI."""
        if use_cache:
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

        if use_cache:
            self._cache.set(uri, result)
        return result

    async def check_server_real_async(
        self, uri: str, use_cache: bool = True
    ) -> RealHealthResult:
        """Asynchronously check real connectivity for a single URI."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, lambda: self.check_server_real(uri, use_cache=use_cache)
        )

    async def check_servers_real_batch(
        self, uris: List[str], use_cache: bool = True
    ) -> List[RealHealthResult]:
        """Asynchronously check a batch of URIs for real connectivity."""
        tasks = [self.check_server_real_async(uri, use_cache=use_cache) for uri in uris]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        out: List[RealHealthResult] = []
        for uri, r in zip(uris, results):
            if isinstance(r, Exception):
                out.append(
                    RealHealthResult(
                        config=uri,
                        protocol=uri.split("://")[0] if "://" in uri else "unknown",
                        error=str(r),
                    )
                )
            else:
                out.append(r)
        out.sort(key=lambda x: (not x.google_204_ok, not x.reachable, x.latency_ms))
        return out

    def clear_cache(self) -> None:
        """Clear the result cache."""
        self._cache.clear()


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

        # SOCKS5 handshake: no auth
        sock.sendall(b"\x05\x01\x00")
        resp = sock.recv(2)
        if len(resp) < 2 or resp[1] != 0x00:
            sock.close()
            return False, 0, (time.monotonic() - t0) * 1000

        # SOCKS5 CONNECT to target_host:target_port
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

        # HTTP GET
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


def check_one(
    uri: str,
    local_port: int = 10808,
    timeout: float = 10.0,
    binary_path: Optional[str] = None,
    auto_download: bool = True,
) -> RealHealthResult:
    """Spin up xray for *uri*, probe Google 204, return result."""
    if "://" not in uri:
        return RealHealthResult(
            config=uri,
            protocol="unknown",
            error="Not a valid URI",
        )

    protocol = uri.split("://")[0].lower()
    xray_cfg = config_to_xray(uri, local_port=local_port)
    if xray_cfg is None:
        return RealHealthResult(
            config=uri,
            protocol=protocol,
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
        return RealHealthResult(
            config=uri,
            protocol=protocol,
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
        return RealHealthResult(
            config=uri,
            protocol=protocol,
            reachable=ok,
            google_204_ok=g204,
            latency_ms=latency,
        )
    finally:
        runner.stop()


def check_real_connectivity_batch(
    uris: List[str],
    max_workers: int = 5,
    local_port_base: int = 10900,
    timeout: float = 10.0,
    binary_path: Optional[str] = None,
    auto_download: bool = True,
) -> List[RealHealthResult]:
    """Run real-connectivity checks on a list of URIs concurrently."""
    results: List[RealHealthResult] = []

    import threading
    port_lock = threading.Lock()
    port_counter = [local_port_base]

    def _get_port() -> int:
        with port_lock:
            p = port_counter[0]
            port_counter[0] += 1
            return p

    def _worker(uri: str) -> RealHealthResult:
        port = _get_port()
        return check_one(
            uri,
            local_port=port,
            timeout=timeout,
            binary_path=binary_path,
            auto_download=auto_download,
        )

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_worker, u): u for u in uris}
        for fut in as_completed(futures):
            try:
                results.append(fut.result())
            except Exception as exc:
                uri = futures[fut]
                results.append(
                    RealHealthResult(
                        config=uri,
                        protocol=uri.split("://")[0] if "://" in uri else "unknown",
                        error=str(exc),
                    )
                )

    results.sort(key=lambda r: (not r.google_204_ok, not r.reachable, r.latency_ms))
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
    }
