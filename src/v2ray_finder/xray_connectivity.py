"""Probe real internet connectivity through a running xray SOCKS5 proxy.

This is the *real* health check: we spin up xray with the server's config,
then send an HTTP request through the SOCKS5 proxy to Google's generate_204
endpoint.  A 204 response means the proxy is alive and has internet access.

All heavy lifting is done by :class:`XrayConnectivityChecker`.
For batch use, call :func:`check_real_connectivity_batch`.
"""

from __future__ import annotations

import logging
import socket
import struct
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
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
    """Spin up xray for *uri*, probe Google 204, return result.

    Each call starts a fresh xray process on *local_port*, so
    sequential calls share no state.  For batch use, prefer
    :func:`check_real_connectivity_batch`.
    """
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
    """Run real-connectivity checks on a list of URIs concurrently.

    Each worker gets its own port (local_port_base + worker_index) so
    processes don't collide.  Keep max_workers low (3-8) because each
    worker runs a separate xray process.

    Args:
        uris:            Server config strings.
        max_workers:     Parallel xray instances (default 5).
        local_port_base: First SOCKS5 port; workers use base+0, base+1, …
        timeout:         HTTP probe timeout per server.
        binary_path:     Explicit xray binary path (or None for auto).
        auto_download:   Download xray if not found.

    Returns:
        List of :class:`RealHealthResult`, sorted reachable-first.
    """
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
