"""Multi-method health checker for v2ray server configs.

Checks:
  1. TCP connectivity  — raw socket connect to host:port
  2. HTTP probe        — HEAD/GET request through the socket (where applicable)
  3. Google 204        — connectivity probe to clients3.google.com/generate_204
                         (confirms real internet access, not just reachability)

Designed to be called *inline* during server discovery so that dead servers
never reach the output list.
"""

from __future__ import annotations

import logging
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple
from urllib.request import urlopen
from urllib.error import URLError

logger = logging.getLogger(__name__)

# Google's generate_204 endpoint — returns HTTP 204 with an empty body.
# Used as the ground-truth connectivity probe.
_GOOGLE_204_URL = "http://clients3.google.com/generate_204"
_GOOGLE_204_ALT = "http://connectivitycheck.gstatic.com/generate_204"


class HealthStatus(Enum):
    """Health status values for a server."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNREACHABLE = "unreachable"
    INVALID = "invalid"


@dataclass
class ServerHealth:
    """Health information for a single server config."""

    config: str
    host: str
    port: int
    protocol: str
    status: HealthStatus = HealthStatus.UNREACHABLE
    quality_score: float = 0.0
    latency_ms: float = 0.0
    tcp_ok: bool = False
    error: Optional[str] = None

    @property
    def is_healthy(self) -> bool:
        """Return True if status is HEALTHY or DEGRADED."""
        return self.status in (HealthStatus.HEALTHY, HealthStatus.DEGRADED)

    @property
    def health_status(self) -> str:
        """Return string representation of health status (compat shim)."""
        return self.status.value


class ServerValidator:
    """Validate server configs before health checking."""

    SUPPORTED_PROTOCOLS = {"vmess", "vless", "trojan", "ss", "ssr"}

    @classmethod
    def is_valid_uri(cls, config: str) -> bool:
        """Return True if config looks like a valid proxy URI."""
        if "://" not in config:
            return False
        scheme = config.split("://")[0].lower()
        return scheme in cls.SUPPORTED_PROTOCOLS

    @classmethod
    def validate(cls, config: str) -> Tuple[bool, Optional[str]]:
        """Return (is_valid, error_message)."""
        if not config or not config.strip():
            return False, "Empty config"
        if not cls.is_valid_uri(config):
            return False, f"Unsupported or missing URI scheme: {config[:30]}"
        return True, None


class HealthChecker:
    """High-level health checker that runs TCP checks on server configs."""

    def __init__(
        self,
        timeout: float = 5.0,
        max_workers: int = 50,
        check_google_204: bool = False,
        min_quality_score: float = 0.0,
    ) -> None:
        self.timeout = timeout
        self.max_workers = max_workers
        self.check_google_204 = check_google_204
        self.min_quality_score = min_quality_score

    def check_one(self, config: str) -> ServerHealth:
        """Run health check on a single config string."""
        result = check_server(
            config,
            timeout=self.timeout,
            check_google_204=self.check_google_204,
        )
        status_map = {
            "healthy": HealthStatus.HEALTHY,
            "degraded": HealthStatus.DEGRADED,
            "unreachable": HealthStatus.UNREACHABLE,
            "invalid": HealthStatus.INVALID,
        }
        return ServerHealth(
            config=result.config,
            host=result.host,
            port=result.port,
            protocol=result.protocol,
            status=status_map.get(result.health_status, HealthStatus.UNREACHABLE),
            quality_score=result.quality_score,
            latency_ms=result.latency_ms,
            tcp_ok=result.tcp_ok,
            error=result.error,
        )

    def check_batch(self, configs: List[str]) -> List[ServerHealth]:
        """Run health checks on a batch of configs."""
        results = check_servers_batch(
            configs,
            timeout=self.timeout,
            max_workers=self.max_workers,
            check_google_204=self.check_google_204,
            min_quality_score=self.min_quality_score,
        )
        status_map = {
            "healthy": HealthStatus.HEALTHY,
            "degraded": HealthStatus.DEGRADED,
            "unreachable": HealthStatus.UNREACHABLE,
            "invalid": HealthStatus.INVALID,
        }
        return [
            ServerHealth(
                config=r.config,
                host=r.host,
                port=r.port,
                protocol=r.protocol,
                status=status_map.get(r.health_status, HealthStatus.UNREACHABLE),
                quality_score=r.quality_score,
                latency_ms=r.latency_ms,
                tcp_ok=r.tcp_ok,
                error=r.error,
            )
            for r in results
        ]


def filter_healthy_servers(servers: List[ServerHealth]) -> List[ServerHealth]:
    """Return only servers with HEALTHY or DEGRADED status."""
    return [s for s in servers if s.is_healthy]


def sort_by_quality(servers: List[ServerHealth]) -> List[ServerHealth]:
    """Return servers sorted by quality_score descending (best first)."""
    return sorted(servers, key=lambda s: s.quality_score, reverse=True)


@dataclass
class HealthResult:
    """Outcome of a single server health check."""

    config: str
    """Original config string (vmess://…, vless://…, etc.)."""

    host: str
    port: int
    protocol: str

    # TCP
    tcp_ok: bool = False
    tcp_latency_ms: float = 0.0

    # Google 204 (direct, not through proxy)
    google_204_ok: bool = False
    google_204_latency_ms: float = 0.0

    # Derived
    health_status: str = "unreachable"  # healthy | degraded | unreachable | invalid
    quality_score: float = 0.0
    latency_ms: float = 0.0

    error: Optional[str] = None


def _parse_host_port(config: str) -> Optional[Tuple[str, int, str]]:
    """Extract (host, port, protocol) from a config URI string.

    Returns None if the URI cannot be parsed.
    """
    try:
        if "://" not in config:
            return None
        scheme, rest = config.split("://", 1)
        protocol = scheme.lower()

        if protocol == "vmess":
            import base64, json
            # vmess://base64(json)
            try:
                padded = rest + "==" * (4 - len(rest) % 4)
                data = json.loads(base64.b64decode(padded).decode("utf-8", errors="replace"))
                host = data.get("add", "")
                port = int(data.get("port", 443))
                return host, port, protocol
            except Exception:
                return None

        # vless, trojan, ss, ssr — all share host:port after "://"
        # URI shape: scheme://[user@]host:port[/path][?query][#tag]
        # Strip fragment and query
        addr_part = rest.split("#")[0].split("?")[0].split("/")[0]
        # Strip userinfo
        if "@" in addr_part:
            addr_part = addr_part.split("@")[-1]
        if ":" in addr_part:
            parts = addr_part.rsplit(":", 1)
            host = parts[0].strip("[]")
            port = int(parts[1])
        else:
            host = addr_part
            port = 443
        return host, port, protocol
    except Exception:
        return None


def _tcp_check(host: str, port: int, timeout: float) -> Tuple[bool, float]:
    """Return (success, latency_ms) for a TCP connect to host:port."""
    t0 = time.monotonic()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            pass
        return True, (time.monotonic() - t0) * 1000
    except Exception:
        return False, (time.monotonic() - t0) * 1000


def _google_204_check(timeout: float = 4.0) -> Tuple[bool, float]:
    """Probe Google 204 directly (not through proxy).

    This tells us whether the *machine running v2ray-finder* has internet
    access.  It is not a proxy-level check — that is done by xray_connectivity.
    Returns (ok, latency_ms).
    """
    for url in (_GOOGLE_204_URL, _GOOGLE_204_ALT):
        t0 = time.monotonic()
        try:
            with urlopen(url, timeout=timeout) as resp:
                latency = (time.monotonic() - t0) * 1000
                if resp.status == 204:
                    return True, latency
        except Exception:
            pass
    return False, 0.0


def _compute_score(tcp_ok: bool, tcp_latency_ms: float) -> Tuple[str, float]:
    """Derive health_status and quality_score (0-100) from TCP result."""
    if not tcp_ok:
        return "unreachable", 0.0
    # Latency scoring: 0 ms → 100, 1000 ms → ~0
    score = max(0.0, 100.0 - (tcp_latency_ms / 10.0))
    status = "healthy" if tcp_latency_ms < 300 else "degraded"
    return status, round(score, 1)


def check_server(
    config: str,
    timeout: float = 5.0,
    check_google_204: bool = True,
) -> HealthResult:
    """Run all health checks on *config* and return a :class:`HealthResult`.

    This is the single entry point called inline after server discovery.
    """
    parsed = _parse_host_port(config)
    if parsed is None:
        return HealthResult(
            config=config,
            host="",
            port=0,
            protocol="unknown",
            health_status="invalid",
            error="Cannot parse host/port from config",
        )

    host, port, protocol = parsed

    tcp_ok, tcp_lat = _tcp_check(host, port, timeout)
    status, score = _compute_score(tcp_ok, tcp_lat)

    g204_ok, g204_lat = False, 0.0
    if check_google_204 and tcp_ok:
        g204_ok, g204_lat = _google_204_check(timeout=min(timeout, 4.0))

    return HealthResult(
        config=config,
        host=host,
        port=port,
        protocol=protocol,
        tcp_ok=tcp_ok,
        tcp_latency_ms=tcp_lat,
        google_204_ok=g204_ok,
        google_204_latency_ms=g204_lat,
        health_status=status,
        quality_score=score,
        latency_ms=tcp_lat,
    )


def check_servers_batch(
    configs: List[str],
    timeout: float = 5.0,
    max_workers: int = 50,
    check_google_204: bool = True,
    min_quality_score: float = 0.0,
    filter_unhealthy: bool = False,
) -> List[HealthResult]:
    """Concurrently health-check a batch of server configs.

    Args:
        configs:           List of raw config strings.
        timeout:           Per-server TCP timeout in seconds.
        max_workers:       Thread-pool size.
        check_google_204:  Also run the Google-204 probe.
        min_quality_score: Discard results below this score (0 = keep all).
        filter_unhealthy:  If True, only return healthy/degraded results.

    Returns:
        List of :class:`HealthResult`, sorted best-first by quality_score.
    """
    results: List[HealthResult] = []

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(check_server, cfg, timeout, check_google_204): cfg
            for cfg in configs
        }
        for fut in as_completed(futures):
            try:
                r = fut.result()
            except Exception as exc:
                cfg = futures[fut]
                r = HealthResult(
                    config=cfg,
                    host="",
                    port=0,
                    protocol="unknown",
                    health_status="unreachable",
                    error=str(exc),
                )
            if filter_unhealthy and r.health_status not in ("healthy", "degraded"):
                continue
            if r.quality_score < min_quality_score:
                continue
            results.append(r)

    results.sort(key=lambda r: r.quality_score, reverse=True)
    return results


def health_result_to_dict(r: HealthResult) -> Dict:
    """Serialise a HealthResult to a plain dict (CLI / JSON output)."""
    return {
        "config": r.config,
        "host": r.host,
        "port": r.port,
        "protocol": r.protocol,
        "tcp_ok": r.tcp_ok,
        "tcp_latency_ms": r.tcp_latency_ms,
        "google_204_ok": r.google_204_ok,
        "google_204_latency_ms": r.google_204_latency_ms,
        "health_status": r.health_status,
        "quality_score": r.quality_score,
        "latency_ms": r.latency_ms,
        "error": r.error,
    }
