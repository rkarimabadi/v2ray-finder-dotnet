"""Tests for health_checker module."""

import asyncio
import base64
import json
from unittest.mock import AsyncMock, Mock, patch

import pytest

from v2ray_finder.health_checker import (
    HealthChecker,
    HealthStatus,
    ServerHealth,
    ServerValidator,
    filter_healthy_servers,
    sort_by_quality,
)

# ---------------------------------------------------------------------------
# ServerHealth -- is_healthy property
# ---------------------------------------------------------------------------


def test_is_healthy_true():
    h = ServerHealth(config="vmess://x", protocol="vmess", status=HealthStatus.HEALTHY)
    assert h.is_healthy is True


def test_is_healthy_false_unreachable():
    h = ServerHealth(
        config="vmess://x", protocol="vmess", status=HealthStatus.UNREACHABLE
    )
    assert h.is_healthy is False


def test_is_healthy_false_invalid():
    h = ServerHealth(config="x", protocol="?", status=HealthStatus.INVALID)
    assert h.is_healthy is False


# ---------------------------------------------------------------------------
# ServerHealth -- quality_score
# ---------------------------------------------------------------------------


def test_quality_score_invalid_is_zero():
    h = ServerHealth(config="x", protocol="?", status=HealthStatus.INVALID)
    assert h.quality_score == 0.0


def test_quality_score_unreachable_is_zero():
    # UNREACHABLE must be 0 so that ANY live server sorts above a dead one.
    h = ServerHealth(config="x", protocol="?", status=HealthStatus.UNREACHABLE)
    assert h.quality_score == 0.0


def test_quality_score_no_latency_is_fifty():
    h = ServerHealth(
        config="x", protocol="?", status=HealthStatus.HEALTHY, latency_ms=None
    )
    assert h.quality_score == 50.0


def test_quality_score_fast_latency_is_hundred():
    # ≤100ms → 100
    h = ServerHealth(
        config="x", protocol="?", status=HealthStatus.HEALTHY, latency_ms=50.0
    )
    assert h.quality_score == 100.0


def test_quality_score_at_100ms_boundary():
    # Exactly 100ms is still 100
    h = ServerHealth(
        config="x", protocol="?", status=HealthStatus.HEALTHY, latency_ms=100.0
    )
    assert h.quality_score == 100.0


def test_quality_score_medium_latency_200ms():
    # 200ms: in the 100-300ms band  → 100 - (200-100)*(30/200) = 100-15 = 85
    h = ServerHealth(
        config="x", protocol="?", status=HealthStatus.HEALTHY, latency_ms=200.0
    )
    assert h.quality_score == pytest.approx(85.0, abs=0.5)


def test_quality_score_at_300ms_boundary():
    # 300ms: boundary of good/acceptable band → 70
    h = ServerHealth(
        config="x", protocol="?", status=HealthStatus.HEALTHY, latency_ms=300.0
    )
    assert h.quality_score == pytest.approx(70.0, abs=0.5)


def test_quality_score_at_1000ms_boundary():
    # 1000ms: boundary of acceptable/poor band → 20
    h = ServerHealth(
        config="x", protocol="?", status=HealthStatus.HEALTHY, latency_ms=1000.0
    )
    assert h.quality_score == pytest.approx(20.0, abs=0.5)


def test_quality_score_at_3000ms_floor():
    # 3000ms → exactly 0
    h = ServerHealth(
        config="x", protocol="?", status=HealthStatus.HEALTHY, latency_ms=3000.0
    )
    assert h.quality_score == 0.0


def test_quality_score_beyond_3000ms_still_zero():
    # Any latency > 3000ms must stay at 0 (no negative scores)
    h = ServerHealth(
        config="x", protocol="?", status=HealthStatus.HEALTHY, latency_ms=9999.0
    )
    assert h.quality_score == 0.0


def test_quality_score_slow_latency_above_floor():
    # 1000ms → 20, well above 0
    h = ServerHealth(
        config="x", protocol="?", status=HealthStatus.HEALTHY, latency_ms=1000.0
    )
    assert h.quality_score >= 20.0


def test_quality_score_live_server_beats_dead_server():
    # Any live server (even very slow) must outscore a dead one
    slow_live = ServerHealth(
        config="x", protocol="vmess", status=HealthStatus.HEALTHY, latency_ms=2999.0
    )
    dead = ServerHealth(
        config="y", protocol="vmess", status=HealthStatus.UNREACHABLE
    )
    assert slow_live.quality_score > dead.quality_score


def test_quality_score_monotone_decreasing():
    """Score must be non-increasing as latency grows."""
    latencies = [50, 100, 200, 300, 500, 1000, 1500, 2000, 3000, 5000]
    scores = [
        ServerHealth(
            config="x", protocol="vmess", status=HealthStatus.HEALTHY,
            latency_ms=float(l)
        ).quality_score
        for l in latencies
    ]
    for i in range(len(scores) - 1):
        assert scores[i] >= scores[i + 1], (
            f"Score not monotone at {latencies[i]}ms ({scores[i]}) "
            f"→ {latencies[i+1]}ms ({scores[i+1]})"
        )


# ---------------------------------------------------------------------------
# ServerValidator -- extract_vmess_info
# ---------------------------------------------------------------------------


def _make_vmess(host: str, port: int) -> str:
    data = {"add": host, "port": port}
    encoded = base64.b64encode(json.dumps(data).encode()).decode()
    return f"vmess://{encoded}"


def _make_ssr(host: str, port: int) -> str:
    """Build a minimal valid SSR config string."""
    ssr_body = f"{host}:{port}:auth_sha1_v4:rc4-md5:http_simple:dGVzdA=="
    encoded = base64.b64encode(ssr_body.encode()).decode().rstrip("=")
    return f"ssr://{encoded}"


def test_extract_vmess_valid():
    config = _make_vmess("example.com", 443)
    result = ServerValidator.extract_vmess_info(config)
    assert result is not None
    assert result["host"] == "example.com"
    assert result["port"] == 443


def test_extract_vmess_uses_address_field():
    data = {"address": "alt.com", "port": 80}
    encoded = base64.b64encode(json.dumps(data).encode()).decode()
    result = ServerValidator.extract_vmess_info(f"vmess://{encoded}")
    assert result["host"] == "alt.com"


def test_extract_vmess_invalid_returns_none():
    result = ServerValidator.extract_vmess_info("vmess://not_valid_base64!!!")
    assert result is None


def test_extract_vless_valid():
    config = "vless://uuid@example.com:443?encryption=none#tag"
    result = ServerValidator.extract_vless_info(config)
    assert result is not None
    assert result["host"] == "example.com"
    assert result["port"] == 443


def test_extract_trojan_valid():
    config = "trojan://password@example.com:443?security=tls#tag"
    result = ServerValidator.extract_trojan_info(config)
    assert result is not None
    assert result["host"] == "example.com"
    assert result["port"] == 443


def test_extract_ss_valid_with_at():
    config = "ss://method:password@example.com:8388#tag"
    result = ServerValidator.extract_ss_info(config)
    assert result is not None
    assert result["host"] == "example.com"
    assert result["port"] == 8388


def test_extract_ssr_valid():
    config = _make_ssr("ssr.example.com", 1080)
    result = ServerValidator.extract_ssr_info(config)
    assert result is not None
    assert result["host"] == "ssr.example.com"
    assert result["port"] == 1080


# ---------------------------------------------------------------------------
# ServerValidator -- validate_config
# ---------------------------------------------------------------------------


def test_validate_config_empty():
    ok, err, host, port = ServerValidator.validate_config("")
    assert ok is False
    assert err == "Empty config"
    assert host is None
    assert port is None


def test_validate_config_no_scheme():
    ok, err, host, port = ServerValidator.validate_config("example.com:443")
    assert ok is False
    assert "scheme" in err.lower() or "uri" in err.lower()


def test_validate_config_unknown_protocol():
    ok, err, host, port = ServerValidator.validate_config("http://example.com:80")
    assert ok is False
    assert "Unknown" in err


def test_validate_config_valid_vless():
    config = "vless://uuid@example.com:443?encryption=none"
    ok, err, host, port = ServerValidator.validate_config(config)
    assert ok is True
    assert err is None
    assert host == "example.com"
    assert port == 443


# ---------------------------------------------------------------------------
# HealthChecker -- check_tcp_connectivity
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_tcp_connectivity_missing_host():
    checker = HealthChecker()
    ok, latency, err = await checker.check_tcp_connectivity("", 443)
    assert ok is False
    assert "host" in err.lower()


@pytest.mark.asyncio
async def test_check_tcp_connectivity_missing_port():
    checker = HealthChecker()
    ok, latency, err = await checker.check_tcp_connectivity("example.com", 0)
    assert ok is False
    assert "port" in err.lower()


@pytest.mark.asyncio
async def test_check_tcp_connectivity_timeout():
    checker = HealthChecker(timeout=0.001)
    ok, latency, err = await checker.check_tcp_connectivity("192.0.2.1", 9999)
    assert ok is False
    assert err is not None


# ---------------------------------------------------------------------------
# HealthChecker -- check_server_health
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_server_health_unsupported_protocol():
    checker = HealthChecker()
    result = await checker.check_server_health("http://example.com", "http")
    assert result.status == HealthStatus.INVALID
    assert "Unsupported" in result.validation_error


@pytest.mark.asyncio
async def test_check_server_health_invalid_format():
    checker = HealthChecker()
    result = await checker.check_server_health("vmess://not_valid!!!", "vmess")
    assert result.status == HealthStatus.INVALID


@pytest.mark.asyncio
async def test_check_server_health_reachable():
    checker = HealthChecker(timeout=5.0)
    vmess_config = _make_vmess("example.com", 443)

    with patch.object(
        checker,
        "check_tcp_connectivity",
        new_callable=AsyncMock,
        return_value=(True, 50.0, None),
    ):
        result = await checker.check_server_health(vmess_config, "vmess")

    assert result.status == HealthStatus.HEALTHY
    assert result.tcp_ok is True
    assert result.latency_ms == 50.0
    assert result.quality_score == 100.0


@pytest.mark.asyncio
async def test_check_server_health_unreachable():
    checker = HealthChecker(timeout=5.0)
    vmess_config = _make_vmess("example.com", 443)

    with patch.object(
        checker,
        "check_tcp_connectivity",
        new_callable=AsyncMock,
        return_value=(False, None, "Connection refused"),
    ):
        result = await checker.check_server_health(vmess_config, "vmess")

    assert result.status == HealthStatus.UNREACHABLE
    assert result.tcp_ok is False
    assert result.error == "Connection refused"
    assert result.quality_score == 0.0


@pytest.mark.asyncio
async def test_check_server_health_degraded_high_latency():
    checker = HealthChecker()
    vmess_config = _make_vmess("example.com", 443)

    with patch.object(
        checker,
        "check_tcp_connectivity",
        new_callable=AsyncMock,
        return_value=(True, 600.0, None),
    ):
        result = await checker.check_server_health(vmess_config, "vmess")

    assert result.status == HealthStatus.DEGRADED
    # 600ms: in 300-1000ms band → 70 - (600-300)*(50/700) ≈ 48.6
    assert 40.0 < result.quality_score < 60.0


# ---------------------------------------------------------------------------
# HealthChecker -- check_servers_batch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_servers_batch_empty():
    checker = HealthChecker()
    results = await checker.check_servers_batch([])
    assert results == []


@pytest.mark.asyncio
async def test_check_servers_batch_multiple():
    checker = HealthChecker()
    vmess1 = _make_vmess("host1.com", 443)
    vmess2 = _make_vmess("host2.com", 8443)

    async def mock_tcp(host, port):
        return (True, 30.0, None)

    with patch.object(checker, "check_tcp_connectivity", side_effect=mock_tcp):
        results = await checker.check_servers_batch([(vmess1, "vmess"), (vmess2, "vmess")])

    assert len(results) == 2
    assert all(r.status == HealthStatus.HEALTHY for r in results)


# ---------------------------------------------------------------------------
# filter_healthy_servers
# ---------------------------------------------------------------------------


def test_filter_healthy_servers_removes_invalid():
    results = [
        ServerHealth(config="x", protocol="?", status=HealthStatus.INVALID),
        ServerHealth(config="y", protocol="vmess", status=HealthStatus.HEALTHY),
    ]
    filtered = filter_healthy_servers(results)
    assert len(filtered) == 1
    assert filtered[0].status == HealthStatus.HEALTHY


def test_filter_healthy_servers_removes_unreachable_by_default():
    results = [
        ServerHealth(config="x", protocol="?", status=HealthStatus.UNREACHABLE),
        ServerHealth(config="y", protocol="vmess", status=HealthStatus.HEALTHY),
    ]
    filtered = filter_healthy_servers(results)
    assert len(filtered) == 1


def test_filter_healthy_servers_keeps_unreachable_when_flag_off():
    results = [
        ServerHealth(config="x", protocol="?", status=HealthStatus.UNREACHABLE),
        ServerHealth(config="y", protocol="vmess", status=HealthStatus.HEALTHY),
    ]
    filtered = filter_healthy_servers(results, exclude_unreachable=False)
    assert len(filtered) == 2


def test_filter_healthy_servers_min_quality_score():
    results = [
        ServerHealth(
            config="x", protocol="vmess", status=HealthStatus.HEALTHY, latency_ms=50.0
        ),
        ServerHealth(
            config="y",
            protocol="vmess",
            status=HealthStatus.UNREACHABLE,
        ),
    ]
    filtered = filter_healthy_servers(
        results, exclude_unreachable=False, min_quality_score=50.0
    )
    assert len(filtered) == 1
    assert filtered[0].config == "x"


# ---------------------------------------------------------------------------
# sort_by_quality
# ---------------------------------------------------------------------------


def test_sort_by_quality_descending():
    results = [
        ServerHealth(
            config="slow", protocol="vmess", status=HealthStatus.HEALTHY, latency_ms=800.0
        ),
        ServerHealth(
            config="fast", protocol="vmess", status=HealthStatus.HEALTHY, latency_ms=50.0
        ),
        ServerHealth(config="dead", protocol="?", status=HealthStatus.UNREACHABLE),
    ]
    sorted_results = sort_by_quality(results)
    assert sorted_results[0].config == "fast"
    assert sorted_results[-1].config == "dead"


def test_sort_by_quality_ascending():
    results = [
        ServerHealth(
            config="fast", protocol="vmess", status=HealthStatus.HEALTHY, latency_ms=50.0
        ),
        ServerHealth(config="dead", protocol="?", status=HealthStatus.UNREACHABLE),
    ]
    sorted_results = sort_by_quality(results, descending=False)
    assert sorted_results[0].config == "dead"
    assert sorted_results[-1].config == "fast"


# ---------------------------------------------------------------------------
# HealthChecker -- sync wrappers
# ---------------------------------------------------------------------------


def test_check_one_invalid_config():
    checker = HealthChecker()
    result = checker.check_one("not-a-valid-config")
    assert result.status == HealthStatus.INVALID


def test_check_batch_empty():
    checker = HealthChecker()
    results = checker.check_batch([])
    assert results == []


def test_check_batch_invalid_configs():
    checker = HealthChecker()
    results = checker.check_batch(["invalid1", "invalid2"])
    assert len(results) == 2
    assert all(r.status == HealthStatus.INVALID for r in results)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
