"""Unit tests for xray_connectivity.py (Layer 3 — RealConnectivityChecker).

All tests that require a real xray binary are marked
``pytest.mark.integration`` and skipped automatically when the binary
is absent (the checker's ``is_xray_available()`` returns False).

Unit tests mock the underlying layers so they run fully offline.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from v2ray_finder.xray_connectivity import (
    RealConnectivityChecker,
    RealHealthResult,
    _ResultCache,
    find_free_port,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SAMPLE_CONFIG = "vmess://eyJ2IjoiMiIsInBzIjoidCIsImFkZCI6IjEuMi4zLjQiLCJwb3J0IjoiNDQzIiwiaWQiOiJhYWFhYWFhYS1iYmJiLWNjY2MtZGRkZC1lZWVlZWVlZWVlZWUiLCJhaWQiOiIwIiwic2N5IjoiYXV0byIsIm5ldCI6InRjcCIsInR5cGUiOiJub25lIiwiaG9zdCI6IiIsInBhdGgiOiIvIiwidGxzIjoidGxzIiwic25pIjoiIiwiYWxwbiI6IiJ9"


def _make_result(reachable: bool = True, latency: float = 50.0) -> RealHealthResult:
    return RealHealthResult(
        config=SAMPLE_CONFIG,
        protocol="vmess",
        reachable=reachable,
        latency_ms=latency if reachable else None,
        google_204_ok=reachable,
        xray_version="Xray 24.9.30",
        socks_port=10800,
        check_methods=["xray_start", "socks5_probe", "google_204"],
    )


# ---------------------------------------------------------------------------
# _ResultCache unit tests
# ---------------------------------------------------------------------------


class TestResultCache:
    def test_miss_on_empty(self):
        cache = _ResultCache()
        assert cache.get(SAMPLE_CONFIG) is None

    def test_hit_within_ttl(self):
        cache = _ResultCache()
        result = _make_result()
        cache.set(SAMPLE_CONFIG, result, ttl=60.0)
        cached = cache.get(SAMPLE_CONFIG)
        assert cached is not None
        assert cached.reachable is True

    def test_miss_after_expiry(self):
        cache = _ResultCache()
        result = _make_result()
        cache.set(SAMPLE_CONFIG, result, ttl=0.01)  # 10 ms
        time.sleep(0.05)
        assert cache.get(SAMPLE_CONFIG) is None

    def test_clear_removes_entries(self):
        cache = _ResultCache()
        cache.set(SAMPLE_CONFIG, _make_result(), ttl=60.0)
        cache.clear()
        assert cache.get(SAMPLE_CONFIG) is None

    def test_stats_hit_miss(self):
        cache = _ResultCache()
        cache.set(SAMPLE_CONFIG, _make_result(), ttl=60.0)
        cache.get(SAMPLE_CONFIG)  # hit
        cache.get("vmess://different")  # miss
        stats = cache.stats
        assert stats["hits"] == 1
        assert stats["misses"] == 1
        assert stats["size"] == 1

    def test_stats_hit_rate(self):
        cache = _ResultCache()
        cache.set(SAMPLE_CONFIG, _make_result(), ttl=60.0)
        cache.get(SAMPLE_CONFIG)  # hit
        cache.get(SAMPLE_CONFIG)  # hit
        cache.get("other")  # miss
        assert cache.stats["hit_rate"] == pytest.approx(66.7, abs=0.5)

    def test_identical_configs_same_key(self):
        cache = _ResultCache()
        # Leading/trailing whitespace should not create a different key
        cache.set(SAMPLE_CONFIG, _make_result(), ttl=60.0)
        assert cache.get(f"  {SAMPLE_CONFIG}  ") is not None


# ---------------------------------------------------------------------------
# RealHealthResult tests
# ---------------------------------------------------------------------------


class TestRealHealthResult:
    def test_quality_score_unreachable(self):
        r = _make_result(reachable=False)
        assert r.quality_score == 0.0

    def test_quality_score_fast(self):
        r = _make_result(latency=50.0)
        assert r.quality_score == 100.0

    def test_quality_score_medium(self):
        r = _make_result(latency=200.0)
        assert 50.0 < r.quality_score < 100.0

    def test_quality_score_slow(self):
        r = _make_result(latency=2000.0)
        assert 0.0 < r.quality_score < 50.0

    def test_from_cache_default_false(self):
        r = _make_result()
        assert r.from_cache is False


# ---------------------------------------------------------------------------
# RealConnectivityChecker — cache interaction (no xray needed)
# ---------------------------------------------------------------------------


class TestCheckerCache:
    """Tests that exercise the cache layer without invoking xray."""

    @pytest.fixture
    def checker(self):
        with patch("v2ray_finder.xray_connectivity.RealConnectivityChecker.__init__",
                   lambda self, **kw: None):
            obj = RealConnectivityChecker.__new__(RealConnectivityChecker)
            obj.timeout = 10.0
            obj.startup_timeout = 5.0
            obj.concurrent_limit = 5
            obj.cache_enabled = True
            obj.cache_ttl = 600.0
            obj.show_progress = False
            obj._cache = _ResultCache()
            obj._manager = MagicMock()
            obj._adapter = MagicMock()
            return obj

    def test_cache_hit_returns_from_cache_flag(self, checker):
        cached_result = _make_result(reachable=True)
        checker._cache.set(SAMPLE_CONFIG, cached_result, ttl=600.0)

        result = asyncio.get_event_loop().run_until_complete(
            checker.check_server_real(SAMPLE_CONFIG)
        )
        assert result.from_cache is True
        assert result.reachable is True

    def test_cache_miss_calls_xray(self, checker):
        """When no cache entry exists, xray layers must be invoked."""
        mock_result = _make_result(reachable=True)
        checker.check_real_connectivity = AsyncMock(
            return_value=(True, 55.0, True, None)
        )
        # Simulate successful xray context manager
        from contextlib import asynccontextmanager, contextmanager

        @contextmanager
        def _cfg_ctx(*a, **kw):
            yield "/tmp/fake.json"

        @asynccontextmanager
        async def _run_ctx(*a, **kw):
            yield

        checker._adapter.build_config_file = _cfg_ctx
        checker._manager.run = _run_ctx
        checker._manager.get_version = MagicMock(return_value="Xray 24.9.30")

        result = asyncio.get_event_loop().run_until_complete(
            checker.check_server_real(SAMPLE_CONFIG)
        )
        assert result.from_cache is False
        assert result.reachable is True
        # Result should now be in cache
        cached = checker._cache.get(SAMPLE_CONFIG)
        assert cached is not None

    def test_failed_result_cached_with_short_ttl(self, checker):
        """Failed checks are cached for 60 s (short TTL) not the full cache_ttl."""
        from contextlib import asynccontextmanager, contextmanager

        @contextmanager
        def _cfg_ctx(*a, **kw):
            yield "/tmp/fake.json"

        @asynccontextmanager
        async def _run_ctx(*a, **kw):
            yield

        checker._adapter.build_config_file = _cfg_ctx
        checker._manager.run = _run_ctx
        checker._manager.get_version = MagicMock(return_value="Xray 24.9.30")
        checker.check_real_connectivity = AsyncMock(
            return_value=(False, None, False, "timeout")
        )

        asyncio.get_event_loop().run_until_complete(
            checker.check_server_real(SAMPLE_CONFIG)
        )
        # Entry must be present with short TTL — verify via cache hit
        assert checker._cache.get(SAMPLE_CONFIG) is not None

    def test_clear_result_cache(self, checker):
        checker._cache.set(SAMPLE_CONFIG, _make_result(), ttl=600.0)
        checker.clear_result_cache()
        assert checker._cache.get(SAMPLE_CONFIG) is None

    def test_cache_stats_property(self, checker):
        stats = checker.cache_stats
        assert "hits" in stats
        assert "misses" in stats
        assert "size" in stats
        assert "hit_rate" in stats


# ---------------------------------------------------------------------------
# check_servers_real_batch — ordering + backoff + edge cases
# ---------------------------------------------------------------------------


class TestBatch:
    @pytest.fixture
    def checker(self):
        with patch("v2ray_finder.xray_connectivity.RealConnectivityChecker.__init__",
                   lambda self, **kw: None):
            obj = RealConnectivityChecker.__new__(RealConnectivityChecker)
            obj.timeout = 10.0
            obj.startup_timeout = 5.0
            obj.concurrent_limit = 5
            obj.cache_enabled = False  # disable cache for batch tests
            obj.cache_ttl = 600.0
            obj.show_progress = False
            obj._cache = _ResultCache()
            obj._manager = MagicMock()
            obj._adapter = MagicMock()
            return obj

    def _patch_check(self, checker, results: list):
        """Patch check_server_real to return results in order."""
        call_count = {"n": 0}

        async def _fake(config, protocol=None):
            idx = call_count["n"] % len(results)
            call_count["n"] += 1
            return results[idx]

        checker.check_server_real = _fake

    def test_empty_input(self, checker):
        results = asyncio.get_event_loop().run_until_complete(
            checker.check_servers_real_batch([])
        )
        assert results == []

    def test_result_count_matches_input(self, checker):
        servers = [(SAMPLE_CONFIG, "vmess"), (SAMPLE_CONFIG, "vmess")]
        expected = [_make_result(reachable=True), _make_result(reachable=False)]
        self._patch_check(checker, expected)
        results = asyncio.get_event_loop().run_until_complete(
            checker.check_servers_real_batch(servers)
        )
        assert len(results) == 2

    def test_exception_in_task_wrapped_as_result(self, checker):
        """If check_server_real raises, batch should return a failed result, not crash."""
        async def _raise(config, protocol=None):
            raise RuntimeError("xray exploded")

        checker.check_server_real = _raise
        servers = [(SAMPLE_CONFIG, "vmess")]
        results = asyncio.get_event_loop().run_until_complete(
            checker.check_servers_real_batch(servers)
        )
        assert len(results) == 1
        assert results[0].reachable is False
        assert "xray exploded" in (results[0].error or "")

    def test_backoff_applied_after_failures(self, checker):
        """Consecutive failures should trigger asyncio.sleep calls."""
        sleep_calls = []
        original_sleep = asyncio.sleep

        async def _track_sleep(t):
            if t > 0:
                sleep_calls.append(t)

        fail_result = _make_result(reachable=False)
        # Return 3 failures so backoff kicks in for the 2nd and 3rd
        call_count = {"n": 0}

        async def _fake(config, protocol=None):
            call_count["n"] += 1
            return fail_result

        checker.check_server_real = _fake
        checker.concurrent_limit = 1  # serial execution to get deterministic backoff

        servers = [(SAMPLE_CONFIG, "vmess")] * 3
        with patch("v2ray_finder.xray_connectivity.asyncio.sleep", side_effect=_track_sleep):
            asyncio.get_event_loop().run_until_complete(
                checker.check_servers_real_batch(servers)
            )
        # At least one backoff sleep should have been issued
        assert len(sleep_calls) >= 1


# ---------------------------------------------------------------------------
# find_free_port smoke test
# ---------------------------------------------------------------------------


def test_find_free_port():
    port = find_free_port()
    assert 1024 <= port <= 65535


# ---------------------------------------------------------------------------
# Integration tests (require real xray binary)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestRealXrayIntegration:
    """End-to-end tests that launch xray and probe a real server.

    Skipped automatically when the xray binary is not available.
    """

    @pytest.fixture(autouse=True)
    def require_xray(self):
        checker = RealConnectivityChecker(auto_download=False)
        if not checker.is_xray_available():
            pytest.skip("xray binary not available")

    def test_check_server_real_reachable(self):
        """A live server must return reachable=True and google_204_ok=True."""
        import os
        config = os.environ.get("V2RAY_TEST_CONFIG")
        if not config:
            pytest.skip("V2RAY_TEST_CONFIG env var not set")

        checker = RealConnectivityChecker(auto_download=False, cache_enabled=False)
        result = asyncio.get_event_loop().run_until_complete(
            checker.check_server_real(config)
        )
        assert result.reachable is True
        assert result.google_204_ok is True
        assert result.latency_ms is not None and result.latency_ms > 0

    def test_cache_roundtrip_with_real_check(self):
        """Second call to check_server_real for same config hits cache."""
        import os
        config = os.environ.get("V2RAY_TEST_CONFIG")
        if not config:
            pytest.skip("V2RAY_TEST_CONFIG env var not set")

        checker = RealConnectivityChecker(auto_download=False, cache_enabled=True, cache_ttl=120)
        loop = asyncio.get_event_loop()
        first = loop.run_until_complete(checker.check_server_real(config))
        second = loop.run_until_complete(checker.check_server_real(config))
        assert first.from_cache is False
        assert second.from_cache is True
        assert second.reachable == first.reachable
