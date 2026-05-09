"""Unit tests for source_registry.py.

Covers:
- SourceStats computed properties (success_rate, avg_servers_per_fetch)
- SourceRegistry.get: create-on-first-access, reuse-on-second
- record_success: counter increments and timestamp updates
- record_failure: failure counter, no success side-effects
- update_overlap: stores ratio correctly
- all_stats: sorted by total_servers_found descending
- summary: human-readable string contains key info
"""

from __future__ import annotations

from datetime import datetime

import pytest

from v2ray_finder.source_registry import SourceRegistry, SourceStats


# ---------------------------------------------------------------------------
# SourceStats computed properties
# ---------------------------------------------------------------------------


class TestSourceStats:
    def test_success_rate_zero_when_no_fetches(self):
        s = SourceStats(url="http://x")
        assert s.success_rate == 0.0

    def test_success_rate_full(self):
        s = SourceStats(url="http://x", fetch_count=4, success_count=4)
        assert abs(s.success_rate - 1.0) < 1e-6

    def test_success_rate_partial(self):
        s = SourceStats(url="http://x", fetch_count=4, success_count=3)
        assert abs(s.success_rate - 0.75) < 1e-6

    def test_avg_servers_zero_when_no_success(self):
        s = SourceStats(url="http://x", success_count=0, total_servers_found=100)
        assert s.avg_servers_per_fetch == 0.0

    def test_avg_servers_normal(self):
        s = SourceStats(url="http://x", success_count=2, total_servers_found=100)
        assert abs(s.avg_servers_per_fetch - 50.0) < 1e-6

    def test_success_rate_rounded_to_4_decimals(self):
        s = SourceStats(url="http://x", fetch_count=3, success_count=1)
        # 1/3 = 0.3333...
        assert len(str(s.success_rate).split(".")[-1]) <= 4


# ---------------------------------------------------------------------------
# SourceRegistry.get
# ---------------------------------------------------------------------------


class TestSourceRegistryGet:
    def test_creates_new_entry(self):
        reg = SourceRegistry()
        stats = reg.get("http://new")
        assert isinstance(stats, SourceStats)
        assert stats.url == "http://new"

    def test_reuses_existing_entry(self):
        reg = SourceRegistry()
        first = reg.get("http://same")
        second = reg.get("http://same")
        assert first is second

    def test_different_urls_different_entries(self):
        reg = SourceRegistry()
        a = reg.get("http://a")
        b = reg.get("http://b")
        assert a is not b


# ---------------------------------------------------------------------------
# record_success
# ---------------------------------------------------------------------------


class TestRecordSuccess:
    def test_increments_fetch_and_success(self):
        reg = SourceRegistry()
        reg.record_success("http://s", server_count=10)
        s = reg.get("http://s")
        assert s.fetch_count == 1
        assert s.success_count == 1
        assert s.failure_count == 0

    def test_accumulates_server_count(self):
        reg = SourceRegistry()
        reg.record_success("http://s", server_count=10)
        reg.record_success("http://s", server_count=5)
        s = reg.get("http://s")
        assert s.total_servers_found == 15
        assert s.last_server_count == 5

    def test_sets_last_fetched_and_last_success(self):
        reg = SourceRegistry()
        before = datetime.utcnow()
        reg.record_success("http://s", server_count=1)
        after = datetime.utcnow()
        s = reg.get("http://s")
        assert s.last_fetched is not None
        assert s.last_success is not None
        assert before <= s.last_fetched <= after

    def test_multiple_calls_cumulative(self):
        reg = SourceRegistry()
        for i in range(5):
            reg.record_success("http://s", server_count=2)
        s = reg.get("http://s")
        assert s.fetch_count == 5
        assert s.success_count == 5
        assert s.total_servers_found == 10


# ---------------------------------------------------------------------------
# record_failure
# ---------------------------------------------------------------------------


class TestRecordFailure:
    def test_increments_failure_and_fetch(self):
        reg = SourceRegistry()
        reg.record_failure("http://f")
        s = reg.get("http://f")
        assert s.failure_count == 1
        assert s.fetch_count == 1
        assert s.success_count == 0

    def test_does_not_touch_server_counts(self):
        reg = SourceRegistry()
        reg.record_failure("http://f")
        s = reg.get("http://f")
        assert s.total_servers_found == 0
        assert s.last_server_count == 0

    def test_sets_last_fetched(self):
        reg = SourceRegistry()
        before = datetime.utcnow()
        reg.record_failure("http://f")
        s = reg.get("http://f")
        assert s.last_fetched is not None
        assert s.last_fetched >= before

    def test_last_success_unchanged_after_failure(self):
        reg = SourceRegistry()
        reg.record_success("http://f", 5)
        success_ts = reg.get("http://f").last_success
        reg.record_failure("http://f")
        assert reg.get("http://f").last_success == success_ts


# ---------------------------------------------------------------------------
# update_overlap
# ---------------------------------------------------------------------------


class TestUpdateOverlap:
    def test_stores_ratio(self):
        reg = SourceRegistry()
        reg.update_overlap("http://o", 0.42)
        assert abs(reg.get("http://o").overlap_ratio - 0.42) < 1e-9

    def test_overwrites_previous(self):
        reg = SourceRegistry()
        reg.update_overlap("http://o", 0.1)
        reg.update_overlap("http://o", 0.9)
        assert abs(reg.get("http://o").overlap_ratio - 0.9) < 1e-9

    def test_creates_entry_if_missing(self):
        reg = SourceRegistry()
        reg.update_overlap("http://new", 0.5)
        assert "http://new" in [s.url for s in reg.all_stats()]


# ---------------------------------------------------------------------------
# all_stats
# ---------------------------------------------------------------------------


class TestAllStats:
    def test_sorted_by_total_servers_descending(self):
        reg = SourceRegistry()
        reg.record_success("http://low", server_count=5)
        reg.record_success("http://high", server_count=100)
        reg.record_success("http://mid", server_count=50)
        stats = reg.all_stats()
        totals = [s.total_servers_found for s in stats]
        assert totals == sorted(totals, reverse=True)

    def test_empty_registry(self):
        reg = SourceRegistry()
        assert reg.all_stats() == []

    def test_failure_only_source_included(self):
        reg = SourceRegistry()
        reg.record_failure("http://bad")
        assert len(reg.all_stats()) == 1


# ---------------------------------------------------------------------------
# summary
# ---------------------------------------------------------------------------


class TestSummary:
    def test_summary_contains_url_substring(self):
        reg = SourceRegistry()
        reg.record_success("http://myhost.com/subs", server_count=10)
        summary = reg.summary()
        assert "myhost.com" in summary

    def test_summary_contains_counts(self):
        reg = SourceRegistry()
        reg.record_success("http://s", server_count=42)
        reg.record_failure("http://s")
        summary = reg.summary()
        # fetch_count should be 2, success_count 1
        assert "ok=1/2" in summary

    def test_summary_is_string(self):
        reg = SourceRegistry()
        assert isinstance(reg.summary(), str)

    def test_empty_registry_summary_has_header(self):
        reg = SourceRegistry()
        assert "SourceRegistry" in reg.summary()
