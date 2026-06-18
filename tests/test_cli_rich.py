"""Tests for cli_rich — PipelineProgress, show_stats, save_results, _run_pipeline (V2-P1)."""

from __future__ import annotations

import os
import sys
import tempfile
import threading
import unittest
from io import StringIO
from unittest.mock import MagicMock, patch

from v2ray_finder.pipeline import PipelineResult, StopController
from v2ray_finder.scorer import ServerScore

VMESS = "vmess://eyJhZGQiOiIxMjcuMC4wLjEiLCJwb3J0Ijo0NDMsImlkIjoiYWJjMTIzIn0="
VLESS = "vless://uuid@1.2.3.4:443?security=tls"
TROJAN = "trojan://password@5.6.7.8:443?security=tls"
SAMPLE = [VMESS, VLESS, TROJAN]


def _make_result(configs=None, scores=None, stats=None):
    return PipelineResult(
        configs=configs or SAMPLE[:],
        scores=scores or [],
        stats=stats
        or {
            "fetched": 3,
            "deduped": 3,
            "healthy": 0,
            "scored": 0,
            "dropped_per_source": 0,
            "dropped_global": 0,
            "cache_hits": 0,
            "cache_misses": 0,
        },
    )


def _run_main(*argv):
    """Run cli_rich.main() and capture stdout; returns (stdout, exit_code)."""
    import v2ray_finder.cli_rich as _cr

    buf = StringIO()
    code = 0
    with (
        patch("sys.argv", ["v2ray-finder-rich"] + list(argv)),
        patch("sys.stdout", buf),
        patch("sys.stderr", StringIO()),
    ):
        try:
            _cr.main()
        except SystemExit as exc:
            code = exc.code if isinstance(exc.code, int) else 0
    return buf.getvalue(), code


# ---------------------------------------------------------------------------
# PipelineProgress
# ---------------------------------------------------------------------------


class TestPipelineProgress(unittest.TestCase):

    def _prog(self):
        """Return a PipelineProgress with Rich mocked out."""
        from v2ray_finder import cli_rich as cr

        # Patch Progress so no real terminal I/O happens
        with patch("v2ray_finder.cli_rich.RICH_AVAILABLE", False):
            prog = cr.PipelineProgress()
        return prog

    def test_callable(self):
        from v2ray_finder.cli_rich import PipelineProgress

        with patch("v2ray_finder.cli_rich.RICH_AVAILABLE", False):
            prog = PipelineProgress()
        self.assertTrue(callable(prog))

    def test_context_manager_no_exception(self):
        from v2ray_finder.cli_rich import PipelineProgress

        with patch("v2ray_finder.cli_rich.RICH_AVAILABLE", False):
            prog = PipelineProgress()
        with prog:
            prog("fetch", 1, 10, "test")

    def test_stages_accepted(self):
        from v2ray_finder.cli_rich import PipelineProgress

        with patch("v2ray_finder.cli_rich.RICH_AVAILABLE", False):
            prog = PipelineProgress()
        # Must not raise for any valid stage
        for stage in ("fetch", "health", "score"):
            prog(stage, 0, 10, "msg")

    def test_zero_total_no_exception(self):
        from v2ray_finder.cli_rich import PipelineProgress

        with patch("v2ray_finder.cli_rich.RICH_AVAILABLE", False):
            prog = PipelineProgress()
        prog("fetch", 0, 0, "empty")


# ---------------------------------------------------------------------------
# show_stats
# ---------------------------------------------------------------------------


class TestShowStats(unittest.TestCase):

    def _capture(self, *args, **kwargs):
        from v2ray_finder import cli_rich as cr

        buf = StringIO()
        with (
            patch("v2ray_finder.cli_rich.RICH_AVAILABLE", False),
            patch("sys.stdout", buf),
        ):
            cr.show_stats(*args, **kwargs)
        return buf.getvalue()

    def test_empty_list(self):
        out = self._capture([])
        self.assertIn("No servers", out)

    def test_total_servers_shown(self):
        out = self._capture(SAMPLE[:])
        self.assertIn("Total servers: 3", out)

    def test_protocols_shown(self):
        out = self._capture(SAMPLE[:])
        for proto in ("vmess", "vless", "trojan"):
            self.assertIn(proto, out)

    def test_result_with_pipeline_stats(self):
        result = _make_result(
            stats={
                "fetched": 10,
                "deduped": 7,
                "healthy": 0,
                "scored": 0,
                "dropped_per_source": 0,
                "dropped_global": 0,
                "cache_hits": 2,
                "cache_misses": 8,
            }
        )
        # With Rich disabled the rich table path is skipped;
        # just verify no exception is raised
        with patch("v2ray_finder.cli_rich.RICH_AVAILABLE", False):
            from v2ray_finder import cli_rich as cr

            buf = StringIO()
            with patch("sys.stdout", buf):
                cr.show_stats(SAMPLE[:], result=result)


# ---------------------------------------------------------------------------
# save_results
# ---------------------------------------------------------------------------


class TestSaveResults(unittest.TestCase):

    def test_writes_configs_to_file(self):
        from v2ray_finder.cli_rich import save_results

        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            fname = f.name
        try:
            with patch("v2ray_finder.cli_rich.RICH_AVAILABLE", False):
                save_results(SAMPLE[:], fname)
            with open(fname) as f:
                lines = [l.strip() for l in f if l.strip()]
            self.assertEqual(lines, SAMPLE)
        finally:
            os.unlink(fname)

    def test_empty_list_prints_message(self):
        from v2ray_finder import cli_rich as cr

        buf = StringIO()
        with (
            patch("v2ray_finder.cli_rich.RICH_AVAILABLE", False),
            patch("sys.stdout", buf),
        ):
            cr.save_results([], "ignored.txt")
        self.assertIn("No servers", buf.getvalue())

    def test_partial_flag_in_message(self):
        from v2ray_finder.cli_rich import save_results

        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            fname = f.name
        try:
            buf = StringIO()
            with (
                patch("v2ray_finder.cli_rich.RICH_AVAILABLE", False),
                patch("sys.stdout", buf),
            ):
                save_results([VMESS], fname, partial=True)
            self.assertIn("partial", buf.getvalue())
        finally:
            os.unlink(fname)


# ---------------------------------------------------------------------------
# _configs_from_result
# ---------------------------------------------------------------------------


class TestConfigsFromResult(unittest.TestCase):

    def test_prefers_top_configs_when_scores_present(self):
        from v2ray_finder.cli_rich import _configs_from_result

        scores = [ServerScore(config=VMESS, protocol="vmess", latency_score=0.9)]
        r = _make_result(configs=[TROJAN], scores=scores)
        out = _configs_from_result(r)
        self.assertIn(VMESS, out)
        self.assertNotIn(TROJAN, out)

    def test_falls_back_to_configs_when_no_scores(self):
        from v2ray_finder.cli_rich import _configs_from_result

        r = _make_result(configs=SAMPLE[:], scores=[])
        out = _configs_from_result(r)
        self.assertEqual(out, SAMPLE)

    def test_limit_applied(self):
        from v2ray_finder.cli_rich import _configs_from_result

        r = _make_result(configs=SAMPLE[:])
        out = _configs_from_result(r, limit=1)
        self.assertEqual(len(out), 1)

    def test_limit_zero_means_all(self):
        from v2ray_finder.cli_rich import _configs_from_result

        r = _make_result(configs=SAMPLE[:])
        out = _configs_from_result(r, limit=0)
        self.assertEqual(len(out), len(SAMPLE))


# ---------------------------------------------------------------------------
# _run_pipeline
# ---------------------------------------------------------------------------


class TestRunPipeline(unittest.TestCase):

    def _patch(self, result=None):
        from v2ray_finder import pipeline as _pl

        return patch.object(_pl.Pipeline, "run", return_value=result or _make_result())

    def test_returns_0_on_success(self):
        from v2ray_finder.cli_rich import _run_pipeline

        stop = StopController()
        with self._patch():
            code = _run_pipeline(
                Pipeline=__import__(
                    "v2ray_finder.pipeline", fromlist=["Pipeline"]
                ).Pipeline(sources=[], check_health=False),
                stop_ctrl=stop,
                output=None,
                limit=0,
                stats_only=False,
            )
        self.assertEqual(code, 0)

    def test_returns_1_when_no_configs(self):
        from v2ray_finder.cli_rich import _run_pipeline
        from v2ray_finder.pipeline import Pipeline

        stop = StopController()
        empty_result = PipelineResult(configs=[], scores=[])
        with patch.object(Pipeline, "run", return_value=empty_result):
            code = _run_pipeline(
                Pipeline=Pipeline(sources=[], check_health=False),
                stop_ctrl=stop,
                output=None,
                limit=0,
                stats_only=False,
            )
        self.assertEqual(code, 1)

    def test_returns_130_when_stopped(self):
        from v2ray_finder.cli_rich import _run_pipeline
        from v2ray_finder.pipeline import Pipeline

        stop = StopController()

        def fake_run(self, stop_event=None, progress_callback=None):
            if stop_event:
                stop_event.set()
            return _make_result()

        with patch.object(Pipeline, "run", fake_run):
            code = _run_pipeline(
                Pipeline=Pipeline(sources=[], check_health=False),
                stop_ctrl=stop,
                output=None,
                limit=0,
                stats_only=False,
            )
        self.assertEqual(code, 130)

    def test_output_file_written_on_success(self):
        from v2ray_finder.cli_rich import _run_pipeline
        from v2ray_finder.pipeline import Pipeline

        stop = StopController()
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            fname = f.name
        try:
            with (
                patch.object(Pipeline, "run", return_value=_make_result()),
                patch("v2ray_finder.cli_rich.RICH_AVAILABLE", False),
            ):
                _run_pipeline(
                    Pipeline=Pipeline(sources=[], check_health=False),
                    stop_ctrl=stop,
                    output=fname,
                    limit=0,
                    stats_only=False,
                )
            with open(fname) as f:
                lines = [l.strip() for l in f if l.strip()]
            self.assertEqual(len(lines), len(SAMPLE))
        finally:
            os.unlink(fname)


# ---------------------------------------------------------------------------
# CLI entry point (non-interactive)
# ---------------------------------------------------------------------------


class TestCLIRichNonInteractive(unittest.TestCase):

    def _patch_pipeline(self, result=None):
        from v2ray_finder import pipeline as _pl

        return patch.object(_pl.Pipeline, "run", return_value=result or _make_result())

    def test_stats_only_exits_0(self):
        with self._patch_pipeline():
            _, code = _run_main("--stats-only")
        self.assertEqual(code, 0)

    def test_output_writes_file(self):
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            fname = f.name
        try:
            with self._patch_pipeline():
                _, code = _run_main("-o", fname)
            self.assertEqual(code, 0)
            with open(fname) as f:
                lines = [l.strip() for l in f if l.strip()]
            self.assertGreater(len(lines), 0)
        finally:
            os.unlink(fname)

    def test_cache_flag_forwarded(self):
        from v2ray_finder import pipeline as _pl

        with (
            patch.object(_pl.Pipeline, "__init__", return_value=None) as mock_init,
            patch.object(_pl.Pipeline, "run", return_value=PipelineResult()),
        ):
            _run_main("--stats-only", "--cache")
            _, kw = mock_init.call_args
            self.assertTrue(kw.get("cache_enabled"))

    def test_cache_ttl_forwarded(self):
        from v2ray_finder import pipeline as _pl

        with (
            patch.object(_pl.Pipeline, "__init__", return_value=None) as mock_init,
            patch.object(_pl.Pipeline, "run", return_value=PipelineResult()),
        ):
            _run_main("--stats-only", "--cache-ttl", "900")
            _, kw = mock_init.call_args
            self.assertEqual(kw.get("cache_ttl"), 900)

    def test_check_health_forwarded(self):
        from v2ray_finder import pipeline as _pl

        with (
            patch.object(_pl.Pipeline, "__init__", return_value=None) as mock_init,
            patch.object(_pl.Pipeline, "run", return_value=PipelineResult()),
        ):
            _run_main("--stats-only", "-c")
            _, kw = mock_init.call_args
            self.assertTrue(kw.get("check_health"))

    def test_min_quality_forwarded(self):
        from v2ray_finder import pipeline as _pl

        with (
            patch.object(_pl.Pipeline, "__init__", return_value=None) as mock_init,
            patch.object(_pl.Pipeline, "run", return_value=PipelineResult()),
        ):
            _run_main("--stats-only", "--min-quality", "60")
            _, kw = mock_init.call_args
            self.assertEqual(kw.get("min_quality_score"), 60.0)


if __name__ == "__main__":
    unittest.main()
