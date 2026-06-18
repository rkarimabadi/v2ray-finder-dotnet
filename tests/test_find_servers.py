"""Tests for the high-level find_servers() public API (V1-A3)."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

import v2ray_finder
from v2ray_finder.pipeline import Pipeline, PipelineResult, StopController
from v2ray_finder.scorer import ServerScore

VMESS = "vmess://eyJhZGQiOiIxMjcuMC4wLjEiLCJwb3J0Ijo0NDMsImlkIjoiYWJjMTIzIn0="
VLESS = "vless://uuid@1.2.3.4:443?security=tls"
TROJAN = "trojan://password@5.6.7.8:443?security=tls"


def _make_scores(*configs):
    return [
        ServerScore(config=c, protocol=c.split("://")[0], latency_score=0.8)
        for c in configs
    ]


# ---------------------------------------------------------------------------
# find_servers — unit
# ---------------------------------------------------------------------------


class TestFindServers(unittest.TestCase):

    def _patch_pipeline_run(self, configs=None, scores=None):
        """Patch Pipeline.run() to return a canned PipelineResult."""
        result = PipelineResult(
            configs=configs or [],
            scores=scores or [],
            stats={
                "fetched": len(configs or []),
                "deduped": 0,
                "healthy": 0,
                "scored": 0,
            },
        )
        return patch.object(Pipeline, "run", return_value=result)

    # -- basic return type --
    def test_returns_list(self):
        with self._patch_pipeline_run(configs=[VMESS, VLESS]):
            out = v2ray_finder.find_servers(check_health=False)
        self.assertIsInstance(out, list)

    def test_returns_strings(self):
        with self._patch_pipeline_run(configs=[VMESS]):
            out = v2ray_finder.find_servers(check_health=False)
        self.assertTrue(all(isinstance(c, str) for c in out))

    # -- prefers top_configs when scores present --
    def test_prefers_top_configs_when_scores_present(self):
        scores = _make_scores(VMESS, VLESS)
        with self._patch_pipeline_run(configs=[TROJAN], scores=scores):
            out = v2ray_finder.find_servers(check_health=False)
        # top_configs are derived from scores, not raw configs
        self.assertIn(VMESS, out)
        self.assertNotIn(TROJAN, out)

    def test_falls_back_to_configs_when_no_scores(self):
        with self._patch_pipeline_run(configs=[VMESS, VLESS], scores=[]):
            out = v2ray_finder.find_servers(check_health=False)
        self.assertIn(VMESS, out)

    # -- limit forwarded to Pipeline --
    def test_limit_passed_to_pipeline(self):
        with (
            patch.object(Pipeline, "__init__", return_value=None) as mock_init,
            patch.object(Pipeline, "run", return_value=PipelineResult()),
        ):
            mock_init.return_value = None
            v2ray_finder.find_servers(limit=10, check_health=False)
            _, kwargs = mock_init.call_args
            self.assertEqual(kwargs.get("limit"), 10)

    # -- github_token forwarded --
    def test_github_token_passed_to_pipeline(self):
        with (
            patch.object(Pipeline, "__init__", return_value=None) as mock_init,
            patch.object(Pipeline, "run", return_value=PipelineResult()),
        ):
            v2ray_finder.find_servers(github_token="ghp_test")
            _, kwargs = mock_init.call_args
            self.assertEqual(kwargs.get("github_token"), "ghp_test")

    # -- empty result --
    def test_empty_result_returns_empty_list(self):
        with self._patch_pipeline_run(configs=[], scores=[]):
            out = v2ray_finder.find_servers()
        self.assertEqual(out, [])

    # -- check_health forwarded --
    def test_check_health_false_forwarded(self):
        with (
            patch.object(Pipeline, "__init__", return_value=None) as mock_init,
            patch.object(Pipeline, "run", return_value=PipelineResult()),
        ):
            v2ray_finder.find_servers(check_health=False)
            _, kwargs = mock_init.call_args
            self.assertFalse(kwargs.get("check_health"))

    def test_check_health_true_forwarded(self):
        with (
            patch.object(Pipeline, "__init__", return_value=None) as mock_init,
            patch.object(Pipeline, "run", return_value=PipelineResult()),
        ):
            v2ray_finder.find_servers(check_health=True)
            _, kwargs = mock_init.call_args
            self.assertTrue(kwargs.get("check_health"))

    # -- min_quality_score forwarded --
    def test_min_quality_score_forwarded(self):
        with (
            patch.object(Pipeline, "__init__", return_value=None) as mock_init,
            patch.object(Pipeline, "run", return_value=PipelineResult()),
        ):
            v2ray_finder.find_servers(min_quality_score=60.0)
            _, kwargs = mock_init.call_args
            self.assertEqual(kwargs.get("min_quality_score"), 60.0)

    # -- importable from package --
    def test_find_servers_in_all(self):
        self.assertIn("find_servers", v2ray_finder.__all__)

    def test_find_servers_callable(self):
        self.assertTrue(callable(v2ray_finder.find_servers))


# ---------------------------------------------------------------------------
# find_servers — cap integration
# ---------------------------------------------------------------------------


class TestFindServersCaps(unittest.TestCase):
    """Verify that cap params are forwarded to Pipeline."""

    def _init_kwargs(self, **kwargs):
        with (
            patch.object(Pipeline, "__init__", return_value=None) as mock_init,
            patch.object(Pipeline, "run", return_value=PipelineResult()),
        ):
            v2ray_finder.find_servers(**kwargs)
            _, kw = mock_init.call_args
            return kw

    def test_max_configs_per_source_default(self):
        kw = self._init_kwargs(check_health=False)
        self.assertEqual(kw.get("max_configs_per_source"), 5_000)

    def test_max_total_configs_default(self):
        kw = self._init_kwargs(check_health=False)
        self.assertEqual(kw.get("max_total_configs"), 50_000)

    def test_max_configs_per_source_custom(self):
        kw = self._init_kwargs(check_health=False, max_configs_per_source=100)
        self.assertEqual(kw.get("max_configs_per_source"), 100)

    def test_max_total_configs_none(self):
        kw = self._init_kwargs(check_health=False, max_total_configs=None)
        self.assertIsNone(kw.get("max_total_configs"))


if __name__ == "__main__":
    unittest.main()
