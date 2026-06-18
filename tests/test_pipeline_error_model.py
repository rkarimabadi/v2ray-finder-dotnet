"""Tests for V1-D2: unified error model across fetcher and pipeline.

Verifies that:
1. FetchResult.structured_error is populated from the V2RayFinderError hierarchy
   for timeout, network, rate-limit, and generic HTTP errors.
2. Pipeline.run() funnels structured errors into stats["errors"] as dicts.
3. PipelineResult.failed_sources returns Dict[str, dict] with error_type keys.
4. PipelineResult.failed_source_messages returns the legacy Dict[str, str] view.
5. Successful fetches do NOT produce structured_error entries.
6. Legacy plain-string errors (from test stubs) are normalised to dict form.
"""

from __future__ import annotations

import threading
import unittest
from unittest.mock import MagicMock, patch

from v2ray_finder.async_fetcher import AsyncFetcher, FetchResult
from v2ray_finder.exceptions import ErrorType
from v2ray_finder.pipeline import Pipeline, PipelineResult
from v2ray_finder.sources import SourceEntry, SourceTrust

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

URL_A = "http://ok.example/sub"
URL_B = "http://timeout.example/sub"
URL_C = "http://network.example/sub"
URL_D = "http://ratelimit.example/sub"
URL_E = "http://http500.example/sub"

VMESS = "vmess://AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=="


def _src(url: str) -> SourceEntry:
    return SourceEntry(url=url, trust=SourceTrust.MEDIUM, enabled=True)


# ---------------------------------------------------------------------------
# 1. FetchResult.structured_error field population
# ---------------------------------------------------------------------------


class TestFetchResultStructuredError(unittest.TestCase):

    def _make_fr(
        self, success: bool, error: str, se: dict | None, status: int | None = None
    ) -> FetchResult:
        return FetchResult(
            url="http://x.example",
            content=None,
            status_code=status,
            success=success,
            error=error,
            elapsed_ms=1.0,
            structured_error=se,
        )

    def test_successful_fetch_has_no_structured_error(self):
        fr = FetchResult(
            url=URL_A,
            content="vmess://abc",
            status_code=200,
            success=True,
            error=None,
            elapsed_ms=10.0,
        )
        self.assertIsNone(fr.structured_error)

    def test_structured_error_has_required_keys(self):
        se = {"error_type": "timeout_error", "message": "timed out", "details": {}}
        fr = self._make_fr(False, "timed out", se)
        for key in ("error_type", "message", "details"):
            self.assertIn(key, fr.structured_error)

    def test_error_type_values_are_strings(self):
        for et in (
            "timeout_error",
            "network_error",
            "rate_limit_exceeded",
            "github_api_error",
            "unknown_error",
        ):
            se = {"error_type": et, "message": "x", "details": {}}
            fr = self._make_fr(False, "x", se)
            self.assertEqual(fr.structured_error["error_type"], et)


# ---------------------------------------------------------------------------
# 2. AsyncFetcher sync path produces structured_error
# ---------------------------------------------------------------------------


class TestAsyncFetcherStructuredErrors(unittest.TestCase):
    """Tests the sync (requests) fallback path which is easiest to unit-test."""

    def _fetcher(self) -> AsyncFetcher:
        f = AsyncFetcher(max_concurrent=1, timeout=1.0)
        f.backend = "sync"  # force sync path
        return f

    def test_timeout_produces_timeout_error_type(self):
        import requests as _req

        f = self._fetcher()
        with patch("requests.get", side_effect=_req.exceptions.Timeout):
            results = f.fetch_many([URL_B])
        fr = results[0]
        self.assertFalse(fr.success)
        self.assertIsNotNone(fr.structured_error)
        self.assertEqual(
            fr.structured_error["error_type"], ErrorType.TIMEOUT_ERROR.value
        )

    def test_connection_error_produces_network_error_type(self):
        import requests as _req

        f = self._fetcher()
        with patch(
            "requests.get", side_effect=_req.exceptions.ConnectionError("refused")
        ):
            results = f.fetch_many([URL_C])
        fr = results[0]
        self.assertFalse(fr.success)
        self.assertIsNotNone(fr.structured_error)
        self.assertEqual(
            fr.structured_error["error_type"], ErrorType.NETWORK_ERROR.value
        )

    def test_rate_limit_429_produces_rate_limit_error_type(self):
        import requests as _req

        mock_resp = MagicMock()
        mock_resp.status_code = 429
        f = self._fetcher()
        with patch("requests.get", return_value=mock_resp):
            results = f.fetch_many([URL_D])
        fr = results[0]
        self.assertFalse(fr.success)
        self.assertIsNotNone(fr.structured_error)
        self.assertEqual(
            fr.structured_error["error_type"], ErrorType.RATE_LIMIT_EXCEEDED.value
        )

    def test_403_produces_rate_limit_error_type(self):
        import requests as _req

        mock_resp = MagicMock()
        mock_resp.status_code = 403
        f = self._fetcher()
        with patch("requests.get", return_value=mock_resp):
            results = f.fetch_many([URL_D])
        fr = results[0]
        self.assertEqual(
            fr.structured_error["error_type"], ErrorType.RATE_LIMIT_EXCEEDED.value
        )

    def test_http_500_produces_github_api_error_type(self):
        import requests as _req

        mock_resp = MagicMock()
        mock_resp.status_code = 500
        f = self._fetcher()
        with patch("requests.get", return_value=mock_resp):
            results = f.fetch_many([URL_E])
        fr = results[0]
        self.assertFalse(fr.success)
        self.assertIsNotNone(fr.structured_error)
        self.assertEqual(
            fr.structured_error["error_type"], ErrorType.GITHUB_API_ERROR.value
        )

    def test_success_200_has_no_structured_error(self):
        import requests as _req

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = VMESS
        f = self._fetcher()
        with patch("requests.get", return_value=mock_resp):
            results = f.fetch_many([URL_A])
        fr = results[0]
        self.assertTrue(fr.success)
        self.assertIsNone(fr.structured_error)


# ---------------------------------------------------------------------------
# 3. Pipeline stats["errors"] is Dict[str, dict]
# ---------------------------------------------------------------------------


class TestPipelineStructuredErrors(unittest.TestCase):

    def _pipeline_with_stub(self, stub: dict) -> Pipeline:
        src_a = _src(URL_A)
        src_b = _src(URL_B)
        p = Pipeline(sources=[src_a, src_b], check_health=False)
        p._fetch_all_sync = lambda stop, cb: stub
        return p

    def test_errors_key_is_dict_of_dicts(self):
        stub = {
            URL_A: [VMESS],
            URL_B: {
                "error_type": "timeout_error",
                "message": "timed out",
                "details": {},
            },
        }
        p = self._pipeline_with_stub(stub)
        result = p.run()
        errors = result.stats["errors"]
        self.assertIsInstance(errors, dict)
        self.assertIn(URL_B, errors)
        self.assertIsInstance(errors[URL_B], dict)

    def test_error_payload_has_error_type_key(self):
        stub = {
            URL_B: {
                "error_type": "timeout_error",
                "message": "timed out",
                "details": {},
            },
        }
        p = self._pipeline_with_stub(stub)
        result = p.run()
        self.assertEqual(result.stats["errors"][URL_B]["error_type"], "timeout_error")

    def test_successful_source_not_in_errors(self):
        stub = {
            URL_A: [VMESS],
            URL_B: {"error_type": "network_error", "message": "refused", "details": {}},
        }
        p = self._pipeline_with_stub(stub)
        result = p.run()
        self.assertNotIn(URL_A, result.stats["errors"])

    def test_legacy_string_error_normalised_to_dict(self):
        """A plain-string error value from old stubs must be wrapped in dict."""
        stub = {URL_B: "old plain string error"}
        p = self._pipeline_with_stub(stub)
        result = p.run()
        payload = result.stats["errors"].get(URL_B)
        self.assertIsNotNone(payload)
        self.assertIsInstance(payload, dict)
        self.assertIn("error_type", payload)
        self.assertIn("message", payload)


# ---------------------------------------------------------------------------
# 4. PipelineResult.failed_sources / failed_source_messages
# ---------------------------------------------------------------------------


class TestPipelineResultFailedSources(unittest.TestCase):

    def _result_with_errors(self, errors: dict) -> PipelineResult:
        return PipelineResult(stats={"errors": errors})

    def test_failed_sources_returns_dict_of_dicts(self):
        r = self._result_with_errors(
            {
                URL_B: {
                    "error_type": "timeout_error",
                    "message": "timed out",
                    "details": {},
                },
            }
        )
        fs = r.failed_sources
        self.assertIsInstance(fs, dict)
        self.assertIn(URL_B, fs)
        self.assertIsInstance(fs[URL_B], dict)

    def test_failed_source_messages_returns_str_values(self):
        r = self._result_with_errors(
            {
                URL_B: {
                    "error_type": "timeout_error",
                    "message": "timed out",
                    "details": {},
                },
            }
        )
        msgs = r.failed_source_messages
        self.assertIsInstance(msgs[URL_B], str)
        self.assertEqual(msgs[URL_B], "timed out")

    def test_failed_source_messages_handles_legacy_string(self):
        r = self._result_with_errors({URL_B: "legacy string"})
        msgs = r.failed_source_messages
        self.assertEqual(msgs[URL_B], "legacy string")

    def test_no_errors_returns_empty_dict(self):
        r = PipelineResult(stats={})
        self.assertEqual(r.failed_sources, {})
        self.assertEqual(r.failed_source_messages, {})

    def test_failed_sources_excludes_plain_strings(self):
        """failed_sources only returns entries whose value is a dict."""
        r = self._result_with_errors(
            {
                URL_B: {"error_type": "timeout_error", "message": "x", "details": {}},
                URL_C: "plain string",  # legacy — should be excluded from failed_sources
            }
        )
        fs = r.failed_sources
        self.assertIn(URL_B, fs)
        self.assertNotIn(URL_C, fs)

    def test_multiple_errors_all_present(self):
        r = self._result_with_errors(
            {
                URL_B: {"error_type": "timeout_error", "message": "t", "details": {}},
                URL_C: {"error_type": "network_error", "message": "n", "details": {}},
                URL_D: {
                    "error_type": "rate_limit_exceeded",
                    "message": "r",
                    "details": {},
                },
            }
        )
        fs = r.failed_sources
        self.assertEqual(set(fs.keys()), {URL_B, URL_C, URL_D})


if __name__ == "__main__":
    unittest.main()
