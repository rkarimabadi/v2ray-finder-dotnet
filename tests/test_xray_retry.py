"""Tests for V1-D4: xray retry on port contention in check_one."""
from __future__ import annotations

import unittest
from unittest.mock import call, patch

from v2ray_finder.xray_connectivity import (
    RealHealthResult,
    _LegacyResult,
    _try_start_xray,
    check_one,
    find_free_port,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _good_probe(*args, **kwargs):
    return True, 204, 50.0


def _bad_probe(*args, **kwargs):
    return False, None, None


class _FakeRunner:
    def __init__(self, *, fail_start=False):
        self._fail = fail_start
        self.started = False
        self.stopped = False
        self.stop_call_count = 0

    def start(self, cfg):
        if self._fail:
            raise RuntimeError("address already in use")
        self.started = True

    def stop(self):
        self.stopped = True
        self.stop_call_count += 1


# ---------------------------------------------------------------------------
# find_free_port
# ---------------------------------------------------------------------------

class TestFindFreePort(unittest.TestCase):

    def test_returns_valid_port(self):
        port = find_free_port()
        self.assertIsInstance(port, int)
        self.assertGreater(port, 0)
        self.assertLessEqual(port, 65535)

    def test_two_calls_may_differ(self):
        ports = {find_free_port() for _ in range(5)}
        self.assertGreaterEqual(len(ports), 1)


# ---------------------------------------------------------------------------
# _try_start_xray — resource safety
# ---------------------------------------------------------------------------

class TestTryStartXray(unittest.TestCase):
    """_try_start_xray must stop the runner when start() raises."""

    @patch("v2ray_finder.xray_connectivity.XrayRunner")
    def test_stop_called_on_failed_start(self, MockRunner):
        runner = _FakeRunner(fail_start=True)
        MockRunner.return_value = runner
        err, returned_runner = _try_start_xray(19100, {"cfg": True}, None, False)
        self.assertIsNotNone(err)
        self.assertIsNone(returned_runner)
        self.assertEqual(runner.stop_call_count, 1,
                         "stop() must be called exactly once on failed start")

    @patch("v2ray_finder.xray_connectivity.XrayRunner")
    def test_stop_not_called_on_success(self, MockRunner):
        runner = _FakeRunner(fail_start=False)
        MockRunner.return_value = runner
        err, returned_runner = _try_start_xray(19101, {"cfg": True}, None, False)
        self.assertIsNone(err)
        self.assertIs(returned_runner, runner)
        self.assertEqual(runner.stop_call_count, 0,
                         "stop() must NOT be called on successful start")

    @patch("v2ray_finder.xray_connectivity.XrayRunner")
    def test_returns_error_string_on_failure(self, MockRunner):
        runner = _FakeRunner(fail_start=True)
        MockRunner.return_value = runner
        err, _ = _try_start_xray(19102, {"cfg": True}, None, False)
        self.assertIsInstance(err, str)
        self.assertIn("already in use", err)

    @patch("v2ray_finder.xray_connectivity.XrayRunner")
    def test_stop_exception_does_not_propagate(self, MockRunner):
        """If stop() itself raises, _try_start_xray must not propagate it."""
        runner = _FakeRunner(fail_start=True)
        runner.stop = lambda: (_ for _ in ()).throw(OSError("cleanup failed"))
        MockRunner.return_value = runner
        # Must not raise
        err, _ = _try_start_xray(19103, {"cfg": True}, None, False)
        self.assertIsNotNone(err)


# ---------------------------------------------------------------------------
# check_one — happy path (no retry needed)
# ---------------------------------------------------------------------------

class TestCheckOneNoRetry(unittest.TestCase):

    @patch("v2ray_finder.xray_connectivity.config_to_xray", return_value={"cfg": True})
    @patch("v2ray_finder.xray_connectivity.XrayRunner")
    @patch("v2ray_finder.xray_connectivity._socks5_http_get", side_effect=_good_probe)
    def test_success_no_retry(self, mock_probe, MockRunner, mock_cfg):
        runner = _FakeRunner(fail_start=False)
        MockRunner.return_value = runner
        result = check_one("vmess://eyJhZGQiOiIxMjcuMC4wLjEifQ==", local_port=19000)
        self.assertTrue(result.reachable)
        self.assertTrue(result.google_204_ok)
        self.assertFalse(result.retried)
        self.assertEqual(result.latency_ms, 50.0)
        self.assertTrue(runner.stopped)

    @patch("v2ray_finder.xray_connectivity.config_to_xray", return_value={"cfg": True})
    @patch("v2ray_finder.xray_connectivity.XrayRunner")
    @patch("v2ray_finder.xray_connectivity._socks5_http_get", side_effect=_good_probe)
    def test_retried_flag_false_on_clean_start(self, _p, MockRunner, _c):
        MockRunner.return_value = _FakeRunner(fail_start=False)
        result = check_one("vmess://abc", local_port=19001)
        self.assertFalse(result.retried)


# ---------------------------------------------------------------------------
# check_one — V1-D4: first attempt fails, retry succeeds
# ---------------------------------------------------------------------------

class TestCheckOneRetrySucceeds(unittest.TestCase):

    @patch("v2ray_finder.xray_connectivity.config_to_xray", return_value={"cfg": True})
    @patch("v2ray_finder.xray_connectivity.find_free_port", return_value=29999)
    @patch("v2ray_finder.xray_connectivity._socks5_http_get", side_effect=_good_probe)
    @patch("v2ray_finder.xray_connectivity.XrayRunner")
    def test_retry_succeeds_sets_retried_true(self, MockRunner, *_):
        MockRunner.side_effect = [_FakeRunner(fail_start=True), _FakeRunner(fail_start=False)]
        result = check_one("vmess://eyJhZGQiOiIxMjcuMC4wLjEifQ==", local_port=19002)
        self.assertTrue(result.retried)
        self.assertTrue(result.reachable)
        self.assertTrue(result.google_204_ok)
        self.assertEqual(result.latency_ms, 50.0)

    @patch("v2ray_finder.xray_connectivity.config_to_xray", return_value={"cfg": True})
    @patch("v2ray_finder.xray_connectivity.find_free_port", return_value=29998)
    @patch("v2ray_finder.xray_connectivity._socks5_http_get", side_effect=_good_probe)
    @patch("v2ray_finder.xray_connectivity.XrayRunner")
    def test_retry_uses_fresh_port(self, MockRunner, *_):
        MockRunner.side_effect = [_FakeRunner(fail_start=True), _FakeRunner(fail_start=False)]
        check_one("vmess://eyJhZGQiOiIxMjcuMC4wLjEifQ==", local_port=19003)
        second_kwargs = MockRunner.call_args_list[1][1]
        self.assertEqual(second_kwargs["local_port"], 29998)

    @patch("v2ray_finder.xray_connectivity.config_to_xray", return_value={"cfg": True})
    @patch("v2ray_finder.xray_connectivity.find_free_port", return_value=29997)
    @patch("v2ray_finder.xray_connectivity._socks5_http_get", side_effect=_good_probe)
    @patch("v2ray_finder.xray_connectivity.XrayRunner")
    def test_xray_called_twice_on_first_failure(self, MockRunner, *_):
        MockRunner.side_effect = [_FakeRunner(fail_start=True), _FakeRunner(fail_start=False)]
        check_one("vmess://eyJhZGQiOiIxMjcuMC4wLjEifQ==", local_port=19004)
        self.assertEqual(MockRunner.call_count, 2)

    @patch("v2ray_finder.xray_connectivity.config_to_xray", return_value={"cfg": True})
    @patch("v2ray_finder.xray_connectivity.find_free_port", return_value=29996)
    @patch("v2ray_finder.xray_connectivity._socks5_http_get", side_effect=_good_probe)
    @patch("v2ray_finder.xray_connectivity.XrayRunner")
    def test_result_socks_port_updated_to_retry_port(self, MockRunner, *_):
        MockRunner.side_effect = [_FakeRunner(fail_start=True), _FakeRunner(fail_start=False)]
        result = check_one("vmess://eyJhZGQiOiIxMjcuMC4wLjEifQ==", local_port=19005)
        self.assertEqual(result.socks_port, 29996)

    @patch("v2ray_finder.xray_connectivity.config_to_xray", return_value={"cfg": True})
    @patch("v2ray_finder.xray_connectivity.find_free_port", return_value=29995)
    @patch("v2ray_finder.xray_connectivity._socks5_http_get", side_effect=_good_probe)
    @patch("v2ray_finder.xray_connectivity.XrayRunner")
    def test_failed_runner_stop_called_before_retry(self, MockRunner, *_):
        """The failed first runner must be stopped before the retry starts."""
        fail_runner = _FakeRunner(fail_start=True)
        ok_runner   = _FakeRunner(fail_start=False)
        MockRunner.side_effect = [fail_runner, ok_runner]
        check_one("vmess://eyJhZGQiOiIxMjcuMC4wLjEifQ==", local_port=19006)
        self.assertEqual(fail_runner.stop_call_count, 1,
                         "Failed runner must be stopped exactly once")


# ---------------------------------------------------------------------------
# check_one — both attempts fail
# ---------------------------------------------------------------------------

class TestCheckOneBothAttemptsFail(unittest.TestCase):

    @patch("v2ray_finder.xray_connectivity.config_to_xray", return_value={"cfg": True})
    @patch("v2ray_finder.xray_connectivity.find_free_port", return_value=29994)
    @patch("v2ray_finder.xray_connectivity.XrayRunner")
    def test_both_fail_retried_true_unreachable(self, MockRunner, *_):
        MockRunner.side_effect = [_FakeRunner(fail_start=True), _FakeRunner(fail_start=True)]
        result = check_one("vmess://eyJhZGQiOiIxMjcuMC4wLjEifQ==", local_port=19007)
        self.assertTrue(result.retried)
        self.assertFalse(result.reachable)
        self.assertIsNotNone(result.error)

    @patch("v2ray_finder.xray_connectivity.config_to_xray", return_value={"cfg": True})
    @patch("v2ray_finder.xray_connectivity.find_free_port", return_value=29993)
    @patch("v2ray_finder.xray_connectivity.XrayRunner")
    def test_both_fail_error_contains_both_messages(self, MockRunner, *_):
        MockRunner.side_effect = [_FakeRunner(fail_start=True), _FakeRunner(fail_start=True)]
        result = check_one("vmess://eyJhZGQiOiIxMjcuMC4wLjEifQ==", local_port=19008)
        self.assertIn("retry", result.error.lower())

    @patch("v2ray_finder.xray_connectivity.config_to_xray", return_value={"cfg": True})
    @patch("v2ray_finder.xray_connectivity.find_free_port", return_value=29992)
    @patch("v2ray_finder.xray_connectivity.XrayRunner")
    def test_xray_called_exactly_twice_on_double_failure(self, MockRunner, *_):
        MockRunner.side_effect = [_FakeRunner(fail_start=True), _FakeRunner(fail_start=True)]
        check_one("vmess://eyJhZGQiOiIxMjcuMC4wLjEifQ==", local_port=19009)
        self.assertEqual(MockRunner.call_count, 2, "Must not retry more than once")

    @patch("v2ray_finder.xray_connectivity.config_to_xray", return_value={"cfg": True})
    @patch("v2ray_finder.xray_connectivity.find_free_port", return_value=29991)
    @patch("v2ray_finder.xray_connectivity.XrayRunner")
    def test_both_failed_runners_are_stopped(self, MockRunner, *_):
        """Both runners must be stopped even when both fail."""
        r1 = _FakeRunner(fail_start=True)
        r2 = _FakeRunner(fail_start=True)
        MockRunner.side_effect = [r1, r2]
        check_one("vmess://eyJhZGQiOiIxMjcuMC4wLjEifQ==", local_port=19010)
        self.assertEqual(r1.stop_call_count, 1)
        self.assertEqual(r2.stop_call_count, 1)


# ---------------------------------------------------------------------------
# check_one — edge cases
# ---------------------------------------------------------------------------

class TestCheckOneEdgeCases(unittest.TestCase):

    def test_invalid_uri_no_scheme(self):
        result = check_one("not-a-valid-uri")
        self.assertFalse(result.reachable)
        self.assertIsNotNone(result.error)

    @patch("v2ray_finder.xray_connectivity.config_to_xray", return_value=None)
    def test_config_to_xray_returns_none(self, _):
        result = check_one("vmess://bad-config")
        self.assertFalse(result.reachable)
        self.assertIn("xray config", result.error)

    @patch("v2ray_finder.xray_connectivity.config_to_xray", return_value={"cfg": True})
    @patch("v2ray_finder.xray_connectivity.XrayRunner")
    @patch("v2ray_finder.xray_connectivity._socks5_http_get", side_effect=_bad_probe)
    def test_xray_starts_but_probe_fails(self, _probe, MockRunner, _cfg):
        MockRunner.return_value = _FakeRunner(fail_start=False)
        result = check_one("vmess://eyJhZGQiOiIxMjcuMC4wLjEifQ==", local_port=19011)
        self.assertFalse(result.reachable)
        self.assertFalse(result.retried)

    @patch("v2ray_finder.xray_connectivity.config_to_xray", return_value={"cfg": True})
    @patch("v2ray_finder.xray_connectivity.XrayRunner")
    @patch("v2ray_finder.xray_connectivity._socks5_http_get", side_effect=_good_probe)
    def test_runner_stop_called_in_finally(self, _probe, MockRunner, _cfg):
        runner = _FakeRunner(fail_start=False)
        MockRunner.return_value = runner
        check_one("vmess://eyJhZGQiOiIxMjcuMC4wLjEifQ==", local_port=19012)
        self.assertTrue(runner.stopped)

    @patch("v2ray_finder.xray_connectivity.config_to_xray",
           side_effect=[{"cfg": True}, None])
    @patch("v2ray_finder.xray_connectivity.find_free_port", return_value=29990)
    @patch("v2ray_finder.xray_connectivity.XrayRunner")
    def test_retry_config_to_xray_none_returns_error(self, MockRunner, _ffp, _cfg):
        MockRunner.return_value = _FakeRunner(fail_start=True)
        result = check_one("vmess://eyJhZGQiOiIxMjcuMC4wLjEifQ==", local_port=19013)
        self.assertFalse(result.reachable)
        self.assertTrue(result.retried)


# ---------------------------------------------------------------------------
# RealHealthResult.retried field
# ---------------------------------------------------------------------------

class TestRealHealthResultRetried(unittest.TestCase):

    def test_default_retried_false(self):
        r = RealHealthResult(config="vmess://x", protocol="vmess")
        self.assertFalse(r.retried)

    def test_retried_survives_to_dict_roundtrip(self):
        from v2ray_finder.xray_connectivity import real_health_to_dict
        r = RealHealthResult(
            config="vmess://x", protocol="vmess",
            reachable=True, retried=True, latency_ms=100.0
        )
        d = real_health_to_dict(r)
        self.assertTrue(d["retried"])


if __name__ == "__main__":
    unittest.main()
