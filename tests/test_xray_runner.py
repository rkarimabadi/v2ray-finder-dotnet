"""Unit tests for xray_runner.py (Layer 1 — XrayBinaryManager).

All subprocess, filesystem, and network calls are mocked so the suite
runs offline with no xray binary present.
"""

from __future__ import annotations

import asyncio
import io
import json
import platform
import stat
import sys
import zipfile
from pathlib import Path
from typing import AsyncIterator
from unittest.mock import AsyncMock, MagicMock, patch, mock_open

import pytest

from v2ray_finder.xray_runner import (
    XrayBinaryManager,
    XrayBinaryNotFoundError,
    _asset_name,
    _binary_name,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_fake_zip(binary_name: str = "xray") -> bytes:
    """Return a minimal in-memory zip containing a fake xray binary."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w") as zf:
        zf.writestr(binary_name, b"\x7fELF fake xray binary")
    return buf.getvalue()


# ---------------------------------------------------------------------------
# _binary_name / _asset_name
# ---------------------------------------------------------------------------


class TestPlatformHelpers:
    def test_binary_name_windows(self):
        with patch("platform.system", return_value="Windows"):
            assert _binary_name() == "xray.exe"

    def test_binary_name_linux(self):
        with patch("platform.system", return_value="Linux"):
            assert _binary_name() == "xray"

    def test_asset_name_linux_x86_64(self):
        with patch("platform.system", return_value="Linux"), \
             patch("platform.machine", return_value="x86_64"):
            assert _asset_name() == "Xray-linux-64.zip"

    def test_asset_name_linux_aarch64(self):
        with patch("platform.system", return_value="Linux"), \
             patch("platform.machine", return_value="aarch64"):
            assert _asset_name() == "Xray-linux-arm64-v8a.zip"

    def test_asset_name_macos(self):
        with patch("platform.system", return_value="Darwin"), \
             patch("platform.machine", return_value="x86_64"):
            assert _asset_name() == "Xray-macos-64.zip"

    def test_asset_name_windows(self):
        with patch("platform.system", return_value="Windows"), \
             patch("platform.machine", return_value="amd64"):
            assert _asset_name() == "Xray-windows-64.zip"

    def test_asset_name_unknown_platform_falls_back_linux(self):
        with patch("platform.system", return_value="FreeBSD"), \
             patch("platform.machine", return_value="x86_64"):
            assert _asset_name().startswith("Xray-linux-")


# ---------------------------------------------------------------------------
# XrayBinaryNotFoundError
# ---------------------------------------------------------------------------


class TestXrayBinaryNotFoundError:
    def test_is_runtime_error(self):
        err = XrayBinaryNotFoundError("gone")
        assert isinstance(err, RuntimeError)

    def test_message_preserved(self):
        assert "gone" in str(XrayBinaryNotFoundError("gone"))


# ---------------------------------------------------------------------------
# find_binary
# ---------------------------------------------------------------------------


class TestFindBinary:
    def test_explicit_path_found(self, tmp_path):
        binary = tmp_path / "xray"
        binary.write_bytes(b"fake")
        mgr = XrayBinaryManager(binary_path=str(binary), auto_download=False)
        assert mgr.find_binary() == binary

    def test_explicit_path_missing_raises(self, tmp_path):
        mgr = XrayBinaryManager(
            binary_path=str(tmp_path / "nonexistent"), auto_download=False
        )
        with pytest.raises(XrayBinaryNotFoundError, match="does not exist"):
            mgr.find_binary()

    def test_found_in_path(self, tmp_path):
        binary = tmp_path / "xray"
        binary.write_bytes(b"fake")
        mgr = XrayBinaryManager(auto_download=False)
        with patch("shutil.which", return_value=str(binary)):
            result = mgr.find_binary()
        assert result == binary

    def test_found_in_common_dir(self, tmp_path):
        binary = tmp_path / "xray"
        binary.write_bytes(b"fake")
        mgr = XrayBinaryManager(auto_download=False)
        with patch("shutil.which", return_value=None), \
             patch(
                 "v2ray_finder.xray_runner._COMMON_INSTALL_DIRS",
                 [str(tmp_path)],
             ):
            result = mgr.find_binary()
        assert result == binary

    def test_found_in_cache(self, tmp_path):
        cached = tmp_path / "xray"
        cached.write_bytes(b"fake")
        mgr = XrayBinaryManager(download_dir=str(tmp_path), auto_download=False)
        with patch("shutil.which", return_value=None), \
             patch(
                 "v2ray_finder.xray_runner._COMMON_INSTALL_DIRS", []
             ):
            result = mgr.find_binary()
        assert result == cached

    def test_auto_download_false_raises(self):
        mgr = XrayBinaryManager(auto_download=False)
        with patch("shutil.which", return_value=None), \
             patch("v2ray_finder.xray_runner._COMMON_INSTALL_DIRS", []):
            with pytest.raises(XrayBinaryNotFoundError, match="not found"):
                mgr.find_binary()

    def test_resolved_path_cached_on_second_call(self, tmp_path):
        binary = tmp_path / "xray"
        binary.write_bytes(b"fake")
        mgr = XrayBinaryManager(auto_download=False)
        with patch("shutil.which", return_value=str(binary)):
            first = mgr.find_binary()
        # Second call must return cached result without calling shutil.which again
        with patch("shutil.which", side_effect=AssertionError("should not be called")):
            second = mgr.find_binary()
        assert first == second


# ---------------------------------------------------------------------------
# _download_binary
# ---------------------------------------------------------------------------


class TestDownloadBinary:
    def _make_release_json(self, asset_name: str, url: str = "https://example.com/xray.zip") -> bytes:
        return json.dumps({
            "tag_name": "v25.0.0",
            "assets": [{"name": asset_name, "browser_download_url": url}],
        }).encode()

    def test_happy_path(self, tmp_path):
        system = platform.system().lower()
        machine = platform.machine().lower()
        arch_map = {
            "x86_64": "64", "amd64": "64",
            "aarch64": "arm64-v8a", "arm64": "arm64-v8a", "armv7l": "arm32-v7a",
        }
        arch = arch_map.get(machine, "64")
        if system == "darwin":
            asset = f"Xray-macos-{arch}.zip"
        elif system == "windows":
            asset = f"Xray-windows-{arch}.zip"
        else:
            asset = f"Xray-linux-{arch}.zip"

        release_json = self._make_release_json(asset)
        fake_zip = _make_fake_zip("xray")

        mgr = XrayBinaryManager(download_dir=str(tmp_path), auto_download=True)

        class _FakeResp:
            def read(self): return release_json
            def __enter__(self): return self
            def __exit__(self, *_): pass

        with patch("urllib.request.urlopen", return_value=_FakeResp()), \
             patch("urllib.request.urlretrieve") as mock_retr, \
             patch("tempfile.NamedTemporaryFile") as mock_tmp, \
             patch("os.unlink"), \
             patch("platform.system", return_value=system.capitalize()):

            # Make the temp file point to our fake zip
            tmp_zip = tmp_path / "dl.zip"
            tmp_zip.write_bytes(fake_zip)
            mock_tmp.return_value.__enter__ = lambda s: s
            mock_tmp.return_value.__exit__ = MagicMock(return_value=False)
            mock_tmp.return_value.name = str(tmp_zip)

            result = mgr._download_binary()

        assert result.name in ("xray", "xray.exe")
        assert result.parent == tmp_path

    def test_missing_asset_raises(self, tmp_path):
        release_json = json.dumps({
            "tag_name": "v1.0",
            "assets": [{"name": "Xray-plan9-mips.zip", "browser_download_url": "http://x"}],
        }).encode()

        class _FakeResp:
            def read(self): return release_json
            def __enter__(self): return self
            def __exit__(self, *_): pass

        mgr = XrayBinaryManager(download_dir=str(tmp_path))
        with patch("urllib.request.urlopen", return_value=_FakeResp()), \
             patch("platform.system", return_value="Linux"), \
             patch("platform.machine", return_value="x86_64"):
            with pytest.raises(XrayBinaryNotFoundError, match="Asset"):
                mgr._download_binary()

    def test_missing_binary_in_zip_raises(self, tmp_path):
        asset_name = "Xray-linux-64.zip"
        release_json = self._make_release_json(asset_name)

        # Zip without xray binary
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, mode="w") as zf:
            zf.writestr("README.md", "nothing here")
        empty_zip = buf.getvalue()

        class _FakeResp:
            def read(self): return release_json
            def __enter__(self): return self
            def __exit__(self, *_): pass

        mgr = XrayBinaryManager(download_dir=str(tmp_path))
        tmp_zip = tmp_path / "dl.zip"
        tmp_zip.write_bytes(empty_zip)

        with patch("urllib.request.urlopen", return_value=_FakeResp()), \
             patch("urllib.request.urlretrieve"), \
             patch("tempfile.NamedTemporaryFile") as mock_tmp, \
             patch("os.unlink"), \
             patch("platform.system", return_value="Linux"), \
             patch("platform.machine", return_value="x86_64"):
            mock_tmp.return_value.__enter__ = lambda s: s
            mock_tmp.return_value.__exit__ = MagicMock(return_value=False)
            mock_tmp.return_value.name = str(tmp_zip)
            with pytest.raises(XrayBinaryNotFoundError, match="binary inside zip"):
                mgr._download_binary()

    def test_network_error_raises(self, tmp_path):
        mgr = XrayBinaryManager(download_dir=str(tmp_path))
        with patch("urllib.request.urlopen", side_effect=OSError("no network")):
            with pytest.raises(XrayBinaryNotFoundError, match="release info"):
                mgr._download_binary()


# ---------------------------------------------------------------------------
# get_version / is_available
# ---------------------------------------------------------------------------


class TestVersionAndAvailability:
    def test_get_version_success(self, tmp_path):
        binary = tmp_path / "xray"
        binary.write_bytes(b"fake")
        mgr = XrayBinaryManager(binary_path=str(binary), auto_download=False)

        mock_result = MagicMock()
        mock_result.stdout = "Xray 24.9.30 (XTLS/Xray-core)\n"
        mock_result.stderr = ""
        with patch("subprocess.run", return_value=mock_result):
            version = mgr.get_version()
        assert "24.9.30" in version

    def test_get_version_exception_returns_unknown(self):
        mgr = XrayBinaryManager(auto_download=False)
        with patch.object(mgr, "find_binary", side_effect=XrayBinaryNotFoundError("no")):
            assert mgr.get_version() == "unknown"

    def test_is_available_true(self, tmp_path):
        binary = tmp_path / "xray"
        binary.write_bytes(b"fake")
        mgr = XrayBinaryManager(binary_path=str(binary), auto_download=False)
        assert mgr.is_available() is True

    def test_is_available_false(self):
        mgr = XrayBinaryManager(auto_download=False)
        with patch("shutil.which", return_value=None), \
             patch("v2ray_finder.xray_runner._COMMON_INSTALL_DIRS", []):
            assert mgr.is_available() is False


# ---------------------------------------------------------------------------
# run() context manager
# ---------------------------------------------------------------------------


class TestRunContextManager:
    """Tests for XrayBinaryManager.run() using mock subprocess."""

    def _make_mock_proc(self, startup_line: bytes = b"[Info] started", rc: int = 0):
        """Return an asyncio.subprocess.Process-like mock."""
        proc = MagicMock()
        proc.pid = 12345
        proc.returncode = None  # running

        async def _readline():
            return startup_line

        async def _aiter(self):
            yield startup_line

        proc.stdout = MagicMock()
        proc.stdout.__aiter__ = _aiter

        async def _wait(): proc.returncode = rc
        proc.wait = _wait
        proc.terminate = MagicMock()
        proc.kill = MagicMock()
        return proc

    @pytest.mark.asyncio
    async def test_run_yields_process_and_terminates(self, tmp_path):
        binary = tmp_path / "xray"
        binary.write_bytes(b"fake")
        config = tmp_path / "cfg.json"
        config.write_text("{}")

        mock_proc = self._make_mock_proc(b"[Info] started")

        with patch.object(
            XrayBinaryManager, "find_binary", return_value=binary
        ), patch(
            "asyncio.create_subprocess_exec", return_value=mock_proc
        ) as mock_exec:
            mgr = XrayBinaryManager(binary_path=str(binary), auto_download=False)
            async with mgr.run(config, socks_port=10800) as proc:
                assert proc is mock_proc

        mock_proc.terminate.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_timeout_kills_process(self, tmp_path):
        binary = tmp_path / "xray"
        binary.write_bytes(b"fake")
        config = tmp_path / "cfg.json"
        config.write_text("{}")

        proc = MagicMock()
        proc.returncode = None
        proc.kill = MagicMock()
        async def _wait(): proc.returncode = -9
        proc.wait = _wait

        async def _slow_wait(*args, **kwargs):
            raise asyncio.TimeoutError()

        with patch.object(XrayBinaryManager, "find_binary", return_value=binary), \
             patch("asyncio.create_subprocess_exec", return_value=proc), \
             patch("asyncio.wait_for", side_effect=asyncio.TimeoutError()):
            mgr = XrayBinaryManager(
                binary_path=str(binary), auto_download=False, startup_timeout=0.01
            )
            with pytest.raises(RuntimeError, match="did not start"):
                async with mgr.run(config, socks_port=10800):
                    pass

        proc.kill.assert_called()

    @pytest.mark.asyncio
    async def test_run_startup_crash_raises(self, tmp_path):
        """Process that immediately exits with non-zero rc should raise."""
        binary = tmp_path / "xray"
        binary.write_bytes(b"fake")
        config = tmp_path / "cfg.json"
        config.write_text("{}")

        proc = MagicMock()
        proc.returncode = 1  # already dead
        proc.kill = MagicMock()
        async def _wait(): pass
        proc.wait = _wait

        async def _aiter(self):
            yield b"some output without startup marker"
            proc.returncode = 1

        proc.stdout = MagicMock()
        proc.stdout.__aiter__ = _aiter

        with patch.object(XrayBinaryManager, "find_binary", return_value=binary), \
             patch("asyncio.create_subprocess_exec", return_value=proc):
            mgr = XrayBinaryManager(
                binary_path=str(binary), auto_download=False, startup_timeout=2.0
            )
            with pytest.raises((RuntimeError, asyncio.TimeoutError)):
                async with mgr.run(config, socks_port=10800):
                    pass
