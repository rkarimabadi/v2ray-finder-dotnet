"""XrayBinaryManager — Layer 1 of real connectivity checking.

Responsibilities
----------------
* Locate the xray binary on the host (PATH, common install dirs).
* Optionally download a pinned release from XTLS/Xray-core when the
  binary is not found and ``auto_download=True``.
* Report the binary version so callers can assert a minimum version.
* Start and stop xray as a subprocess with a caller-supplied outbound
  config, exposing a local SOCKS5 port for subsequent HTTP probes.

This module is **intentionally free of v2ray_finder domain logic** so it
can be tested in isolation without any config parsing or health-checker
concerns.

Typical usage
-------------
::

    manager = XrayBinaryManager()          # auto-discovers binary
    async with manager.run(config_path, socks_port=10800) as proc:
        # xray is running; send requests through 127.0.0.1:10800
        ...
    # xray process is guaranteed to be terminated here

Layer 2 (ConfigAdapter) and Layer 3 (RealConnectivityChecker) will be
added in subsequent PRs.
"""

from __future__ import annotations

import asyncio
import logging
import os
import platform
import shutil
import stat
import sys
import tempfile
import zipfile
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_XTLS_REPO = "XTLS/Xray-core"
_GITHUB_RELEASES_API = f"https://api.github.com/repos/{_XTLS_REPO}/releases/latest"

# Directories checked (in order) when scanning for a pre-installed binary.
_COMMON_INSTALL_DIRS: List[str] = [
    "/usr/local/bin",
    "/usr/bin",
    "/opt/xray",
    "/opt/v2ray",
    str(Path.home() / ".local" / "bin"),
    str(Path.home() / ".xray"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _binary_name() -> str:
    """Return platform-appropriate binary filename."""
    return "xray.exe" if platform.system() == "Windows" else "xray"


def _asset_name() -> str:
    """Return the GitHub release asset name for the current platform/arch.

    Mapping follows XTLS release naming conventions::

        Xray-linux-64.zip
        Xray-linux-arm64-v8a.zip
        Xray-macos-64.zip
        Xray-windows-64.zip
    """
    system = platform.system().lower()
    machine = platform.machine().lower()

    arch_map = {
        "x86_64": "64",
        "amd64": "64",
        "aarch64": "arm64-v8a",
        "arm64": "arm64-v8a",
        "armv7l": "arm32-v7a",
    }
    arch = arch_map.get(machine, "64")

    if system == "linux":
        return f"Xray-linux-{arch}.zip"
    if system == "darwin":
        return f"Xray-macos-{arch}.zip"
    if system == "windows":
        return f"Xray-windows-{arch}.zip"
    return f"Xray-linux-{arch}.zip"


# ---------------------------------------------------------------------------
# XrayBinaryManager
# ---------------------------------------------------------------------------


class XrayBinaryNotFoundError(RuntimeError):
    """Raised when the xray binary cannot be located or downloaded."""


class XrayBinaryManager:
    """Manages lifecycle of the xray binary.

    Parameters
    ----------
    binary_path:
        Explicit path to the xray binary.  When *None* the manager will
        search PATH and common install directories automatically.
    auto_download:
        If *True* and the binary is not found, download the latest
        release from GitHub into *download_dir*.
    download_dir:
        Directory where the binary is cached after download.  Defaults
        to ``~/.cache/v2ray-finder/xray``.
    startup_timeout:
        Seconds to wait for xray to write its startup log line before
        declaring it failed to start.
    """

    def __init__(
        self,
        binary_path: Optional[str] = None,
        auto_download: bool = True,
        download_dir: Optional[str] = None,
        startup_timeout: float = 5.0,
    ) -> None:
        self._explicit_path = binary_path
        self.auto_download = auto_download
        self.download_dir = Path(
            download_dir
            or Path.home() / ".cache" / "v2ray-finder" / "xray"
        )
        self.startup_timeout = startup_timeout
        self._resolved_path: Optional[Path] = None

    # ------------------------------------------------------------------
    # Binary discovery
    # ------------------------------------------------------------------

    def find_binary(self) -> Path:
        """Return path to a usable xray binary.

        Search order:
        1. Explicit path supplied at construction time.
        2. ``PATH`` (via :func:`shutil.which`).
        3. Common install directories.
        4. Previously auto-downloaded binary in *download_dir*.
        5. Auto-download from GitHub (only when ``auto_download=True``).

        Raises
        ------
        XrayBinaryNotFoundError
            When no binary is found and auto-download is disabled or fails.
        """
        if self._resolved_path and self._resolved_path.is_file():
            return self._resolved_path

        name = _binary_name()

        # 1. Explicit path
        if self._explicit_path:
            p = Path(self._explicit_path)
            if p.is_file():
                logger.debug(f"[xray] Using explicit binary: {p}")
                self._resolved_path = p
                return p
            raise XrayBinaryNotFoundError(
                f"Explicit xray path does not exist: {self._explicit_path}"
            )

        # 2. PATH
        which = shutil.which(name)
        if which:
            logger.debug(f"[xray] Found in PATH: {which}")
            self._resolved_path = Path(which)
            return self._resolved_path

        # 3. Common install dirs
        for d in _COMMON_INSTALL_DIRS:
            candidate = Path(d) / name
            if candidate.is_file():
                logger.debug(f"[xray] Found in common dir: {candidate}")
                self._resolved_path = candidate
                return self._resolved_path

        # 4. Previously downloaded
        cached = self.download_dir / name
        if cached.is_file():
            logger.debug(f"[xray] Using cached download: {cached}")
            self._resolved_path = cached
            return self._resolved_path

        # 5. Auto-download
        if self.auto_download:
            logger.info("[xray] Binary not found — attempting auto-download…")
            return self._download_binary()

        raise XrayBinaryNotFoundError(
            f"xray binary '{name}' not found. "
            "Install xray-core or set auto_download=True."
        )

    def _download_binary(self) -> Path:
        """Download the latest xray release from GitHub and cache it.

        Uses only stdlib (``urllib``) to avoid extra dependencies.
        """
        import json
        import urllib.request

        logger.info(f"[xray] Fetching latest release info from {_GITHUB_RELEASES_API}")
        try:
            with urllib.request.urlopen(_GITHUB_RELEASES_API, timeout=15) as resp:
                release = json.loads(resp.read())
        except Exception as exc:
            raise XrayBinaryNotFoundError(
                f"Failed to fetch xray release info: {exc}"
            ) from exc

        version = release.get("tag_name", "unknown")
        asset_name = _asset_name()
        assets = release.get("assets", [])
        download_url = next(
            (a["browser_download_url"] for a in assets if a["name"] == asset_name),
            None,
        )
        if not download_url:
            available = [a["name"] for a in assets]
            raise XrayBinaryNotFoundError(
                f"Asset '{asset_name}' not found in release {version}. "
                f"Available: {available}"
            )

        logger.info(f"[xray] Downloading {asset_name} ({version}) from {download_url}")
        self.download_dir.mkdir(parents=True, exist_ok=True)

        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            urllib.request.urlretrieve(download_url, tmp_path)
            name = _binary_name()
            dest = self.download_dir / name
            with zipfile.ZipFile(tmp_path) as zf:
                # The xray binary sits at the root of the zip
                members = zf.namelist()
                binary_member = next(
                    (m for m in members if Path(m).name in (name, "xray")),
                    None,
                )
                if not binary_member:
                    raise XrayBinaryNotFoundError(
                        f"Could not find binary inside zip. Members: {members}"
                    )
                with zf.open(binary_member) as src, open(dest, "wb") as out:
                    out.write(src.read())

            # Make executable on Unix
            if platform.system() != "Windows":
                dest.chmod(dest.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP)

            logger.info(f"[xray] Downloaded and cached at {dest}")
            self._resolved_path = dest
            return dest
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    # ------------------------------------------------------------------
    # Version detection
    # ------------------------------------------------------------------

    def get_version(self) -> str:
        """Return the xray version string (e.g. ``'Xray 24.9.30'``).

        Returns ``'unknown'`` if the binary cannot report its version.
        """
        try:
            binary = self.find_binary()
            import subprocess

            result = subprocess.run(
                [str(binary), "version"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            first_line = (result.stdout or result.stderr or "").splitlines()[0]
            return first_line.strip() or "unknown"
        except Exception as exc:
            logger.debug(f"[xray] version check failed: {exc}")
            return "unknown"

    def is_available(self) -> bool:
        """Return *True* if xray is findable without raising."""
        try:
            self.find_binary()
            return True
        except XrayBinaryNotFoundError:
            return False

    # ------------------------------------------------------------------
    # Process lifecycle
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def run(
        self,
        config_path: str | Path,
        socks_port: int,
    ) -> AsyncIterator[asyncio.subprocess.Process]:
        """Async context manager: start xray and yield the running process.

        The process is **always** terminated when the context exits, even
        if an exception is raised inside the ``async with`` block.

        Parameters
        ----------
        config_path:
            Path to a complete xray JSON config file.  Layer 2
            (ConfigAdapter) is responsible for generating this file.
        socks_port:
            The local SOCKS5 port declared in the inbound section of
            *config_path*.  Stored on the context for callers to use;
            not validated here.

        Yields
        ------
        asyncio.subprocess.Process
            The running xray process.  Callers should not terminate it
            manually — the context manager handles cleanup.

        Raises
        ------
        XrayBinaryNotFoundError
            If no binary is available.
        RuntimeError
            If xray fails to start within *startup_timeout* seconds.
        """
        binary = self.find_binary()
        config_path = Path(config_path)

        logger.info(
            f"[xray] Starting: {binary} run -c {config_path} (SOCKS5 :{socks_port})"
        )

        proc = await asyncio.create_subprocess_exec(
            str(binary),
            "run",
            "-c",
            str(config_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )

        # Wait for the startup confirmation line or timeout.
        try:
            await asyncio.wait_for(
                self._wait_for_startup(proc),
                timeout=self.startup_timeout,
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise RuntimeError(
                f"[xray] Process did not start within {self.startup_timeout}s "
                f"(port {socks_port})"
            )
        except Exception:
            proc.kill()
            await proc.wait()
            raise

        try:
            yield proc
        finally:
            if proc.returncode is None:
                logger.debug(f"[xray] Terminating process (pid={proc.pid})")
                try:
                    proc.terminate()
                    await asyncio.wait_for(proc.wait(), timeout=3.0)
                except asyncio.TimeoutError:
                    logger.warning("[xray] Process did not exit cleanly — killing")
                    proc.kill()
                    await proc.wait()
            logger.debug(f"[xray] Process exited (rc={proc.returncode})")

    @staticmethod
    async def _wait_for_startup(proc: asyncio.subprocess.Process) -> None:
        """Read stdout until xray prints its listening confirmation."""
        if proc.stdout is None:
            return
        # xray prints a line containing "started" or the port number when ready.
        startup_markers = (b"started", b"listening", b"inbound", b"[Info]")
        async for line in proc.stdout:
            logger.debug(f"[xray-stdout] {line.decode(errors='replace').rstrip()}")
            if any(m in line for m in startup_markers):
                return
            # If the process crashes immediately its returncode will be set
            if proc.returncode is not None and proc.returncode != 0:
                raise RuntimeError(
                    f"[xray] Process exited with code {proc.returncode} during startup"
                )
