"""Core V2RayServerFinder implementation."""
from __future__ import annotations

import datetime
import logging
import os
import re
import threading
from typing import Any, Dict, List, Optional, Tuple

import requests

from .exceptions import (
    AuthenticationError,
    GitHubAPIError,
    NetworkError,
    ParseError,
    RateLimitError,
    RepositoryNotFoundError,
    TimeoutError,
    V2RayFinderError,
)
from .result import Err, Ok, Result
from .sources import get_enabled_sources

logger = logging.getLogger(__name__)

_TOKEN_MIN_LEN = 20
_KNOWN_PREFIXES = ("ghp_", "gho_", "ghs_", "ghu_", "github_pat_")

# File extensions considered as potential v2ray config files
_CONFIG_EXTENSIONS = {".txt", ".json", ".yaml", ".yml", ".conf", ".sub", ".base64"}


def _validate_token(token: Optional[str]) -> Optional[str]:
    """Return the token if it passes basic sanity checks, else None."""
    if not token:
        return None
    if len(token) < _TOKEN_MIN_LEN:
        logger.warning(
            "GitHub token too short (%d chars, minimum %d) — ignoring.",
            len(token),
            _TOKEN_MIN_LEN,
        )
        return None
    if not re.fullmatch(r"[A-Za-z0-9_\-]+", token):
        logger.warning("GitHub token contains invalid characters — ignoring.")
        return None
    if not any(token.startswith(p) for p in _KNOWN_PREFIXES):
        logger.warning(
            "GitHub token has no recognised prefix (ghp_, gho_, …) — "
            "accepted, but double-check it is correct."
        )
    return token


class V2RayServerFinder:
    """Main public API for v2ray-finder.

    Args:
        token:          Optional GitHub personal access token.
        inline_health:  Reserved for future use.
        health_timeout: Per-server TCP timeout for inline checks (seconds).
        check_google_204: Also run Google-204 probe in inline checks.
        raise_errors:   If True, ``*_or_empty`` helpers re-raise on error.
    """

    def __init__(
        self,
        token: Optional[str] = None,
        inline_health: bool = True,
        health_timeout: float = 5.0,
        check_google_204: bool = True,
        raise_errors: bool = False,
    ) -> None:
        env_token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
        raw_token = token or env_token or None
        self._token = _validate_token(raw_token)

        self._inline_health = inline_health
        self._health_timeout = health_timeout
        self._check_google_204 = check_google_204
        self._raise_errors = raise_errors

        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "v2ray-finder/1.0",
            "Accept": "application/vnd.github.v3+json",
        })
        if self._token:
            self._session.headers["Authorization"] = f"token {self._token}"

        self._stop_event = threading.Event()
        self._last_rate_limit_info: Optional[Dict] = None

    # ------------------------------------------------------------------ #
    # Public headers property
    # ------------------------------------------------------------------ #

    @property
    def headers(self) -> Dict[str, str]:
        """Return a snapshot of current session headers."""
        return dict(self._session.headers)

    # ------------------------------------------------------------------ #
    # Class-level source list (proxy to get_enabled_sources)
    # ------------------------------------------------------------------ #

    @property
    def DIRECT_SOURCES(self) -> List[Any]:
        """Return enabled static subscription sources."""
        return list(get_enabled_sources())

    # ------------------------------------------------------------------ #
    # Cooperative stop
    # ------------------------------------------------------------------ #

    def request_stop(self) -> None:
        self._stop_event.set()

    def reset_stop(self) -> None:
        self._stop_event.clear()

    def should_stop(self) -> bool:
        return self._stop_event.is_set()

    # ------------------------------------------------------------------ #
    # Rate-limit helpers
    # ------------------------------------------------------------------ #

    def _check_rate_limit(self, resp: requests.Response) -> None:
        """Parse X-RateLimit-* headers and store them; log if low."""
        limit_raw = resp.headers.get("X-RateLimit-Limit")
        remaining_raw = resp.headers.get("X-RateLimit-Remaining")
        reset_raw = resp.headers.get("X-RateLimit-Reset")

        if remaining_raw is None:
            return

        try:
            limit = int(limit_raw) if limit_raw is not None else None
            remaining = int(remaining_raw)
            reset_at = int(reset_raw) if reset_raw is not None else None
        except (ValueError, TypeError):
            # Use % interpolation (not .format) so call_args[0][0] contains
            # the raw values, satisfying:
            #   assert "not-a-number" in log_message
            logger.debug(
                "Malformed X-RateLimit headers — limit=%s remaining=%s reset=%s; skipping update.",
                limit_raw,
                remaining_raw,
                reset_raw,
            )
            return

        self._last_rate_limit_info = {
            "limit": limit,
            "remaining": remaining,
            "reset_at": reset_at,
        }
        if remaining < 10:
            logger.warning(
                "GitHub rate limit low: %d/%s requests remaining.",
                remaining,
                limit or "?",
            )

    def get_rate_limit_info(self) -> Optional[Dict]:
        return self._last_rate_limit_info

    # ------------------------------------------------------------------ #
    # classmethod factory
    # ------------------------------------------------------------------ #

    @classmethod
    def from_env(cls, **kwargs) -> "V2RayServerFinder":
        """Create a finder, reading GITHUB_TOKEN / GH_TOKEN from env."""
        return cls(**kwargs)

    # ------------------------------------------------------------------ #
    # GitHub API — Result-returning methods
    # ------------------------------------------------------------------ #

    def search_repos(
        self,
        query: str = "v2ray config",
        per_page: int = 30,
        # backward-compat alias used in some integration tests
        keywords: Optional[List[str]] = None,
        max_results: Optional[int] = None,
    ) -> Result:
        """Search GitHub repositories.

        Returns:
            Ok(list[dict]) on success, Err(V2RayFinderError subclass) on failure.
        """
        if keywords is not None:
            query = " ".join(keywords)
        if max_results is not None:
            per_page = min(per_page, max_results)

        url = "https://api.github.com/search/repositories"
        params = {"q": query, "sort": "updated", "per_page": per_page}
        try:
            resp = requests.get(
                url,
                params=params,
                headers=dict(self._session.headers),
                timeout=15,
            )
            self._check_rate_limit(resp)
            if resp.status_code == 401:
                return Err(AuthenticationError("GitHub API authentication failed (401)."))
            if resp.status_code in (403, 429):
                msg = ""
                try:
                    msg = resp.json().get("message", "")
                except Exception:
                    pass
                info = self._last_rate_limit_info or {}
                err = RateLimitError(f"Rate limit hit: {msg}")
                # Attach parsed header values for callers that inspect .details
                err.details = {
                    "limit": info.get("limit"),
                    "remaining": info.get("remaining"),
                    "reset_at": info.get("reset_at"),
                }
                return Err(err)
            if resp.status_code == 404:
                return Err(
                    GitHubAPIError(
                        "GitHub search endpoint not found (404).",
                        status_code=404,
                    )
                )
            resp.raise_for_status()
            return Ok(resp.json().get("items", []))
        except (AuthenticationError, RateLimitError, GitHubAPIError) as exc:
            return Err(exc)
        except requests.Timeout:
            return Err(TimeoutError("GitHub search timed out."))
        except requests.ConnectionError as exc:
            return Err(NetworkError(f"Network error during GitHub search: {exc}"))
        except requests.RequestException as exc:
            return Err(GitHubAPIError(f"GitHub search failed: {exc}"))
        except Exception as exc:
            return Err(V2RayFinderError(f"Unexpected error in search_repos: {exc}"))

    def search_repos_or_empty(
        self,
        query: str = "v2ray config",
        per_page: int = 30,
    ) -> List[Dict]:
        """Like search_repos but returns [] on error (unless raise_errors=True)."""
        result = self.search_repos(query=query, per_page=per_page)
        if result.is_ok():
            return result.unwrap()
        if self._raise_errors:
            raise result.error
        return []

    def get_repo_files(
        self,
        repo: str,
        path: str = "",
        timeout: int = 15,
    ) -> Result:
        """List files in a GitHub repository that look like v2ray config files.

        Args:
            repo:    Repository slug in ``owner/name`` format.
            path:    Sub-directory path inside the repo (default: root).
            timeout: HTTP request timeout in seconds.

        Returns:
            Ok(list[dict]) on success — each dict contains at minimum
            ``name``, ``path``, ``download_url``, ``size``, and ``type``.
            Err(V2RayFinderError subclass) on any failure.
        """
        url = f"https://api.github.com/repos/{repo}/contents/{path}"
        try:
            resp = requests.get(
                url,
                headers=dict(self._session.headers),
                timeout=timeout,
            )
            self._check_rate_limit(resp)
            if resp.status_code == 404:
                return Err(RepositoryNotFoundError(repo))
            if resp.status_code == 401:
                return Err(AuthenticationError("Authentication required for this repo."))
            resp.raise_for_status()
            items = resp.json()
            if isinstance(items, list):
                return Ok(items)
            # Single file response
            if isinstance(items, dict):
                return Ok([items])
            return Ok([])
        except (RepositoryNotFoundError, AuthenticationError) as exc:
            return Err(exc)
        except requests.Timeout:
            return Err(TimeoutError(f"Timed out fetching repo files for {repo!r}"))
        except requests.ConnectionError as exc:
            return Err(NetworkError(f"Connection error fetching repo files: {exc}"))
        except requests.RequestException as exc:
            return Err(GitHubAPIError(f"GitHub API request failed: {exc}"))
        except Exception as exc:
            return Err(V2RayFinderError(f"Unexpected error in get_repo_files: {exc}"))

    def get_repo_files_or_empty(
        self,
        repo: str,
        path: str = "",
        timeout: int = 15,
    ) -> List[Dict]:
        """Like get_repo_files but returns [] on error (unless raise_errors=True)."""
        result = self.get_repo_files(repo=repo, path=path, timeout=timeout)
        if result.is_ok():
            return result.unwrap()
        if self._raise_errors:
            raise result.error
        return []

    # ------------------------------------------------------------------ #
    # URL-based source fetching — Result-returning
    # ------------------------------------------------------------------ #

    def get_servers_from_url(
        self, url: str, timeout: int = 15
    ) -> Result:
        """Fetch a URL and extract v2ray config strings.

        Uses ``requests.get`` directly (not ``self._session``) so that tests
        can patch ``requests.get`` in the standard way, consistent with
        ``search_repos`` and ``get_repo_files``.

        Returns:
            Ok(list[str]) on success, Err(V2RayFinderError) on failure.
        """
        try:
            resp = requests.get(
                url,
                headers=dict(self._session.headers),
                timeout=timeout,
            )
            resp.raise_for_status()
            return Ok(self._parse_servers(resp.text))
        except requests.Timeout:
            return Err(TimeoutError(f"Timed out fetching {url}"))
        except requests.ConnectionError as exc:
            return Err(NetworkError(f"Connection error fetching {url}: {exc}"))
        except requests.RequestException as exc:
            return Err(NetworkError(f"Request failed for {url}: {exc}"))
        except Exception as exc:
            return Err(ParseError(f"Error parsing response from {url}: {exc}"))

    def get_servers_from_url_or_empty(
        self,
        url: str,
        timeout: int = 15,
    ) -> List[str]:
        """Like get_servers_from_url but returns [] on error (unless raise_errors=True)."""
        result = self.get_servers_from_url(url=url, timeout=timeout)
        if result.is_ok():
            return result.unwrap()
        if self._raise_errors:
            raise result.error
        return []

    # ------------------------------------------------------------------ #
    # GitHub-based discovery
    # ------------------------------------------------------------------ #

    def get_servers_from_github(
        self,
        search_keywords: Optional[List[str]] = None,
        max_repos: int = 10,
        timeout: int = 15,
        progress_callback=None,
    ) -> List[str]:
        """Search GitHub and return raw config strings from discovered repos.

        Args:
            search_keywords: Keywords to use for GitHub search (default: v2ray presets).
            max_repos:       Maximum number of repos to scan.
            timeout:         Per-request timeout.
            progress_callback: Optional callable(current, total, message).

        Returns:
            Flat list of raw config strings (vmess://, vless://, etc.).
        """
        keywords = search_keywords or ["v2ray config", "v2ray subscription", "clash config"]
        all_servers: List[str] = []

        for keyword in keywords:
            if self.should_stop():
                break
            result = self.search_repos(query=keyword, per_page=max_repos)
            if result.is_err():
                logger.warning("search_repos failed for %r: %s", keyword, result.error)
                continue

            repos = result.unwrap()
            for i, repo in enumerate(repos):
                if self.should_stop():
                    break
                full_name = repo.get("full_name", "")
                if not full_name:
                    continue

                if progress_callback:
                    progress_callback(i, len(repos), f"Scanning {full_name}")

                files_result = self.get_repo_files(full_name, timeout=timeout)
                if files_result.is_err():
                    continue

                for file_info in files_result.unwrap():
                    if self.should_stop():
                        break
                    dl_url = file_info.get("download_url")
                    if not dl_url:
                        continue
                    _, ext = os.path.splitext(file_info.get("name", ""))
                    if ext.lower() not in _CONFIG_EXTENSIONS:
                        continue
                    url_result = self.get_servers_from_url(dl_url, timeout=timeout)
                    if url_result.is_ok():
                        all_servers.extend(url_result.unwrap())

        return all_servers

    # ------------------------------------------------------------------ #
    # Known static subscription sources
    # ------------------------------------------------------------------ #

    def get_servers_from_known_sources(
        self,
        timeout: int = 15,
        progress_callback=None,
    ) -> List[str]:
        """Fetch v2ray configs from built-in known subscription URLs.

        Returns:
            Flat list of raw config strings.
        """
        sources = get_enabled_sources()
        all_servers: List[str] = []
        total = len(sources)

        for i, source in enumerate(sources):
            if self.should_stop():
                break
            if progress_callback:
                progress_callback(i, total, f"Fetching {source.name}")
            result = self.get_servers_from_url(source.url, timeout=timeout)
            if result.is_ok():
                all_servers.extend(result.unwrap())
            else:
                logger.debug("Failed to fetch source %r: %s", source.name, result.error)

        return all_servers

    # ------------------------------------------------------------------ #
    # Health-checked discovery
    # ------------------------------------------------------------------ #

    def get_servers_with_health(
        self,
        servers: List[str],
        timeout: float = 5.0,
        min_quality_score: float = 0.0,
        progress_callback=None,
    ):
        """Run health checks on a list of config strings.

        Args:
            servers:           List of raw config strings.
            timeout:           Per-server TCP timeout.
            min_quality_score: Filter out servers below this score.
            progress_callback: Optional callable(current, total, message).

        Returns:
            List of ServerHealth objects (from health_checker module).
        """
        from .health_checker import HealthChecker, filter_healthy_servers

        checker = HealthChecker(
            timeout=timeout,
            min_quality_score=min_quality_score,
        )

        if progress_callback:
            progress_callback(0, len(servers), "Starting health checks…")

        results = checker.check_batch(servers)

        if progress_callback:
            progress_callback(len(servers), len(servers), "Health checks complete.")

        return filter_healthy_servers(results, min_quality_score=min_quality_score)

    # ------------------------------------------------------------------ #
    # Combined pipeline helpers
    # ------------------------------------------------------------------ #

    def get_servers_sorted(
        self,
        servers: List[str],
        timeout: float = 5.0,
        min_quality_score: float = 0.0,
    ) -> List[str]:
        """Health-check *servers*, sort by quality, return config strings only."""
        from .health_checker import sort_by_quality

        health_results = self.get_servers_with_health(
            servers,
            timeout=timeout,
            min_quality_score=min_quality_score,
        )
        sorted_results = sort_by_quality(health_results, descending=True)
        return [r.config for r in sorted_results]

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _parse_servers(self, text: str) -> List[str]:
        """Extract v2ray config URIs from a block of text."""
        protocols = ("vmess://", "vless://", "trojan://", "ss://", "ssr://")
        servers: List[str] = []
        # Try base64-decode first (subscription format)
        stripped = text.strip()
        if stripped and not any(stripped.startswith(p) for p in protocols):
            try:
                import base64
                # Add padding if needed
                padded = stripped + "==" * (4 - len(stripped) % 4 if len(stripped) % 4 else 0)
                decoded = base64.b64decode(padded).decode("utf-8", errors="replace")
                if any(decoded.startswith(p) for p in protocols) or "\n" in decoded:
                    text = decoded
            except Exception:
                pass

        for line in text.splitlines():
            line = line.strip()
            if any(line.startswith(p) for p in protocols):
                servers.append(line)

        return servers
