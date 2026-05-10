"""Core orchestrator for v2ray-finder.

V2RayServerFinder is the single public API:
  - get_all_servers()           — fetch + deduplicate from all sources
  - get_servers_with_health()   — fetch + inline TCP/Google-204 health check
  - get_servers_with_real_health() — fetch + real xray connectivity check
  - get_rate_limit_info()       — GitHub API quota
  - request_stop() / reset_stop() / should_stop() — cooperative cancellation

Inline health check (the original feature request)
---------------------------------------------------
Every server is checked **immediately after discovery** so that dead configs
never accumulate in the pipeline.  The flow is:

  source → parse → [inline check_server()] → yield healthy configs

This is different from the old batch-check-at-the-end approach.
"""

from __future__ import annotations

import base64
import importlib
import logging
import re
import threading
import time
from typing import Dict, Generator, Iterator, List, Optional, Set

import requests

from .async_fetcher import fetch_urls_concurrently
from .exceptions import AuthenticationError, GitHubAPIError, RateLimitError
from .health_checker import (
    check_server,
    check_servers_batch,
    health_result_to_dict,
)
from .sources import GITHUB_TOPICS, STATIC_SOURCES, SourceEntry
from .xray_connectivity import (
    RealHealthResult,
    check_real_connectivity_batch,
    real_health_to_dict,
)

logger = logging.getLogger(__name__)

# Compiled once — matches any known v2ray/xray config scheme
_CONFIG_RE = re.compile(
    r"(vmess|vless|trojan|ss|ssr)://[A-Za-z0-9+/=@:._\-?#%&]+",
    re.IGNORECASE,
)

_GITHUB_API = "https://api.github.com"
_DEFAULT_TIMEOUT = 10.0


class V2RayServerFinder:
    """Main public API for v2ray-finder.

    Args:
        token:          Optional GitHub personal access token.  Raises the
                        rate limit from 60 to 5000 requests/hour.
        inline_health:  If True, health-check every server immediately after
                        discovery.  Default True (the desired behaviour).
        health_timeout: Per-server TCP timeout for inline checks (seconds).
        check_google_204: Also run Google-204 probe in inline checks.
    """

    def __init__(
        self,
        token: Optional[str] = None,
        inline_health: bool = True,
        health_timeout: float = 5.0,
        check_google_204: bool = True,
    ) -> None:
        self._token = token
        self._inline_health = inline_health
        self._health_timeout = health_timeout
        self._check_google_204 = check_google_204

        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "v2ray-finder/1.0"})
        if token:
            self._session.headers["Authorization"] = f"token {token}"

        self._stop_event = threading.Event()
        self._rate_limit_info: Optional[Dict] = None

    # ------------------------------------------------------------------ #
    # Cooperative stop
    # ------------------------------------------------------------------ #

    def request_stop(self) -> None:
        """Signal the finder to stop after the current unit of work."""
        self._stop_event.set()

    def reset_stop(self) -> None:
        """Clear the stop flag (call before starting a new operation)."""
        self._stop_event.clear()

    def should_stop(self) -> bool:
        """Return True if a stop has been requested."""
        return self._stop_event.is_set()

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _get_headers(self) -> Dict[str, str]:
        h: Dict[str, str] = {"User-Agent": "v2ray-finder/1.0"}
        if self._token:
            h["Authorization"] = f"token {self._token}"
        return h

    def _parse_configs_from_text(self, text: str) -> List[str]:
        """Extract all config URIs from arbitrary text (plain or base64)."""
        configs: List[str] = []

        # Direct extraction
        found = _CONFIG_RE.findall(text)
        configs.extend(found)

        # Try base64 decode if text looks encoded
        stripped = text.strip()
        if not found and len(stripped) > 20 and "\n" not in stripped[:100]:
            try:
                padded = stripped + "==" * (4 - len(stripped) % 4)
                decoded = base64.b64decode(padded).decode("utf-8", errors="replace")
                configs.extend(_CONFIG_RE.findall(decoded))
            except Exception:
                pass

        # Multi-line base64 (each line separately)
        for line in text.splitlines():
            line = line.strip()
            if not line or _CONFIG_RE.match(line):
                continue
            try:
                padded = line + "==" * (4 - len(line) % 4)
                decoded = base64.b64decode(padded).decode("utf-8", errors="replace")
                found_in_line = _CONFIG_RE.findall(decoded)
                configs.extend(found_in_line)
            except Exception:
                pass

        return list(dict.fromkeys(configs))  # deduplicate, preserve order

    def _fetch_url(self, url: str) -> Optional[str]:
        """Fetch a single URL, update rate-limit info, return text or None."""
        try:
            resp = self._session.get(url, timeout=_DEFAULT_TIMEOUT)
            self._update_rate_limit(resp)
            if resp.status_code == 200:
                return resp.text
            if resp.status_code in (403, 429):
                raise RateLimitError(
                    "GitHub rate limit hit",
                    details={
                        "limit": resp.headers.get("X-RateLimit-Limit"),
                        "remaining": resp.headers.get("X-RateLimit-Remaining", 0),
                        "reset_at": resp.headers.get("X-RateLimit-Reset"),
                    },
                )
            logger.debug("Non-200 response %s from %s", resp.status_code, url)
            return None
        except RateLimitError:
            raise
        except Exception as exc:
            logger.debug("Fetch failed for %s: %s", url, exc)
            return None

    def _update_rate_limit(self, resp: requests.Response) -> None:
        remaining = resp.headers.get("X-RateLimit-Remaining")
        limit = resp.headers.get("X-RateLimit-Limit")
        reset = resp.headers.get("X-RateLimit-Reset")
        if remaining is not None:
            self._rate_limit_info = {
                "remaining": int(remaining),
                "limit": int(limit) if limit else None,
                "reset": int(reset) if reset else None,
            }

    def _inline_check(self, config: str) -> Optional[str]:
        """Run inline health check.  Return config if healthy/degraded, else None."""
        if not self._inline_health:
            return config
        result = check_server(
            config,
            timeout=self._health_timeout,
            check_google_204=self._check_google_204,
        )
        if result.health_status in ("healthy", "degraded"):
            return config
        logger.debug(
            "Inline check failed for %s (%s): %s",
            config[:60],
            result.health_status,
            result.error,
        )
        return None

    # ------------------------------------------------------------------ #
    # GitHub search helpers
    # ------------------------------------------------------------------ #

    def _search_github_repos(self, topic: str) -> List[str]:
        """Return raw-content URLs for README/subscription files in repos
        tagged with *topic*.
        """
        url = (
            f"{_GITHUB_API}/search/repositories"
            f"?q=topic:{topic}&sort=stars&per_page=10"
        )
        try:
            resp = self._session.get(url, timeout=_DEFAULT_TIMEOUT)
            self._update_rate_limit(resp)
            if resp.status_code != 200:
                return []
            data = resp.json()
        except Exception as exc:
            logger.debug("GitHub search failed for topic %s: %s", topic, exc)
            return []

        raw_urls: List[str] = []
        for item in data.get("items", []):
            owner = item["owner"]["login"]
            repo = item["name"]
            default_branch = item.get("default_branch", "main")
            for filename in ("sub.txt", "v2ray.txt", "configs.txt", "all.txt",
                             "Sub.txt", "README.md"):
                raw_urls.append(
                    f"https://raw.githubusercontent.com/{owner}/{repo}/"
                    f"{default_branch}/{filename}"
                )
        return raw_urls

    # ------------------------------------------------------------------ #
    # Public API — fetching
    # ------------------------------------------------------------------ #

    def get_all_servers(
        self,
        use_github_search: bool = False,
    ) -> List[str]:
        """Fetch all servers from static sources (+ GitHub search if requested).

        Inline health-checks each server immediately after discovery
        (when ``inline_health=True``, the default).

        Returns:
            Deduplicated list of healthy config strings.
        """
        seen: Set[str] = set()
        result: List[str] = []

        # --- static sources (concurrent) ---
        urls = [s.url for s in STATIC_SOURCES if s.enabled]
        logger.info("Fetching %d static sources...", len(urls))
        fetch_results = fetch_urls_concurrently(urls, timeout=_DEFAULT_TIMEOUT)

        for fr in fetch_results:
            if self.should_stop():
                break
            if not fr.success or not fr.content:
                continue
            for cfg in self._parse_configs_from_text(fr.content):
                if cfg in seen:
                    continue
                seen.add(cfg)
                # Inline health check immediately after discovery
                checked = self._inline_check(cfg)
                if checked:
                    result.append(checked)

        # --- GitHub search ---
        if use_github_search and not self.should_stop():
            logger.info("Running GitHub topic search...")
            extra_urls: List[str] = []
            for topic in GITHUB_TOPICS:
                if self.should_stop():
                    break
                extra_urls.extend(self._search_github_repos(topic))

            if extra_urls:
                extra_results = fetch_urls_concurrently(
                    extra_urls, timeout=_DEFAULT_TIMEOUT
                )
                for fr in extra_results:
                    if self.should_stop():
                        break
                    if not fr.success or not fr.content:
                        continue
                    for cfg in self._parse_configs_from_text(fr.content):
                        if cfg in seen:
                            continue
                        seen.add(cfg)
                        checked = self._inline_check(cfg)
                        if checked:
                            result.append(checked)

        logger.info(
            "Discovery complete: %d healthy servers from %d candidates",
            len(result),
            len(seen),
        )
        return result

    def get_servers_with_health(
        self,
        use_github_search: bool = False,
        check_health: bool = True,
        health_timeout: float = 5.0,
        min_quality_score: float = 0.0,
        filter_unhealthy: bool = False,
    ) -> List[Dict]:
        """Fetch servers and return rich health dicts.

        Unlike get_all_servers(), this returns dicts with TCP latency,
        quality score, and health_status fields.

        Note: inline checking is still applied during discovery; this
        method additionally runs a *batch* quality-score pass on the
        survivors.

        The health_checker module is imported dynamically so that tests can
        swap it via ``patch.dict(sys.modules, {'v2ray_finder.health_checker': mock})``
        without being defeated by a module-level ``from .health_checker import ...``
        that already bound the names at import time.
        """
        # Temporarily disable inline health so batch check runs on all
        orig = self._inline_health
        self._inline_health = False

        try:
            raw = self.get_all_servers(use_github_search=use_github_search)
        finally:
            self._inline_health = orig

        if not check_health:
            return [
                {
                    "config": c,
                    "protocol": c.split("://")[0] if "://" in c else "unknown",
                    "health_checked": False,
                }
                for c in raw
            ]

        # Dynamic import so tests can mock via patch.dict(sys.modules, ...)
        try:
            hc = importlib.import_module("v2ray_finder.health_checker")
        except ImportError:
            logger.warning(
                "health_checker module unavailable — returning servers without health data"
            )
            return [
                {
                    "config": c,
                    "protocol": c.split("://")[0] if "://" in c else "unknown",
                    "health_checked": False,
                }
                for c in raw
            ]

        logger.info("Running batch health check on %d servers...", len(raw))

        checker = hc.HealthChecker(
            timeout=health_timeout,
            check_google_204=self._check_google_204,
        )
        health_results = checker.check_servers(raw)

        if filter_unhealthy or min_quality_score > 0:
            health_results = hc.filter_healthy_servers(
                health_results,
                min_quality_score=min_quality_score,
                exclude_unreachable=filter_unhealthy,
            )

        return [hc.health_result_to_dict(r) for r in health_results]

    def get_servers_with_real_health(
        self,
        servers: Optional[List[str]] = None,
        use_github_search: bool = False,
        max_workers: int = 5,
        timeout: float = 10.0,
        binary_path: Optional[str] = None,
        auto_download: bool = True,
    ) -> List[Dict]:
        """Fetch servers and run real xray connectivity checks.

        Spins up xray for each server, routes an HTTP request through it
        to Google's generate_204.  A 204 response = truly working proxy.

        Args:
            servers:         Pre-fetched list of config strings.  If None,
                             calls get_all_servers() first.
            use_github_search: Passed to get_all_servers() if servers is None.
            max_workers:     Parallel xray instances (keep small: 3-8).
            timeout:         HTTP probe timeout per server.
            binary_path:     Explicit xray binary path.
            auto_download:   Download xray binary if not found.

        Returns:
            List of dicts with keys: config, protocol, reachable,
            google_204_ok, latency_ms, error.
        """
        if servers is None:
            servers = self.get_all_servers(use_github_search=use_github_search)

        logger.info(
            "Running xray real-connectivity check on %d servers "
            "(max_workers=%d)...",
            len(servers),
            max_workers,
        )
        results = check_real_connectivity_batch(
            servers,
            max_workers=max_workers,
            timeout=timeout,
            binary_path=binary_path,
            auto_download=auto_download,
        )
        return [real_health_to_dict(r) for r in results]

    # ------------------------------------------------------------------ #
    # Rate limit info
    # ------------------------------------------------------------------ #

    def get_rate_limit_info(self) -> Optional[Dict]:
        """Return last-seen GitHub rate-limit info (or None if no API call yet)."""
        return self._rate_limit_info
