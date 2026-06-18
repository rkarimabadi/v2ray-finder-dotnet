"""Pipeline orchestrator for v2ray-finder.

Provides a single :class:`Pipeline` entry point that owns the full
discovery → fetch → dedup → health → score → output chain.

CLI, Rich TUI, and GUI callers should instantiate ``Pipeline`` and call
``Pipeline.run()`` instead of re-implementing the sequence themselves.

Progress callback protocol
--------------------------
All progress callbacks follow the signature::

    callback(stage: str, current: int, total: int, message: str) -> None

``stage`` is one of ``"fetch"``, ``"health"``, ``"score"``.

Cancellation
------------
Pass a :class:`StopController` (or any ``threading.Event``) as
``stop_event`` to ``Pipeline.run()``.  The pipeline checks the event
before every source fetch and every health-check batch.

Async fetch
-----------
All HTTP fetching is delegated to :class:`~async_fetcher.AsyncFetcher`
which automatically uses aiohttp (preferred), httpx, or falls back to
sync ``requests`` if neither async library is available (V1-D1).
Connection pooling and retry/backoff are handled entirely by
``AsyncFetcher``.

GitHub rate-limit handling (V1-C3)
-----------------------------------
Sources hosted on ``api.github.com`` or ``raw.githubusercontent.com``
are detected after fetch via ``FetchResult.status_code``.  When a
403/429 response is returned, all remaining GitHub-host sources are
skipped for this run.  Pass ``github_token`` to :class:`Pipeline` to
attach ``Authorization: token <tok>`` to GitHub requests only.

Memory caps (V1-C4)
--------------------
``max_configs_per_source`` (default 5 000) truncates each source's
parsed config list before they are aggregated.  ``max_total_configs``
(default 50 000) truncates the global list after structural dedup but
before health checks.  Both caps log how many entries were dropped.

Source attribution
------------------
During fetch each config string is mapped to the source URL it came
from (highest-trust source wins on collision) via
``_build_config_source_map()``.  In ``_run_health`` every health-result
dict receives the correct ``source_url``, ``source_trust``, and
``overlap_ratio`` for that specific config.

Example
-------
::

    from v2ray_finder.pipeline import Pipeline, StopController

    stop = StopController()
    pipeline = Pipeline(check_health=True, check_google_204=True)
    result = pipeline.run(stop_event=stop.event)
    for score in result.scores[:10]:
        print(score.grade, score.config[:80])
"""

from __future__ import annotations

import logging
import re
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set
from urllib.parse import urlparse

from .async_fetcher import AsyncFetcher, FetchResult
from .normalizer import deduplicate_across_sources
from .scorer import ServerScore, score_servers, sort_by_quality
from .sources import SourceEntry, SourceTrust, get_enabled_sources

logger = logging.getLogger(__name__)

# Regex shared across fetch paths
_PROTO_RE = re.compile(
    r"(?:vmess|vless|trojan|ss|ssr)://[A-Za-z0-9+/=_\-@:.?&#%]+",
    re.IGNORECASE,
)

# GitHub hosts that require rate-limit awareness
_GITHUB_HOSTS: frozenset = frozenset({
    "api.github.com",
    "raw.githubusercontent.com",
})

# Default concurrency cap for async source fetches
_DEFAULT_FETCH_CONCURRENCY = 10

# Default memory caps (V1-C4)
_DEFAULT_MAX_CONFIGS_PER_SOURCE: int = 5_000
_DEFAULT_MAX_TOTAL_CONFIGS: int      = 50_000

# Type alias for the progress callback.
ProgressCallback = Optional[Callable[[str, int, int, str], None]]


def _is_github_url(url: str) -> bool:
    """Return True if *url* is hosted on a known GitHub API/raw domain."""
    try:
        return urlparse(url).hostname in _GITHUB_HOSTS
    except Exception:
        return False


def _parse_configs(text: str) -> List[str]:
    """Extract deduplicated proxy URIs from raw subscription text."""
    return list(dict.fromkeys(_PROTO_RE.findall(text)))


# ---------------------------------------------------------------------------
# StopController
# ---------------------------------------------------------------------------

class StopController:
    """Thin wrapper around :class:`threading.Event` for pipeline cancellation."""

    def __init__(self) -> None:
        self.event: threading.Event = threading.Event()

    def stop(self) -> None:
        self.event.set()

    def reset(self) -> None:
        self.event.clear()

    def is_set(self) -> bool:
        return self.event.is_set()


# ---------------------------------------------------------------------------
# PipelineResult
# ---------------------------------------------------------------------------

@dataclass
class PipelineResult:
    """Container for the output of a completed pipeline run."""

    configs:      List[str]                  = field(default_factory=list)
    health_dicts: List[Dict[str, Any]]        = field(default_factory=list)
    scores:       List[ServerScore]           = field(default_factory=list)
    overlap_map:  Dict[str, float]            = field(default_factory=dict)
    stats:        Dict[str, Any]              = field(default_factory=dict)

    @property
    def top_configs(self) -> List[str]:
        return [s.config for s in self.scores]


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

class Pipeline:
    """Full discovery → fetch → dedup → health → score pipeline.

    Parameters
    ----------
    sources:
        List of :class:`~sources.SourceEntry` objects to fetch.
        Defaults to all enabled sources from :func:`~sources.get_enabled_sources`.
    check_health:
        Run TCP health checks (Layer 1).  Default: ``True``.
    check_http_probe:
        Run direct HTTP probe (Layer 2).  Default: ``False``.
    check_google_204:
        Run xray SOCKS5 / Google 204 probe (Layer 3).  Default: ``False``.
    timeout:
        Per-server probe timeout in seconds.  Default: ``5.0``.
    min_quality_score:
        Exclude servers scoring below this threshold.  Default: ``0.0``.
    health_batch_size:
        Number of servers per async health-check batch.  Default: ``100``.
    fetch_timeout:
        HTTP timeout for source fetches in seconds.  Default: ``15``.
    fetch_concurrency:
        Maximum number of concurrent async source fetches.  Default: ``10``.
    limit:
        Cap the number of configs returned after dedup.  Default: ``None``.
    binary_path:
        Explicit path to the xray binary (Layer 3 only).
    github_token:
        Optional GitHub personal-access token.  When provided, an
        ``Authorization: token <tok>`` header is added **only** to
        requests targeting ``api.github.com`` or
        ``raw.githubusercontent.com``.
    max_configs_per_source:
        Maximum configs to keep from a single source after parsing.
        Excess entries are dropped and logged.  Default: ``5_000``.
    max_total_configs:
        Maximum configs to keep across all sources after structural
        dedup, before health checks.  Default: ``50_000``.
        Pass ``None`` to disable the global cap.
    """

    def __init__(
        self,
        sources: Optional[List[SourceEntry]] = None,
        check_health: bool = True,
        check_http_probe: bool = False,
        check_google_204: bool = False,
        timeout: float = 5.0,
        min_quality_score: float = 0.0,
        health_batch_size: int = 100,
        fetch_timeout: int = 15,
        fetch_concurrency: int = _DEFAULT_FETCH_CONCURRENCY,
        limit: Optional[int] = None,
        binary_path: Optional[str] = None,
        github_token: Optional[str] = None,
        max_configs_per_source: int = _DEFAULT_MAX_CONFIGS_PER_SOURCE,
        max_total_configs: Optional[int] = _DEFAULT_MAX_TOTAL_CONFIGS,
    ) -> None:
        self.sources                = sources or get_enabled_sources()
        self.check_health           = check_health
        self.check_http_probe       = check_http_probe
        self.check_google_204       = check_google_204
        self.timeout                = timeout
        self.min_quality_score      = min_quality_score
        self.health_batch_size      = health_batch_size
        self.fetch_timeout          = fetch_timeout
        self.fetch_concurrency      = fetch_concurrency
        self.limit                  = limit
        self.binary_path            = binary_path
        self.github_token           = github_token
        self.max_configs_per_source = max_configs_per_source
        self.max_total_configs      = max_total_configs

        self._source_trust_map: Dict[str, int] = {
            s.url: s.trust.value for s in self.sources
        }

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(
        self,
        stop_event: Optional[threading.Event] = None,
        progress_callback: ProgressCallback = None,
    ) -> PipelineResult:
        """Execute the full pipeline and return a :class:`PipelineResult`."""
        _stop = stop_event or threading.Event()
        result = PipelineResult()
        stats: Dict[str, Any] = {
            "fetched": 0, "deduped": 0, "healthy": 0, "scored": 0,
            "dropped_per_source": 0, "dropped_global": 0,
        }

        # ── Stage 1: Fetch ──────────────────────────────────────────────
        servers_by_source = self._fetch_all(_stop, progress_callback)

        # Apply per-source cap (V1-C4)
        for url in list(servers_by_source.keys()):
            full = servers_by_source[url]
            if len(full) > self.max_configs_per_source:
                dropped = len(full) - self.max_configs_per_source
                servers_by_source[url] = full[: self.max_configs_per_source]
                stats["dropped_per_source"] += dropped
                logger.warning(
                    "[pipeline] %s: capped at %d configs (%d dropped).",
                    url, self.max_configs_per_source, dropped,
                )

        stats["fetched"] = sum(len(v) for v in servers_by_source.values())
        if _stop.is_set():
            result.stats = stats
            return result

        # ── Stage 2: Structural dedup ────────────────────────────────────
        configs, overlap_map = deduplicate_across_sources(servers_by_source)

        # Apply global post-dedup cap (V1-C4)
        if self.max_total_configs is not None and len(configs) > self.max_total_configs:
            dropped_global = len(configs) - self.max_total_configs
            configs = configs[: self.max_total_configs]
            stats["dropped_global"] = dropped_global
            logger.warning(
                "[pipeline] Global cap: kept %d configs, dropped %d after dedup.",
                self.max_total_configs, dropped_global,
            )

        if self.limit:
            configs = configs[: self.limit]

        stats["deduped"]   = len(configs)
        result.configs     = configs
        result.overlap_map = overlap_map
        logger.info(
            "[pipeline] Fetch complete: %d raw → %d unique configs.",
            stats["fetched"], stats["deduped"],
        )
        if _stop.is_set():
            result.stats = stats
            return result

        config_source_map = self._build_config_source_map(servers_by_source)

        # ── Stage 3: Health checks ──────────────────────────────────────
        if not self.check_health:
            result.health_dicts = [
                self._make_unchecked_dict(c, config_source_map, overlap_map)
                for c in configs
            ]
        else:
            result.health_dicts = self._run_health(
                configs, config_source_map, overlap_map, _stop, progress_callback
            )
        stats["healthy"] = len(result.health_dicts)
        if _stop.is_set():
            result.stats = stats
            return result

        # ── Stage 4: Score ─────────────────────────────────────────────
        self._emit(progress_callback, "score", 0, 1, "Scoring servers…")
        result.scores = score_servers(
            result.health_dicts,
            overlap_map=overlap_map,
            descending=True,
        )
        stats["scored"] = len(result.scores)
        self._emit(progress_callback, "score", 1, 1, "Scoring complete.")
        logger.info(
            "[pipeline] Pipeline complete: %d servers scored.",
            stats["scored"],
        )

        result.stats = stats
        return result

    # ------------------------------------------------------------------
    # Source attribution helpers  (V1-C1)
    # ------------------------------------------------------------------

    def _build_config_source_map(
        self,
        servers_by_source: Dict[str, List[str]],
    ) -> Dict[str, str]:
        """Return config string → source URL mapping (highest-trust wins)."""
        config_source: Dict[str, str] = {}
        sorted_sources = sorted(
            servers_by_source.keys(),
            key=lambda url: self._source_trust_map.get(url, 1),
            reverse=False,
        )
        for url in sorted_sources:
            for cfg in servers_by_source[url]:
                config_source[cfg] = url
        return config_source

    def _make_unchecked_dict(
        self,
        config: str,
        config_source_map: Dict[str, str],
        overlap_map: Dict[str, float],
    ) -> Dict[str, Any]:
        src_url   = config_source_map.get(config, "")
        src_trust = self._source_trust_map.get(src_url, 1)
        return {
            "config":         config,
            "health_checked": False,
            "source_url":     src_url,
            "source_trust":   src_trust,
            "overlap_ratio":  overlap_map.get(src_url, 0.0),
        }

    # ------------------------------------------------------------------
    # Stage 1: Fetch  (V1-D1 + V1-C3)
    # ------------------------------------------------------------------

    def _fetch_all(
        self,
        stop_event: threading.Event,
        progress_callback: ProgressCallback,
    ) -> Dict[str, List[str]]:
        """Fetch all sources, with GitHub rate-limit handling (V1-C3)."""
        github_urls     = [s.url for s in self.sources if _is_github_url(s.url)]
        non_github_urls = [s.url for s in self.sources if not _is_github_url(s.url)]
        total           = len(self.sources)

        self._emit(progress_callback, "fetch", 0, total, "Starting fetch…")

        base_headers   = {"User-Agent": "v2ray-finder/1.0"}
        github_headers = dict(base_headers)
        if self.github_token:
            github_headers["Authorization"] = f"token {self.github_token}"

        servers_by_source: Dict[str, List[str]] = {}
        completed = 0

        if non_github_urls and not stop_event.is_set():
            fetcher = AsyncFetcher(
                max_concurrent=self.fetch_concurrency,
                timeout=float(self.fetch_timeout),
                headers=base_headers,
            )
            for fr in fetcher.fetch_many(non_github_urls):
                if stop_event.is_set():
                    break
                self._process_fetch_result(fr, servers_by_source)
                completed += 1
                self._emit(
                    progress_callback, "fetch", completed, total,
                    f"Fetched {completed}/{total} sources…",
                )

        if github_urls and not stop_event.is_set():
            github_fetcher = AsyncFetcher(
                max_concurrent=min(self.fetch_concurrency, 5),
                timeout=float(self.fetch_timeout),
                headers=github_headers,
            )
            github_rate_limited = False
            for fr in github_fetcher.fetch_many(github_urls):
                if stop_event.is_set():
                    break
                if github_rate_limited:
                    logger.debug(
                        "[pipeline] Skipping %s (GitHub rate-limited).", fr.url
                    )
                elif fr.status_code in (403, 429):
                    logger.warning(
                        "[pipeline] GitHub rate limit hit on %s (HTTP %d). "
                        "Skipping remaining GitHub sources. "
                        "Pass github_token= to Pipeline to raise the limit.",
                        fr.url, fr.status_code,
                    )
                    github_rate_limited = True
                else:
                    self._process_fetch_result(fr, servers_by_source)
                completed += 1
                self._emit(
                    progress_callback, "fetch", completed, total,
                    f"Fetched {completed}/{total} sources…",
                )

        self._emit(progress_callback, "fetch", total, total, "Fetch complete.")
        return servers_by_source

    @staticmethod
    def _process_fetch_result(
        fr: FetchResult,
        servers_by_source: Dict[str, List[str]],
    ) -> None:
        if fr.success and fr.content:
            parsed = _parse_configs(fr.content)
            if parsed:
                servers_by_source[fr.url] = parsed
                logger.debug("[pipeline] %s: %d configs.", fr.url, len(parsed))
        else:
            logger.warning("[pipeline] %s: fetch failed — %s.", fr.url, fr.error)

    # ------------------------------------------------------------------
    # Stage 3: Health
    # ------------------------------------------------------------------

    def _run_health(
        self,
        configs: List[str],
        config_source_map: Dict[str, str],
        overlap_map: Dict[str, float],
        stop_event: threading.Event,
        progress_callback: ProgressCallback,
    ) -> List[Dict[str, Any]]:
        from .health_checker import HealthChecker, filter_healthy_servers

        checker = HealthChecker(
            timeout=self.timeout,
            min_quality_score=self.min_quality_score,
            check_http_probe=self.check_http_probe,
            check_google_204=self.check_google_204,
            binary_path=self.binary_path,
        )

        total     = len(configs)
        all_health: list = []

        for batch_start in range(0, total, self.health_batch_size):
            if stop_event.is_set():
                break
            batch = configs[batch_start: batch_start + self.health_batch_size]
            self._emit(
                progress_callback, "health",
                batch_start, total,
                f"Health checking "
                f"{batch_start + 1}–{min(batch_start + self.health_batch_size, total)}…",
            )
            try:
                all_health.extend(checker.check_batch(batch))
            except Exception as exc:
                logger.warning("[pipeline] Health batch error: %s", exc)

        self._emit(progress_callback, "health", total, total, "Health checks complete.")

        healthy = filter_healthy_servers(
            all_health, min_quality_score=self.min_quality_score
        )

        result_dicts: List[Dict[str, Any]] = []
        for h in healthy:
            src_url   = config_source_map.get(h.config, "")
            src_trust = self._source_trust_map.get(src_url, 1)
            result_dicts.append({
                "config":         h.config,
                "protocol":       h.protocol,
                "tcp_ok":         h.tcp_ok,
                "http_ok":        h.http_probe_ok,
                "google_204_ok":  h.google_204_ok,
                "latency_ms":     h.latency_ms,
                "health_checked": True,
                "source_url":     src_url,
                "source_trust":   src_trust,
                "overlap_ratio":  overlap_map.get(src_url, 0.0),
            })
        return result_dicts

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    @staticmethod
    def _emit(
        cb: ProgressCallback,
        stage: str,
        current: int,
        total: int,
        message: str,
    ) -> None:
        if cb is not None:
            try:
                cb(stage, current, total, message)
            except Exception:
                pass
