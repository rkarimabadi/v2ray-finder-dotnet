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
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from .normalizer import deduplicate_across_sources
from .scorer import ServerScore, score_servers, sort_by_quality
from .sources import SourceEntry, SourceTrust, get_enabled_sources

logger = logging.getLogger(__name__)

# Concurrency cap for Layer-3 xray probes (one xray process per slot).
_LAYER3_MAX_CONCURRENT = 5

# Type alias for the progress callback.
ProgressCallback = Optional[Callable[[str, int, int, str], None]]


# ---------------------------------------------------------------------------
# StopController
# ---------------------------------------------------------------------------

class StopController:
    """Thin wrapper around :class:`threading.Event` for pipeline cancellation.

    CLI ``KeyboardInterrupt`` handlers and GUI worker threads both call
    ``controller.stop()``.  The pipeline polls ``controller.event``.

    Usage::

        ctrl = StopController()
        # In a signal handler or GUI button:
        ctrl.stop()
        # In the pipeline:
        if ctrl.is_set():
            break
    """

    def __init__(self) -> None:
        self.event: threading.Event = threading.Event()

    def stop(self) -> None:
        """Signal the pipeline to stop."""
        self.event.set()

    def reset(self) -> None:
        """Clear the stop signal (e.g. before a fresh run)."""
        self.event.clear()

    def is_set(self) -> bool:
        """Return True if stop has been requested."""
        return self.event.is_set()


# ---------------------------------------------------------------------------
# PipelineResult
# ---------------------------------------------------------------------------

@dataclass
class PipelineResult:
    """Container for the output of a completed pipeline run.

    Attributes:
        configs:      Structurally deduplicated raw config strings (unsorted).
        health_dicts: Health-check result dicts (one per healthy server).
        scores:       Scored + sorted :class:`~scorer.ServerScore` objects.
        overlap_map:  Per-source overlap ratios from
                      :func:`~normalizer.deduplicate_across_sources`.
        stats:        Miscellaneous counters (fetched, deduped, healthy, …).
    """

    configs:      List[str]                  = field(default_factory=list)
    health_dicts: List[Dict[str, Any]]        = field(default_factory=list)
    scores:       List[ServerScore]           = field(default_factory=list)
    overlap_map:  Dict[str, float]            = field(default_factory=dict)
    stats:        Dict[str, int]              = field(default_factory=dict)

    @property
    def top_configs(self) -> List[str]:
        """Return config strings sorted by score (best first)."""
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
        HTTP timeout for source fetches.  Default: ``15``.
    limit:
        Cap the number of configs returned after dedup.  Default: ``None``.
    binary_path:
        Explicit path to the xray binary (Layer 3 only).
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
        limit: Optional[int] = None,
        binary_path: Optional[str] = None,
    ) -> None:
        self.sources         = sources or get_enabled_sources()
        self.check_health    = check_health
        self.check_http_probe = check_http_probe
        self.check_google_204 = check_google_204
        self.timeout         = timeout
        self.min_quality_score = min_quality_score
        self.health_batch_size = health_batch_size
        self.fetch_timeout   = fetch_timeout
        self.limit           = limit
        self.binary_path     = binary_path

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(
        self,
        stop_event: Optional[threading.Event] = None,
        progress_callback: ProgressCallback = None,
    ) -> PipelineResult:
        """Execute the full pipeline and return a :class:`PipelineResult`.

        Args:
            stop_event:        A :class:`threading.Event` that, when set,
                               causes the pipeline to exit at the next
                               cancellation checkpoint.
            progress_callback: Optional callback with signature
                               ``(stage, current, total, message)``.

        Returns:
            :class:`PipelineResult` with configs, scores, and stats.
        """
        _stop = stop_event or threading.Event()  # no-op event if not provided
        result = PipelineResult()
        stats: Dict[str, int] = {"fetched": 0, "deduped": 0, "healthy": 0, "scored": 0}

        # ── Stage 1: Fetch ──────────────────────────────────────────────────
        servers_by_source = self._fetch_all(_stop, progress_callback)
        stats["fetched"] = sum(len(v) for v in servers_by_source.values())
        if _stop.is_set():
            result.stats = stats
            return result

        # ── Stage 2: Structural dedup ───────────────────────────────────────
        configs, overlap_map = deduplicate_across_sources(servers_by_source)
        if self.limit:
            configs = configs[: self.limit]
        stats["deduped"] = len(configs)
        result.configs   = configs
        result.overlap_map = overlap_map
        logger.info(
            "[pipeline] Fetch complete: %d raw → %d unique configs.",
            stats["fetched"], stats["deduped"],
        )
        if _stop.is_set():
            result.stats = stats
            return result

        # ── Stage 3: Health checks ──────────────────────────────────────────
        if not self.check_health:
            result.health_dicts = [{"config": c, "health_checked": False} for c in configs]
        else:
            result.health_dicts = self._run_health(
                configs, overlap_map, _stop, progress_callback
            )
        stats["healthy"] = len(result.health_dicts)
        if _stop.is_set():
            result.stats = stats
            return result

        # ── Stage 4: Score ──────────────────────────────────────────────────
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
    # Stage helpers
    # ------------------------------------------------------------------

    def _fetch_all(
        self,
        stop_event: threading.Event,
        progress_callback: ProgressCallback,
    ) -> Dict[str, List[str]]:
        """Fetch all enabled sources and return {source_url: [configs]}."""
        import re
        import requests as _requests

        _PROTO_RE = re.compile(
            r"(?:vmess|vless|trojan|ss|ssr)://[A-Za-z0-9+/=_\-@:.?&#%]+",
            re.IGNORECASE,
        )

        def _parse(text: str) -> List[str]:
            return list(dict.fromkeys(_PROTO_RE.findall(text)))

        servers_by_source: Dict[str, List[str]] = {}
        total = len(self.sources)

        for i, source in enumerate(self.sources):
            if stop_event.is_set():
                break
            self._emit(
                progress_callback, "fetch", i, total,
                f"Fetching {source.label}…",
            )
            try:
                resp = _requests.get(
                    source.url,
                    timeout=self.fetch_timeout,
                    headers={"User-Agent": "v2ray-finder/1.0"},
                )
                if resp.status_code == 200:
                    parsed = _parse(resp.text)
                    servers_by_source[source.url] = parsed
                    logger.debug(
                        "[pipeline] %s: %d configs fetched.",
                        source.label, len(parsed),
                    )
                else:
                    logger.warning(
                        "[pipeline] %s: HTTP %d.",
                        source.label, resp.status_code,
                    )
            except Exception as exc:
                logger.warning("[pipeline] %s: fetch error — %s.", source.label, exc)

        self._emit(progress_callback, "fetch", total, total, "Fetch complete.")
        return servers_by_source

    def _run_health(
        self,
        configs: List[str],
        overlap_map: Dict[str, float],
        stop_event: threading.Event,
        progress_callback: ProgressCallback,
    ) -> List[Dict[str, Any]]:
        """Run health checks on *configs* and return annotated result dicts."""
        from .health_checker import HealthChecker, filter_healthy_servers
        from .sources import get_enabled_sources

        # Build source lookup: config raw text -> (source_url, trust_int)
        # We can’t reliably reverse-map config -> source after dedup, so we
        # use a best-effort scan: check if the raw config string was seen in
        # any source URL’s result.  overlap_map keys ARE source URLs.
        source_trust_map: Dict[str, int] = {
            s.url: s.trust.value for s in get_enabled_sources()
        }

        checker = HealthChecker(
            timeout=self.timeout,
            min_quality_score=self.min_quality_score,
            check_http_probe=self.check_http_probe,
            check_google_204=self.check_google_204,
            binary_path=self.binary_path,
        )

        total = len(configs)
        all_health = []

        for batch_start in range(0, total, self.health_batch_size):
            if stop_event.is_set():
                break
            batch = configs[batch_start: batch_start + self.health_batch_size]
            self._emit(
                progress_callback, "health",
                batch_start, total,
                f"Health checking {batch_start + 1}–{min(batch_start + self.health_batch_size, total)}…",
            )
            try:
                results = checker.check_batch(batch)
                all_health.extend(results)
            except Exception as exc:
                logger.warning("[pipeline] Health batch error: %s", exc)

        self._emit(progress_callback, "health", total, total, "Health checks complete.")

        healthy = filter_healthy_servers(
            all_health, min_quality_score=self.min_quality_score
        )

        # Annotate with source metadata for scorer
        result_dicts = []
        for h in healthy:
            # Find the source URL that contains this config’s overlap score.
            # We use the first overlap_map key that matches; if none, default.
            src_url   = ""
            src_trust = 1
            for url in overlap_map:
                if url in source_trust_map:
                    src_url   = url
                    src_trust = source_trust_map.get(url, 1)
                    break

            result_dicts.append({
                "config":        h.config,
                "protocol":      h.protocol,
                "tcp_ok":        h.tcp_ok,
                "http_ok":       h.http_probe_ok,
                "google_204_ok": h.google_204_ok,
                "latency_ms":    h.latency_ms,
                "health_checked": True,
                "source_url":    src_url,
                "source_trust":  src_trust,
                "overlap_ratio": overlap_map.get(src_url, 0.0),
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
        """Fire the progress callback if one was provided."""
        if cb is not None:
            try:
                cb(stage, current, total, message)
            except Exception:
                pass  # never let a callback crash the pipeline
