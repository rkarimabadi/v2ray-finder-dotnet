"""Runtime source registry — tracks per-source health statistics.

Each :class:`SourceStats` record is populated *in-memory* during a single run.
No persistence yet (faz 3 will add optional JSON/SQLite backend).

Part of the multi-source ingestion pipeline (closes #4 roadmap, faz 2).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class SourceStats:
    """Runtime statistics accumulated for a single source URL during one run.

    Attributes:
        url:               The source URL being tracked.
        fetch_count:       Total fetch attempts (success + failure).
        fail_count:        Number of failed fetches.
        last_fetched:      Timestamp of the most recent attempt.
        last_success:      Timestamp of the most recent successful fetch.
        last_server_count: Number of servers returned in the last successful fetch.
        total_servers_found: Cumulative server count across all successful fetches.
        overlap_ratio:     Fraction of this source's configs also seen in other sources.
                           0.0 = fully unique, 1.0 = 100 % recycled.
        avg_latency_ms:    Average observed server latency (populated by health-checker).
    """

    url: str
    fetch_count: int = 0
    fail_count: int = 0
    last_fetched: Optional[datetime] = None
    last_success: Optional[datetime] = None
    last_server_count: int = 0
    total_servers_found: int = 0
    overlap_ratio: float = 0.0
    avg_latency_ms: Optional[float] = None

    @property
    def success_count(self) -> int:
        """Number of successful fetches."""
        return max(0, self.fetch_count - self.fail_count)

    @property
    def reliability_score(self) -> float:
        """0.0-1.0 score combining success-rate, freshness, and uniqueness."""
        if self.fetch_count == 0:
            return 0.5
        success_rate = self.success_count / self.fetch_count
        freshness_factor = 1.0 if self.last_server_count > 0 else 0.3
        overlap_penalty = self.overlap_ratio * 0.30
        score = success_rate * freshness_factor * (1.0 - overlap_penalty)
        return round(min(max(score, 0.0), 1.0), 4)

    @property
    def is_healthy(self) -> bool:
        """True if the source has at least one success and reliability >= 0.5."""
        return self.success_count > 0 and self.reliability_score >= 0.5

    def __repr__(self) -> str:
        return (
            f"<SourceStats url={self.url!r} fetches={self.fetch_count} "
            f"fails={self.fail_count} servers={self.total_servers_found} "
            f"reliability={self.reliability_score:.2f}>"
        )


class SourceRegistry:
    """In-memory registry that accumulates :class:`SourceStats` per URL."""

    def __init__(self) -> None:
        self._stats: Dict[str, SourceStats] = {}

    def get(self, url: str) -> SourceStats:
        """Return the :class:`SourceStats` for *url*, creating it if absent."""
        if url not in self._stats:
            self._stats[url] = SourceStats(url=url)
        return self._stats[url]

    def record_success(self, url: str, server_count: int) -> None:
        """Record a successful fetch that returned *server_count* configs."""
        s = self.get(url)
        now = datetime.utcnow()
        s.fetch_count += 1
        s.last_fetched = now
        s.last_success = now
        s.last_server_count = server_count
        s.total_servers_found += server_count
        logger.debug(
            f"[SourceRegistry] success {url!r} -> {server_count} servers"
        )

    def record_failure(self, url: str) -> None:
        """Record a failed fetch for *url*."""
        s = self.get(url)
        s.fetch_count += 1
        s.fail_count += 1
        s.last_fetched = datetime.utcnow()
        logger.debug(
            f"[SourceRegistry] failure {url!r} fails={s.fail_count}/{s.fetch_count}"
        )

    def update_overlap(self, url: str, overlap_ratio: float) -> None:
        """Set the overlap_ratio (0.0-1.0) for *url*."""
        s = self.get(url)
        s.overlap_ratio = round(min(max(overlap_ratio, 0.0), 1.0), 4)

    def update_avg_latency(self, url: str, avg_latency_ms: float) -> None:
        """Update the average observed server latency for *url*."""
        self.get(url).avg_latency_ms = avg_latency_ms

    def all_stats(self) -> List[SourceStats]:
        """Return all tracked sources sorted by reliability (descending)."""
        return sorted(
            self._stats.values(),
            key=lambda s: s.reliability_score,
            reverse=True,
        )

    def healthy_sources(self) -> List[SourceStats]:
        """Return only sources considered healthy."""
        return [s for s in self.all_stats() if s.is_healthy]

    def summary(self) -> str:
        """Human-readable summary table of all tracked sources."""
        lines = [
            f"{'URL':<60} {'fetches':>7} {'fails':>5} {'servers':>8} {'reliability':>12}",
            "-" * 96,
        ]
        for s in self.all_stats():
            short_url = s.url if len(s.url) <= 58 else s.url[:55] + "..."
            lines.append(
                f"{short_url:<60} {s.fetch_count:>7} {s.fail_count:>5} "
                f"{s.total_servers_found:>8} {s.reliability_score:>12.4f}"
            )
        lines.append("-" * 96)
        lines.append(
            f"Total: {len(self._stats)} sources tracked, "
            f"{sum(s.total_servers_found for s in self._stats.values())} servers found."
        )
        return "\n".join(lines)
