## Session Log

### [2026-06-18] Initial Code Review
**Critical Issues Found:**
- `is_xray_available()` in xray_connectivity.py checks nonexistent `runner.binary_path`, so Layer 3 health checks never run.
- Layer 3 in health_checker.py creates a new RealConnectivityChecker per server, risking overlapping xray port binds under high concurrency.
- `get_all_servers()` in core.py uses naive string dedup, bypassing normalizer's SHA-256 structural deduplication.
- Source trust and overlap_ratio never reach scorer.py, leaving two of six scoring dimensions as dead weight.

**Decisions Made:**
- Introduce a single `Pipeline` orchestrator (new pipeline.py) owning discovery, dedup, health-check, and scoring.
- Replace the plain `_stop_requested` bool with a shared `threading.Event` for true cross-thread cancellation.
- Route real fetching through `AsyncFetcher.fetch_many` instead of serial `requests.get` in core.py.
- Consolidate duplicated `_socks5_http_get` and divergent `_latency_to_score` curves into shared modules.

**Next Steps:**
- Fix `is_xray_available` to use `runner.find_binary()` (unblocks Layer 3).
- Wire normalizer dedup + source trust/overlap into the main pipeline and scorer.
- Build the `Pipeline` orchestrator and migrate CLI/Rich/GUI to it.
- Migrate stop mechanism to `threading.Event`.
- Rebalance scorer reachability weights once Layer 3 runs; rename `get_servers_sorted`.
- Populate the CHANGELOG `[Unreleased]` section.

---

## Roadmap

### Summary Of Work Done This Session
- Performed a full architecture review of the discover → fetch → dedup → health-check → score → output pipeline.
- Catalogued critical correctness bugs (xray availability, per-server checker churn, missing structural dedup, dead scoring dimensions).
- Recorded architectural decisions (Pipeline orchestrator, threading.Event stop, async fetch in main path, shared probe/scoring helpers).
- Appended the dated session log above as the project's source of truth.
- Verified against the latest pasted source that the four critical fixes are **not yet applied** — they remain open below.

### Status Legend
- [ ] TODO — not started / not yet in code
- [~] IN PROGRESS — partially applied
- [x] DONE — verified present in source

---

### P0 — Critical (correctness; pipeline silently produces wrong/empty results)
- [ ] **C1. xray availability check is broken** — `xray_connectivity.py::RealConnectivityChecker.is_xray_available()` returns `runner.binary_path is not None`, but `XrayRunner` only has `_binary_path` (the *passed* value), so it is `None` whenever no explicit path is given even when xray is on PATH. Effect: Layer 3 (Google 204 via real xray) never runs.
  - Fix: `return runner.find_binary() is not None` (or `runner.is_available()`).
- [ ] **C2. Per-server RealConnectivityChecker churn** — `health_checker.py::HealthChecker.check_server_health()` builds a new `RealConnectivityChecker` for every server; under `check_servers_batch` (max_workers=50) this risks overlapping xray port binds and 50 concurrent heavy processes.
  - Fix: instantiate one shared checker in `HealthChecker.__init__` (guarded by `check_google_204`); add a separate, low Layer-3 concurrency cap (≈5).
- [ ] **C3. Structural dedup bypassed** — `core.py::get_all_servers()` dedupes with `dict.fromkeys`-style string matching; `normalizer.deduplicate_servers` / `deduplicate_across_sources` is never called in the main path.
  - Fix: route `get_all_servers` through `normalizer.deduplicate_servers`; feed per-source results into `deduplicate_across_sources` to capture overlap ratios.
- [ ] **C4. Trust & overlap never reach the scorer** — `score_servers` accepts `source_trust`/`overlap_ratio`, but nothing in `core.py` maps `SourceEntry.trust` or the overlap map into the health-result dicts. Two of six scoring dimensions are dead weight.
  - Fix: thread `source_url → SourceEntry` and the overlap map from `deduplicate_across_sources` into each health-result dict before calling `score_servers`.

### P1 — Architecture (structural; unlocks correctness and maintainability)
- [ ] **A1. Pipeline orchestrator** — create `pipeline.py` with a `Pipeline` class owning `SourceRegistry`, dedup, `HealthChecker`, and `scorer.score_servers`, exposing one `run(stop_event, progress_callback)`; migrate CLI / Rich TUI / GUI to it.
- [ ] **A2. threading.Event stop mechanism** — replace `core.py::_stop_requested` (bool) with an injected `threading.Event`; share it across both `StopController` variants and the GUI `WorkerThread`.
- [ ] **A3. Use AsyncFetcher in the real path** — `core.py::get_servers_from_known_sources` fetches 32 sources serially via `requests.get`; switch to `AsyncFetcher.fetch_many`, preserving per-URL success/failure recording in `SourceRegistry`.
- [ ] **A4. De-duplicate probe/scoring helpers** — `_socks5_http_get` is duplicated in `health_checker.py` and `xray_connectivity.py`; `_latency_to_score` is triplicated (`scorer.py` 0–1 scale vs `health_checker.py`/`xray_connectivity.py` 0–100 scale). Extract `probes.py` (SOCKS5) and `scoring_curves.py` (single curve) to prevent divergence.

### P2 — Technical Debt
- [ ] **D1. google_204 weight = 0** — `scorer.py` `_REACH_W_G204 = 0.00`. After C1/C2 land, rebalance (e.g. TCP 0.4 / HTTP 0.2 / G204 0.4); the highest-signal dimension is currently discarded.
- [ ] **D2. `from_env` kwarg collision** — `core.py::from_env(**kwargs)` raises `TypeError` if a caller also passes `token=`; guard or document.
- [ ] **D3. `XrayRunner.run` broken stub** — base `XrayRunner.run` raises `NotImplementedError` while `XrayBinaryManager.run` is the real async ctx; same name, opposite semantics. Rename/remove the stub.
- [ ] **D4. `get_servers_sorted` does not sort** — it only enumerates with metadata. Either implement score-based sorting or rename to `get_servers_with_metadata`.

### P3 — Quick Wins (small, high-impact)
- [ ] **Q1. Fix `is_xray_available`** — one-line change (same as C1); listed here because it is trivial and unblocks an entire layer.
- [ ] **Q2. Document MemoryCache eviction** — `cache.py::MemoryCache` is FIFO, not LRU; add a docstring note to prevent misuse.
- [ ] **Q3. Warn on dropped token** — `core.py::_validate_token` silently ignores invalid tokens; emit a `logger.warning`.
- [ ] **Q4. Hoist zero-score sentinel** — `scorer.py::sort_by_quality` builds `ServerScore("", "")` per item in the sort key; replace with a module-level `_ZERO_SCORE`.
- [ ] **Q5. Populate CHANGELOG `[Unreleased]`** — currently empty.

### Keep As-Is (well-designed; do not refactor without cause)
- [x] `result.py` Ok/Err Result type — clean and well-used.
- [x] `sources.py` SourceEntry / get_enabled_sources — filterable, trust-tagged.
- [x] `normalizer.py` structural fingerprinting — correct (needs wiring per C3).
- [x] `exceptions.py` hierarchy — comprehensive, with to_dict/details.
- [x] `cache.py` backend abstraction — clean ABC, graceful diskcache fallback.
- [x] Piecewise-linear latency curve — sound thresholds (consolidate per A4).

### Recommended Execution Order
1. C1 (unblocks Layer 3) → C2 (make Layer 3 safe under concurrency)
2. C3 + C4 (real dedup + trust/overlap wiring → scoring becomes meaningful)
3. A1 (Pipeline orchestrator) → A2 (Event-based stop)
4. A3 (async fetch) → A4 (shared helpers)
5. D1 (rebalance weights now that G204 runs) → remaining D/Q items

---
