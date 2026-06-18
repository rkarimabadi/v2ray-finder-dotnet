# Changelog

All notable changes to v2ray-finder will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

---

## [0.6.0] — 2026-06-18

### Added

#### `pipeline.py` — Full orchestration layer (new module)
- **`Pipeline`** — single entry point for the complete
  discovery → fetch → dedup → health → score chain.
  - `check_health`, `check_http_probe`, `check_google_204` flags
  - `timeout`, `min_quality_score`, `health_batch_size` params
  - `fetch_timeout`, `fetch_concurrency` (default 10) params
  - `limit` — cap configs after dedup
  - `binary_path` — explicit xray binary override
- **`StopController`** — `threading.Event` wrapper for GUI/CLI
  cancellation: `stop()`, `reset()`, `is_set()`, `.event` property.
- **`PipelineResult`** dataclass — unified output container:
  `configs`, `health_dicts`, `scores`, `overlap_map`, `stats`,
  `top_configs` (property, sorted by score).
- **Async fetch** (`asyncio` + `httpx`) — `_fetch_all_async()`
  with per-source semaphore capped at `fetch_concurrency`;
  1 retry on transient errors; `stop_event` checked before
  every task dispatch and inside `as_completed` loop;
  tasks cancelled cleanly on stop.
- **Sync fallback** — `_fetch_all_sync()` used automatically when
  `httpx` is not installed; no exception surfaces to caller.
- **Progress callback protocol** —
  `(stage: str, current: int, total: int, message: str) → None`;
  stage is one of `"fetch"`, `"health"`, `"score"`.
- `Pipeline._emit()` — safe callback dispatcher; exceptions inside
  the callback are silently swallowed.

#### `core.py` — `threading.Event` stop (roadmap 2.2)
- `_stop_event: threading.Event` replaces `_stop_requested: bool`.
- `request_stop()`, `should_stop()`, `reset_stop()` behaviour
  unchanged — fully backward-compatible.
- New `stop_event` property exposes the raw `threading.Event` so
  `Pipeline` can pass it directly to health batches.

#### `__init__.py`
- Exports `Pipeline`, `PipelineResult`, `StopController` under a
  graceful `try/except ImportError` block (same pattern as
  `health_checker`).
- All three added to `__all__`.
- `__version__` bumped `0.5.2` → `0.6.0`.

#### `tests/test_pipeline.py` (new — 40 test cases)
- `TestStopController` (6): lifecycle, idempotent stop/reset,
  `threading.Event` type assertion.
- `TestPipelineResult` (4): defaults, `top_configs` order,
  empty state.
- `TestPipelineInit` (4): default params, custom params,
  sources override, default sources non-empty.
- `TestPipelineRun` (5): no-health run, stop preset, progress
  callback, stats keys, limit respected.
- `TestPipelineFetchSync` (5): happy path, HTTP 404, network
  error, stop preset, multi-source.
- `TestPipelineFetchAsyncFallback` (1): httpx absent → sync.
- `TestPipelineRunHealth` (4): annotated keys, `health_checked`
  flag, stop mid-batch, empty configs.
- `TestPipelineEmit` (4): fires callback, None safe, exception
  suppressed, stage values.
- `TestPipelineIntegration` (3): full round-trip sorted scores,
  no-health path, `StopController` integration.
- `TestInitExports` (4): `Pipeline`/`StopController`/
  `PipelineResult` importable, version ≥ 0.6.

### Changed
- `scorer.py` — no API change; `google_204_score` weight remains
  at `0.10` (live via `RealConnectivityChecker`).
- `Pipeline` async fetch is **10× faster** than sequential sync
  for 30+ sources (3 batches × 15 s vs. up to 450 s sequential).

### Notes
- `httpx` is an **optional** dependency. Install with
  `pip install "v2ray-finder[async]"` or `pip install httpx`
  to enable concurrent fetch.
- Zero breaking changes. All existing public symbols unchanged.

---

## [0.5.2] — 2026-05-10

### Fixed

- **`__init__.py`** — removed stale `KnownSource` and `SourceConfig` imports that
  were left over from before the `sources.py` refactor (v0.4.0); replaced with
  the correct `SourceEntry` and `SourceTrust` exports that already exist in
  `sources.py`. This was causing an `ImportError` at collection time which
  cascaded into all 13 test files failing to load.
- **`xray_connectivity.py`** — upgraded to match the API expected by
  `tests/test_xray_connectivity.py` (written for v0.5.1 but not yet implemented):
  - `find_free_port()` — added (OS-assigned TCP port, no collisions)
  - `RealHealthResult` — added `from_cache`, `xray_version`, `socks_port`,
    `check_methods` fields; `latency_ms` now `Optional[float]`;
    `quality_score` property (0–100, latency-based)
  - `_ResultCache` — upgraded to per-entry TTL (`set(key, result, ttl=)`),
    `stats` property (`hits`, `misses`, `size`, `hit_rate`), whitespace-stripped
    keys for robustness
  - `RealConnectivityChecker` — added `cache_stats` property,
    `clear_result_cache()` (old `clear_cache()` kept as alias),
    `concurrent_limit`, `startup_timeout`, `cache_enabled`, `show_progress`
    params; failed results cached with short TTL (60 s)
- **`pyproject.toml`** — version was stuck at `0.2.1`; bumped to `0.5.1` to
  match `CHANGELOG.md` and `__version__`.
- **`__init__.py` `__version__`** — synced to `0.5.1`.

### Notes
- No public API symbols were removed. `SourceEntry` and `SourceTrust` were
  already public since v0.4.0; this release simply ensures they are correctly
  re-exported from the top-level package.

---

## [0.5.1] — 2026-05-09

### Added

#### `xray_connectivity.py` — Nice-to-have batch enhancements
- **Rate limiting / exponential backoff** — consecutive failures in
  `check_servers_real_batch()` trigger a sleep of
  `min(8s, 0.5s × 2^n) × random(0, 1)` (full jitter) before the next
  attempt; a single success resets the counter.
- **Progress bar** — `show_progress=True` on `RealConnectivityChecker`
  activates a live `tqdm.asyncio` bar; falls back to periodic
  `logger.info` lines (every ~10 %) when tqdm is not installed.
- **Result cache** — `_ResultCache` (SHA-256-keyed in-memory store);
  successful results cached for `cache_ttl` (default 10 min), failed
  results for 60 s; `from_cache: bool` field added to
  `RealHealthResult` so callers can distinguish live vs cached.
- `clear_result_cache()` — public method to invalidate all entries.
- `cache_stats` property — returns `hits`, `misses`, `size`,
  `hit_rate` dict.

#### Tests
- `tests/test_xray_config_adapter.py` — **new** — 8 unit tests for
  Layer 2 (vmess, vless, trojan, ss parsing; temp-file context manager;
  socks port injection; log level; UnsupportedProtocolError).
- `tests/test_xray_connectivity.py` — **new** — unit + integration
  tests for Layer 3: `_ResultCache` (7 tests), `RealHealthResult`
  quality score (5 tests), cache-hit/miss paths, failed-result short
  TTL, backoff sleep verification, empty-batch edge case,
  exception-wrapping, `find_free_port` smoke test, and
  `pytest.mark.integration` end-to-end tests (auto-skipped without
  xray binary or `V2RAY_TEST_CONFIG` env var).

---

## [0.5.0] — 2026-05-08

### Added

#### `xray_runner.py` — Layer 1: Binary management
- `XrayBinaryManager` — locates or auto-downloads the xray binary
  (PATH → common dirs → cached → GitHub release download)
- Platform/arch detection: linux-64, linux-arm64, macos-64, windows-64
- `get_version()`, `is_available()`
- `async context manager run(config_path, socks_port)` — starts xray,
  waits for startup confirmation line, guarantees termination on exit
- `XrayBinaryNotFoundError` raised when binary is unavailable and
  `auto_download=False`

#### `xray_config_adapter.py` — Layer 2: Config generation
- `ConfigAdapter.build_config(raw_config, socks_port)` — converts a
  raw vmess/vless/trojan/ss string into a complete xray JSON config dict
- `ConfigAdapter.build_config_file(raw_config, socks_port)` — context
  manager that writes the config to a temp file and auto-deletes it
- Reuses `NormalizedServer` from `normalizer.py`; zero duplicate parsing
- Protocol coverage: vmess (full JSON incl. WS/TLS), vless (WS/gRPC/
  TLS/XTLS/Reality), trojan (auto-TLS), shadowsocks
- `UnsupportedProtocolError` on unknown protocols

#### `xray_connectivity.py` — Layer 3: Real end-to-end check
- `RealConnectivityChecker` — orchestrates all three layers
- `find_free_port()` — OS-assigned port allocation (no collisions)
- `check_real_connectivity(socks_port)` — HTTP probe through the local
  SOCKS5 proxy to `connectivitycheck.gstatic.com/generate_204`;
  measures true end-to-end latency; expects HTTP 204
- `check_server_real(config)` — full single-server check: build config
  → start xray → probe → stop → return `RealHealthResult`
- `check_servers_real_batch(servers)` — concurrent batch with semaphore
- `check_servers_real(servers)` — sync wrapper
- `RealHealthResult` dataclass with `quality_score` property (0–100)
- **`google_204_ok` now reflects actual proxy connectivity**, not runner
- Requires `aiohttp-socks` (optional extra `xray`); import error is
  raised with a clear install instruction

#### CLI (`cli.py`)
- `--xray-check` flag — routes through `get_servers_with_real_health()`
- `--xray-binary` — explicit path to xray binary
- `--xray-no-download` — disables auto-download
- Interactive menu option 7: Real connectivity check via xray

#### Core (`core.py`)
- `get_servers_with_real_health()` — discover + real batch check
- `get_scored_servers(use_real_health=True)` — routes through xray path
- `_passes_realtime_check()` — prefers xray when available, falls back
  to TCP/HTTP
- `_run_async()` helper — loop-safe asyncio runner (no RuntimeError
  when caller is already in an event loop)

#### `__init__.py`
- Exports: `RealConnectivityChecker`, `RealHealthResult`,
  `XrayBinaryManager`, `ConfigAdapter`, `find_free_port`
- Graceful optional import: xray symbols are silently absent when
  `aiohttp-socks` is not installed

#### Tests
- `tests/test_xray_integration.py` — `pytest.mark.integration` tests
  for `ConfigAdapter`, `RealConnectivityChecker`, and
  `V2RayServerFinder(xray_realtime_check=True)`; auto-skipped without
  xray binary

### Changed
- `scorer.py` — `google_204_ok` weight set to **zero** until callers
  migrate to `RealConnectivityChecker`; `tcp_ok` weight 0.30 → 0.70
- `__version__` bumped 0.4.0 → 0.5.0
- **Zero breaking changes**

---

## [0.4.0] - 2026-05-08

### Added — Multi-Source Ingestion Pipeline (closes #4)

#### `sources.py` — 32 curated sources (faz 1)
- **`SourceTrust` enum** (HIGH / MEDIUM / LOW) per source
- **`SourceEntry` dataclass** — `url`, `source_type`, `trust`, `label`, `enabled`, `tags`
- **`GITHUB_TOPICS`** — 10 GitHub topics for dynamic repo discovery
- **`get_enabled_sources(source_type, min_trust, tags)`** — filter helper
- `DIRECT_SOURCES` on `V2RayServerFinder` now auto-derived as backward-compat alias

#### `source_registry.py` — Runtime source health (faz 2)
- **`SourceStats`** — per-source counters: `fetch_count`, `fail_count`, `last_fetched`, `total_servers_found`, `overlap_ratio`, `avg_latency_ms`
- **`SourceStats.reliability_score`** — 0–1 composite of success rate, freshness, and uniqueness
- **`SourceRegistry`** — `record_success()`, `record_failure()`, `update_overlap()`, `update_avg_latency()`, `all_stats()`, `healthy_sources()`, `summary()`
- **`V2RayServerFinder.get_source_registry()`** — exposes live registry after a run

#### `normalizer.py` — Structural deduplication (faz 3)
- **`NormalizedServer`** dataclass with `structural_key` (16-char SHA-256)
- Protocol parsers for vmess, vless, trojan, ss/ssr
- **`normalize_server()`**, **`deduplicate_servers()`**, **`deduplicate_across_sources()`**

#### `scorer.py` — Ranking engine (faz 4)
- **`ServerScore`** with weighted composite total (0.0–1.0) and A–F grade
- **`score_servers(health_results, …)`** — batch scoring, sorted descending
- **`V2RayServerFinder.get_scored_servers()`** — fetch + health + score

#### `core.py` updates
- `get_servers_from_topic_discovery()`, `get_source_registry()`
- Per-fetch dedup and source registry integration

### Changed
- `__version__` bumped to `0.4.0`
- **Zero breaking changes**

---

## [0.3.0] - 2026-05-08

### Added

- **Real-time health checking** — servers health-checked immediately on discovery
- **Google 204 check** and **HTTP reachability check** in `health_checker.py`
- **`ServerHealth` extended fields:** `tcp_ok`, `http_ok`, `google_204_ok`, `check_methods`
- **`HealthChecker.check_server_now()`** — synchronous single-server check
- **Generator-based discovery pipeline** — streaming real-time health filter

### Changed
- `health_checker.py` uses `aiohttp` for HTTP-level checks
- `HealthChecker.__init__` gains `enable_google_204` / `enable_http_check` flags
- **Zero breaking changes**

---

## [0.2.1] - 2026-02-24

### Fixed
- **Graceful stop / Ctrl+C** — all fetch layers catch `KeyboardInterrupt`
- `StopController` with `threading.Event`; `health_batch_size` param

---

## [0.2.0] - 2026-02-20

### Added
- Async HTTP fetching, smart caching, error handling, health checker,
  secure token handling, rate limit tracking, test suite (78% coverage)

---

## [0.1.0] - 2026-01-15

### First Release
- GitHub search, curated sources, 5 protocols, dedup, Python API + CLI + GUI

---

## Project Statistics

| Metric | Value |
|--------|-------|
| Source files | 17 |
| Test files | 22 |
| Test coverage | ~85% |
| Supported protocols | 5 (vmess, vless, trojan, ss, ssr) |
| Health check layers | 4 (TCP, HTTP, xray+SOCKS5, Google 204) |
| Curated sources | 32 |
| Interfaces | 3 (Python API, CLI, GUI) |
| Python versions | 3.8 – 3.12 |
| Platforms | Linux, macOS, Windows |

---

## Contributors

- Ali Sadeghi Aghili ([@alisadeghiaghili](https://github.com/alisadeghiaghili)) — Creator & Maintainer

---

## License

MIT License — see [LICENSE](LICENSE) for details.
