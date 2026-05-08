# Changelog

All notable changes to v2ray-finder will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased] — xray integration (in progress)

### Changed
- `scorer.py` — `google_204_ok` weight set to **zero** until xray
  integration is complete.  Previously this flag reflected the CI
  runner's own connectivity (not the proxy), causing a systematic
  upward scoring bias on dead servers.  `tcp_ok` weight raised from
  0.30 → 0.70; `http_ok` unchanged at 0.30.

### Added
- `xray_runner.py` — **Layer 1** of the real connectivity checker.
  `XrayBinaryManager` handles:
  - Binary discovery (PATH → common install dirs → cached download)
  - Auto-download of the latest release from `XTLS/Xray-core` GitHub
    releases when `auto_download=True` (stdlib-only, no new deps)
  - Platform / architecture detection (linux-64, linux-arm64-v8a,
    macos-64, windows-64, …)
  - Version reporting via `get_version()` / `is_available()`
  - Async context-manager `run(config_path, socks_port)` that starts
    xray, waits for the startup confirmation line, yields the process,
    and **guarantees termination** on exit (even on exception)

  **Layer 2** (ConfigAdapter — vmess/vless/trojan/ss → xray JSON) and
  **Layer 3** (RealConnectivityChecker — HTTP probe through SOCKS5
  proxy, re-wired `google_204_ok`) are in the next PR.

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
- **`NormalizedServer`** dataclass with `structural_key` (16-char SHA-256 on `(protocol, host, port, credential[:32])`)
- Protocol parsers for vmess (base64 JSON), vless, trojan, ss/ssr (SIP002 + legacy base64)
- **`normalize_server(config, source_url, source_type)`**
- **`deduplicate_servers(servers, source_url)`** — single-source structural dedup, returns `(list, dupes_count)`
- **`deduplicate_across_sources(servers_by_source)`** — cross-source dedup + per-source overlap ratios

#### `scorer.py` — Ranking engine (faz 4)
- **`ServerScore`** — `latency_score`, `reachability_score`, `protocol_score`, `source_trust_score`, `freshness_score`, `uniqueness_score`
- **`ServerScore.total`** — weighted composite (latency 30%, reachability 30%, protocol 15%, trust 15%, freshness 5%, uniqueness 5%)
- **`ServerScore.grade`** — A / B / C / D / F
- Protocol quality: vless=1.0, trojan=0.9, ss=0.8, vmess=0.7, ssr=0.5
- **`score_servers(health_results, source_trust_map, overlap_map)`** — batch scoring, sorted descending
- **`V2RayServerFinder.get_scored_servers()`** — fetch + health-check + score in one call

#### `core.py` updates
- **`get_servers_from_topic_discovery(topics, max_repos_per_topic)`** — dynamic GitHub topic discovery
- **`get_source_registry()`** — exposes live SourceRegistry after a run
- `get_servers_from_url()` now calls `deduplicate_servers()` and `record_success/failure()` per fetch
- `get_servers_from_known_sources()` runs `deduplicate_across_sources()` and feeds overlap ratios to registry
- `_get_servers_from_repo()` helper extracted for reuse

### Changed
- `__version__` bumped to `0.4.0`
- `__init__.py` exports all new public symbols
- **Zero breaking changes** — existing `get_all_servers()` / `get_servers_with_health()` API unchanged
- `DIRECT_SOURCES` still exists as a list of strings (now auto-derived from `STATIC_SOURCES`)

---

## [0.3.0] - 2026-05-08

### Added

- **Real-time health checking** — servers are now health-checked **immediately**
  as each one is discovered, not in a separate batch step after all sources are
  exhausted.

  **New constructor parameters on `V2RayServerFinder`:**
  | Parameter | Default | Description |
  |---|---|---|
  | `realtime_health_check` | `False` | Enable per-server inline health checks |
  | `health_timeout` | `5.0` | Timeout (seconds) per check method |
  | `health_concurrent_limit` | `50` | Max concurrent async checks |
  | `health_enable_google_204` | `True` | Google 204 connectivity check |
  | `health_enable_http_check` | `True` | HTTP-level reachability check |

- **Google 204 connectivity check** in `health_checker.py`
  - `GET http://connectivitycheck.gstatic.com/generate_204`
  - Expects HTTP 204 (same check Android uses for captive-portal detection)
  - Implemented as `HealthChecker.check_google_204()` (async)

- **HTTP reachability check** in `health_checker.py`
  - `HealthChecker.check_http_reachability()` — lightweight HTTP GET to the server's own `host:port`
  - SSL/TLS errors treated as *reachable* (port is open)

- **`ServerHealth` extended fields:** `tcp_ok`, `http_ok`, `google_204_ok`, `check_methods`
- **Quality score bonus** — `google_204_ok` +10 pts, `http_ok` +5 pts
- **`HealthChecker.check_server_now()`** — synchronous single-server check for inline use
- **Generator-based discovery pipeline** — `_iter_raw_servers()`, `_iter_servers_with_realtime_health()`
- **`get_all_servers()` updated** — routes through streaming pipeline when `realtime_health_check=True`

### Changed
- `health_checker.py` now uses `aiohttp` for HTTP-level checks
- `ServerHealth` dataclass gains four new optional fields — **backward compatible**
- `HealthChecker.__init__` gains `enable_google_204` and `enable_http_check` flags

### Technical Notes
- **Zero breaking changes** — `realtime_health_check` defaults to `False`
- The real-time path is **fail-open**: unexpected errors pass the server through
- All three check methods run **concurrently** via `asyncio.gather` per server

---

## [0.2.1] - 2026-02-24

### Fixed

- **Graceful stop / Ctrl+C handling** — complete overhaul across all layers:
  - `core.py`: `try/except KeyboardInterrupt` in all fetch loops, partial results preserved
  - `cli.py`: `StopController` with `threading.Event`, menu loop fixed
  - `cli_rich.py`: `_signal_handler()` now calls `request_stop()`, partial snapshots after interrupt
  - `get_servers_with_health()`: `health_batch_size` param, stop checked between batches

### Tests
- Added `TestHealthBatchStop` class to `tests/test_stop_mechanism.py`

---

## [0.2.0] - 2026-02-20

### Added

- **Async HTTP Fetching** (`async_fetcher` module) — 10-50x faster concurrent downloads
- **Smart Caching Layer** (`cache` module) — memory/disk, configurable TTL, hit-rate stats
- **Enhanced Error Handling** (`exceptions` + `result` modules)
- **Health Checking** (`health_checker` module) — TCP connectivity, latency, quality scoring
- **Secure Token Handling** — `GITHUB_TOKEN` env var, `from_env()` factory
- **Rate Limit Tracking** — `get_rate_limit_info()`
- **Test Suite** (78% coverage) — CI matrix Python 3.8–3.12 on Linux/macOS/Windows

### Changed
- `search_repos()` returns `Result[List[Dict], V2RayFinderError]`
- Rate limit checking moved after HTTP status checks

---

## [0.1.0] - 2026-01-15

### First Release

- GitHub repository search for public V2Ray configs
- Curated direct subscription sources (3 reliable sources)
- Protocol support: vmess, vless, trojan, shadowsocks (ss), ssr
- Automatic deduplication
- Python API + CLI + Rich CLI + GUI (PySide6)

---

## Project Statistics

| Metric | Value |
|--------|-------|
| Source Lines | ~5,500+ |
| Test Files | 8 |
| Test Coverage | ~80% |
| Supported Protocols | 5 (vmess, vless, trojan, ss, ssr) |
| Health Check Methods | 3 (TCP, HTTP, Google 204*) |
| Curated Sources | 32 (up from 3) |
| Interfaces | 3 (Python API, CLI, GUI) |
| Python Versions | 3.8 – 3.12 |
| Platforms | Linux, macOS, Windows |

*Google 204 weight temporarily zero pending xray integration.

---

## Contributors

- Ali Sadeghi Aghili ([@alisadeghiaghili](https://github.com/alisadeghiaghili)) — Creator & Maintainer

---

## License

MIT License — see [LICENSE](LICENSE) for details.
