# Changelog

All notable changes to v2ray-finder will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased] — xray real connectivity (in progress)

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

### Changed
- `scorer.py` — `google_204_ok` weight set to **zero** until callers
  migrate to `RealConnectivityChecker`; `tcp_ok` weight 0.30 → 0.70

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
| Source files | 14 |
| Test files | 8 |
| Test coverage | ~80% |
| Supported protocols | 5 (vmess, vless, trojan, ss, ssr) |
| Health check layers | 4 (TCP, HTTP, xray+SOCKS5, Google 204*) |
| Curated sources | 32 |
| Interfaces | 3 (Python API, CLI, GUI) |
| Python versions | 3.8 – 3.12 |
| Platforms | Linux, macOS, Windows |

*Google 204 via real proxy (xray). TCP/HTTP scorer weight temporarily adjusted.

---

## Contributors

- Ali Sadeghi Aghili ([@alisadeghiaghili](https://github.com/alisadeghiaghili)) — Creator & Maintainer

---

## License

MIT License — see [LICENSE](LICENSE) for details.
