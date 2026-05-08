# v2ray-finder

[![PyPI version](https://badge.fury.io/py/v2ray-finder.svg)](https://badge.fury.io/py/v2ray-finder)
[![Python Versions](https://img.shields.io/pypi/pyversions/v2ray-finder.svg)](https://pypi.org/project/v2ray-finder/)
[![Tests](https://github.com/alisadeghiaghili/v2ray-finder/workflows/Tests/badge.svg)](https://github.com/alisadeghiaghili/v2ray-finder/actions)
[![Code Quality](https://github.com/alisadeghiaghili/v2ray-finder/workflows/Code%20Quality/badge.svg)](https://github.com/alisadeghiaghili/v2ray-finder/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![GitHub Stars](https://img.shields.io/github/stars/alisadeghiaghili/v2ray-finder?style=flat)](https://github.com/alisadeghiaghili/v2ray-finder/stargazers)

[English](README.en.md) | [فارسی](README.fa.md) | [Deutsch](README.de.md) | [📋 CHANGELOG](CHANGELOG.md)

---

A **high-performance** tool to **fetch, aggregate, validate and health-check public V2Ray server configs** from GitHub and curated subscription sources.

هدف این ابزار این است که بدون دردسر، یک لیست تمیز و dedup شده از لینک‌های `vmess://`، `vless://`، `trojan://`، `ss://`، `ssr://` بهت بده.

**با عشق برای آزادی همیشگی ❤️**  
**Built with love for eternal freedom ❤️**

---

## 🚀 What's New — xray Real Connectivity (in progress)

### True proxy validation via xray-core — three-layer architecture

Previous health checks (TCP connect, HTTP reachability) told us the port
is open — **not** that the proxy actually forwards traffic.  The new
xray integration fixes this.

| Layer | Module | What it does |
|-------|--------|--------------|
| **1** | `xray_runner.py` | Locate / auto-download xray binary; start/stop the process |
| **2** | `xray_config_adapter.py` | Convert `vmess://` / `vless://` / `trojan://` / `ss://` → xray JSON config |
| **3** | `xray_connectivity.py` | Route `GET generate_204` through the SOCKS5 proxy; measure real latency |

```python
from v2ray_finder.xray_connectivity import RealConnectivityChecker

checker = RealConnectivityChecker()   # auto-downloads xray if needed

# Single server — full three-layer check
import asyncio
result = asyncio.run(checker.check_server_real("vless://uuid@host:443?..."))
print(result.reachable, result.latency_ms, result.google_204_ok)

# Batch — concurrent, semaphore-limited
servers = [(config, protocol), ...]
results = checker.check_servers_real(servers)   # sync wrapper
```

> Requires `pip install aiohttp-socks` (or `pip install "v2ray-finder[xray]"`)

---

## 🚀 What's New in v0.4.0 — Multi-Source Pipeline

### 32 Sources · Structural Dedup · Scoring Engine (closes #4)

| Before | After |
|--------|-------|
| 3 hardcoded URLs | **32 curated sources** across 4 categories |
| String `dict.fromkeys()` dedup | **Structural fingerprint** on `(protocol, host, port, uuid)` |
| No source tracking | **`SourceRegistry`** — per-source reliability scores |
| Raw list output | **Scoring engine** — A–F grades by quality |

```python
from v2ray_finder import V2RayServerFinder

finder = V2RayServerFinder.from_env()
ranked = finder.get_scored_servers(check_health=True, min_score=0.5)
for srv in ranked[:10]:
    print(f"[{srv['grade']}] {srv['total_score']:.2f}  {srv['config'][:60]}")

print(finder.get_source_registry().summary())
```

---

## 🚀 What's New in v0.3.0 — Real-Time Health Checking

Three check methods run **concurrently** per server:

| Method | What it checks |
|--------|----------------|
| 🔌 **TCP** | Raw socket connect to `host:port` |
| 🌐 **HTTP** | Lightweight HTTP GET to `host:port` |
| ✅ **xray + Google 204** | Traffic through the proxy → `generate_204` *(new layer)* |

```python
finder = V2RayServerFinder(
    realtime_health_check=True,
    health_timeout=5.0,
)
servers = finder.get_all_servers()
```

---

## 🎯 Features

### Core
- 🔍 **GitHub repository search** + **32 curated sources**
- 🚀 **Three interfaces**: Python API, CLI (simple & rich), GUI (PySide6)
- 📦 **Structural deduplication** on `(protocol, host, port, uuid)`
- 🌐 **Supports**: vmess, vless, trojan, shadowsocks, ssr
- 💾 **Export** to text files
- 📊 **Statistics** by protocol

### Performance & Reliability
- ⚡ **Async HTTP fetching**: 10-50x faster concurrent downloads
- 💾 **Smart caching**: 80-95% fewer API calls
- 🔌 **TCP + HTTP health checks**: fast pre-filters
- ✅ **xray real connectivity**: true end-to-end proxy validation
- 🏆 **Scoring engine**: rank servers A–F by quality
- 📡 **Source registry**: per-source reliability tracking
- ⛔ **Graceful interruption**: Ctrl+C saves partial results

---

## 📋 Requirements

- **Python** 3.8 – 3.14
- **Internet connection**
- **Optional**: `aiohttp` (async + health checks), `aiohttp-socks` (xray
  real connectivity), `diskcache` (caching), `PySide6` (GUI)

---

## 📦 Installation

```bash
pip install v2ray-finder
pip install "v2ray-finder[async]"      # async + TCP/HTTP health checks
pip install "v2ray-finder[xray]"       # real connectivity via xray-core
pip install "v2ray-finder[cache]"      # caching
pip install "v2ray-finder[all]"        # everything
```

### From source

```bash
git clone https://github.com/alisadeghiaghili/v2ray-finder.git
cd v2ray-finder
pip install -e ".[all,dev]"
```

---

## 🔒 Token Security

```bash
export GITHUB_TOKEN="ghp_your_token_here"
```

**Rate Limits:** without token: 60 req/h — with token: 5000 req/h

---

## 📚 Library Usage

### Real Connectivity Check (xray) — New ✨

```python
from v2ray_finder.xray_connectivity import RealConnectivityChecker
import asyncio

checker = RealConnectivityChecker(timeout=10.0, concurrent_limit=5)

# Single server
result = asyncio.run(checker.check_server_real("vless://..."))
print(result.reachable, result.latency_ms, result.google_204_ok)

# Batch
results = checker.check_servers_real([
    ("vmess://...", "vmess"),
    ("vless://...", "vless"),
])
for r in results:
    print(f"{r.protocol:8s} | ok={r.reachable} | {r.latency_ms:.0f}ms")
```

### Scored Output

```python
from v2ray_finder import V2RayServerFinder

finder = V2RayServerFinder.from_env()
ranked = finder.get_scored_servers(check_health=True, min_score=0.5)
for srv in ranked[:10]:
    print(f"[{srv['grade']}] {srv['total_score']:.2f}  {srv['config'][:60]}")
```

### Basic Usage

```python
finder = V2RayServerFinder()
servers = finder.get_all_servers()
servers = finder.get_all_servers(use_github_search=True)
count, filename = finder.save_to_file(filename="servers.txt", limit=200)
```

---

## ⚡ CLI Usage

```bash
export GITHUB_TOKEN="ghp_your_token_here"

v2ray-finder                          # Interactive TUI
v2ray-finder -o servers.txt           # Quick save
v2ray-finder -s -l 200 -o servers.txt # GitHub search + limit
v2ray-finder -c --min-quality 60 -o healthy_servers.txt
```

---

## 🤝 Contributing

```bash
pytest tests/ -v
black --target-version py38 . && isort . && flake8 src/
```

---

## 📝 License

MIT License © 2026 Ali Sadeghi Aghili

---

## 🔗 Links

- [Repository](https://github.com/alisadeghiaghili/v2ray-finder)
- [PyPI](https://pypi.org/project/v2ray-finder)
- [Issues](https://github.com/alisadeghiaghili/v2ray-finder/issues)
- [CHANGELOG](CHANGELOG.md)

---

## 🙏 Acknowledgments

- [ebrasha/free-v2ray-public-list](https://github.com/ebrasha/free-v2ray-public-list)
- [barry-far/V2ray-Config](https://github.com/barry-far/V2ray-Config)
- [Epodonios/v2ray-configs](https://github.com/Epodonios/v2ray-configs)

و تمامی توسعه‌دهندگانی که کانفیگ‌های آزاد منتشر می‌کنند ❤️
