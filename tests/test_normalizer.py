"""Unit tests for normalizer.py.

Covers:
- _safe_b64decode helper
- All four protocol parsers (vmess / vless / trojan / ss)
- normalize_server dispatch
- NormalizedServer.structural_key determinism
- deduplicate_servers (within-source)
- deduplicate_across_sources (cross-source overlap)
"""

from __future__ import annotations

import base64
import json
from typing import Dict, List

import pytest

from v2ray_finder.normalizer import (
    NormalizedServer,
    _safe_b64decode,
    deduplicate_across_sources,
    deduplicate_servers,
    normalize_server,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode().rstrip("=")


def _vmess_config(
    host: str = "1.2.3.4",
    port: int = 443,
    uuid: str = "aaaa-bbbb",
    tls: str = "tls",
) -> str:
    payload = json.dumps({"add": host, "port": port, "id": uuid, "tls": tls})
    return "vmess://" + base64.b64encode(payload.encode()).decode()


def _vless_config(
    host: str = "1.2.3.4",
    port: int = 8443,
    uuid: str = "cccc-dddd",
    security: str = "tls",
) -> str:
    return f"vless://{uuid}@{host}:{port}?security={security}&type=tcp"


def _trojan_config(
    host: str = "5.6.7.8",
    port: int = 443,
    password: str = "s3cr3t",
) -> str:
    return f"trojan://{password}@{host}:{port}?security=tls"


def _ss_sip002(
    host: str = "9.10.11.12", port: int = 8388, password: str = "pass"
) -> str:
    cred = base64.b64encode(f"aes-256-gcm:{password}".encode()).decode()
    return f"ss://{cred}@{host}:{port}"


# ---------------------------------------------------------------------------
# _safe_b64decode
# ---------------------------------------------------------------------------


class TestSafeB64Decode:
    def test_valid_padded(self):
        raw = base64.b64encode(b"hello world").decode()
        assert _safe_b64decode(raw) == "hello world"

    def test_valid_unpadded(self):
        # Strip padding
        raw = base64.b64encode(b"hello world").decode().rstrip("=")
        assert _safe_b64decode(raw) == "hello world"

    def test_invalid_returns_empty(self):
        result = _safe_b64decode("!!!not valid base64!!!")
        # Should not raise; returns empty or partial string
        assert isinstance(result, str)

    def test_whitespace_stripped(self):
        raw = "  " + base64.b64encode(b"hi").decode() + "  "
        assert _safe_b64decode(raw) == "hi"


# ---------------------------------------------------------------------------
# _parse_vmess
# ---------------------------------------------------------------------------


class TestParseVmess:
    def test_happy_path(self):
        cfg = _vmess_config()
        result = normalize_server(cfg, source_url="http://src", source_type="http")
        assert result is not None
        assert result.protocol == "vmess"
        assert result.host == "1.2.3.4"
        assert result.port == 443
        assert result.tls is True

    def test_tls_reality(self):
        cfg = _vmess_config(tls="reality")
        result = normalize_server(cfg)
        assert result is not None
        assert result.tls is True

    def test_no_tls(self):
        cfg = _vmess_config(tls="")
        result = normalize_server(cfg)
        assert result is not None
        assert result.tls is False

    def test_missing_host_returns_none(self):
        payload = json.dumps({"port": 443, "id": "x", "tls": "tls"})
        cfg = "vmess://" + base64.b64encode(payload.encode()).decode()
        assert normalize_server(cfg) is None

    def test_missing_port_returns_none(self):
        payload = json.dumps({"add": "1.2.3.4", "id": "x", "tls": "tls"})
        cfg = "vmess://" + base64.b64encode(payload.encode()).decode()
        assert normalize_server(cfg) is None

    def test_bad_json_returns_none(self):
        cfg = "vmess://" + base64.b64encode(b"not-json").decode()
        assert normalize_server(cfg) is None

    def test_port_with_comma_parsed(self):
        """Some configs encode port as '443,0' — only the first part should be used."""
        payload = json.dumps(
            {"add": "1.2.3.4", "port": "443,0", "id": "x", "tls": "tls"}
        )
        cfg = "vmess://" + base64.b64encode(payload.encode()).decode()
        result = normalize_server(cfg)
        assert result is not None
        assert result.port == 443


# ---------------------------------------------------------------------------
# _parse_vless
# ---------------------------------------------------------------------------


class TestParseVless:
    def test_happy_path(self):
        cfg = _vless_config()
        result = normalize_server(cfg)
        assert result is not None
        assert result.protocol == "vless"
        assert result.host == "1.2.3.4"
        assert result.port == 8443
        assert result.tls is True

    def test_reality_security(self):
        cfg = _vless_config(security="reality")
        result = normalize_server(cfg)
        assert result is not None
        assert result.tls is True

    def test_no_security(self):
        cfg = _vless_config(security="none")
        result = normalize_server(cfg)
        assert result is not None
        assert result.tls is False

    def test_missing_port_returns_none(self):
        cfg = "vless://uuid@host.example?security=tls"
        assert normalize_server(cfg) is None


# ---------------------------------------------------------------------------
# _parse_trojan
# ---------------------------------------------------------------------------


class TestParseTrojan:
    def test_happy_path(self):
        cfg = _trojan_config()
        result = normalize_server(cfg)
        assert result is not None
        assert result.protocol == "trojan"
        assert result.host == "5.6.7.8"
        assert result.port == 443

    def test_missing_host_returns_none(self):
        cfg = "trojan://pass@:443?security=tls"
        assert normalize_server(cfg) is None


# ---------------------------------------------------------------------------
# _parse_ss
# ---------------------------------------------------------------------------


class TestParseSs:
    def test_sip002_format(self):
        cfg = _ss_sip002()
        result = normalize_server(cfg)
        assert result is not None
        assert result.protocol == "ss"
        assert result.host == "9.10.11.12"
        assert result.port == 8388

    def test_legacy_base64_format(self):
        decoded = "aes-256-gcm:password@10.0.0.1:8388"
        cfg = "ss://" + base64.b64encode(decoded.encode()).decode()
        result = normalize_server(cfg)
        assert result is not None
        assert result.protocol == "ss"
        assert result.host == "10.0.0.1"
        assert result.port == 8388

    def test_garbage_returns_none(self):
        assert normalize_server("ss://totalgarbage!!!") is None


# ---------------------------------------------------------------------------
# normalize_server dispatch
# ---------------------------------------------------------------------------


class TestNormalizeServer:
    def test_unknown_scheme_returns_none(self):
        assert normalize_server("http://something") is None

    def test_empty_string_returns_none(self):
        assert normalize_server("") is None

    def test_whitespace_only_returns_none(self):
        assert normalize_server("   ") is None

    def test_source_fields_stored(self):
        cfg = _vless_config()
        result = normalize_server(cfg, source_url="https://src", source_type="telegram")
        assert result is not None
        assert result.source_url == "https://src"
        assert result.source_type == "telegram"


# ---------------------------------------------------------------------------
# NormalizedServer.structural_key
# ---------------------------------------------------------------------------


class TestStructuralKey:
    def test_deterministic(self):
        cfg = _vmess_config()
        a = normalize_server(cfg)
        b = normalize_server(cfg)
        assert a is not None and b is not None
        assert a.structural_key == b.structural_key

    def test_case_insensitive_host(self):
        cfg_lower = _vless_config(host="host.example")
        cfg_upper = _vless_config(host="HOST.EXAMPLE")
        a = normalize_server(cfg_lower)
        b = normalize_server(cfg_upper)
        assert a is not None and b is not None
        assert a.structural_key == b.structural_key

    def test_different_hosts_different_keys(self):
        a = normalize_server(_vless_config(host="1.1.1.1"))
        b = normalize_server(_vless_config(host="2.2.2.2"))
        assert a is not None and b is not None
        assert a.structural_key != b.structural_key

    def test_length_is_16(self):
        result = normalize_server(_vmess_config())
        assert result is not None
        assert len(result.structural_key) == 16


# ---------------------------------------------------------------------------
# deduplicate_servers
# ---------------------------------------------------------------------------


class TestDeduplicateServers:
    def test_removes_exact_duplicates(self):
        cfg = _vmess_config()
        result, removed = deduplicate_servers([cfg, cfg, cfg])
        assert len(result) == 1
        assert removed == 2

    def test_keeps_different_servers(self):
        a = _vmess_config(host="1.1.1.1")
        b = _vmess_config(host="2.2.2.2")
        result, removed = deduplicate_servers([a, b])
        assert len(result) == 2
        assert removed == 0

    def test_empty_input(self):
        result, removed = deduplicate_servers([])
        assert result == []
        assert removed == 0

    def test_unknown_protocol_kept_once(self):
        raw = "socks5://1.2.3.4:1080"
        result, removed = deduplicate_servers([raw, raw])
        assert len(result) == 1

    def test_structural_dedup_same_server_different_encoding(self):
        """Two vmess configs with identical fields but different JSON key order."""
        host = "1.2.3.4"
        port = 443
        uuid = "test-uuid-1234"
        payload_a = json.dumps({"add": host, "port": port, "id": uuid, "tls": "tls"})
        payload_b = json.dumps({"id": uuid, "tls": "tls", "add": host, "port": port})
        cfg_a = "vmess://" + base64.b64encode(payload_a.encode()).decode()
        cfg_b = "vmess://" + base64.b64encode(payload_b.encode()).decode()
        result, removed = deduplicate_servers([cfg_a, cfg_b])
        assert len(result) == 1
        assert removed == 1


# ---------------------------------------------------------------------------
# deduplicate_across_sources
# ---------------------------------------------------------------------------


class TestDeduplicateAcrossSources:
    def test_unique_servers_per_source(self):
        src_a = {"http://src-a": [_vmess_config(host="1.1.1.1")]}
        src_b = {"http://src-b": [_vmess_config(host="2.2.2.2")]}
        servers_by_source = {**src_a, **src_b}
        unique, ratios = deduplicate_across_sources(servers_by_source)
        assert len(unique) == 2
        assert ratios["http://src-a"] == 0.0
        assert ratios["http://src-b"] == 0.0

    def test_shared_server_increases_overlap(self):
        shared = _vmess_config(host="shared.host")
        servers_by_source = {
            "http://src-a": [shared],
            "http://src-b": [shared],
        }
        unique, ratios = deduplicate_across_sources(servers_by_source)
        assert len(unique) == 1
        assert ratios["http://src-a"] > 0.0
        assert ratios["http://src-b"] > 0.0

    def test_empty_sources(self):
        unique, ratios = deduplicate_across_sources({})
        assert unique == []
        assert ratios == {}

    def test_overlap_ratio_bounded(self):
        shared = _vmess_config(host="shared.host")
        servers_by_source = {
            "http://a": [shared, _vmess_config(host="1.1.1.1")],
            "http://b": [shared],
        }
        unique, ratios = deduplicate_across_sources(servers_by_source)
        for ratio in ratios.values():
            assert 0.0 <= ratio <= 1.0
