"""Unit tests for xray_config_adapter.py (Layer 2 — ConfigAdapter).

These tests run entirely offline — no xray binary, no network.
All protocol parsing and config generation is pure Python.
"""

from __future__ import annotations

import base64
import json
import os

import pytest

from v2ray_finder.xray_config_adapter import ConfigAdapter, UnsupportedProtocolError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _vmess_uri(overrides: dict | None = None) -> str:
    """Build a minimal vmess:// URI."""
    payload = {
        "v": "2",
        "ps": "test-server",
        "add": "1.2.3.4",
        "port": "443",
        "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "aid": "0",
        "scy": "auto",
        "net": "tcp",
        "type": "none",
        "host": "",
        "path": "/",
        "tls": "tls",
        "sni": "",
        "alpn": "",
    }
    if overrides:
        payload.update(overrides)
    encoded = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()
    return f"vmess://{encoded}"


def _vless_uri() -> str:
    return (
        "vless://aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee@1.2.3.4:443"
        "?encryption=none&security=tls&type=ws&host=example.com&path=%2Fws"
        "#test-vless"
    )


def _trojan_uri() -> str:
    return "trojan://s3cr3t@1.2.3.4:443?security=tls&sni=example.com#test-trojan"


def _ss_uri() -> str:
    userinfo = base64.b64encode(b"aes-256-gcm:password123").decode()
    return f"ss://{userinfo}@1.2.3.4:8388#test-ss"


# ---------------------------------------------------------------------------
# ConfigAdapter tests
# ---------------------------------------------------------------------------


@pytest.fixture
def adapter() -> ConfigAdapter:
    return ConfigAdapter(log_level="none")


class TestBuildConfigVmess:
    def test_basic_fields(self, adapter: ConfigAdapter):
        cfg = adapter.build_config(_vmess_uri(), socks_port=10800)
        out = cfg["outbounds"][0]
        assert out["protocol"] == "vmess"
        settings = out["settings"]["vnext"][0]
        assert settings["address"] == "1.2.3.4"
        assert settings["port"] == 443

    def test_socks_inbound(self, adapter: ConfigAdapter):
        cfg = adapter.build_config(_vmess_uri(), socks_port=10801)
        inbound = cfg["inbounds"][0]
        assert inbound["protocol"] == "socks"
        assert inbound["port"] == 10801

    def test_log_level_none(self, adapter: ConfigAdapter):
        cfg = adapter.build_config(_vmess_uri(), socks_port=10802)
        assert cfg["log"]["loglevel"] == "none"


class TestBuildConfigVless:
    def test_basic_fields(self, adapter: ConfigAdapter):
        cfg = adapter.build_config(_vless_uri(), socks_port=10810)
        out = cfg["outbounds"][0]
        assert out["protocol"] == "vless"
        settings = out["settings"]["vnext"][0]
        assert settings["address"] == "1.2.3.4"
        assert settings["port"] == 443


class TestBuildConfigTrojan:
    def test_basic_fields(self, adapter: ConfigAdapter):
        cfg = adapter.build_config(_trojan_uri(), socks_port=10820)
        out = cfg["outbounds"][0]
        assert out["protocol"] == "trojan"
        settings = out["settings"]["servers"][0]
        assert settings["address"] == "1.2.3.4"
        assert settings["port"] == 443


class TestBuildConfigShadowsocks:
    def test_basic_fields(self, adapter: ConfigAdapter):
        cfg = adapter.build_config(_ss_uri(), socks_port=10830)
        out = cfg["outbounds"][0]
        assert out["protocol"] == "shadowsocks"
        settings = out["settings"]["servers"][0]
        assert settings["address"] == "1.2.3.4"
        assert settings["port"] == 8388


class TestUnsupportedProtocol:
    def test_raises(self, adapter: ConfigAdapter):
        with pytest.raises(UnsupportedProtocolError):
            adapter.build_config("ssr://somebase64data==", socks_port=10840)


class TestBuildConfigFile:
    def test_file_exists_inside_context(self, adapter: ConfigAdapter):
        with adapter.build_config_file(_vmess_uri(), socks_port=10850) as path:
            assert os.path.exists(path)
            with open(path) as f:
                data = json.load(f)
            assert "inbounds" in data
            assert "outbounds" in data

    def test_file_deleted_after_exit(self, adapter: ConfigAdapter):
        with adapter.build_config_file(_vmess_uri(), socks_port=10851) as path:
            tmp_path = path
        assert not os.path.exists(tmp_path)

    def test_socks_port_in_inbound(self, adapter: ConfigAdapter):
        with adapter.build_config_file(_vmess_uri(), socks_port=19999) as path:
            with open(path) as f:
                data = json.load(f)
        assert data["inbounds"][0]["port"] == 19999
