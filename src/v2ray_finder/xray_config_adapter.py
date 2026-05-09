"""Convert v2ray/xray URI strings to xray JSON config.

Supported URI schemes: vmess, vless, trojan, ss (shadowsocks).
The generated config uses a SOCKS5 inbound on 127.0.0.1:<local_port>
so that other tools (xray_connectivity.py) can route HTTP through it.
"""

from __future__ import annotations

import base64
import json
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def _decode_vmess(uri_body: str) -> Optional[Dict[str, Any]]:
    """Decode a vmess:// URI body (base64-encoded JSON)."""
    try:
        padded = uri_body + "==" * (4 - len(uri_body) % 4)
        return json.loads(base64.b64decode(padded).decode("utf-8", errors="replace"))
    except Exception as exc:
        logger.debug("vmess decode failed: %s", exc)
        return None


def _socks_inbound(local_port: int) -> Dict:
    return {
        "tag": "socks-in",
        "port": local_port,
        "listen": "127.0.0.1",
        "protocol": "socks",
        "settings": {"auth": "noauth", "udp": False},
        "sniffing": {"enabled": False},
    }


def _base_config(local_port: int) -> Dict:
    return {
        "log": {"loglevel": "none"},
        "inbounds": [_socks_inbound(local_port)],
        "outbounds": [],
        "routing": {
            "domainStrategy": "IPIfNonMatch",
            "rules": [
                {"type": "field", "outboundTag": "proxy", "network": "tcp,udp"}
            ],
        },
    }


def vmess_to_xray(uri: str, local_port: int = 10808) -> Optional[Dict]:
    """Return an xray config dict for a vmess:// URI."""
    body = uri[len("vmess://"):]
    data = _decode_vmess(body)
    if data is None:
        return None

    tls_settings: Dict = {}
    stream_settings: Dict = {
        "network": data.get("net", "tcp"),
        "security": data.get("tls", "none"),
    }
    if data.get("tls") == "tls":
        stream_settings["tlsSettings"] = {
            "serverName": data.get("sni") or data.get("add", ""),
            "allowInsecure": False,
        }
    if data.get("net") == "ws":
        stream_settings["wsSettings"] = {
            "path": data.get("path", "/"),
            "headers": {"Host": data.get("host", "")},
        }

    outbound = {
        "tag": "proxy",
        "protocol": "vmess",
        "settings": {
            "vnext": [
                {
                    "address": data.get("add", ""),
                    "port": int(data.get("port", 443)),
                    "users": [
                        {
                            "id": data.get("id", ""),
                            "alterId": int(data.get("aid", 0)),
                            "security": data.get("scy", "auto"),
                        }
                    ],
                }
            ]
        },
        "streamSettings": stream_settings,
    }

    cfg = _base_config(local_port)
    cfg["outbounds"].append(outbound)
    return cfg


def vless_to_xray(uri: str, local_port: int = 10808) -> Optional[Dict]:
    """Return an xray config dict for a vless:// URI."""
    from urllib.parse import urlparse, parse_qs

    try:
        parsed = urlparse(uri)
        uuid = parsed.username or ""
        host = parsed.hostname or ""
        port = parsed.port or 443
        qs = parse_qs(parsed.query)

        security = qs.get("security", ["none"])[0]
        flow = qs.get("flow", [""])[0]
        sni = qs.get("sni", [host])[0]
        fp = qs.get("fp", [""])[0]
        net = qs.get("type", ["tcp"])[0]
        path = qs.get("path", ["/"])[0]
        hdr_host = qs.get("host", [""])[0]

        stream_settings: Dict = {"network": net, "security": security}
        if security == "tls":
            stream_settings["tlsSettings"] = {
                "serverName": sni,
                "allowInsecure": False,
                "fingerprint": fp,
            }
        elif security == "reality":
            pub_key = qs.get("pbk", [""])[0]
            short_id = qs.get("sid", [""])[0]
            spider_x = qs.get("spx", ["/"])[0]
            stream_settings["realitySettings"] = {
                "serverName": sni,
                "fingerprint": fp,
                "publicKey": pub_key,
                "shortId": short_id,
                "spiderX": spider_x,
            }
        if net == "ws":
            stream_settings["wsSettings"] = {
                "path": path,
                "headers": {"Host": hdr_host},
            }
        elif net == "grpc":
            stream_settings["grpcSettings"] = {
                "serviceName": qs.get("serviceName", [""])[0]
            }

        outbound = {
            "tag": "proxy",
            "protocol": "vless",
            "settings": {
                "vnext": [
                    {
                        "address": host,
                        "port": port,
                        "users": [{"id": uuid, "flow": flow, "encryption": "none"}],
                    }
                ]
            },
            "streamSettings": stream_settings,
        }
        cfg = _base_config(local_port)
        cfg["outbounds"].append(outbound)
        return cfg
    except Exception as exc:
        logger.debug("vless parse failed: %s", exc)
        return None


def trojan_to_xray(uri: str, local_port: int = 10808) -> Optional[Dict]:
    """Return an xray config dict for a trojan:// URI."""
    from urllib.parse import urlparse, parse_qs

    try:
        parsed = urlparse(uri)
        password = parsed.username or ""
        host = parsed.hostname or ""
        port = parsed.port or 443
        qs = parse_qs(parsed.query)
        sni = qs.get("sni", [host])[0]
        fp = qs.get("fp", [""])[0]
        security = qs.get("security", ["tls"])[0]

        stream_settings: Dict = {
            "network": "tcp",
            "security": security,
            "tlsSettings": {
                "serverName": sni,
                "allowInsecure": False,
                "fingerprint": fp,
            },
        }

        outbound = {
            "tag": "proxy",
            "protocol": "trojan",
            "settings": {
                "servers": [
                    {"address": host, "port": port, "password": password}
                ]
            },
            "streamSettings": stream_settings,
        }
        cfg = _base_config(local_port)
        cfg["outbounds"].append(outbound)
        return cfg
    except Exception as exc:
        logger.debug("trojan parse failed: %s", exc)
        return None


def ss_to_xray(uri: str, local_port: int = 10808) -> Optional[Dict]:
    """Return an xray config dict for a ss:// (Shadowsocks) URI."""
    from urllib.parse import urlparse
    import base64

    try:
        # ss://BASE64(method:password)@host:port#tag
        # OR ss://method:password@host:port
        parsed = urlparse(uri)
        host = parsed.hostname or ""
        port = parsed.port or 8388

        if parsed.username and not parsed.password:
            # Old-style: ss://base64@host:port
            padded = parsed.username + "==" * (4 - len(parsed.username) % 4)
            user_info = base64.b64decode(padded).decode("utf-8", errors="replace")
            method, password = user_info.split(":", 1)
        elif parsed.username and parsed.password:
            method = parsed.username
            password = parsed.password
        else:
            return None

        outbound = {
            "tag": "proxy",
            "protocol": "shadowsocks",
            "settings": {
                "servers": [
                    {
                        "address": host,
                        "port": port,
                        "method": method,
                        "password": password,
                        "uot": False,
                    }
                ]
            },
        }
        cfg = _base_config(local_port)
        cfg["outbounds"].append(outbound)
        return cfg
    except Exception as exc:
        logger.debug("ss parse failed: %s", exc)
        return None


def config_to_xray(
    uri: str,
    local_port: int = 10808,
) -> Optional[Dict]:
    """Dispatch URI to the correct converter.  Returns None on failure."""
    uri = uri.strip()
    if uri.startswith("vmess://"):
        return vmess_to_xray(uri, local_port)
    if uri.startswith("vless://"):
        return vless_to_xray(uri, local_port)
    if uri.startswith("trojan://"):
        return trojan_to_xray(uri, local_port)
    if uri.startswith("ss://"):
        return ss_to_xray(uri, local_port)
    logger.debug("Unsupported scheme: %s", uri[:20])
    return None
