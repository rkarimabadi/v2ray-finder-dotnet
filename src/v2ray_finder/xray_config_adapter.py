"""Layer 2 — ConfigAdapter: convert raw v2ray config strings to xray JSON.

This module is the bridge between the raw config strings discovered by
the collection pipeline and the xray binary managed by
:mod:`xray_runner`.  It produces a complete, minimal xray-core JSON
config that the runner can pass directly to ``xray run -c``.

Design principles
-----------------
* **No network I/O** — pure data transformation, fully unit-testable.
* **Reuse** :class:`~v2ray_finder.normalizer.NormalizedServer` parsing;
  we do *not* duplicate the base64/URL parsing logic.
* **Minimal outbound config** — only the fields xray strictly requires
  are included; omitting optional fields keeps configs readable and
  avoids version-specific compatibility issues.
* **Inbound is always SOCKS5 on localhost** — the port is chosen by the
  caller (typically :func:`~v2ray_finder.xray_runner.find_free_port`).

Typical usage
-------------
::

    adapter = ConfigAdapter()
    with adapter.build_config_file(raw_config, socks_port=10800) as path:
        async with manager.run(path, socks_port=10800) as proc:
            ...
"""

from __future__ import annotations

import json
import logging
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Generator, Optional

from v2ray_finder.normalizer import NormalizedServer, normalize_server

logger = logging.getLogger(__name__)


class UnsupportedProtocolError(ValueError):
    """Raised when a config string uses a protocol we cannot adapt."""


class ConfigAdapter:
    """Convert raw v2ray config strings into xray-core JSON config dicts.

    Parameters
    ----------
    log_level:
        xray log level written into the generated config.
        ``'none'`` suppresses all xray output (recommended for batch
        checks to avoid log noise).
    """

    def __init__(self, log_level: str = "none") -> None:
        self.log_level = log_level

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build_config(
        self,
        raw_config: str,
        socks_port: int,
    ) -> Dict[str, Any]:
        """Return a complete xray JSON config dict for *raw_config*.

        Parameters
        ----------
        raw_config:
            A single vmess://, vless://, trojan://, or ss:// string.
        socks_port:
            The local port xray should listen on as a SOCKS5 inbound.

        Raises
        ------
        UnsupportedProtocolError
            When the protocol is unknown or parsing fails.
        """
        ns = normalize_server(raw_config)
        if ns is None:
            raise UnsupportedProtocolError(
                f"Cannot parse config: {raw_config[:80]}"
            )
        outbound = self._build_outbound(ns)
        return self._wrap_config(outbound, socks_port)

    @contextmanager
    def build_config_file(
        self,
        raw_config: str,
        socks_port: int,
    ) -> Generator[Path, None, None]:
        """Context manager: write config to a temp file and yield its path.

        The temp file is **deleted automatically** when the context exits.

        Example
        -------
        ::

            with adapter.build_config_file(raw_config, 10800) as cfg_path:
                async with manager.run(cfg_path, 10800) as proc:
                    ...
        """
        config = self.build_config(raw_config, socks_port)
        tmp = tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".json",
            delete=False,
            prefix="v2ray_finder_cfg_",
        )
        try:
            json.dump(config, tmp, ensure_ascii=False, indent=2)
            tmp.flush()
            tmp.close()
            logger.debug(f"[adapter] Wrote xray config to {tmp.name}")
            yield Path(tmp.name)
        finally:
            try:
                Path(tmp.name).unlink(missing_ok=True)
            except OSError:
                pass

    # ------------------------------------------------------------------
    # Outbound builders (one per protocol)
    # ------------------------------------------------------------------

    def _build_outbound(self, ns: NormalizedServer) -> Dict[str, Any]:
        builders = {
            "vmess": self._outbound_vmess,
            "vless": self._outbound_vless,
            "trojan": self._outbound_trojan,
            "ss": self._outbound_ss,
            "ssr": self._outbound_ss,
        }
        builder = builders.get(ns.protocol)
        if builder is None:
            raise UnsupportedProtocolError(
                f"Protocol '{ns.protocol}' is not supported by ConfigAdapter"
            )
        return builder(ns)

    @staticmethod
    def _tls_settings(ns: NormalizedServer) -> Optional[Dict[str, Any]]:
        """Return streamSettings TLS block when TLS is enabled, else None."""
        if not ns.tls:
            return None
        security = ns.extra.get("security", "tls")
        sni = ns.extra.get("sni") or ns.extra.get("host") or ns.host
        return {
            "security": security,
            "tlsSettings": {
                "serverName": sni,
                "allowInsecure": False,
            },
        }

    def _outbound_vmess(self, ns: NormalizedServer) -> Dict[str, Any]:
        vnext_user: Dict[str, Any] = {
            "id": ns.uuid_or_password or "",
            "alterId": int(ns.extra.get("aid", 0)),
            "security": ns.extra.get("scy") or ns.extra.get("security") or "auto",
        }
        stream: Dict[str, Any] = {
            "network": ns.extra.get("net", "tcp"),
        }
        tls = self._tls_settings(ns)
        if tls:
            stream.update(tls)
        # WebSocket path / host header
        net = ns.extra.get("net", "tcp")
        if net == "ws":
            stream["wsSettings"] = {
                "path": ns.extra.get("path", "/"),
                "headers": {"Host": ns.extra.get("host", ns.host)},
            }
        return {
            "tag": "proxy",
            "protocol": "vmess",
            "settings": {
                "vnext": [
                    {
                        "address": ns.host,
                        "port": ns.port,
                        "users": [vnext_user],
                    }
                ]
            },
            "streamSettings": stream,
        }

    def _outbound_vless(self, ns: NormalizedServer) -> Dict[str, Any]:
        user: Dict[str, Any] = {
            "id": ns.uuid_or_password or "",
            "encryption": "none",
        }
        flow = ns.extra.get("flow", "")
        if flow:
            user["flow"] = flow

        stream: Dict[str, Any] = {
            "network": ns.extra.get("type", "tcp"),
        }
        tls = self._tls_settings(ns)
        if tls:
            stream.update(tls)

        net = ns.extra.get("type", "tcp")
        if net == "ws":
            stream["wsSettings"] = {
                "path": ns.extra.get("path", "/"),
                "headers": {"Host": ns.extra.get("host", ns.host)},
            }
        elif net == "grpc":
            stream["grpcSettings"] = {
                "serviceName": ns.extra.get("serviceName", ""),
            }
        return {
            "tag": "proxy",
            "protocol": "vless",
            "settings": {
                "vnext": [
                    {
                        "address": ns.host,
                        "port": ns.port,
                        "users": [user],
                    }
                ]
            },
            "streamSettings": stream,
        }

    def _outbound_trojan(self, ns: NormalizedServer) -> Dict[str, Any]:
        stream: Dict[str, Any] = {"network": "tcp"}
        tls = self._tls_settings(ns)
        if tls:
            stream.update(tls)
        else:
            # Trojan requires TLS; enable it even when not explicitly flagged
            stream["security"] = "tls"
            stream["tlsSettings"] = {
                "serverName": ns.host,
                "allowInsecure": False,
            }
        return {
            "tag": "proxy",
            "protocol": "trojan",
            "settings": {
                "servers": [
                    {
                        "address": ns.host,
                        "port": ns.port,
                        "password": ns.uuid_or_password or "",
                    }
                ]
            },
            "streamSettings": stream,
        }

    @staticmethod
    def _outbound_ss(ns: NormalizedServer) -> Dict[str, Any]:
        method = (
            ns.extra.get("method")
            or ns.extra.get("cipher")
            or "chacha20-poly1305"
        )
        return {
            "tag": "proxy",
            "protocol": "shadowsocks",
            "settings": {
                "servers": [
                    {
                        "address": ns.host,
                        "port": ns.port,
                        "method": method,
                        "password": ns.uuid_or_password or "",
                    }
                ]
            },
        }

    # ------------------------------------------------------------------
    # Full config wrapper
    # ------------------------------------------------------------------

    def _wrap_config(
        self,
        outbound: Dict[str, Any],
        socks_port: int,
    ) -> Dict[str, Any]:
        """Wrap *outbound* in a minimal but complete xray config."""
        return {
            "log": {"loglevel": self.log_level},
            "inbounds": [
                {
                    "tag": "socks",
                    "port": socks_port,
                    "listen": "127.0.0.1",
                    "protocol": "socks",
                    "settings": {
                        "auth": "noauth",
                        "udp": False,
                    },
                }
            ],
            "outbounds": [
                outbound,
                {"tag": "direct", "protocol": "freedom"},
                {"tag": "block", "protocol": "blackhole"},
            ],
            "routing": {
                "domainStrategy": "AsIs",
                "rules": [
                    {
                        "type": "field",
                        "inboundTag": ["socks"],
                        "outboundTag": "proxy",
                    }
                ],
            },
        }
