"""
ProxyParser: Parses modern proxy protocol URI schemes into structured objects.
Supports VLESS (REALITY), VMess, Trojan, Shadowsocks (SS), Hysteria2, TUIC.
"""
from __future__ import annotations

import base64
import json
import re
import logging
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import parse_qs, unquote, urlparse

logger = logging.getLogger(__name__)


@dataclass
class ParsedProxy:
    protocol: str
    uuid: str = ""
    password: str = ""
    host: str = ""
    port: int = 0
    path: str = ""
    host_header: str = ""
    sni: str = ""
    fingerprint: str = ""
    public_key: str = ""
    short_id: str = ""
    spider_x: str = ""
    transport: str = "tcp"
    security: str = "none"
    flow: str = ""
    alpn: str = ""
    service_name: str = ""
    mode: str = ""
    congestion: str = ""
    allow_insecure: bool = False
    raw: str = ""
    extra: dict = field(default_factory=dict)

    @property
    def display_name(self) -> str:
        """Human-readable label for this proxy."""
        label = (
            self.extra.get("name")
            or self.extra.get("ps")
            or self.sni
            or self.host
            or "Unknown"
        )
        return f"[{self.protocol.upper()}] {label}"

    @property
    def tags(self) -> list[str]:
        """Extract tags from remarks or hash fragment."""
        name = self.extra.get("name", "") or self.extra.get("ps", "") or ""
        tags: list[str] = []
        if "剩余" in name:
            tags.append("has-traffic-info")
        if "回国" in name or "中国" in name:
            tags.append("china-route")
        return tags


class ProxyParser:
    """Parses proxy URIs into ParsedProxy objects."""

    @staticmethod
    def parse(raw: str) -> Optional[ParsedProxy]:
        """Parse a single proxy URI string. Returns None on failure."""
        raw = raw.strip()
        if not raw:
            return None

        try:
            if raw.startswith("vless://"):
                return ProxyParser._parse_vless(raw)
            elif raw.startswith("vmess://"):
                return ProxyParser._parse_vmess(raw)
            elif raw.startswith("trojan://"):
                return ProxyParser._parse_trojan(raw)
            elif raw.startswith("ss://"):
                return ProxyParser._parse_ss(raw)
            elif raw.startswith("hy2://") or raw.startswith("hysteria2://"):
                return ProxyParser._parse_hysteria2(raw)
            elif raw.startswith("tuic://"):
                return ProxyParser._parse_tuic(raw)
            else:
                logger.warning("Unknown protocol: %s", raw[:30])
                return None
        except Exception as exc:
            logger.warning("Parse error for %s: %s", raw[:60], exc)
            return None

    # ------------------------------------------------------------------
    # VLESS: vless://<uuid>@<host>:<port>?<params>#<name>
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_vless(raw: str) -> ParsedProxy:
        parsed = urlparse(raw)
        uuid = parsed.username or ""
        host = parsed.hostname or ""
        port = parsed.port or 443
        qs = parse_qs(parsed.query)
        fragment = unquote(parsed.fragment) if parsed.fragment else ""

        proxy = ParsedProxy(
            protocol="vless",
            uuid=uuid,
            host=host,
            port=port,
            raw=raw,
            transport=_first(qs, "type", "tcp"),
            security=_first(qs, "security", "none"),
            flow=_first(qs, "flow", ""),
            sni=_first(qs, "sni", host),
            fingerprint=_first(qs, "fp", ""),
            public_key=_first(qs, "pbk", ""),
            short_id=_first(qs, "sid", ""),
            spider_x=_first(qs, "spx", ""),
            path=_first(qs, "path", ""),
            host_header=_first(qs, "host", ""),
            alpn=_first(qs, "alpn", ""),
            service_name=_first(qs, "serviceName", ""),
            mode=_first(qs, "mode", ""),
            allow_insecure=_bool_first(qs, "allowInsecure"),
            extra={"name": fragment, **{k: v[0] for k, v in qs.items()}},
        )

        # Detect REALITY
        if proxy.security == "reality":
            proxy.extra["is_reality"] = True

        return proxy

    # ------------------------------------------------------------------
    # VMess: vmess://<base64-json>  or  vmess://<uuid>@<host>:<port>?... (legacy)
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_vmess(raw: str) -> ParsedProxy:
        body = raw[len("vmess://"):]

        # Try Base64 JSON (most common)
        try:
            padded = body
            missing = len(padded) % 4
            if missing:
                padded += "=" * (4 - missing)
            decoded = base64.b64decode(padded).decode("utf-8")
            cfg = json.loads(decoded)
            return ParsedProxy(
                protocol="vmess",
                uuid=cfg.get("id", ""),
                host=cfg.get("add", ""),
                port=int(cfg.get("port", 0)),
                path=cfg.get("path", ""),
                host_header=cfg.get("host", ""),
                sni=cfg.get("sni", cfg.get("host", "")),
                fingerprint=cfg.get("fp", ""),
                transport=cfg.get("net", "tcp"),
                security=cfg.get("tls", "none"),
                alpn=cfg.get("alpn", ""),
                allow_insecure=cfg.get("allowInsecure", False),
                raw=raw,
                extra={
                    "name": cfg.get("ps", ""),
                    "aid": cfg.get("aid", 0),
                    "type": cfg.get("type", "none"),
                    "v": cfg.get("v", 2),
                },
            )
        except Exception:
            pass

        # Fallback: legacy URI format
        parsed = urlparse(raw.replace("vmess://", "http://"))
        qs = parse_qs(parsed.query)
        fragment = unquote(parsed.fragment) if parsed.fragment else ""
        return ParsedProxy(
            protocol="vmess",
            uuid=fragment or "",
            host=parsed.hostname or "",
            port=parsed.port or 0,
            raw=raw,
            extra={},
        )

    # ------------------------------------------------------------------
    # Trojan: trojan://<password>@<host>:<port>?<params>#<name>
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_trojan(raw: str) -> ParsedProxy:
        parsed = urlparse(raw)
        qs = parse_qs(parsed.query)
        fragment = unquote(parsed.fragment) if parsed.fragment else ""

        return ParsedProxy(
            protocol="trojan",
            password=parsed.username or "",
            host=parsed.hostname or "",
            port=parsed.port or 443,
            sni=_first(qs, "sni", parsed.hostname or ""),
            fingerprint=_first(qs, "fp", ""),
            alpn=_first(qs, "alpn", ""),
            path=_first(qs, "path", ""),
            transport=_first(qs, "type", "tcp"),
            security=_first(qs, "security", "tls"),
            allow_insecure=_bool_first(qs, "allowInsecure"),
            raw=raw,
            extra={"name": fragment, **{k: v[0] for k, v in qs.items()}},
        )

    # ------------------------------------------------------------------
    # Shadowsocks: ss://<base64(method:password)>@<host>:<port>  or
    #              ss://<base64(method:password@host:port)>#<name>
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_ss(raw: str) -> ParsedProxy:
        no_scheme = raw[len("ss://"):]

        # SIP002 format: ss://<base64>@<host>:<port>?plugin=...#name
        if "@" in no_scheme:
            b64_part, rest = no_scheme.split("@", 1)
            try:
                padded = b64_part
                missing = len(padded) % 4
                if missing:
                    padded += "=" * (4 - missing)
                userinfo = base64.b64decode(padded).decode("utf-8")
                method, _, password = userinfo.partition(":")
            except Exception:
                method, password = "aes-256-gcm", ""

            host_port = rest.split("?")[0].split("#")[0]
            host, _, port_str = host_port.partition(":")
            port = int(port_str) if port_str else 8388

            parsed_qs = parse_qs(rest.split("?")[1] if "?" in rest else "")
            fragment = ""
            if "#" in rest:
                fragment = unquote(rest.split("#", 1)[1])

            return ParsedProxy(
                protocol="shadowsocks",
                password=password,
                host=host,
                port=port,
                raw=raw,
                extra={
                    "name": fragment,
                    "method": method,
                    "plugin": _first(parsed_qs, "plugin", ""),
                },
            )

        # Legacy Base64 format: ss://<base64(method:password@host:port)>
        try:
            padded = no_scheme
            missing = len(padded) % 4
            if missing:
                padded += "=" * (4 - missing)
            decoded = base64.b64decode(padded).decode("utf-8")
            # decoded = method:password@host:port or method:password@host:port#name
            userinfo_host, _, name = decoded.partition("#")
            userinfo, _, host_port = userinfo_host.rpartition("@")
            method, _, password = userinfo.partition(":")
            host, _, port_str = host_port.partition(":")
            port = int(port_str) if port_str else 8388
            return ParsedProxy(
                protocol="shadowsocks",
                password=password,
                host=host,
                port=port,
                raw=raw,
                extra={"name": name, "method": method},
            )
        except Exception:
            pass

        return ParsedProxy(protocol="shadowsocks", raw=raw)

    # ------------------------------------------------------------------
    # Hysteria2: hy2://<password>@<host>:<port>?<params>#<name>
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_hysteria2(raw: str) -> ParsedProxy:
        scheme = "hysteria2://"
        if raw.startswith("hy2://"):
            scheme = "hy2://"
        normalized = raw.replace(scheme, "http://")
        parsed = urlparse(normalized)
        qs = parse_qs(parsed.query)
        fragment = unquote(parsed.fragment) if parsed.fragment else ""

        return ParsedProxy(
            protocol="hysteria2",
            password=parsed.username or "",
            host=parsed.hostname or "",
            port=parsed.port or 443,
            sni=_first(qs, "sni", parsed.hostname or ""),
            allow_insecure=_bool_first(qs, "insecure"),
            congestion=_first(qs, "obfs", ""),
            raw=raw,
            extra={"name": fragment, **{k: v[0] for k, v in qs.items()}},
        )

    # ------------------------------------------------------------------
    # TUIC: tuic://<uuid>:<password>@<host>:<port>?<params>#<name>
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_tuic(raw: str) -> ParsedProxy:
        parsed = urlparse(raw)
        qs = parse_qs(parsed.query)
        fragment = unquote(parsed.fragment) if parsed.fragment else ""

        # TUIC uses uuid:password as userinfo
        userinfo = parsed.username or ""
        uuid = userinfo
        password = ""
        if ":" in userinfo:
            uuid, _, password = userinfo.partition(":")

        return ParsedProxy(
            protocol="tuic",
            uuid=uuid,
            password=password,
            host=parsed.hostname or "",
            port=parsed.port or 443,
            sni=_first(qs, "sni", parsed.hostname or ""),
            alpn=_first(qs, "alpn", ""),
            fingerprint=_first(qs, "fp", ""),
            congestion=_first(qs, "congestion_control", ""),
            allow_insecure=_bool_first(qs, "allow_insecure"),
            raw=raw,
            extra={"name": fragment, **{k: v[0] for k, v in qs.items()}},
        )


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _first(qs: dict, key: str, default: str = "") -> str:
    vals = qs.get(key, [])
    return vals[0] if vals else default


def _bool_first(qs: dict, key: str) -> bool:
    v = _first(qs, key, "0").lower()
    return v in ("1", "true", "yes")