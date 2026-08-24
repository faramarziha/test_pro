"""
SingBoxTester: Async concurrent proxy testing engine.

Uses asyncio with semaphore-based concurrency control, ephemeral sing-box
subprocesses (when available) or direct socket/HTTP tests for connectivity
validation. Measures real latency for each working node.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import shutil
import signal
import socket
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import aiohttp
from aiohttp_socks import ProxyConnector, ProxyType

from proxyhub.parser import ParsedProxy
from proxyhub.installer import find_singbox_sync, SingBoxInstaller

logger = logging.getLogger(__name__)

# Verification endpoints
VERIFY_URLS = [
    "https://www.gstatic.com/generate_204",
    "https://1.1.1.1/cdn-cgi/trace",
    "http://httpbin.org/ip",
]

# Concurrency & timeout settings
DEFAULT_SEMAPHORE = 50
CONNECT_TIMEOUT = 5.0
TOTAL_TIMEOUT = 8.0


@dataclass
class TestResult:
    proxy: ParsedProxy
    working: bool
    latency_ms: float = 0.0
    resolved_ip: str = ""
    error: str = ""


@dataclass
class BatchTestResult:
    results: list[TestResult] = field(default_factory=list)
    total: int = 0
    working: int = 0
    dead: int = 0
    elapsed_seconds: float = 0.0


class SingBoxTester:
    """Async proxy testing engine with configurable concurrency."""

    def __init__(
        self,
        concurrency: int = DEFAULT_SEMAPHORE,
        connect_timeout: float = CONNECT_TIMEOUT,
        singbox_path: Optional[str] = None,
        installer: Optional[SingBoxInstaller] = None,
    ):
        self._concurrency = concurrency
        self._connect_timeout = connect_timeout
        self._installer = installer or SingBoxInstaller()
        self._singbox_path = singbox_path or find_singbox_sync()
        self._semaphore: Optional[asyncio.Semaphore] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def test_all(
        self,
        proxies: list[ParsedProxy],
        progress_callback=None,
    ) -> BatchTestResult:
        """Test all proxies concurrently. Returns aggregated results."""
        self._semaphore = asyncio.Semaphore(self._concurrency)
        start = time.monotonic()

        tasks = [self._test_one(p) for p in proxies]
        results: list[TestResult] = []

        if progress_callback:
            completed = 0
            for coro in asyncio.as_completed(tasks):
                r = await coro
                results.append(r)
                completed += 1
                progress_callback(completed, len(tasks), r)
        else:
            results = await asyncio.gather(*tasks)

        elapsed = time.monotonic() - start
        working = sum(1 for r in results if r.working)

        return BatchTestResult(
            results=results,
            total=len(results),
            working=working,
            dead=len(results) - working,
            elapsed_seconds=round(elapsed, 2),
        )

    # ------------------------------------------------------------------
    # Single proxy test
    # ------------------------------------------------------------------

    async def _test_one(self, proxy: ParsedProxy) -> TestResult:
        async with self._semaphore:  # type: ignore[union-attr]
            return await self._do_test(proxy)

    async def _do_test(self, proxy: ParsedProxy) -> TestResult:
        """Dispatch to the appropriate test method based on protocol."""
        proto = proxy.protocol

        if proto in ("shadowsocks", "trojan", "vless", "vmess", "hysteria2", "tuic"):
            # Lazy-init: try downloading sing-box on first real-protocol test
            if not self._singbox_path:
                self._singbox_path = await self._installer.ensure_installed()
            if self._singbox_path:
                return await self._test_via_singbox(proxy)
            else:
                return await self._test_tcp_fallback(proxy)
        else:
            return await self._test_tcp_fallback(proxy)

    # ------------------------------------------------------------------
    # sing-box subprocess testing
    # ------------------------------------------------------------------

    async def _test_via_singbox(self, proxy: ParsedProxy) -> TestResult:
        """Spin up sing-box with an ephemeral config, test, then tear down."""
        local_port = self._find_free_port()
        config_path: Optional[str] = None
        process: Optional[subprocess.Popen] = None

        try:
            # Generate sing-box config
            sb_config = self._build_singbox_config(proxy, local_port)
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", delete=False
            ) as f:
                json.dump(sb_config, f, indent=2)
                config_path = f.name

            # Launch sing-box
            process = subprocess.Popen(
                [self._singbox_path, "run", "-c", config_path],  # type: ignore[union-attr]
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                preexec_fn=os.setsid if os.name != "nt" else None,
            )

            # Wait for listener readiness
            await asyncio.sleep(0.5)

            # Run HTTP test through the local SOCKS5 listener
            latency, ip = await self._http_test_via_socks(local_port)

            if latency > 0:
                return TestResult(
                    proxy=proxy,
                    working=True,
                    latency_ms=round(latency * 1000, 1),
                    resolved_ip=ip,
                )
            else:
                return TestResult(
                    proxy=proxy, working=False, error="HTTP test failed through SOCKS"
                )

        except Exception as exc:
            return TestResult(proxy=proxy, working=False, error=str(exc)[:120])
        finally:
            self._kill_process(process)
            if config_path:
                try:
                    os.unlink(config_path)
                except OSError:
                    pass

    def _build_singbox_config(
        self, proxy: ParsedProxy, local_port: int
    ) -> dict:
        """Build a minimal sing-box config with one outbound + mixed inbound."""
        outbound = self._make_outbound(proxy)
        return {
            "log": {"level": "error"},
            "inbounds": [
                {
                    "type": "mixed",
                    "tag": "mixed-in",
                    "listen": "127.0.0.1",
                    "listen_port": local_port,
                }
            ],
            "outbounds": [outbound],
        }

    def _make_outbound(self, proxy: ParsedProxy) -> dict:
        """Build the sing-box outbound object for a given proxy."""
        p = proxy
        tag = f"{p.protocol}-{p.host}:{p.port}"

        base: dict = {
            "tag": tag,
            "server": p.host,
            "server_port": p.port,
        }

        if p.protocol == "shadowsocks":
            return {
                **base,
                "type": "shadowsocks",
                "method": p.extra.get("method", "aes-256-gcm"),
                "password": p.password,
            }
        elif p.protocol == "trojan":
            return {
                **base,
                "type": "trojan",
                "password": p.password,
                "tls": {
                    "enabled": True,
                    "server_name": p.sni or p.host,
                    "insecure": p.allow_insecure,
                },
            }
        elif p.protocol == "vless":
            tls: dict = {"enabled": True, "server_name": p.sni or p.host}
            if p.security == "reality":
                tls["reality"] = {
                    "enabled": True,
                    "public_key": p.public_key,
                    "short_id": p.short_id or "",
                }
            obj: dict = {
                **base,
                "type": "vless",
                "uuid": p.uuid,
                "flow": p.flow or "",
                "tls": tls,
                "transport": self._make_transport(p),
            }
            return obj
        elif p.protocol == "vmess":
            return {
                **base,
                "type": "vmess",
                "uuid": p.uuid,
                "security": "auto",
                "alter_id": int(p.extra.get("aid", 0)),
                "tls": {
                    "enabled": p.security in ("tls", "auto"),
                    "server_name": p.sni or p.host,
                },
                "transport": self._make_transport(p),
            }
        elif p.protocol in ("hysteria2", "hysteria"):
            return {
                **base,
                "type": "hysteria2",
                "password": p.password,
                "tls": {
                    "enabled": True,
                    "server_name": p.sni or p.host,
                    "insecure": p.allow_insecure,
                },
            }
        elif p.protocol == "tuic":
            return {
                **base,
                "type": "tuic",
                "uuid": p.uuid,
                "password": p.password,
                "tls": {
                    "enabled": True,
                    "server_name": p.sni or p.host,
                    "insecure": p.allow_insecure,
                },
            }
        else:
            raise ValueError(f"Unsupported protocol: {p.protocol}")

    def _make_transport(self, proxy: ParsedProxy) -> dict:
        """Build sing-box transport settings."""
        t = proxy.transport
        if t == "ws":
            return {
                "type": "ws",
                "path": proxy.path or "/",
                "headers": (
                    {"Host": proxy.host_header} if proxy.host_header else {}
                ),
            }
        elif t == "grpc":
            return {
                "type": "grpc",
                "service_name": proxy.service_name or "",
            }
        elif t == "httpupgrade":
            return {
                "type": "httpupgrade",
                "path": proxy.path or "/",
                "host": proxy.host_header or "",
            }
        return {"type": t}

    # ------------------------------------------------------------------
    # TCP fallback (no sing-box)
    # ------------------------------------------------------------------

    async def _test_tcp_fallback(self, proxy: ParsedProxy) -> TestResult:
        """Test connectivity by opening a raw TCP connection + timing it."""
        try:
            loop = asyncio.get_running_loop()
            start = time.monotonic()

            # Resolve and connect
            addrs = await loop.getaddrinfo(
                proxy.host, proxy.port, proto=socket.IPPROTO_TCP
            )
            ip = addrs[0][4][0] if addrs else proxy.host

            _, writer = await asyncio.wait_for(
                asyncio.open_connection(proxy.host, proxy.port),
                timeout=self._connect_timeout,
            )
            latency = time.monotonic() - start
            writer.close()
            await writer.wait_closed()

            # If TCP works, try a quick HTTP test too (for HTTP proxies)
            http_latency = await self._try_direct_http(proxy.host, proxy.port)

            return TestResult(
                proxy=proxy,
                working=True,
                latency_ms=round(
                    (http_latency if http_latency > 0 else latency) * 1000, 1
                ),
                resolved_ip=ip,
            )
        except asyncio.TimeoutError:
            return TestResult(proxy=proxy, working=False, error="Connection timeout")
        except OSError as exc:
            return TestResult(proxy=proxy, working=False, error=str(exc)[:120])
        except Exception as exc:
            return TestResult(proxy=proxy, working=False, error=str(exc)[:120])

    # ------------------------------------------------------------------
    # HTTP test via local SOCKS5 listener
    # ------------------------------------------------------------------

    async def _http_test_via_socks(
        self, socks_port: int
    ) -> tuple[float, str]:
        """Test HTTP connectivity through a local SOCKS5 proxy."""
        connector = ProxyConnector(
            proxy_type=ProxyType.SOCKS5,
            host="127.0.0.1",
            port=socks_port,
        )
        timeout = aiohttp.ClientTimeout(total=TOTAL_TIMEOUT)
        ip = ""

        try:
            async with aiohttp.ClientSession(
                connector=connector, timeout=timeout
            ) as session:
                for url in VERIFY_URLS:
                    try:
                        start = time.monotonic()
                        async with session.get(url) as resp:
                            await resp.read()
                            latency = time.monotonic() - start
                            if resp.status in (200, 204, 301, 302):
                                # Also try to get our external IP
                                ip = await self._fetch_ip(session)
                                return latency, ip
                    except Exception:
                        continue
        except Exception:
            pass

        return 0.0, ""

    async def _try_direct_http(self, host: str, port: int) -> float:
        """Try a direct HTTP connection to measure HTTP-level latency."""
        try:
            timeout = aiohttp.ClientTimeout(total=3.0)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                for url in [f"http://{host}:{port}", "https://1.1.1.1"]:
                    try:
                        start = time.monotonic()
                        async with session.get(url) as resp:
                            await resp.read()
                            return time.monotonic() - start
                    except Exception:
                        continue
        except Exception:
            pass
        return 0.0

    async def _fetch_ip(self, session: aiohttp.ClientSession) -> str:
        """Get the external IP as seen through the proxy."""
        try:
            async with session.get(
                "http://httpbin.org/ip",
                timeout=aiohttp.ClientTimeout(total=3),
            ) as resp:
                data = await resp.json()
                return data.get("origin", "")
        except Exception:
            return ""

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @property
    def has_singbox(self) -> bool:
        """Whether sing-box is currently available (PATH, cache, or installed)."""
        return self._singbox_path is not None or self._installer.is_available()

    @staticmethod
    def _find_free_port() -> int:
        """Find an available ephemeral port."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    @staticmethod
    def _kill_process(process: Optional[subprocess.Popen]) -> None:
        """Kill a subprocess and its children."""
        if process is None:
            return
        try:
            if os.name != "nt":
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            else:
                process.kill()
        except Exception:
            pass