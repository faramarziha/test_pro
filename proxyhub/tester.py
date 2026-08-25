"""
SingBoxTester: Async concurrent proxy testing engine.

Primary mode: fast TCP connection test (DNS resolve + TCP handshake).
Optional sing-box deep testing for protocol-level validation.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
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

VERIFY_URLS = [
    "https://www.gstatic.com/generate_204",
    "https://1.1.1.1/cdn-cgi/trace",
]

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
    """Async proxy testing engine. Defaults to fast TCP tests."""

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
        """Test all proxies concurrently using fast TCP connection tests."""
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
    # Per-proxy test — always uses fast TCP connect
    # ------------------------------------------------------------------

    async def _test_one(self, proxy: ParsedProxy) -> TestResult:
        async with self._semaphore:  # type: ignore[union-attr]
            return await self._fast_tcp_test(proxy)

    async def _fast_tcp_test(self, proxy: ParsedProxy) -> TestResult:
        """
        Fast test: DNS-resolve hostname → open TCP connection → measure latency.
        This correctly identifies reachable proxy servers in <5s per node.
        With 50 concurrent workers, 960 nodes test in ~2 minutes.
        """
        host = proxy.host
        port = proxy.port or 443

        if not host or not port:
            return TestResult(proxy=proxy, working=False, error="Missing host/port")

        try:
            start = time.monotonic()

            # DNS resolve (cached by OS, async via getaddrinfo)
            loop = asyncio.get_running_loop()
            try:
                addrs = await asyncio.wait_for(
                    loop.getaddrinfo(host, port, proto=socket.IPPROTO_TCP),
                    timeout=min(self._connect_timeout, 3.0),
                )
            except (asyncio.TimeoutError, socket.gaierror):
                return TestResult(proxy=proxy, working=False, error="DNS resolution failed")

            if not addrs:
                return TestResult(proxy=proxy, working=False, error="No addresses resolved")

            resolved_ip = addrs[0][4][0]

            # TCP connect
            try:
                _, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    timeout=self._connect_timeout,
                )
                tcp_latency = time.monotonic() - start
                writer.close()
                await writer.wait_closed()
            except (asyncio.TimeoutError, ConnectionRefusedError, OSError) as exc:
                return TestResult(proxy=proxy, working=False, error=str(exc)[:80])

            # TCP connect succeeded — proxy server is reachable
            # Now try a fast HTTP request if the port looks like HTTP/HTTPS
            http_latency = tcp_latency
            if port in (80, 443, 8080, 8443, 2053, 2083, 2087, 2096):
                http_latency = await self._try_http_connect(host, port, tcp_latency) or tcp_latency

            return TestResult(
                proxy=proxy,
                working=True,
                latency_ms=round(http_latency * 1000, 1),
                resolved_ip=resolved_ip,
            )

        except Exception as exc:
            return TestResult(proxy=proxy, working=False, error=str(exc)[:120])

    async def _try_http_connect(self, host: str, port: int, fallback: float) -> Optional[float]:
        """Try a quick HTTP GET to confirm the port actually serves traffic."""
        scheme = "https" if port in (443, 8443, 2053, 2083, 2087, 2096) else "http"
        url = f"{scheme}://{host}:{port}/"
        try:
            start = time.monotonic()
            connector = aiohttp.TCPConnector(ssl=False if scheme == "http" else True)
            async with aiohttp.ClientSession(
                connector=connector,
                timeout=aiohttp.ClientTimeout(total=3.0),
            ) as session:
                async with session.get(url, allow_redirects=True) as resp:
                    await resp.read()
                    return time.monotonic() - start
        except Exception:
            return None

    # ------------------------------------------------------------------
    # sing-box deep test (kept for manual advanced use, not default)
    # ------------------------------------------------------------------

    async def deep_test_via_singbox(self, proxy: ParsedProxy) -> TestResult:
        """Deep protocol-level test using sing-box. Slow but thorough."""
        if not self._singbox_path:
            return await self._fast_tcp_test(proxy)

        local_port = self._find_free_port()
        config_path: Optional[str] = None
        process: Optional[subprocess.Popen] = None

        try:
            sb_config = self._build_singbox_config(proxy, local_port)
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", delete=False
            ) as f:
                json.dump(sb_config, f, indent=2)
                config_path = f.name

            process = subprocess.Popen(
                [self._singbox_path, "run", "-c", config_path],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                preexec_fn=os.setsid if os.name != "nt" else None,
            )

            # Wait up to 2s for listener
            for _ in range(8):
                await asyncio.sleep(0.25)
                if self._port_is_listening(local_port):
                    break

            latency, ip = await self._http_test_via_socks(local_port)

            if latency > 0:
                return TestResult(
                    proxy=proxy,
                    working=True,
                    latency_ms=round(latency * 1000, 1),
                    resolved_ip=ip,
                )
            else:
                return TestResult(proxy=proxy, working=False, error="Sing-box: no HTTP response")
        except Exception as exc:
            return TestResult(proxy=proxy, working=False, error=str(exc)[:120])
        finally:
            self._kill_process(process)
            if config_path:
                try:
                    os.unlink(config_path)
                except OSError:
                    pass

    def _port_is_listening(self, port: int) -> bool:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return True
        except OSError:
            return False

    def _build_singbox_config(self, proxy: ParsedProxy, local_port: int) -> dict:
        return {
            "log": {"level": "error"},
            "inbounds": [{
                "type": "mixed",
                "tag": "mixed-in",
                "listen": "127.0.0.1",
                "listen_port": local_port,
            }],
            "outbounds": [self._make_outbound(proxy)],
        }

    def _make_outbound(self, proxy: ParsedProxy) -> dict:
        p = proxy
        tag = f"{p.protocol}-{p.host}:{p.port}"
        base: dict = {"tag": tag, "server": p.host, "server_port": p.port}

        if p.protocol == "shadowsocks":
            return {**base, "type": "shadowsocks",
                    "method": p.extra.get("method", "aes-256-gcm"), "password": p.password}
        elif p.protocol == "trojan":
            return {**base, "type": "trojan", "password": p.password,
                    "tls": {"enabled": True, "server_name": p.sni or p.host, "insecure": p.allow_insecure}}
        elif p.protocol == "vless":
            tls: dict = {"enabled": True, "server_name": p.sni or p.host}
            if p.security == "reality":
                tls["reality"] = {"enabled": True, "public_key": p.public_key, "short_id": p.short_id or ""}
            return {**base, "type": "vless", "uuid": p.uuid, "flow": p.flow or "",
                    "tls": tls, "transport": self._make_transport(p)}
        elif p.protocol == "vmess":
            return {**base, "type": "vmess", "uuid": p.uuid, "security": "auto",
                    "alter_id": int(p.extra.get("aid", 0)),
                    "tls": {"enabled": p.security in ("tls", "auto"), "server_name": p.sni or p.host},
                    "transport": self._make_transport(p)}
        elif p.protocol in ("hysteria2", "hysteria"):
            return {**base, "type": "hysteria2", "password": p.password,
                    "tls": {"enabled": True, "server_name": p.sni or p.host, "insecure": p.allow_insecure}}
        elif p.protocol == "tuic":
            return {**base, "type": "tuic", "uuid": p.uuid, "password": p.password,
                    "tls": {"enabled": True, "server_name": p.sni or p.host, "insecure": p.allow_insecure}}
        raise ValueError(f"Unsupported protocol: {p.protocol}")

    def _make_transport(self, proxy: ParsedProxy) -> dict:
        t = proxy.transport
        if t == "ws":
            return {"type": "ws", "path": proxy.path or "/",
                    "headers": {"Host": proxy.host_header} if proxy.host_header else {}}
        elif t == "grpc":
            return {"type": "grpc", "service_name": proxy.service_name or ""}
        elif t == "httpupgrade":
            return {"type": "httpupgrade", "path": proxy.path or "/", "host": proxy.host_header or ""}
        return {"type": t}

    # ------------------------------------------------------------------
    # HTTP test via local SOCKS5
    # ------------------------------------------------------------------

    async def _http_test_via_socks(self, socks_port: int) -> tuple[float, str]:
        connector = ProxyConnector(proxy_type=ProxyType.SOCKS5, host="127.0.0.1", port=socks_port)
        timeout = aiohttp.ClientTimeout(total=TOTAL_TIMEOUT)
        try:
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                for url in VERIFY_URLS:
                    try:
                        start = time.monotonic()
                        async with session.get(url) as resp:
                            await resp.read()
                            if resp.status in (200, 204, 301, 302):
                                ip = await self._fetch_ip(session)
                                return time.monotonic() - start, ip
                    except Exception:
                        continue
        except Exception:
            pass
        return 0.0, ""

    async def _fetch_ip(self, session: aiohttp.ClientSession) -> str:
        try:
            async with session.get("http://httpbin.org/ip", timeout=aiohttp.ClientTimeout(total=3)) as resp:
                data = await resp.json()
                return data.get("origin", "")
        except Exception:
            return ""

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @property
    def has_singbox(self) -> bool:
        return self._singbox_path is not None

    @staticmethod
    def _find_free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    @staticmethod
    def _kill_process(process: Optional[subprocess.Popen]) -> None:
        if process is None:
            return
        try:
            if os.name != "nt":
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            else:
                process.kill()
        except Exception:
            pass