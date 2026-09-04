"""Proxy connectivity and throughput testing through sing-box."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import socket
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from typing import Optional

import aiohttp
from aiohttp_socks import ProxyConnector, ProxyType

from proxyhub.parser import ParsedProxy
from proxyhub.installer import find_singbox_sync, SingBoxInstaller

logger = logging.getLogger(__name__)

REAL_DELAY_URL = "https://www.gstatic.com/generate_204"
# A real body is required for throughput measurement; keep it modest and cache-proof.
SPEED_TEST_URL = "https://speed.cloudflare.com/__down?bytes=262144"
# Plain-text IP echo used to learn the tunnel's real egress IP (not the server host).
EXIT_IP_URL = "https://api.ipify.org"
DEFAULT_SEMAPHORE = 50
CONNECT_TIMEOUT = 5.0
BATCH_SIZE = 50
STARTUP_TIMEOUT = 5.0
MIN_DOWNLOAD_KBPS = 100.0
SPEED_SAMPLE_BYTES = 262144
MAX_TEST_ATTEMPTS = 2


@dataclass
class TestResult:
    proxy: ParsedProxy
    working: bool
    latency_ms: float = 0.0
    download_kbps: float = 0.0
    resolved_ip: str = ""
    exit_ip: str = ""
    error: str = ""
    attempts: int = 1
    quality: str = ""          # "fast" | "acceptable" | "unverified"
    speed_verified: bool = True


@dataclass
class BatchTestResult:
    results: list[TestResult] = field(default_factory=list)
    total: int = 0
    working: int = 0
    dead: int = 0
    elapsed_seconds: float = 0.0


class SingBoxTester:
    """Run a connectivity check followed by a minimum-throughput check."""

    def __init__(self, concurrency: int = DEFAULT_SEMAPHORE,
                 connect_timeout: float = CONNECT_TIMEOUT,
                 singbox_path: Optional[str] = None,
                 installer: Optional[SingBoxInstaller] = None,
                 batch_size: int = BATCH_SIZE,
                 min_download_kbps: float = MIN_DOWNLOAD_KBPS):
        self._concurrency = concurrency
        self._connect_timeout = connect_timeout
        self._installer = installer or SingBoxInstaller()
        self._singbox_path = singbox_path or find_singbox_sync()
        self._install_lock = asyncio.Lock()
        self._batch_size = batch_size
        self._min_download_kbps = min_download_kbps

    async def test_all(self, proxies: list[ParsedProxy], progress_callback=None) -> BatchTestResult:
        start = time.monotonic()
        if not self._singbox_path:
            async with self._install_lock:
                if not self._singbox_path:
                    self._singbox_path = await self._installer.ensure_installed()
        if not self._singbox_path:
            results = [TestResult(p, False, error="sing-box not available") for p in proxies]
            return BatchTestResult(results, len(results), 0, len(results), round(time.monotonic() - start, 2))

        results: list[TestResult] = []
        total = len(proxies)
        for batch in self._chunk(proxies, self._batch_size):
            batch_results = await self._test_batch(batch)
            results.extend(batch_results)
            if progress_callback:
                for index, result in enumerate(batch_results, 1):
                    progress_callback(min(len(results) - len(batch_results) + index, total), total, result)

        working = sum(result.working for result in results)
        return BatchTestResult(results, len(results), working, len(results) - working,
                               round(time.monotonic() - start, 2))

    async def _test_batch(self, batch: list[ParsedProxy]) -> list[TestResult]:
        ports = [self._find_free_port() for _ in batch]
        config_path: Optional[str] = None
        process: Optional[subprocess.Popen] = None
        try:
            config = self._build_batch_config(batch, ports)
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as file:
                json.dump(config, file, ensure_ascii=False)
                config_path = file.name
            process = subprocess.Popen([self._singbox_path, "run", "-c", config_path],
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                       preexec_fn=os.setsid if os.name != "nt" else None)
            ready = await self._wait_ports_ready(ports, STARTUP_TIMEOUT)
            sem = asyncio.Semaphore(max(self._concurrency, 1))

            async def run_one(index: int, proxy: ParsedProxy):
                async with sem:
                    return index, await self._quality_test(proxy, ports[index])

            pairs = await asyncio.gather(*(run_one(i, proxy) for i, proxy in enumerate(batch)))
            results = {index: result for index, result in pairs}
            for index, proxy in enumerate(batch):
                if not ready[index]:
                    results[index] = TestResult(proxy, False, error="sing-box inbound failed to start")
            return [results[i] for i in range(len(batch))]
        except Exception as exc:
            logger.exception("Batch test failed")
            return [TestResult(proxy, False, error=str(exc)[:100]) for proxy in batch]
        finally:
            self._kill_process(process)
            if config_path:
                try:
                    os.unlink(config_path)
                except OSError:
                    pass

    async def _quality_test(self, proxy: ParsedProxy, local_port: int) -> TestResult:
        """Connectivity must pass; throughput is enforced when it can be measured.

        A config is only killed when (a) it cannot connect at all on every
        attempt, or (b) throughput was actually measured and is below the
        configured minimum. If the speed sample itself fails for infra reasons
        (HTTP error, reset, truncation) the config stays working with
        quality="unverified" — a config that works must not be killed by our
        test infrastructure.
        """
        last_error = "unknown error"
        for attempt in range(1, MAX_TEST_ATTEMPTS + 1):
            try:
                connector = ProxyConnector(proxy_type=ProxyType.SOCKS5, host="127.0.0.1", port=local_port)
                timeout = aiohttp.ClientTimeout(total=self._connect_timeout)
                async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                    # 1. Connectivity — any 2xx/3xx proves the tunnel works.
                    start = time.monotonic()
                    async with session.get(REAL_DELAY_URL, headers={"Cache-Control": "no-cache"}) as response:
                        await response.read()
                        if response.status < 200 or response.status >= 400:
                            raise RuntimeError(f"HTTP {response.status} on connectivity check")
                    latency_ms = (time.monotonic() - start) * 1000

                    # 2. Real egress IP through the tunnel (accurate geolocation).
                    exit_ip = await self._get_exit_ip(session)

                    # 3. Throughput sample.
                    speed_start = time.monotonic()
                    received = 0
                    async with session.get(SPEED_TEST_URL, headers={"Cache-Control": "no-cache"}) as response:
                        if response.status != 200:
                            raise SpeedSampleError(f"HTTP {response.status} on speed check")
                        async for chunk in response.content.iter_chunked(16384):
                            received += len(chunk)
                            if received >= SPEED_SAMPLE_BYTES:
                                break
                    duration = max(time.monotonic() - speed_start, 0.001)
                    download_kbps = received / duration / 1024
                    resolved_ip = await self._resolve_host(proxy.host)

                    if received < SPEED_SAMPLE_BYTES:
                        # Server truncated the body — cannot judge speed fairly.
                        return TestResult(proxy, True, round(latency_ms, 1), round(download_kbps, 1),
                                          resolved_ip, exit_ip, quality="unverified",
                                          speed_verified=False, attempts=attempt)
                    if download_kbps < self._min_download_kbps:
                        return TestResult(proxy, False, round(latency_ms, 1), round(download_kbps, 1),
                                          resolved_ip, exit_ip,
                                          error=(f"speed below minimum ({download_kbps:.1f} KB/s "
                                                 f"< {self._min_download_kbps:.0f} KB/s)"),
                                          attempts=attempt)
                    quality = "fast" if download_kbps >= 500 else "acceptable"
                    return TestResult(proxy, True, round(latency_ms, 1), round(download_kbps, 1),
                                      resolved_ip, exit_ip, quality=quality, attempts=attempt)
            except asyncio.TimeoutError:
                last_error = "timeout"
            except SpeedSampleError as exc:
                # Speed endpoint misbehaved — the tunnel works, keep the config.
                return TestResult(proxy, True, error="", quality="unverified",
                                  speed_verified=False, attempts=attempt)
            except Exception as exc:
                last_error = str(exc)[:100]
            if attempt < MAX_TEST_ATTEMPTS:
                await asyncio.sleep(0.15)
        return TestResult(proxy, False, error=last_error, attempts=MAX_TEST_ATTEMPTS)

    async def _get_exit_ip(self, session: aiohttp.ClientSession) -> str:
        """Ask an echo endpoint through the tunnel for the real egress IP."""
        try:
            async with session.get(EXIT_IP_URL, headers={"Cache-Control": "no-cache"}) as response:
                if response.status != 200:
                    return ""
                text = (await response.text()).strip()
            return text if ":" in text or "." in text else ""
        except Exception:
            return ""

    async def _resolve_host(self, host: str) -> str:
        try:
            loop = asyncio.get_running_loop()
            addresses = await asyncio.wait_for(loop.getaddrinfo(host, None), timeout=3.0)
            return addresses[0][4][0] if addresses else ""
        except Exception:
            return ""

    def _build_batch_config(self, batch: list[ParsedProxy], ports: list[int]) -> dict:
        inbounds, outbounds, rules = [], [], []
        for index, (proxy, port) in enumerate(zip(batch, ports)):
            inbound_tag, outbound_tag = f"in-{index}", f"out-{index}"
            inbounds.append({"type": "mixed", "tag": inbound_tag, "listen": "127.0.0.1", "listen_port": port})
            try:
                outbound = self._make_outbound(proxy)
                outbound["tag"] = outbound_tag
            except Exception:
                outbound = {"type": "block", "tag": outbound_tag}
            outbounds.append(outbound)
            rules.append({"inbound": [inbound_tag], "outbound": outbound_tag})
        outbounds.append({"type": "direct", "tag": "direct-fallback"})
        return {"log": {"level": "error"}, "inbounds": inbounds, "outbounds": outbounds,
                "route": {"rules": rules, "final": "direct-fallback"}}

    async def _wait_ports_ready(self, ports: list[int], timeout: float) -> list[bool]:
        ready = [False] * len(ports)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for index, port in enumerate(ports):
                if not ready[index] and self._port_open(port):
                    ready[index] = True
            if all(ready):
                break
            await asyncio.sleep(0.15)
        return ready

    @staticmethod
    def _port_open(port: int) -> bool:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return True
        except OSError:
            return False

    def _make_outbound(self, proxy: ParsedProxy) -> dict:
        p = proxy
        base = {"server": p.host, "server_port": p.port or 443}
        if p.protocol == "shadowsocks":
            return {**base, "type": "shadowsocks", "method": p.extra.get("method", "aes-256-gcm"), "password": p.password}
        if p.protocol == "trojan":
            return {**base, "type": "trojan", "password": p.password, "tls": {"enabled": True, "server_name": p.sni or p.host, "insecure": p.allow_insecure}}
        if p.protocol == "vless":
            tls_enabled = p.security in ("tls", "reality", "auto")
            tls = {"enabled": tls_enabled}
            if tls_enabled:
                tls["server_name"] = p.sni or p.host
            if p.security == "reality":
                tls["utls"] = {"enabled": True, "fingerprint": p.fingerprint or "chrome"}
                tls["reality"] = {"enabled": True, "public_key": p.public_key, "short_id": p.short_id or ""}
            elif p.fingerprint:
                tls["utls"] = {"enabled": True, "fingerprint": p.fingerprint}
            obj = {**base, "type": "vless", "uuid": p.uuid, "flow": p.flow or "", "tls": tls}
            transport = self._make_transport(p)
            if transport: obj["transport"] = transport
            return obj
        if p.protocol == "vmess":
            tls_enabled = p.security in ("tls", "auto")
            obj = {**base, "type": "vmess", "uuid": p.uuid, "security": "auto",
                   "alter_id": int(p.extra.get("aid", 0) or 0),
                   "tls": {"enabled": tls_enabled, "server_name": p.sni or p.host}}
            transport = self._make_transport(p)
            if transport: obj["transport"] = transport
            return obj
        if p.protocol in ("hysteria2", "hysteria"):
            out = {**base, "type": "hysteria2", "password": p.password,
                   "tls": {"enabled": True, "server_name": p.sni or p.host, "insecure": p.allow_insecure}}
            obfs = (p.extra.get("obfs") or "").strip()
            if obfs:
                out["obfs"] = {"type": "salamander", "password": obfs}
            return out
        if p.protocol == "tuic":
            return {**base, "type": "tuic", "uuid": p.uuid, "password": p.password,
                    "tls": {"enabled": True, "server_name": p.sni or p.host, "insecure": p.allow_insecure}}
        raise ValueError(f"Unsupported protocol: {p.protocol}")

    @staticmethod
    def _make_transport(proxy: ParsedProxy) -> Optional[dict]:
        transport = (proxy.transport or "tcp").lower()
        if transport in ("raw", "tcp", "none"): return None
        if transport == "ws":
            result = {"type": "ws", "path": proxy.path or "/"}
            if proxy.host_header: result["headers"] = {"Host": proxy.host_header}
            return result
        if transport == "grpc": return {"type": "grpc", "service_name": proxy.service_name or ""}
        if transport == "httpupgrade": return {"type": "httpupgrade", "path": proxy.path or "/", "host": proxy.host_header or ""}
        if transport in ("http", "h2", "http2"):
            result = {"type": "http"}
            if proxy.path: result["path"] = proxy.path
            if proxy.host_header: result["host"] = [proxy.host_header]
            return result
        if transport in ("xhttp", "splithttp"):
            result = {"type": transport, "path": proxy.path or "/"}
            host = proxy.host_header or proxy.sni or proxy.host
            if host: result["host"] = [host]
            return result
        # Unknown transport — do not silently fall back to raw TCP (that breaks
        # working configs); let sing-box surface the config error instead.
        logger.debug("Unknown transport %r for %s", transport, proxy.host)
        return {"type": transport, "path": proxy.path or "/"}

    @property
    def has_singbox(self) -> bool:
        return self._singbox_path is not None

    @staticmethod
    def _chunk(values: list, size: int):
        for index in range(0, len(values), size):
            yield values[index:index + size]

    @staticmethod
    def _find_free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return sock.getsockname()[1]

    @staticmethod
    def _kill_process(process: Optional[subprocess.Popen]) -> None:
        if process is None: return
        try:
            if os.name != "nt": os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            else: process.kill()
        except Exception: pass


class SpeedSampleError(RuntimeError):
    """Raised when the speed endpoint itself misbehaves (not the proxy)."""
