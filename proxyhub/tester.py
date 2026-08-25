"""
SingBoxTester: v2rayNG-style REAL DELAY testing engine.

Real delay = HTTP GET https://www.gstatic.com/generate_204 THROUGH the proxy,
measuring full round-trip. 204 response = working, timeout = dead.

Efficiency strategy (unlike one-subprocess-per-proxy):
  ONE sing-box process per batch, with N mixed inbounds → N outbounds,
  routed 1:1 via route rules. All inbounds tested concurrently.
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

# v2rayNG uses https://www.gstatic.com/generate_204 for real delay
REAL_DELAY_URL = "https://www.gstatic.com/generate_204"

DEFAULT_SEMAPHORE = 50
CONNECT_TIMEOUT = 5.0
BATCH_SIZE = 50          # proxies per sing-box instance
STARTUP_TIMEOUT = 5.0    # max wait for sing-box inbounds to listen


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
    """Real-delay proxy tester using batched sing-box instances."""

    def __init__(
        self,
        concurrency: int = DEFAULT_SEMAPHORE,
        connect_timeout: float = CONNECT_TIMEOUT,
        singbox_path: Optional[str] = None,
        installer: Optional[SingBoxInstaller] = None,
        batch_size: int = BATCH_SIZE,
    ):
        self._concurrency = concurrency
        self._connect_timeout = connect_timeout
        self._installer = installer or SingBoxInstaller()
        self._singbox_path = singbox_path or find_singbox_sync()
        self._install_lock = asyncio.Lock()
        self._batch_size = batch_size

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def test_all(
        self,
        proxies: list[ParsedProxy],
        progress_callback=None,
    ) -> BatchTestResult:
        """REAL DELAY test for all proxies (v2rayNG-style), batched."""
        start = time.monotonic()

        # Ensure sing-box is available (single download if needed)
        if not self._singbox_path:
            async with self._install_lock:
                if not self._singbox_path:
                    self._singbox_path = await self._installer.ensure_installed()

        if not self._singbox_path:
            logger.warning("sing-box unavailable — cannot run real delay test")
            return BatchTestResult(
                results=[TestResult(proxy=p, working=False,
                                    error="sing-box not available") for p in proxies],
                total=len(proxies),
                working=0,
                dead=len(proxies),
                elapsed_seconds=round(time.monotonic() - start, 2),
            )

        results: list[TestResult] = []
        total = len(proxies)
        done = 0

        for batch in self._chunk(proxies, self._batch_size):
            batch_results = await self._test_batch(batch)
            results.extend(batch_results)
            done += len(batch)
            if progress_callback:
                for r in batch_results:
                    progress_callback(done, total, r)

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
    # Batch: one sing-box process, N inbounds → N outbounds
    # ------------------------------------------------------------------

    async def _test_batch(self, batch: list[ParsedProxy]) -> list[TestResult]:
        """Test a batch through a single sing-box instance."""
        # Allocate a local port per proxy
        ports = [self._find_free_port() for _ in batch]

        config = self._build_batch_config(batch, ports)
        config_path: Optional[str] = None
        process: Optional[subprocess.Popen] = None

        try:
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", delete=False, encoding="utf-8"
            ) as f:
                json.dump(config, f, ensure_ascii=False)
                config_path = f.name

            process = subprocess.Popen(
                [self._singbox_path, "run", "-c", config_path],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                preexec_fn=os.setsid if os.name != "nt" else None,
            )

            # Wait until all inbounds are listening (or timeout)
            ready = await self._wait_ports_ready(ports, timeout=STARTUP_TIMEOUT)

            # Test all ports concurrently
            sem = asyncio.Semaphore(max(self._concurrency, 1))

            batch_results: dict[int, TestResult] = {}

            async def _run(idx: int, proxy: ParsedProxy):
                async with sem:
                    return idx, await self._real_delay_test(proxy, ports[idx])

            pending = [
                _run(i, p) for i, p in enumerate(batch) if ready[i]
            ]
            if pending:
                for coro in asyncio.as_completed(pending):
                    idx, result = await coro
                    batch_results[idx] = result

            # Ports that never opened = config error
            for i, (port, proxy) in enumerate(zip(ports, batch)):
                if not ready[i]:
                    batch_results[i] = TestResult(
                        proxy=proxy, working=False,
                        error="sing-box inbound failed to start")

            return [batch_results.get(i, TestResult(proxy=p, working=False,
                                                    error="untested"))
                    for i, p in enumerate(batch)]

        except Exception as exc:
            logger.error("Batch test failed: %s", exc)
            return [TestResult(proxy=p, working=False, error=str(exc)[:100])
                    for p in batch]
        finally:
            self._kill_process(process)
            if config_path:
                try:
                    os.unlink(config_path)
                except OSError:
                    pass

    def _build_batch_config(
        self, batch: list[ParsedProxy], ports: list[int]
    ) -> dict:
        """One sing-box config: N mixed inbounds, N outbounds, 1:1 routing."""
        inbounds = []
        outbounds = []
        rules = []

        for i, (proxy, port) in enumerate(zip(batch, ports)):
            in_tag = f"in-{i}"
            out_tag = f"out-{i}"
            inbounds.append({
                "type": "mixed",
                "tag": in_tag,
                "listen": "127.0.0.1",
                "listen_port": port,
            })
            try:
                outbound = self._make_outbound(proxy)
                outbound["tag"] = out_tag
                outbounds.append(outbound)
            except Exception:
                # Invalid config — route to a dead-end direct outbound
                outbounds.append({
                    "type": "direct", "tag": out_tag,
                })
            rules.append({"inbound": [in_tag], "outbound": out_tag})

        # Fallback outbound so unmatched traffic never crosses streams
        outbounds.append({"type": "direct", "tag": "direct-fallback"})

        return {
            "log": {"level": "error"},
            "inbounds": inbounds,
            "outbounds": outbounds,
            "route": {
                "rules": rules,
                "final": "direct-fallback",
            },
        }

    async def _wait_ports_ready(
        self, ports: list[int], timeout: float
    ) -> list[bool]:
        """Poll until each port accepts TCP connections."""
        ready = [False] * len(ports)
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            all_ready = True
            for i, port in enumerate(ports):
                if ready[i]:
                    continue
                if self._port_open(port):
                    ready[i] = True
                else:
                    all_ready = False
            if all_ready:
                break
            await asyncio.sleep(0.15)

        # Process may have crashed — check
        return ready

    @staticmethod
    def _port_open(port: int) -> bool:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return True
        except OSError:
            return False

    # ------------------------------------------------------------------
    # The REAL DELAY test itself (v2rayNG-identical)
    # ------------------------------------------------------------------

    async def _real_delay_test(self, proxy: ParsedProxy, local_port: int) -> TestResult:
        """HTTP GET generate_204 THROUGH the proxy; measure round-trip."""
        connector = ProxyConnector(
            proxy_type=ProxyType.SOCKS5,
            host="127.0.0.1",
            port=local_port,
        )
        timeout = aiohttp.ClientTimeout(total=self._connect_timeout)

        try:
            async with aiohttp.ClientSession(
                connector=connector, timeout=timeout
            ) as session:
                start = time.monotonic()
                async with session.get(REAL_DELAY_URL) as resp:
                    await resp.read()
                    latency_ms = (time.monotonic() - start) * 1000

                    if resp.status in (200, 204):
                        return TestResult(
                            proxy=proxy,
                            working=True,
                            latency_ms=round(latency_ms, 1),
                        )
                    return TestResult(
                        proxy=proxy, working=False,
                        error=f"HTTP {resp.status}",
                    )
        except asyncio.TimeoutError:
            return TestResult(proxy=proxy, working=False, error="timeout")
        except Exception as exc:
            return TestResult(proxy=proxy, working=False,
                              error=str(exc)[:80])

    # ------------------------------------------------------------------
    # Outbound builder (same as before, per-protocol)
    # ------------------------------------------------------------------

    def _make_outbound(self, proxy: ParsedProxy) -> dict:
        p = proxy
        base: dict = {"server": p.host, "server_port": p.port or 443}

        if p.protocol == "shadowsocks":
            return {**base, "type": "shadowsocks",
                    "method": p.extra.get("method", "aes-256-gcm"),
                    "password": p.password}
        elif p.protocol == "trojan":
            return {**base, "type": "trojan", "password": p.password,
                    "tls": {"enabled": True, "server_name": p.sni or p.host,
                            "insecure": p.allow_insecure}}
        elif p.protocol == "vless":
            tls: dict = {"enabled": True, "server_name": p.sni or p.host}
            if p.security == "reality":
                tls["utls"] = {"enabled": True,
                               "fingerprint": p.fingerprint or "chrome"}
                tls["reality"] = {"enabled": True,
                                  "public_key": p.public_key,
                                  "short_id": p.short_id or ""}
            elif p.fingerprint:
                tls["utls"] = {"enabled": True,
                               "fingerprint": p.fingerprint}
            transport = self._make_transport(p)
            obj = {**base, "type": "vless", "uuid": p.uuid,
                   "flow": p.flow or "", "tls": tls}
            if transport:
                obj["transport"] = transport
            return obj
        elif p.protocol == "vmess":
            transport = self._make_transport(p)
            obj = {**base, "type": "vmess", "uuid": p.uuid, "security": "auto",
                   "alter_id": int(p.extra.get("aid", 0) or 0),
                   "tls": {"enabled": p.security in ("tls", "auto"),
                           "server_name": p.sni or p.host}}
            if transport:
                obj["transport"] = transport
            return obj
        elif p.protocol in ("hysteria2", "hysteria"):
            return {**base, "type": "hysteria2", "password": p.password,
                    "tls": {"enabled": True, "server_name": p.sni or p.host,
                            "insecure": p.allow_insecure}}
        elif p.protocol == "tuic":
            return {**base, "type": "tuic", "uuid": p.uuid,
                    "password": p.password,
                    "tls": {"enabled": True, "server_name": p.sni or p.host,
                            "insecure": p.allow_insecure}}
        raise ValueError(f"Unsupported protocol: {p.protocol}")

    def _make_transport(self, proxy: ParsedProxy) -> Optional[dict]:
        t = (proxy.transport or "tcp").lower()
        if t in ("raw", "tcp", "none"):
            return None  # plain TCP — no transport block
        elif t == "ws":
            tr: dict = {"type": "ws", "path": proxy.path or "/"}
            if proxy.host_header:
                tr["headers"] = {"Host": proxy.host_header}
            return tr
        elif t == "grpc":
            return {"type": "grpc", "service_name": proxy.service_name or ""}
        elif t == "httpupgrade":
            return {"type": "httpupgrade", "path": proxy.path or "/",
                    "host": proxy.host_header or ""}
        elif t in ("http", "h2", "http2"):
            tr2: dict = {"type": "http"}
            if proxy.path:
                tr2["path"] = proxy.path
            if proxy.host_header:
                tr2["host"] = [proxy.host_header]
            return tr2
        else:
            # Unknown transport (xhttp, splithttp, quic…) — fall back to raw
            logger.debug("Unknown transport %r for %s — using raw TCP", t, proxy.host)
            return None

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @property
    def has_singbox(self) -> bool:
        return self._singbox_path is not None

    @staticmethod
    def _chunk(lst: list, size: int):
        for i in range(0, len(lst), size):
            yield lst[i:i + size]

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