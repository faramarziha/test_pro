"""
SubscriptionFetcher: Downloads and decodes subscription sources.
Handles URL fetching, Base64 decoding, and text parsing.
"""
from __future__ import annotations

import base64
import re
import logging
from dataclasses import dataclass
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

# Timeout for fetching subscription URLs (seconds)
FETCH_TIMEOUT = aiohttp.ClientTimeout(total=30)


@dataclass
class FetchResult:
    source: str
    raw_lines: list[str]
    proxy_count: int
    is_base64: bool


class SubscriptionFetcher:
    """Fetches and decodes proxy subscription content from URLs or raw text."""

    BASE64_PATTERN = re.compile(
        r"^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)?$"
    )

    PROXY_PROTOCOLS = frozenset({
        "vless://", "vmess://", "trojan://", "ss://",
        "hy2://", "hysteria2://", "tuic://",
    })

    async def fetch_url(self, url: str) -> FetchResult:
        """Fetch and decode a proxy subscription from a URL."""
        logger.info("Fetching subscription from %s", url)
        async with aiohttp.ClientSession(timeout=FETCH_TIMEOUT) as session:
            async with session.get(url) as resp:
                resp.raise_for_status()
                content = await resp.text()
        return self._process_content(content, source=url)

    def parse_text(self, text: str, source: str = "manual") -> FetchResult:
        """Parse raw text or Base64 input directly."""
        return self._process_content(text, source=source)

    def _process_content(self, content: str, source: str) -> FetchResult:
        """Detect encoding, decode if needed, and extract proxy lines."""
        content = content.strip()

        # Try Base64 decode
        is_b64 = False
        decoded = content
        if self._looks_like_base64(content):
            try:
                # Add padding if needed
                padded = content
                missing = len(padded) % 4
                if missing:
                    padded += "=" * (4 - missing)
                raw = base64.b64decode(padded, validate=True).decode("utf-8", errors="replace")
                # Check if decoded content contains proxy URIs
                if any(raw.lstrip().startswith(p) for p in self.PROXY_PROTOCOLS):
                    decoded = raw
                    is_b64 = True
            except Exception:
                pass  # Not valid Base64; treat as plain text

        lines = self._extract_proxy_lines(decoded)
        return FetchResult(
            source=source,
            raw_lines=lines,
            proxy_count=len(lines),
            is_base64=is_b64,
        )

    def _looks_like_base64(self, text: str) -> bool:
        """Quick heuristic: single-line, no proxy protocol prefix, Base64-ish chars."""
        single = text.split("\n")[0].strip()
        if any(single.startswith(p) for p in self.PROXY_PROTOCOLS):
            return False
        return bool(self.BASE64_PATTERN.match(single)) and len(single) > 20

    def _extract_proxy_lines(self, text: str) -> list[str]:
        """Extract non-empty lines that start with a known proxy protocol."""
        lines: list[str] = []
        seen: set[str] = set()
        for raw in text.splitlines():
            line = raw.strip()
            if not line:
                continue
            for proto in self.PROXY_PROTOCOLS:
                if line.startswith(proto):
                    if line not in seen:
                        seen.add(line)
                        lines.append(line)
                    break
        return lines