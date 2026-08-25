"""
IPIntelligenceEngine: Batch IP geolocation and network categorization.

Uses ip-api.com/batch (free tier: 45 req/min, 100 IPs per batch) with
rate-limit management. Falls back to freeipapi.com for any failed lookups.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

# Batch API settings
BATCH_SIZE = 100
BATCH_DELAY = 1.5  # seconds between batches (ratelimit compliance)
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=15)

# Well-known hosting/cloud ASN prefixes and organizations
CLOUD_ORGS: set[str] = {
    "hetzner", "digitalocean", "amazon", "aws", "google cloud",
    "microsoft", "azure", "ovh", "vultr", "linode", "alibaba",
    "tencent", "oracle", "ibm", "rackspace", "leaseweb",
    "psychz", "colocrossing", "buyvm", "ramnode", "interserver",
}
CONSUMER_ISPS: set[str] = {
    "comcast", "at&t", "verizon", "t-mobile", "sprint",
    "charter", "cox", "centurylink", "frontier", "altice",
    "china telecom", "china mobile", "china unicom",
    "ntt", "kddi", "softbank", "bt", "sky", "vodafone",
    "deutsche telekom", "orange", "telefonica", "movistar",
    "rogers", "bell", "telus", "optus", "telstra",
    "jio", "airtel", "vodafone idea",
}


@dataclass
class IPInfo:
    ip: str
    country: str = ""
    country_code: str = ""
    city: str = ""
    isp: str = ""
    asn: str = ""
    hosting: bool = False
    proxy: bool = False
    category: str = "Unknown"


@dataclass
class EnrichedResult:
    proxy_raw: str
    protocol: str
    host: str
    port: int
    latency_ms: float
    ip: str
    country: str
    country_code: str
    city: str
    isp: str
    category: str
    asn: str
    is_working: bool


class IPIntelligenceEngine:
    """Batch IP geolocation with rate-limit management."""

    PRIMARY_URL = "http://ip-api.com/batch"
    FALLBACK_URL = "https://freeipapi.com/api/batch"

    def __init__(self, batch_size: int = BATCH_SIZE):
        self._batch_size = batch_size
        self._session: Optional[aiohttp.ClientSession] = None

    async def lookup_batch(self, ips: list[str]) -> dict[str, IPInfo]:
        """Look up geolocation data for a list of unique IPs."""
        unique = list(dict.fromkeys(ips))  # dedup preserving order
        results: dict[str, IPInfo] = {}

        async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT) as session:
            self._session = session

            for i in range(0, len(unique), self._batch_size):
                batch = unique[i : i + self._batch_size]
                batch_results = await self._query_primary(batch)
                results.update(batch_results)

                # Wait between batches (rate limiting)
                if i + self._batch_size < len(unique):
                    await asyncio.sleep(BATCH_DELAY)

        # Ensure every IP has at least a stub result
        for ip in unique:
            if ip not in results:
                results[ip] = IPInfo(ip=ip, category="Unknown")

        return results

    async def _query_primary(self, ips: list[str]) -> dict[str, IPInfo]:
        """Query ip-api.com/batch with POST body of IPs. Retries on 429."""
        payload = [
            {
                "query": ip,
                "fields": "query,status,country,countryCode,city,isp,as,hosting,proxy",
            }
            for ip in ips
        ]

        for attempt in range(3):
            try:
                async with self._session.post(  # type: ignore[union-attr]
                    self.PRIMARY_URL, json=payload
                ) as resp:
                    if resp.status == 429:
                        wait = 5.0 * (attempt + 1)
                        logger.warning(
                            "ip-api.com rate-limited (429), waiting %.0fs", wait
                        )
                        await asyncio.sleep(wait)
                        continue
                    if resp.status != 200:
                        logger.warning(
                            "ip-api.com returned %d, falling back", resp.status
                        )
                        return await self._query_fallback(ips)
                    data = await resp.json()
                    return self._parse_primary_response(data)
            except Exception as exc:
                logger.warning("ip-api.com error: %s (attempt %d)", exc, attempt + 1)
                await asyncio.sleep(2.0 * (attempt + 1))

        logger.warning("ip-api.com failed after retries, falling back")
        return await self._query_fallback(ips)

    async def _query_fallback(self, ips: list[str]) -> dict[str, IPInfo]:
        """Query freeipapi.com for any IPs not resolved by primary."""
        results: dict[str, IPInfo] = {}
        try:
            async with self._session.post(  # type: ignore[union-attr]
                self.FALLBACK_URL,
                json={"ips": ips},
            ) as resp:
                if resp.status == 200:
                    fb_data = await resp.json()
                    for item in fb_data if isinstance(fb_data, list) else []:
                        ip = item.get("ip", "")
                        if ip:
                            results[ip] = IPInfo(
                                ip=ip,
                                country=item.get("countryName", ""),
                                country_code=item.get("countryCode", ""),
                                city=item.get("city", ""),
                                isp=item.get("isp", ""),
                                asn=item.get("asn", ""),
                                hosting=item.get("hosting", False),
                                proxy=item.get("proxy", False),
                            )
        except Exception as exc:
            logger.warning("Fallback API error: %s", exc)
        return results

    def _parse_primary_response(self, data: list) -> dict[str, IPInfo]:
        """Parse ip-api.com batch response into IPInfo objects."""
        results: dict[str, IPInfo] = {}
        for item in data:
            if not isinstance(item, dict):
                continue
            ip = item.get("query", "")
            if not ip or item.get("status") != "success":
                continue
            info = IPInfo(
                ip=ip,
                country=item.get("country", ""),
                country_code=item.get("countryCode", ""),
                city=item.get("city", ""),
                isp=item.get("isp", ""),
                asn=item.get("as", ""),
                hosting=item.get("hosting", False),
                proxy=item.get("proxy", False),
            )
            info.category = categorize_ip(info)
            results[ip] = info
        return results


# ------------------------------------------------------------------
# Network categorization (module-level, no state needed)
# ------------------------------------------------------------------

def categorize_ip(info: IPInfo) -> str:
    """Classify an IP into Residential, Datacenter, Proxy/VPN, or Business/Education."""
    isp_lower = info.isp.lower()
    org_lower = _extract_org(info.asn).lower()

    # Public proxy / VPN flag
    if info.proxy:
        return "Public Proxy / VPN"

    # Datacenter / Hosting
    if info.hosting:
        return "Datacenter / Hosting"

    # Check ASN against known cloud providers
    for kw in CLOUD_ORGS:
        if kw in isp_lower or kw in org_lower:
            return "Datacenter / Hosting"

    # Check for consumer ISPs
    for kw in CONSUMER_ISPS:
        if kw in isp_lower or kw in org_lower:
            return "Residential / ISP"

    # Business / Education heuristics
    if any(kw in isp_lower for kw in ("university", "college", "edu", "school")):
        return "Business / Education"
    if any(kw in org_lower for kw in ("university", "college", "edu", "school")):
        return "Business / Education"

    if any(kw in isp_lower for kw in ("corp", "inc", "ltd", "llc", "enterprise")):
        return "Business / Education"

    # Fallback: if hosting == false, proxy == false, and not matched above
    return "Residential / ISP"


def _extract_org(asn: str) -> str:
    """Extract organization name from an AS string like 'AS15169 Google LLC'."""
    if not asn:
        return ""
    parts = asn.split(" ", 1)
    return parts[1] if len(parts) > 1 else ""


# ------------------------------------------------------------------
# Enrichment pipeline
# ------------------------------------------------------------------

import ipaddress


def _is_ip(s: str) -> bool:
    """Check whether a string is a literal IPv4/IPv6 address."""
    try:
        ipaddress.ip_address(s)
        return True
    except ValueError:
        return False


async def enrich_test_results(
    test_results: list,  # list[TestResult]
    engine: Optional[IPIntelligenceEngine] = None,
) -> list[EnrichedResult]:
    """Take test results and IP lookups, produce enriched display objects."""
    if engine is None:
        engine = IPIntelligenceEngine()

    # Collect unique IPs for working nodes (resolved IP, or host if literal IP)
    ips_for_lookup: list[str] = []
    for tr in test_results:
        if not tr.working:
            continue
        ip = tr.resolved_ip
        if not ip and _is_ip(tr.proxy.host):
            ip = tr.proxy.host
        if ip:
            ips_for_lookup.append(ip)

    # Batch lookup
    ip_map: dict[str, IPInfo] = {}
    if ips_for_lookup:
        ip_map = await engine.lookup_batch(ips_for_lookup)

    enriched: list[EnrichedResult] = []
    for tr in test_results:
        p = tr.proxy
        ip = tr.resolved_ip or (p.host if _is_ip(p.host) else "")
        info = ip_map.get(ip) if ip else None
        if info is None:
            info = IPInfo(ip=ip or p.host)

        enriched.append(
            EnrichedResult(
                proxy_raw=p.raw,
                protocol=p.protocol.upper(),
                host=p.host,
                port=p.port,
                latency_ms=tr.latency_ms,
                ip=ip or "—",
                country=info.country,
                country_code=info.country_code,
                city=info.city,
                isp=info.isp or info.asn,
                category=info.category or categorize_ip(info),
                asn=info.asn,
                is_working=tr.working,
            )
        )

    return enriched