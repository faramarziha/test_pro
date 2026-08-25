"""
IPIntelligenceEngine: Batch IP geolocation and STRICT network categorization.

Uses ip-api.com/batch (free tier: 45 req/min, 100 IPs per batch) with
rate-limit management + retry. Falls back to freeipapi.com on failures.

Categorization priority (strict, evidence-based — no blind fallback):
  1. proxy flag        → Public Proxy / VPN
  2. hosting flag      → Datacenter / Hosting
  3. mobile flag       → Mobile / Cellular
  4. ASN/ISP/org/asname keyword match (cloud / consumer / edu-biz)
  5. PTR (reverse DNS) heuristics
  6. otherwise         → Unknown  (never guessed as residential)
"""
from __future__ import annotations

import asyncio
import ipaddress
import logging
from dataclasses import dataclass, field
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

# Batch API settings
BATCH_SIZE = 100
BATCH_DELAY = 1.6  # seconds between batches (45 req/min free tier)
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=20)

API_FIELDS = "query,status,country,countryCode,city,isp,org,as,asname,reverse,mobile,proxy,hosting"

# ---------------------------------------------------------------------------
# Keyword databases (lowercase substring matching)
# ---------------------------------------------------------------------------

# Cloud / VPS / hosting providers
CLOUD_ORGS: frozenset[str] = frozenset({
    # Major clouds
    "hetzner", "digitalocean", "amazon", "aws", "google cloud", "gcp",
    "microsoft", "azure", "ovh", "vultr", "linode", "akamai", "alibaba",
    "tencent", "oracle cloud", "ibm cloud", "rackspace", "leaseweb",
    "cloudflare", "fastly", "gcore", "vercel", "netlify", "fly.io",
    # Regional VPS / hosting
    "psychz", "colocrossing", "buyvm", "ramnode", "interserver", "contabo",
    "scaleway", "hostinger", "namecheap", "godaddy", "hostwinds", "kamatera",
    "selectel", "timeweb", "aeza", "vdsina", "firstvds", "beget",
    "sprinthost", "ihor", "melbikomas", "xhost", "bluevps", "aeza",
    "m247", "datacamp", "packethub", "tzulo", "nuclearfallout", "serverion",
    "stark industries", "operavps", "hostbrr", "clouvider", "ukraine hosting",
    "xhost internet", "hostslim", "worldstream", "transip", "netcup",
    "strato", "ionos", "1&1", "1and1", "hosteons", "spartanhost",
    "frantech", "cloudvps", "aruba", "hetzner online", "digital ocean",
    "choopa", "cogent", "zayo", "tier.net", "path.net", "lookhosting",
    "avalanche", "vpscity", "dmit", "bandwagon", "bwh", "hostdare",
    "racknerd", "greencloud", "buyshared", "virpus", "hostodo",
    "nexusbytes", "advin", "myhostin", "servarica", "hostens",
})

# Consumer / residential ISPs — global + Iran + CIS + MENA + APAC + Americas
CONSUMER_ISPS: frozenset[str] = frozenset({
    # Iran
    "mci", "hamrahe aval", "hamrah-e aval", "irancell", "mtn irancell",
    "tci", "tct", "mokhaberat", "shatel", "rightel", "asiatech",
    "pishgaman", "pars online", "parsonline", "datak", "fanava",
    "respina", "mobinnet", "hiweb", "zironet", "aptel", "iran telecom",
    "information technology company", "tcij", "sabanet", "azarnet",
    "pars packtech", "hostiran", "safanet", "iran host", "iranserver",
    # North America
    "comcast", "xfinity", "verizon", "at&t", "at t", "t-mobile", "sprint",
    "spectrum", "charter", "cox", "centurylink", "frontier", "optimum",
    "altice", "mediacom", "windstream", "cable one", "wow!", "rcn",
    "rogers", "bell canada", "bell", "telus", "sasktel", "videotron",
    "cogeco", "eastlink", "shaw",
    # Europe
    "vodafone", "orange", "telefonica", "movistar", "o2", "o2 uk",
    "deutsche telekom", "telekom", "dtag", "british telecom", "bt ",
    "sky broadband", "virgin media", "kpn", "proximus", "swisscom",
    "magenta", "telekom austria", "play", "iliad", "free sas", "sfr",
    "bouygues", "telecom italia", "tim ", "wind tre", "telenor", "telia",
    "tele2", "elisa", "dna oy", "telia", "eir", "vodafone ireland",
    "ziggo", "odido", "plus.pl", "play mobile", "upc", "sunrise",
    "salt mobile", "yettel", "one austria", "hooray", "spusu",
    # CIS / Russia
    "mts", "beeline", "megafon", "rostelecom", "er-telecom", "dom.ru",
    "yota", "ttk", "transtelecom", "vimpelcom", "kazakhtelecom",
    "ukrtelecom", "kyivstar", "lifecell", "volia", "triolan",
    # MENA / Turkey
    "turk telekom", "türk telekom", "turkcell", "vodafone tr",
    "etisalat", "e& ", "du telecom", "stc", "mobily", "zain",
    "orange egypt", "vodafone egypt", "etisalat misr", "telecom egypt",
    "we egypt", "ooredoo", "asiacell", "zain iq", "korek",
    # Asia-Pacific
    "ntt", "kddi", "softbank", "docomo", "ntt east", "ntt west",
    "sk broadband", "kt corp", "lg uplus", "lg u+", "olleh",
    "china telecom", "china unicom", "china mobile", "cmcc",
    "hinet", "chunghwa telecom", "taiwan mobile", "aptg", "tfn",
    "pccw", "hkt", "smarTone", "3hk", "csl",
    "singtel", "starhub", "m1 limited", "myrepublic", "viewqwest",
    "maxis", "celcom", "digi", "unifi", "tm net", "time dotcom",
    "indosat", "telkomsel", "xl axiata", "smartfren", "tri indonesia",
    "pldt", "globe telecom", "converge", "sky fiber",
    "viettel", "vnpt", "fpt telecom", "mobifone", "vinaphone",
    "ais", "true internet", "dtac", "3bb", "nt plc",
    "airtel", "jio", "reliance jio", "vodafone idea", "bsnl", "mtnl",
    "act fibernet", "hathway", "tikona", "excitel", "tata play",
    "ptcl", "zong", "jazz", "telenor pk", "ufone", "banglalink",
    "grameenphone", "robi", "nepal telecom", "ntc", "ncell",
    # Oceania / Africa
    "telstra", "optus", "tpg", "aussie broadband", "superloop",
    "myrepublic au", "vodafone nz", "spark nz", "2degrees",
    "mtn", "safaricom", "airtel africa", "ethiotelecom", "maroc telecom",
    "djezzy", "ooredoo Algeria", "tunisie telecom", "orange tunisie",
})

# Education / research keywords
EDU_KEYWORDS: frozenset[str] = frozenset({
    "university", "universite", "universität", "universidad", "università",
    "college", "school", "academy", "institute", "institut", "research",
    "edu", "ac.ir", "ac.uk", "education", "faculty", "campus",
    "cern", "nasa", "dfn", "janet", "internet2", "rediris", "garr",
})

# Business / enterprise keywords
BIZ_KEYWORDS: frozenset[str] = frozenset({
    "corp", "corporate", "corporation", "enterprise", "inc", "llc",
    "ltd", "limited", "gmbh", "plc", "s.a.", "b.v.", "oy", "ab ",
    "holdings", "group", "industries", "solutions", "systems",
    "technologies", "technology", "consulting", "services", "bank",
    "insurance", "hospital", "clinic", "government", "ministry",
    "authority", "administration", "municipal", "city of",
})

# PTR (reverse DNS) heuristics
PTR_RESIDENTIAL_HINTS: tuple[str, ...] = (
    "res", "dyn", "dynamic", "pool", "dsl", "pppoe", "ppp", "cable",
    "home", "customer", "client", "user", "dial", "adsl", "vdsl",
    "fttx", "ftth", "wifi", "mobile", "gprs", "lte", "umts",
)
PTR_HOSTING_HINTS: tuple[str, ...] = (
    "static", "vps", "cloud", "dedicated", "server", "host", "colo",
    "datacenter", "data-center", "dc ", "vds", "root", "node",
)


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class IPInfo:
    ip: str
    country: str = ""
    country_code: str = ""
    city: str = ""
    isp: str = ""
    org: str = ""
    asn: str = ""          # e.g. "AS15169 Google LLC"
    asname: str = ""       # e.g. "GOOGLE"
    reverse: str = ""      # PTR record
    hosting: bool = False
    proxy: bool = False
    mobile: bool = False
    category: str = "Unknown"

    @property
    def text_blob(self) -> str:
        """All classification-relevant text, lowercased."""
        return " ".join([
            self.isp, self.org, self.asn, self.asname,
        ]).lower()


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


# ---------------------------------------------------------------------------
# STRICT categorization
# ---------------------------------------------------------------------------

def categorize_ip(info: IPInfo) -> str:
    """
    Evidence-based classification. Never guesses 'Residential' without
    positive evidence — unmatched IPs are 'Unknown'.
    """
    # 1-3. Explicit flags from the API (highest confidence)
    if info.proxy:
        return "Public Proxy / VPN"
    if info.hosting:
        return "Datacenter / Hosting"
    if info.mobile:
        return "Mobile / Cellular"

    text = info.text_blob
    ptr = (info.reverse or "").lower()

    # 4. ASN / ISP / org keyword matching
    if any(kw in text for kw in CLOUD_ORGS):
        return "Datacenter / Hosting"

    if any(kw in text for kw in EDU_KEYWORDS):
        return "Business / Education"

    if any(kw in text for kw in CONSUMER_ISPS):
        # Consumer ISP, but double-check PTR for hosting hints
        if any(h in ptr for h in PTR_HOSTING_HINTS) and not any(
                h in ptr for h in PTR_RESIDENTIAL_HINTS):
            return "Datacenter / Hosting"
        return "Residential / ISP"

    if any(kw in text for kw in BIZ_KEYWORDS):
        return "Business / Education"

    # 5. PTR-only heuristics (ISP name unmatched but PTR tells a story)
    if ptr:
        if any(h in ptr for h in PTR_RESIDENTIAL_HINTS):
            return "Residential / ISP"
        if any(h in ptr for h in PTR_HOSTING_HINTS):
            return "Datacenter / Hosting"

    # 6. Strict fallback — no positive evidence
    return "Unknown"


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class IPIntelligenceEngine:
    """Batch IP geolocation with rate-limit management."""

    PRIMARY_URL = "http://ip-api.com/batch"
    FALLBACK_URL = "https://freeipapi.com/api/json"

    def __init__(self, batch_size: int = BATCH_SIZE):
        self._batch_size = batch_size
        self._session: Optional[aiohttp.ClientSession] = None

    async def lookup_batch(self, ips: list[str]) -> dict[str, IPInfo]:
        """Look up geolocation data for a list of unique IPs."""
        unique = list(dict.fromkeys(ips))  # dedupe, preserve order
        results: dict[str, IPInfo] = {}

        async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT) as session:
            self._session = session

            for i in range(0, len(unique), self._batch_size):
                batch = unique[i:i + self._batch_size]
                batch_results = await self._query_primary(batch)
                results.update(batch_results)

                if i + self._batch_size < len(unique):
                    await asyncio.sleep(BATCH_DELAY)

        # Fallback for any misses (single-IP, capped to avoid rate abuse)
        missing = [ip for ip in unique if ip not in results][:50]
        for ip in missing:
            info = await self._query_fallback_single(ip)
            if info:
                results[ip] = info
            await asyncio.sleep(0.15)

        for ip in unique:
            if ip not in results:
                results[ip] = IPInfo(ip=ip, category="Unknown")

        return results

    async def _query_primary(self, ips: list[str]) -> dict[str, IPInfo]:
        """Query ip-api.com/batch. Retries on 429 with backoff."""
        payload = [{"query": ip, "fields": API_FIELDS} for ip in ips]

        for attempt in range(3):
            try:
                async with self._session.post(  # type: ignore[union-attr]
                    self.PRIMARY_URL, json=payload
                ) as resp:
                    if resp.status == 429:
                        wait = 6.0 * (attempt + 1)
                        logger.warning("ip-api rate-limited (429), waiting %.0fs", wait)
                        await asyncio.sleep(wait)
                        continue
                    if resp.status != 200:
                        logger.warning("ip-api returned %d", resp.status)
                        return {}
                    data = await resp.json()
                    return self._parse_primary_response(data)
            except Exception as exc:
                logger.warning("ip-api error: %s (attempt %d)", exc, attempt + 1)
                await asyncio.sleep(2.0 * (attempt + 1))

        return {}

    async def _query_fallback_single(self, ip: str) -> Optional[IPInfo]:
        """Single-IP fallback via freeipapi.com."""
        try:
            async with self._session.get(  # type: ignore[union-attr]
                f"{self.FALLBACK_URL}/{ip}"
            ) as resp:
                if resp.status != 200:
                    return None
                d = await resp.json()
                return IPInfo(
                    ip=ip,
                    country=d.get("countryName", ""),
                    country_code=d.get("countryCode", ""),
                    city=d.get("city", ""),
                    isp=d.get("isp", "") or (d.get("connection") or {}).get("isp", ""),
                    asn=str((d.get("connection") or {}).get("asn", "") or ""),
                    hosting=bool(d.get("hosting", False)),
                    proxy=bool(d.get("proxy", False)),
                )
        except Exception as exc:
            logger.debug("freeipapi fallback failed for %s: %s", ip, exc)
            return None

    def _parse_primary_response(self, data: list) -> dict[str, IPInfo]:
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
                org=item.get("org", ""),
                asn=item.get("as", ""),
                asname=item.get("asname", ""),
                reverse=item.get("reverse", ""),
                hosting=bool(item.get("hosting", False)),
                proxy=bool(item.get("proxy", False)),
                mobile=bool(item.get("mobile", False)),
            )
            info.category = categorize_ip(info)
            results[ip] = info
        return results


# ---------------------------------------------------------------------------
# Enrichment pipeline
# ---------------------------------------------------------------------------

def _is_ip(s: str) -> bool:
    try:
        ipaddress.ip_address(s)
        return True
    except ValueError:
        return False


async def enrich_test_results(
    test_results: list,
    engine: Optional[IPIntelligenceEngine] = None,
) -> list[EnrichedResult]:
    """Take test results + IP lookups → enriched display objects."""
    if engine is None:
        engine = IPIntelligenceEngine()

    ips_for_lookup: list[str] = []
    for tr in test_results:
        if not tr.working:
            continue
        ip = tr.resolved_ip or (tr.proxy.host if _is_ip(tr.proxy.host) else "")
        if ip:
            ips_for_lookup.append(ip)

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

        enriched.append(EnrichedResult(
            proxy_raw=p.raw,
            protocol=p.protocol.upper(),
            host=p.host,
            port=p.port,
            latency_ms=tr.latency_ms,
            ip=ip or "—",
            country=info.country,
            country_code=info.country_code,
            city=info.city,
            isp=info.isp or info.org or info.asn,
            category=info.category,
            asn=info.asn,
            is_working=tr.working,
        ))

    return enriched