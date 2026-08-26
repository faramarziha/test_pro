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

# Business / enterprise keywords. Corporate suffixes (Inc/LLC/Ltd/GmbH…) are
# intentionally excluded — they appear in consumer ISP names too and would
# drown out residential signals.
BIZ_KEYWORDS: frozenset[str] = frozenset({
    "enterprise", "holdings", "group", "industries", "solutions", "systems",
    "technologies", "technology", "consulting", "services", "bank",
    "insurance", "hospital", "clinic", "government", "ministry",
    "authority", "administration", "municipal", "city of",
})

# PTR (reverse DNS) heuristics
PTR_RESIDENTIAL_HINTS: tuple[str, ...] = (
    "res", "dyn", "dynamic", "pool", "dsl", "pppoe", "ppp", "cable",
    "home", "customer", "client", "user", "dial", "adsl", "vdsl",
    "fttx", "ftth", "wifi", "mobile", "gprs", "lte", "umts", "hsd",
    "sub", "broadband", "cust", "cpe", "modem", "docsis",
)
PTR_HOSTING_HINTS: tuple[str, ...] = (
    "static", "vps", "cloud", "dedicated", "server", "host", "colo",
    "datacenter", "data-center", "dc ", "vds", "root", "node",
)

# Generic consumer-ISP wording found in org/ASN/company names. Catches
# residential ISPs that are not in the explicit CONSUMER_ISPS list.
ISP_HINTS: frozenset[str] = frozenset({
    "broadband", "internet service", "internet services", "internet provider",
    "internet access", "internet", "dsl", "adsl", "vdsl", "xdsl", "ftth",
    "fttx", "fiber", "fibre", "cable modem", "cable tv", "docsis", "hfc",
    "residential", "pppoe", "dial-up", "dialup", "wimax", "fixed wireless",
    "wireless broadband", "home internet", "telecom", "telecommunication",
    "telecommunications", "subscriber", "cable company", "isp",
})

# Words that contradict a consumer-ISP reading of the same text.
HOSTING_WORDS: frozenset[str] = frozenset({
    "cloud", "hosting", "host", "datacenter", "data center", "vps", "colo",
    "dedicated", "server", "cdn", "colo", "networks", "network solutions",
})


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
    confidence: str = "low"
    evidence: tuple[str, ...] = ()
    # Secondary source (ipapi.is) signals — WHOIS/BGP-based, more reliable
    # than ip-api keyword flags. None = not queried.
    alt_hosting: Optional[bool] = None
    alt_proxy: Optional[bool] = None
    alt_vpn: Optional[bool] = None
    alt_company: str = ""

    @property
    def text_blob(self) -> str:
        """All classification-relevant text, lowercased."""
        return " ".join([
            self.isp, self.org, self.asn, self.asname, self.alt_company,
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
    download_kbps: float = 0.0
    test_method: str = "real_delay"
    quality: str = ""
    speed_verified: bool = True
    exit_ip: str = ""
    test_error: str = ""
    ip_confidence: str = ""
    ip_evidence: list[str] = field(default_factory=list)
    download_kbps: float = 0.0
    test_error: str = ""
    test_attempts: int = 1
    quality: str = ""
    speed_verified: bool = True
    exit_ip: str = ""
    ip_confidence: str = "low"
    ip_evidence: tuple[str, ...] = ()


# ---------------------------------------------------------------------------
# STRICT categorization
# ---------------------------------------------------------------------------

def classify_ip(info: IPInfo) -> tuple[str, str, tuple[str, ...]]:
    """
    Evidence-based classification with a reliable secondary source.

    ipapi.is (WHOIS/BGP-based) overrides ip-api's keyword-guessed flags and
    fills the gaps, so residential exits are not mislabeled as datacenter or
    proxy, and unknown ISPs are not just dropped as 'Unknown'.
    """
    # 0. Secondary source (ipapi.is) — most trustworthy, decides conflicts.
    if info.alt_proxy or info.alt_vpn:
        return "Public Proxy / VPN", "high", ("ipapi.is proxy/VPN flag",)
    if info.alt_hosting:
        return "Datacenter / Hosting", "high", ("ipapi.is datacenter flag",)

    # 1. Primary flags — kept only when the secondary source did not
    #    explicitly rule them out (ip-api flags have false positives).
    if info.proxy and not (info.alt_proxy is False and info.alt_vpn is False):
        return "Public Proxy / VPN", "high", ("ip-api proxy flag",)
    if info.hosting and info.alt_hosting is not False:
        return "Datacenter / Hosting", "high", ("ip-api hosting flag",)
    if info.mobile:
        return "Mobile / Cellular", "high", ("provider mobile flag",)

    text = info.text_blob
    ptr = (info.reverse or "").lower()
    combined = f"{text} {ptr}"

    # 2. ASN / ISP / org keyword matching
    if any(kw in text for kw in CLOUD_ORGS):
        return "Datacenter / Hosting", "high", ("cloud/hosting ASN or provider",)

    if any(kw in text for kw in EDU_KEYWORDS):
        return "Business / Education", "medium", ("education/research provider",)

    if any(kw in combined for kw in CONSUMER_ISPS):
        # Consumer ISP, but double-check PTR for hosting hints
        if any(h in ptr for h in PTR_HOSTING_HINTS) and not any(
                h in ptr for h in PTR_RESIDENTIAL_HINTS):
            return "Datacenter / Hosting", "medium", ("consumer ISP with hosting-like PTR",)
        return "Residential / ISP", "medium", ("consumer ISP/ASN match",)

    # 3. Generic consumer-ISP wording (broadband/dsl/fiber/telecom…) — catches
    #    residential ISPs that are not in the explicit name list. Runs before
    #    business keywords so corporate suffixes do not overpower it.
    if any(kw in combined for kw in ISP_HINTS) and not any(h in combined for h in HOSTING_WORDS):
        return "Residential / ISP", "low", ("consumer ISP wording in org/ASN",)

    if any(kw in text for kw in BIZ_KEYWORDS):
        return "Business / Education", "medium", ("business provider match",)

    # 4. PTR-only heuristics (ISP name unmatched but PTR tells a story)
    if ptr:
        if any(h in ptr for h in PTR_RESIDENTIAL_HINTS):
            return "Residential / ISP", "low", ("residential PTR pattern",)
        if any(h in ptr for h in PTR_HOSTING_HINTS):
            return "Datacenter / Hosting", "low", ("hosting PTR pattern",)

    return "Unknown", "low", ()


def categorize_ip(info: IPInfo) -> str:
    """Compatibility wrapper returning only the category."""
    category, confidence, evidence = classify_ip(info)
    info.confidence, info.evidence = confidence, evidence
    return category


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class IPIntelligenceEngine:
    """Batch IP geolocation with rate-limit management."""

    PRIMARY_URL = "http://ip-api.com/batch"
    FALLBACK_URL = "https://freeipapi.com/api/json"
    # WHOIS/BGP-based secondary opinion (anonymous tier: 100 req/day).
    ALT_API_URL = "https://api.ipapi.is"
    ALT_DAILY_CAP = 80

    def __init__(self, batch_size: int = BATCH_SIZE):
        self._batch_size = batch_size
        self._session: Optional[aiohttp.ClientSession] = None

    async def lookup_batch(self, ips: list[str]) -> dict[str, IPInfo]:
        """Look up geolocation data for a list of unique IPs."""
        unique = list(dict.fromkeys(ips))  # dedupe, preserve order
        results: dict[str, IPInfo] = {}

        async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT) as session:
            self._session = session

            # Primary: ip-api.com batch (country/ISP/flags)
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
                    info.category = categorize_ip(info)
                    results[ip] = info
                await asyncio.sleep(0.15)

            # Secondary opinion (ipapi.is) for the IPs where it matters most:
            # unknown category, or ip-api flagged proxy/hosting (veto check).
            candidates = [
                ip for ip in unique
                if (info := results.get(ip)) is not None
                and (info.category == "Unknown" or info.proxy or info.hosting)
            ]
            if candidates:
                alt_map = await self._query_alt(candidates)
                for ip, alt in alt_map.items():
                    info = results.get(ip)
                    if info is None:
                        continue
                    info.alt_hosting = bool(alt.get("is_datacenter"))
                    info.alt_proxy = bool(alt.get("is_proxy"))
                    info.alt_vpn = bool(alt.get("is_vpn"))
                    info.alt_company = (
                        alt.get("company_name") or alt.get("asn_org") or ""
                    )
                    info.category = categorize_ip(info)

        self._session = None

        for ip in unique:
            if ip not in results:
                results[ip] = IPInfo(ip=ip, category="Unknown")

        return results

    async def _query_alt(self, ips: list[str]) -> dict[str, dict]:
        """Best-effort ipapi.is lookups (anonymous tier, capped per day)."""
        out: dict[str, dict] = {}
        for ip in ips[:self.ALT_DAILY_CAP]:
            try:
                async with self._session.get(  # type: ignore[union-attr]
                    f"{self.ALT_API_URL}/?q={ip}"
                ) as resp:
                    if resp.status != 200:
                        continue
                    d = await resp.json()
                    if not isinstance(d, dict) or "error" in d:
                        continue
                    out[ip] = d
            except Exception as exc:
                logger.debug("ipapi.is failed for %s: %s", ip, exc)
            await asyncio.sleep(0.05)
        return out

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
                    org=d.get("org", "") or (d.get("connection") or {}).get("org", ""),
                    asname=d.get("asname", "") or "",
                    reverse=d.get("reverse", "") or "",
                    hosting=bool(d.get("hosting", False)),
                    proxy=bool(d.get("proxy", False)),
                    mobile=bool(d.get("mobile", False)),
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
        # Prefer the real egress IP seen through the tunnel (accurate), then
        # the resolved server host, then a literal IP host.
        ip = tr.exit_ip or tr.resolved_ip or (tr.proxy.host if _is_ip(tr.proxy.host) else "")
        if ip:
            ips_for_lookup.append(ip)

    ip_map: dict[str, IPInfo] = {}
    if ips_for_lookup:
        ip_map = await engine.lookup_batch(ips_for_lookup)

    enriched: list[EnrichedResult] = []
    for tr in test_results:
        p = tr.proxy
        ip = tr.exit_ip or tr.resolved_ip or (p.host if _is_ip(p.host) else "")
        info = ip_map.get(ip) if ip else None
        if info is None:
            info = IPInfo(ip=ip or p.host)

        enriched.append(EnrichedResult(
            proxy_raw=p.raw,
            protocol=p.protocol.upper(),
            host=p.host,
            port=p.port,
            latency_ms=tr.latency_ms,
            download_kbps=getattr(tr, "download_kbps", 0.0),
            ip=ip or "—",
            country=info.country,
            country_code=info.country_code,
            city=info.city,
            isp=info.isp or info.org or info.asn,
            category=info.category,
            asn=info.asn,
            is_working=tr.working,
            test_method=getattr(tr, "test_method", "real_delay"),
            test_error=getattr(tr, "error", ""),
            test_attempts=getattr(tr, "attempts", 1),
            quality=getattr(tr, "quality", ""),
            speed_verified=getattr(tr, "speed_verified", True),
            exit_ip=getattr(tr, "exit_ip", ""),
            ip_confidence=info.confidence,
            ip_evidence=info.evidence,
        ))

    return enriched