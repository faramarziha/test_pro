"""
ProxyDashboard: Interactive Streamlit web dashboard.

Displays enriched proxy results in a searchable, filterable table with:
- Select / Select All checkboxes
- Copy Config button per row (clipboard)
- Bulk export (JSON, TXT subscription)
- Category and protocol filters
- Latency charts and statistics
"""
from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

from proxyhub.fetcher import SubscriptionFetcher
from proxyhub.parser import ProxyParser
from proxyhub.tester import SingBoxTester
from proxyhub.installer import SingBoxInstaller, find_singbox_sync
from proxyhub.intelligence import (
    IPIntelligenceEngine,
    enrich_test_results,
    EnrichedResult,
)

# Setup file logging so users can send logs for debugging
LOG_DIR = Path.home() / ".proxyhub" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "proxyhub.log"

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stderr),
    ],
)
logger = logging.getLogger(__name__)

# Default subscription URL
DEFAULT_URL = (
    "https://raw.githubusercontent.com/Diversan313/"
    "apex-parser/main/alive_full.txt"
)

# Page config
st.set_page_config(
    page_title="ProxyHub • Proxy Intelligence Dashboard",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------

def _inject_css() -> None:
    st.markdown("""
    <style>
    /* Clean dark-themed table */
    .stDataFrame td, .stDataFrame th {
        font-size: 0.82rem !important;
        padding: 0.35rem 0.5rem !important;
    }
    /* Copy button styling */
    .copy-btn {
        font-size: 0.7rem;
        padding: 2px 8px;
        border-radius: 4px;
        border: 1px solid rgba(255,255,255,0.15);
        background: rgba(255,255,255,0.06);
        color: #ccc;
        cursor: pointer;
        transition: all 0.15s;
    }
    .copy-btn:hover {
        background: rgba(255,255,255,0.14);
        border-color: rgba(255,255,255,0.3);
    }
    /* Metric cards */
    .metric-card {
        background: rgba(255,255,255,0.04);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 10px;
        padding: 1rem 1.25rem;
        text-align: center;
    }
    .metric-card .label {
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: #888;
        margin-bottom: 0.3rem;
    }
    .metric-card .value {
        font-size: 1.6rem;
        font-weight: 700;
        color: #e0e0e0;
    }
    .metric-card .sub {
        font-size: 0.72rem;
        color: #666;
    }
    /* Category badges */
    .badge-residential { background: #1b5e20; color: #a5d6a7; padding: 2px 8px; border-radius: 10px; font-size: 0.7rem; }
    .badge-datacenter { background: #0d47a1; color: #90caf9; padding: 2px 8px; border-radius: 10px; font-size: 0.7rem; }
    .badge-proxy { background: #b71c1c; color: #ef9a9a; padding: 2px 8px; border-radius: 10px; font-size: 0.7rem; }
    .badge-business { background: #4a148c; color: #ce93d8; padding: 2px 8px; border-radius: 10px; font-size: 0.7rem; }
    .badge-unknown { background: #424242; color: #bdbdbd; padding: 2px 8px; border-radius: 10px; font-size: 0.7rem; }
    </style>
    """, unsafe_allow_html=True)


CATEGORY_BADGE_MAP = {
    "Residential / ISP": "badge-residential",
    "Datacenter / Hosting": "badge-datacenter",
    "Public Proxy / VPN": "badge-proxy",
    "Business / Education": "badge-business",
}
CATEGORY_BADGE_MAP_DEFAULT = "badge-unknown"


def _badge_html(cat: str) -> str:
    cls = CATEGORY_BADGE_MAP.get(cat, CATEGORY_BADGE_MAP_DEFAULT)
    return f'<span class="{cls}">{cat}</span>'


# ---------------------------------------------------------------------------
# Session state init
# ---------------------------------------------------------------------------

def _init_session() -> None:
    defaults = {
        "results": None,
        "enriched": [],
        "df": None,
        "testing": False,
        "error": None,
        "error_traceback": None,
        "pipeline_logs": [],
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


# ---------------------------------------------------------------------------
# Pipeline runner (async)
# ---------------------------------------------------------------------------

async def _run_pipeline(
    source_url: str,
    text_input: str,
    concurrency: int,
    timeout: float,
) -> list[EnrichedResult]:
    """Full pipeline: fetch → parse → test → enrich."""
    log_msgs: list[str] = []

    def _log(msg: str) -> None:
        logger.info(msg)
        log_msgs.append(msg)
        # Store in session state so the UI can show live-ish logs
        st.session_state.pipeline_logs = log_msgs

    fetcher = SubscriptionFetcher()
    parser = ProxyParser()

    # 1. Fetch
    _log("Fetching subscription...")
    if text_input.strip():
        result = fetcher.parse_text(text_input, source="manual")
    else:
        result = await fetcher.fetch_url(source_url)
    _log(f"Fetched {result.proxy_count} lines (Base64: {result.is_base64})")

    if not result.raw_lines:
        raise ValueError("No proxy configurations found in the source.")

    # 2. Parse
    _log("Parsing proxy configurations...")
    parsed = []
    for line in result.raw_lines:
        p = parser.parse(line)
        if p:
            parsed.append(p)
    _log(f"Parsed {len(parsed)} proxies")

    if not parsed:
        raise ValueError("Failed to parse any proxy configurations.")

    # 3. Test
    # Install sing-box first (downloaded once at pipeline start, not during testing)
    installer = SingBoxInstaller()
    sb_path = await installer.ensure_installed()
    if sb_path:
        _log(f"sing-box ready: {sb_path}")
    else:
        _log("sing-box not available — using TCP fallback tests")

    tester = SingBoxTester(
        concurrency=concurrency,
        connect_timeout=timeout,
        singbox_path=sb_path,  # pre-resolved path avoids race in workers
        installer=installer,
    )

    _log(f"Testing {len(parsed)} proxies with {concurrency} workers...")
    batch = await tester.test_all(parsed)
    _log(f"Tested: {batch.working} working, {batch.dead} dead ({batch.elapsed_seconds}s)")

    # 4. Enrich
    _log("Enriching results with IP geolocation...")
    engine = IPIntelligenceEngine()
    enriched = await enrich_test_results(batch.results, engine)
    _log(f"Enrichment complete.")

    # Sort: working first, then by latency
    enriched.sort(key=lambda r: (not r.is_working, r.latency_ms))

    return enriched


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

def _render_singbox_status() -> None:
    """Show sing-box availability status in the sidebar."""
    path = find_singbox_sync()
    if path:
        st.success(f"✅ sing-box ready ({Path(path).name})")
    else:
        st.info("⬇️ sing-box will be auto-downloaded on first run")


def _render_sidebar() -> tuple[str, str, int, float]:
    st.sidebar.title("⚙️ Configuration")

    with st.sidebar.expander("📡 Source", expanded=True):
        source_url = st.text_input(
            "Subscription URL",
            value=DEFAULT_URL,
            key="source_url",
            placeholder="https://...",
        )
        text_input = st.text_area(
            "Or paste raw configs / Base64",
            height=100,
            key="text_input",
            placeholder="vless://...\nvmess://...",
        )

    with st.sidebar.expander("🧪 Testing", expanded=True):
        concurrency = st.slider(
            "Concurrent workers",
            min_value=10,
            max_value=200,
            value=50,
            step=10,
            help="Higher = faster but more system load",
        )
        timeout = st.slider(
            "Timeout (seconds)",
            min_value=2.0,
            max_value=10.0,
            value=5.0,
            step=0.5,
        )

        # sing-box status indicator
        _render_singbox_status()

    return source_url, text_input, concurrency, timeout


# ---------------------------------------------------------------------------
# Main content
# ---------------------------------------------------------------------------

def _render_header() -> None:
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title("🌐 ProxyHub")
        st.caption("Parse, test, enrich, and export proxy configurations at scale.")
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1:
            run = st.button(
                "▶️ Run Pipeline",
                type="primary",
                use_container_width=True,
                disabled=st.session_state.testing,
            )
        with c2:
            if st.button("🔄 Reset", use_container_width=True):
                st.session_state.enriched = []
                st.session_state.df = None
                st.session_state.testing = False
                st.session_state.error = None
                st.session_state.error_traceback = None
                st.session_state.pipeline_logs = []
                st.rerun()
    if run:
        st.session_state.testing = True
        st.session_state.enriched = []
        st.session_state.error = None
        st.session_state.error_traceback = None
        st.session_state.pipeline_logs = []
        st.rerun()


def _render_progress() -> None:
    """Show pipeline logs while running."""
    if not st.session_state.testing:
        return
    logs = st.session_state.get("pipeline_logs", [])
    if logs:
        with st.expander("📝 Pipeline Log", expanded=True):
            for msg in logs:
                st.text(f"  • {msg}")


def _render_metrics(enriched: list[EnrichedResult]) -> None:
    working = [r for r in enriched if r.is_working]
    dead = len(enriched) - len(working)
    avg_latency = (
        sum(r.latency_ms for r in working) / len(working) if working else 0.0
    )
    cats = {}
    for r in working:
        cats[r.category] = cats.get(r.category, 0) + 1

    cols = st.columns(5)
    cols[0].markdown(
        f'<div class="metric-card"><div class="label">Total</div>'
        f'<div class="value">{len(enriched)}</div></div>',
        unsafe_allow_html=True,
    )
    cols[1].markdown(
        f'<div class="metric-card"><div class="label">Working</div>'
        f'<div class="value" style="color:#4caf50">{len(working)}</div></div>',
        unsafe_allow_html=True,
    )
    cols[2].markdown(
        f'<div class="metric-card"><div class="label">Dead</div>'
        f'<div class="value" style="color:#f44336">{dead}</div></div>',
        unsafe_allow_html=True,
    )
    cols[3].markdown(
        f'<div class="metric-card"><div class="label">Avg Latency</div>'
        f'<div class="value">{avg_latency:.0f} ms</div></div>',
        unsafe_allow_html=True,
    )
    cols[4].markdown(
        f'<div class="metric-card"><div class="label">Residential</div>'
        f'<div class="value">{cats.get("Residential / ISP", 0)}</div></div>',
        unsafe_allow_html=True,
    )


def _render_table(enriched: list[EnrichedResult]) -> None:
    if not enriched:
        st.info("No results to display. Run the pipeline first.")
        return

    # Filters
    col_f1, col_f2, col_f3, col_f4 = st.columns(4)
    with col_f1:
        protocols = sorted({r.protocol for r in enriched})
        sel_proto = st.multiselect("Protocol", protocols, default=protocols, key="f_proto")
    with col_f2:
        categories = sorted({r.category for r in enriched})
        sel_cat = st.multiselect("Category", categories, default=categories, key="f_cat")
    with col_f3:
        countries = sorted({r.country for r in enriched if r.country})
        sel_country = st.multiselect("Country", countries, default=[], key="f_country")
    with col_f4:
        search = st.text_input("🔍 Search", placeholder="IP, ISP, host...", key="f_search")

    # Build dataframe
    data = []
    for i, r in enumerate(enriched):
        if r.protocol not in sel_proto:
            continue
        if r.category not in sel_cat:
            continue
        if sel_country and r.country not in sel_country:
            continue
        if search:
            q = search.lower()
            if not any(
                q in field.lower()
                for field in [r.host, r.ip, r.isp, r.country, r.city, r.proxy_raw]
                if field
            ):
                continue

        status_icon = "✅" if r.is_working else "❌"
        data.append({
            "": status_icon,
            "Protocol": r.protocol,
            "Server": f"{r.host}:{r.port}",
            "IP": r.ip,
            "Latency": f"{r.latency_ms:.0f} ms" if r.latency_ms > 0 else "—",
            "Country": r.country,
            "City": r.city,
            "ISP / ASN": r.isp,
            "Category": _badge_html(r.category),
            "Config": r.proxy_raw,
            "_idx": i,
        })

    df = pd.DataFrame(data)
    if df.empty:
        st.warning("No results match the current filters.")
        return

    # Display as interactive dataframe with selection
    display_cols = [
        "", "Protocol", "Server", "IP", "Latency", "Country",
        "City", "ISP / ASN", "Category",
    ]
    display_df = df[display_cols]

    st.dataframe(
        display_df,
        use_container_width=True,
        height=600,
        hide_index=True,
        column_config={
            "": st.column_config.Column(width="small"),
            "Category": st.column_config.Column(width="medium"),
        },
    )

    # Row-level actions: Copy Config buttons
    st.markdown("---")
    st.subheader("📋 Copy Individual Configs")

    cols = st.columns(4)
    for idx, (_, row) in enumerate(df.iterrows()):
        col = cols[idx % 4]
        cfg = row["Config"]
        label = f"{row['Protocol']} • {row['Server']} ({row['Latency']})"
        truncated = cfg[:80] + "..." if len(cfg) > 80 else cfg

        # Use a button paired with st.code for easy copy
        with col:
            with st.expander(label, expanded=False):
                st.code(cfg, language=None)
                st.button(
                    f"📋 Copy Config",
                    key=f"copy_{row['_idx']}",
                    use_container_width=True,
                    on_click=_copy_to_clipboard,
                    args=(cfg,),
                )

    # Bulk export
    st.markdown("---")
    st.subheader("💾 Bulk Export")
    working_configs = [
        r.proxy_raw for r in enriched
        if r.is_working and r.protocol in sel_proto and r.category in sel_cat
    ]

    col_x1, col_x2, col_x3 = st.columns(3)
    with col_x1:
        if working_configs:
            txt_data = "\n".join(working_configs)
            st.download_button(
                "📄 Export TXT (Subscription)",
                data=txt_data,
                file_name=f"proxyhub_export_{_timestamp()}.txt",
                mime="text/plain",
                use_container_width=True,
            )
    with col_x2:
        if working_configs:
            json_data = json.dumps(
                [
                    {
                        "protocol": r.protocol,
                        "host": r.host,
                        "port": r.port,
                        "latency_ms": r.latency_ms,
                        "country": r.country,
                        "city": r.city,
                        "isp": r.isp,
                        "category": r.category,
                        "config": r.proxy_raw,
                    }
                    for r in enriched
                    if r.is_working
                    and r.protocol in sel_proto
                    and r.category in sel_cat
                ],
                indent=2,
            )
            st.download_button(
                "📊 Export JSON",
                data=json_data,
                file_name=f"proxyhub_export_{_timestamp()}.json",
                mime="application/json",
                use_container_width=True,
            )

    with col_x3:
        by_cat: dict[str, list[str]] = {}
        for r in enriched:
            if r.is_working:
                by_cat.setdefault(r.category, []).append(r.proxy_raw)
        if by_cat:
            cat_txt = ""
            for cat, configs in by_cat.items():
                cat_txt += f"# {cat}\n" + "\n".join(configs) + "\n\n"
            st.download_button(
                "📂 Export by Category",
                data=cat_txt,
                file_name=f"proxyhub_categories_{_timestamp()}.txt",
                mime="text/plain",
                use_container_width=True,
            )


def _copy_to_clipboard(text: str) -> None:
    """Attempt clipboard copy via pyperclip; fallback to streamlit info."""
    try:
        import pyperclip

        pyperclip.copy(text)
        st.toast("✅ Copied to clipboard!", icon="📋")
    except Exception:
        st.info("Copy manually:\n\n```\n{}\n```".format(text[:500]))


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


# ---------------------------------------------------------------------------
# Main entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    _inject_css()
    _init_session()
    _render_header()

    source_url, text_input, concurrency, timeout = _render_sidebar()

    # Run pipeline if triggered
    if st.session_state.testing and not st.session_state.enriched:
        with st.spinner("Running pipeline..."):
            try:
                # Get or create event loop for this thread
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)

                enriched = loop.run_until_complete(
                    _run_pipeline(source_url, text_input, concurrency, timeout)
                )
                st.session_state.enriched = enriched
                st.session_state.testing = False
                logger.info(f"Pipeline complete: {len(enriched)} results")
                st.rerun()
            except Exception as exc:
                logger.error(f"Pipeline failed: {exc}", exc_info=True)
                st.session_state.error = str(exc)
                st.session_state.error_traceback = traceback.format_exc()
                st.session_state.testing = False
                st.rerun()

    _render_progress()

    if st.session_state.error:
        st.error(f"❌ Pipeline error: {st.session_state.error}")
        if st.session_state.error_traceback:
            with st.expander("🔍 Full error details", expanded=False):
                st.code(st.session_state.error_traceback, language="python")
            st.info(
                f"📄 Full logs are saved to: `{LOG_FILE}`\n\n"
                "Send this file if you need help debugging."
            )

    if st.session_state.enriched:
        _render_metrics(st.session_state.enriched)
        _render_table(st.session_state.enriched)
    elif not st.session_state.testing:
        st.info(
            "👆 Click **Run Pipeline** to fetch, test, and analyze proxy configurations."
        )
        with st.expander("📖 How it works", expanded=False):
            st.markdown("""
            1. **Fetch**: Downloads subscription data from a URL or parses pasted configs. Auto-detects Base64 encoding.
            2. **Parse**: Extracts structured data from VLESS, VMess, Trojan, Shadowsocks, Hysteria2, and TUIC URIs.
            3. **Test**: Validates connectivity through sing-box ephemeral listeners with configurable concurrency (10–200 workers).
            4. **Enrich**: Queries ip-api.com batch API for geolocation, ISP, and hosting data.
            5. **Categorize**: Classifies nodes into Residential, Datacenter, Proxy/VPN, or Business/Education.
            """)


if __name__ == "__main__":
    main()