"""
ProxyDashboard: Interactive Streamlit web dashboard.

Threaded pipeline with live native-Streamlit progress (no raw HTML),
dark glassmorphism theme, and fast TCP-based proxy testing.
"""
from __future__ import annotations

import asyncio
import json
import logging
import sys
import threading
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

# ---------------------------------------------------------------------------
# Logging (file only — never terminal)
# ---------------------------------------------------------------------------
LOG_DIR = Path.home() / ".proxyhub" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "proxyhub.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8")],
)
logger = logging.getLogger(__name__)

DEFAULT_URL = (
    "https://raw.githubusercontent.com/Diversan313/"
    "apex-parser/main/alive_full.txt"
)

st.set_page_config(
    page_title="ProxyHub — Proxy Intelligence",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Global CSS — everything in one place, NO inline <style> in markdown
# ---------------------------------------------------------------------------

GLOBAL_CSS = """
<style>
:root {
    --bg: #0a0e14;
    --surface: #131820;
    --surface2: #1a212b;
    --border: rgba(255,255,255,0.06);
    --text: #c8ccd4;
    --text-dim: #6b7280;
    --accent: #10b981;
    --accent2: #06b6d4;
    --accent-grad: linear-gradient(135deg, #10b981 0%, #06b6d4 100%);
    --red: #ef4444;
    --amber: #f59e0b;
    --purple: #8b5cf6;
}

/* Page background */
.stApp {
    background: linear-gradient(160deg, #0a0e14 0%, #0f1724 50%, #0a0e14 100%);
}
.stMainBlockContainer { padding-top: 1.5rem !important; }

/* Sidebar */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #111827 0%, #0f1724 100%) !important;
    border-right: 1px solid rgba(255,255,255,0.05) !important;
}

/* Headers */
h1 {
    font-size: 2rem !important;
    font-weight: 800 !important;
    letter-spacing: -0.02em !important;
    background: var(--accent-grad);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    padding-bottom: 0.25rem !important;
}
h2 { font-size: 1.15rem !important; font-weight: 700 !important; color: #e5e7eb !important; }
h3 { font-size: 1rem !important; font-weight: 600 !important; color: #d1d5db !important; }

/* Buttons */
.stButton > button {
    border-radius: 10px !important;
    font-weight: 600 !important;
    transition: all 0.2s !important;
    border: 1px solid var(--border) !important;
    background: var(--surface2) !important;
    color: var(--text) !important;
}
.stButton > button:hover {
    background: rgba(255,255,255,0.08) !important;
    border-color: rgba(255,255,255,0.15) !important;
}
.stButton > button[kind="primary"] {
    background: var(--accent-grad) !important;
    border: none !important;
    color: #fff !important;
    box-shadow: 0 4px 20px rgba(16,185,129,0.25);
}
.stButton > button[kind="primary"]:hover {
    box-shadow: 0 6px 28px rgba(16,185,129,0.4);
}
.stButton > button:disabled {
    opacity: 0.4 !important;
    cursor: not-allowed !important;
}

/* Inputs */
.stTextInput input, .stTextArea textarea {
    background: rgba(255,255,255,0.04) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
    color: var(--text) !important;
}
.stTextInput input:focus, .stTextArea textarea:focus {
    border-color: var(--accent) !important;
    box-shadow: 0 0 0 2px rgba(16,185,129,0.15) !important;
}

/* Dataframe */
.stDataFrame {
    border: 1px solid var(--border) !important;
    border-radius: 12px !important;
    overflow: hidden !important;
}

/* Metric cards */
.metric-card {
    background: linear-gradient(145deg, rgba(255,255,255,0.04) 0%, rgba(255,255,255,0.01) 100%);
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 14px;
    padding: 1.2rem 1.4rem;
    position: relative;
    overflow: hidden;
    height: 100%;
}
.metric-card::before {
    content: "";
    position: absolute;
    left: 0; top: 0; bottom: 0;
    width: 3px;
    background: var(--accent-grad);
    border-radius: 0 2px 2px 0;
}
.metric-card .m-label {
    font-size: 0.68rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #6b7280;
    margin-bottom: 0.3rem;
    font-weight: 600;
}
.metric-card .m-value {
    font-size: 1.65rem;
    font-weight: 800;
    color: #e5e7eb;
    letter-spacing: -0.02em;
    line-height: 1.1;
}
.metric-card .m-sub {
    font-size: 0.66rem;
    color: #4b5563;
    margin-top: 0.2rem;
}

/* Category badges */
.badge {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 20px;
    font-size: 0.66rem;
    font-weight: 600;
    letter-spacing: 0.02em;
    white-space: nowrap;
}
.badge-residential { background: rgba(16,185,129,0.18); color: #6ee7b7; }
.badge-datacenter  { background: rgba(59,130,246,0.18); color: #93c5fd; }
.badge-proxy       { background: rgba(239,68,68,0.18); color: #fca5a5; }
.badge-business    { background: rgba(139,92,246,0.18); color: #c4b5fd; }
.badge-unknown     { background: rgba(107,114,128,0.18); color: #9ca3af; }

/* Hero badge */
.hero-badge {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: rgba(16,185,129,0.1);
    border: 1px solid rgba(16,185,129,0.2);
    border-radius: 20px;
    padding: 4px 14px;
    font-size: 0.7rem;
    color: #6ee7b7;
    font-weight: 500;
    margin-bottom: 0.5rem;
}

/* Status dot */
.status-dot {
    display: inline-block;
    width: 8px; height: 8px;
    border-radius: 50%;
    margin-right: 6px;
    animation: ph-pulse 2s infinite;
}
.status-dot.green  { background: #10b981; box-shadow: 0 0 8px rgba(16,185,129,0.5); }
.status-dot.yellow { background: #f59e0b; box-shadow: 0 0 8px rgba(245,158,11,0.5); }
.status-dot.red    { background: #ef4444; box-shadow: 0 0 8px rgba(239,68,68,0.5); }
@keyframes ph-pulse {
    0%, 100% { opacity: 1; }
    50%      { opacity: 0.35; }
}

/* Step cards */
.step-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 1.1rem 0.8rem;
    text-align: center;
    height: 100%;
}
.step-card .s-icon { font-size: 1.6rem; margin-bottom: 0.4rem; }
.step-card .s-title { font-weight: 700; font-size: 0.8rem; color: #e5e7eb; margin-bottom: 0.2rem; }
.step-card .s-desc { font-size: 0.62rem; color: #6b7280; }

/* Welcome hero */
.welcome-hero {
    text-align: center;
    padding: 2.5rem 1rem 1.5rem;
}
.welcome-hero .w-icon { font-size: 3rem; margin-bottom: 0.5rem; }
.welcome-hero .w-title { font-size: 1.3rem; font-weight: 700; color: #e5e7eb; margin-bottom: 0.3rem; }
.welcome-hero .w-sub { color: #6b7280; max-width: 480px; margin: 0 auto; font-size: 0.85rem; }

/* Scrollbar */
::-webkit-scrollbar { width: 6px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.08); border-radius: 3px; }

/* Download buttons */
.stDownloadButton > button {
    border-radius: 10px !important;
    font-weight: 600 !important;
    background: var(--surface2) !important;
    border: 1px solid var(--border) !important;
    color: var(--text) !important;
}
.stDownloadButton > button:hover {
    background: rgba(255,255,255,0.08) !important;
    border-color: rgba(255,255,255,0.15) !important;
}

/* Expander */
.stExpander {
    border: 1px solid var(--border) !important;
    border-radius: 10px !important;
    background: var(--surface) !important;
}

/* Code blocks */
.stCode {
    background: #0b0f16 !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
}
</style>
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
CATEGORY_BADGE = {
    "Residential / ISP": "badge-residential",
    "Datacenter / Hosting": "badge-datacenter",
    "Public Proxy / VPN": "badge-proxy",
    "Business / Education": "badge-business",
}

def _badge(cat: str) -> str:
    cls = CATEGORY_BADGE.get(cat, "badge-unknown")
    return f'<span class="badge {cls}">{cat}</span>'


def _init_run_state() -> dict:
    return {
        "stage": "idle",
        "message": "",
        "log_lines": [],
        "tested": 0,
        "total": 0,
        "working_count": 0,
        "start_ts": 0.0,
        "elapsed": 0.0,
        "finished": False,
        "result": None,
        "error": None,
        "error_tb": None,
    }


def _log(rs: dict, msg: str, kind: str = "info") -> None:
    logger.info(msg)
    rs["log_lines"].append((kind, msg))
    rs["message"] = msg


# ---------------------------------------------------------------------------
# Background pipeline
# ---------------------------------------------------------------------------

def _run_pipeline_thread(source_url, text_input, concurrency, timeout, rs) -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(
            _async_pipeline(source_url, text_input, concurrency, timeout, rs)
        )
    except Exception as exc:
        logger.error(f"Pipeline crashed: {exc}", exc_info=True)
        rs["error"] = str(exc)
        rs["error_tb"] = traceback.format_exc()
        rs["stage"] = "error"
        rs["finished"] = True
    finally:
        loop.close()


async def _async_pipeline(source_url, text_input, concurrency, timeout, rs) -> None:
    fetcher = SubscriptionFetcher()
    parser = ProxyParser()

    # Stage 1: Fetch
    rs["stage"] = "fetching"
    rs["start_ts"] = time.time()
    _log(rs, "[FETCH] Downloading subscription...", "stage")

    if text_input.strip():
        result = fetcher.parse_text(text_input, source="manual")
    else:
        result = await fetcher.fetch_url(source_url)

    _log(rs, f"  Got {result.proxy_count} configs (Base64: {result.is_base64})", "info")

    if not result.raw_lines:
        raise ValueError("No proxy configurations found in the source.")

    # Stage 2: Parse
    rs["stage"] = "parsing"
    _log(rs, "[PARSE] Parsing protocols...", "stage")
    parsed = [p for line in result.raw_lines if (p := parser.parse(line))]
    _log(rs, f"  Parsed {len(parsed)} valid proxies", "info")

    if not parsed:
        raise ValueError("Failed to parse any proxy configurations.")

    # Stage 3: REAL DELAY test (v2rayNG-style, via sing-box)
    rs["stage"] = "testing"
    rs["total"] = len(parsed)
    rs["tested"] = 0

    installer = SingBoxInstaller()
    sb_path = await installer.ensure_installed()
    if sb_path:
        _log(rs, f"  sing-box ready ({Path(sb_path).name})", "info")
    else:
        raise RuntimeError("sing-box could not be installed — real delay test unavailable")

    tester = SingBoxTester(
        concurrency=concurrency,
        connect_timeout=timeout,
        singbox_path=sb_path,
        installer=installer,
    )
    _log(rs, f"[TEST] REAL DELAY: testing {len(parsed)} nodes via generate_204 ({concurrency} workers)...", "stage")

    def _on_progress(done: int, total: int, _tr) -> None:
        rs["tested"] = done
        rs["total"] = total
        rs["elapsed"] = time.time() - rs["start_ts"]

    batch = await tester.test_all(parsed, progress_callback=_on_progress)
    rs["working_count"] = batch.working
    _log(rs, f"  {batch.working} alive, {batch.dead} dead ({batch.elapsed_seconds:.1f}s)",
         "done" if batch.working > 0 else "warn")

    # Stage 4: Enrich
    rs["stage"] = "enriching"
    _log(rs, "[ENRICH] Querying IP geolocation (ip-api.com batch)...", "stage")
    engine = IPIntelligenceEngine()
    enriched = await enrich_test_results(batch.results, engine)
    enriched.sort(key=lambda r: (not r.is_working, r.latency_ms))
    _log(rs, f"  Enrichment complete — {len(enriched)} results", "done")

    # Done
    rs["stage"] = "done"
    rs["result"] = enriched
    rs["finished"] = True
    rs["elapsed"] = time.time() - rs["start_ts"]


# ---------------------------------------------------------------------------
# Live progress — NATIVE Streamlit widgets only
# ---------------------------------------------------------------------------

def _render_live_progress(rs: dict) -> None:
    """Live progress using only native Streamlit widgets."""
    total = max(rs.get("total", 0), 1)
    tested = rs.get("tested", 0)
    elapsed = rs.get("elapsed", time.time() - rs.get("start_ts", time.time()))

    stage_icons = {
        "fetching":  "⬇️",
        "parsing":   "🔍",
        "testing":   "🧪",
        "enriching": "🌍",
    }
    stage_labels = {
        "fetching":  "Fetching subscription",
        "parsing":   "Parsing configs",
        "testing":   "Testing proxies",
        "enriching": "Enriching with IP data",
        "done":      "Pipeline complete",
        "error":     "Pipeline failed",
    }
    stage = rs["stage"]
    icon = stage_icons.get(stage, "⏳")
    label = stage_labels.get(stage, "Running")

    # Status header
    status = st.status(f"{icon}  {label}", state="running", expanded=True)
    with status:
        # Progress bar (only meaningful during testing)
        if stage == "testing" and total > 0:
            pct = min(tested / total, 1.0)
            st.progress(pct, text=f"{tested:,} / {total:,} tested ({pct*100:.0f}%)")

            # Stats row
            c1, c2, c3 = st.columns(3)
            c1.metric("Tested", f"{tested:,}")
            c2.metric("Elapsed", f"{elapsed:.0f}s")
            if pct > 0.02:
                eta = (elapsed / pct) - elapsed
                if eta < 60:
                    c3.metric("Remaining", f"~{eta:.0f}s")
                elif eta < 3600:
                    c3.metric("Remaining", f"~{eta/60:.0f}m")
                else:
                    c3.metric("Remaining", f"~{eta/3600:.1f}h")

        # Log console — use st.code (native, monospace, no HTML issues)
        logs = rs.get("log_lines", [])
        if logs:
            log_text = "\n".join(f"{k:>5} | {msg}" for k, msg in logs[-15:])
            st.code(log_text, language="text")

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

def _render_sidebar():
    st.sidebar.markdown(
        '<div style="display:flex;align-items:center;gap:8px;margin-bottom:0.25rem;">'
        '<span style="font-size:1.6rem;">⚡</span>'
        '<span style="font-size:1.1rem;font-weight:800;color:#10b981;">ProxyHub</span>'
        '</div>',
        unsafe_allow_html=True,
    )
    st.sidebar.caption("Proxy Intelligence Engine")

    with st.sidebar.expander("📡  Subscription Source", expanded=True):
        source_url = st.text_input(
            "URL", value=DEFAULT_URL, key="source_url",
            placeholder="https://...", label_visibility="collapsed",
        )
        text_input = st.text_area(
            "Or paste raw configs", height=90, key="text_input",
            placeholder="vless://...\nvmess://...", label_visibility="collapsed",
        )

    with st.sidebar.expander("⚙️  Test Settings", expanded=True):
        concurrency = st.slider("Workers", 10, 200, 50, 10,
                                help="More = faster, heavier CPU")
        timeout = st.slider("Timeout (s)", 2.0, 10.0, 5.0, 0.5,
                            help="Per connection attempt")

    st.sidebar.markdown("---")
    path = find_singbox_sync()
    dot_cls = "green" if path else "yellow"
    dot_text = "sing-box ready" if path else "sing-box: auto-install on demand"
    st.sidebar.markdown(
        f'<div style="font-size:0.75rem;color:#9ca3af;">'
        f'<span class="status-dot {dot_cls}"></span>{dot_text}</div>',
        unsafe_allow_html=True,
    )
    st.sidebar.caption(f"Log: {LOG_FILE}")

    return source_url, text_input, concurrency, timeout

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

def _render_header(rs: dict) -> None:
    running = rs.get("stage", "idle") not in ("idle", "done", "error")

    c1, c2 = st.columns([3, 1])
    with c1:
        st.markdown('<div class="hero-badge">⚡ PROXY INTELLIGENCE HUB</div>',
                    unsafe_allow_html=True)
        st.title("ProxyHub")
        st.caption("Fetch, test, enrich & categorize thousands of proxy configs.")

    with c2:
        st.markdown("<br>", unsafe_allow_html=True)
        col_a, col_b = st.columns(2)
        with col_a:
            if st.button(
                "▶  Run" if not running else "⏳  Running…",
                type="primary",
                use_container_width=True,
                disabled=running,
                key="btn_run",
            ):
                st.session_state._trigger_run = True
        with col_b:
            if st.button("↻  Clear", use_container_width=True, key="btn_clear"):
                st.session_state._run_state = _init_run_state()
                st.session_state._thread = None
                st.session_state._last_error = None
                st.rerun()
                st.stop()

# ---------------------------------------------------------------------------
# Metric cards
# ---------------------------------------------------------------------------

def _render_metrics(enriched: list[EnrichedResult]) -> None:
    if not enriched:
        return
    working = [r for r in enriched if r.is_working]
    dead = len(enriched) - len(working)
    avg_ms = sum(r.latency_ms for r in working) / len(working) if working else 0
    cats: dict[str, int] = {}
    for r in working:
        cats[r.category] = cats.get(r.category, 0) + 1
    protocols = len({r.protocol for r in enriched})
    countries = len({r.country for r in working if r.country})
    success_pct = len(working) / max(len(enriched), 1) * 100

    cols = st.columns(6)
    metrics = [
        ("TOTAL", str(len(enriched)), f"{protocols} protocols"),
        ("WORKING", str(len(working)), f"{success_pct:.0f}% success"),
        ("DEAD", str(dead), ""),
        ("AVG LATENCY", f"{avg_ms:.0f} ms",
         f"best {min((r.latency_ms for r in working), default=0):.0f}ms"),
        ("RESIDENTIAL", str(cats.get("Residential / ISP", 0)),
         f"{countries} countries"),
        ("DATACENTER", str(cats.get("Datacenter / Hosting", 0)), ""),
    ]
    for col, (label, value, sub) in zip(cols, metrics):
        with col:
            st.markdown(
                f'<div class="metric-card">'
                f'<div class="m-label">{label}</div>'
                f'<div class="m-value">{value}</div>'
                f'<div class="m-sub">{sub}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

# ---------------------------------------------------------------------------
# Results table
# ---------------------------------------------------------------------------

def _render_table(enriched: list[EnrichedResult]) -> None:
    if not enriched:
        return

    st.markdown("---")

    # ── Stats cards (TOTAL / ALIVE / DEAD) ──
    working = [r for r in enriched if r.is_working]
    dead = len(enriched) - len(working)
    sc1, sc2, sc3 = st.columns(3)
    with sc1:
        st.markdown(
            '<div class="metric-card"><div class="m-label">TOTAL</div>'
            f'<div class="m-value">{len(enriched)}</div></div>',
            unsafe_allow_html=True)
    with sc2:
        st.markdown(
            '<div class="metric-card"><div class="m-label">ALIVE</div>'
            f'<div class="m-value" style="color:#10b981">{len(working)}</div></div>',
            unsafe_allow_html=True)
    with sc3:
        st.markdown(
            '<div class="metric-card"><div class="m-label">DEAD</div>'
            f'<div class="m-value" style="color:#ef4444">{dead}</div></div>',
            unsafe_allow_html=True)

    # ── Filter row ──
    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
    f1, f2 = st.columns([2, 3])
    with f1:
        search = st.text_input("Search", placeholder="جستجو (اسم، سرور، کشور)...",
                               key="fs", label_visibility="collapsed")
    with f2:
        all_cats = sorted({r.category for r in enriched})
        sel_cats = st.pills("Categories", all_cats, selection_mode="multi",
                            key="fpills", label_visibility="collapsed")

    working_only = st.checkbox("فقط کانفیگ‌های سالم", value=False, key="fwork")

    # ── Apply filters ──
    rows = []
    for r in enriched:
        if working_only and not r.is_working:
            continue
        if sel_cats and r.category not in sel_cats:
            continue
        if search:
            q = search.lower()
            if not any(q in str(f).lower() for f in
                       [r.host, r.ip, r.isp, r.country, r.city, r.proxy_raw] if f):
                continue
        rows.append(r)

    if not rows:
        st.info("نتیجه‌ای با فیلترهای فعلی پیدا نشد.")
        return

    # ── Table header ──
    h1, h2, h3, h4, h5, h6 = st.columns(
        [2.6, 1.2, 0.9, 1.5, 0.9, 0.8], vertical_alignment="center")
    h1.markdown("**NAME**")
    h2.markdown("**COUNTRY**")
    h3.markdown("**PING**")
    h4.markdown("**TYPE**")
    h5.markdown("**STATUS**")
    h6.markdown("**COPY**")
    st.markdown('<div style="border-top:1px solid rgba(255,255,255,0.08);margin:4px 0 8px;"></div>',
                unsafe_allow_html=True)

    # ── Pagination ──
    PAGE_SIZE = 20
    total_pages = max(1, (len(rows) + PAGE_SIZE - 1) // PAGE_SIZE)
    if "table_page" not in st.session_state:
        st.session_state.table_page = 0
    st.session_state.table_page = min(st.session_state.table_page, total_pages - 1)
    page = st.session_state.table_page
    page_rows = rows[page * PAGE_SIZE : (page + 1) * PAGE_SIZE]

    # ── Rows ──
    for i, r in enumerate(page_rows):
        c1, c2, c3, c4, c5, c6 = st.columns(
            [2.6, 1.2, 0.9, 1.5, 0.9, 0.8], vertical_alignment="center")
        name = _config_name(r)
        c1.markdown(f"<span style='font-size:0.8rem;color:#d1d5db;'>{name}</span>",
                    unsafe_allow_html=True)
        country = r.country or "—"
        flag = _country_flag(r.country_code) if r.country_code else ""
        c2.markdown(f"<span style='font-size:0.78rem;color:#9ca3af;'>{flag} {country}</span>",
                    unsafe_allow_html=True)
        ping = f"{r.latency_ms:.0f} ms" if r.is_working and r.latency_ms > 0 else "—"
        c3.markdown(f"<span style='font-size:0.78rem;color:#9ca3af;'>{ping}</span>",
                    unsafe_allow_html=True)
        c4.markdown(_badge(r.category), unsafe_allow_html=True)
        status_icon = "🟢" if r.is_working else "🔴"
        c5.markdown(f"<span style='font-size:0.85rem;'>{status_icon}</span>",
                    unsafe_allow_html=True)
        if c6.button("Copy", key=f"cpy_{page}_{i}", use_container_width=True):
            _do_copy(r.proxy_raw)
            st.session_state.last_copied = r.proxy_raw
            st.toast("Copied ✓", icon="📋")
        st.markdown('<div style="border-bottom:1px solid rgba(255,255,255,0.04);margin:2px 0;"></div>',
                    unsafe_allow_html=True)

    # ── Pagination controls ──
    if total_pages > 1:
        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        pc1, pc2, pc3 = st.columns([1, 2, 1])
        with pc1:
            if st.button("◀  Prev", disabled=page == 0, use_container_width=True):
                st.session_state.table_page -= 1
                st.rerun()
                st.stop()
        with pc2:
            st.markdown(
                f"<div style='text-align:center;color:#6b7280;font-size:0.78rem;padding-top:0.5rem;'>"
                f"Page {page + 1} / {total_pages} · {len(rows):,} results</div>",
                unsafe_allow_html=True)
        with pc3:
            if st.button("Next  ▶", disabled=page >= total_pages - 1, use_container_width=True):
                st.session_state.table_page += 1
                st.rerun()
                st.stop()

    # ── Last copied fallback (works in every environment) ──
    last = st.session_state.get("last_copied")
    if last:
        with st.expander("📋  آخرین کانفیگ کپی‌شده", expanded=False):
            st.code(last, language=None)

    # ── Bulk export ──
    st.markdown("---")
    st.subheader("💾  Export")
    working_raws = [r.proxy_raw for r in rows if r.is_working]
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    ex1, ex2, ex3 = st.columns(3)
    with ex1:
        st.download_button("📄  TXT Subscription", "\n".join(working_raws),
                           f"proxyhub_{ts}.txt", "text/plain",
                           use_container_width=True, disabled=not working_raws)
    with ex2:
        json_out = json.dumps([
            {"protocol": r.protocol, "host": r.host, "port": r.port,
             "latency_ms": r.latency_ms, "country": r.country, "city": r.city,
             "isp": r.isp, "category": r.category, "config": r.proxy_raw}
            for r in rows if r.is_working
        ], indent=2, ensure_ascii=False)
        st.download_button("📊  JSON Report", json_out,
                           f"proxyhub_{ts}.json", "application/json",
                           use_container_width=True, disabled=not working_raws)
    with ex3:
        by_cat: dict[str, list[str]] = {}
        for r in rows:
            if r.is_working:
                by_cat.setdefault(r.category, []).append(r.proxy_raw)
        cat_txt = "\n\n".join(f"# {c}\n" + "\n".join(cfgs) for c, cfgs in by_cat.items())
        st.download_button("📂  By Category", cat_txt,
                           f"proxyhub_cats_{ts}.txt", "text/plain",
                           use_container_width=True, disabled=not by_cat)


def _config_name(r: EnrichedResult) -> str:
    """Extract a display name from the raw config fragment, fallback to host."""
    raw = r.proxy_raw
    if "#" in raw:
        from urllib.parse import unquote
        name = unquote(raw.split("#", 1)[1]).strip()
        if name:
            return name[:40]
    return f"{r.protocol} · {r.host}:{r.port}"[:40]


_FLAG_MAP = {
    "US": "🇺🇸", "DE": "🇩🇪", "FR": "🇫🇷", "GB": "🇬🇧", "NL": "🇳🇱",
    "RU": "🇷🇺", "TR": "🇹🇷", "SE": "🇸🇪", "FI": "🇫🇮", "CA": "🇨🇦",
    "JP": "🇯🇵", "SG": "🇸🇬", "HK": "🇭🇰", "KR": "🇰🇷", "IN": "🇮🇳",
    "IR": "🇮🇷", "AE": "🇦🇪", "BR": "🇧🇷", "AU": "🇦🇺", "CH": "🇨🇭",
    "AT": "🇦🇹", "PL": "🇵🇱", "IT": "🇮🇹", "ES": "🇪🇸", "UA": "🇺🇦",
    "CN": "🇨🇳", "TW": "🇹🇼", "VN": "🇻🇳", "TH": "🇹🇭", "ID": "🇮🇩",
    "MY": "🇲🇾", "ZA": "🇿🇦", "AR": "🇦🇷", "MX": "🇲🇽", "IL": "🇮🇱",
}


def _country_flag(code: str) -> str:
    """Return flag emoji for a country code, computing regional indicators if unknown."""
    if not code or len(code) != 2:
        return ""
    code = code.upper()
    if code in _FLAG_MAP:
        return _FLAG_MAP[code]
    try:
        return "".join(chr(ord(c) + 127397) for c in code)
    except Exception:
        return ""


def _do_copy(text: str) -> None:
    try:
        import pyperclip
        pyperclip.copy(text)
    except Exception:
        pass

# ---------------------------------------------------------------------------
# Welcome
# ---------------------------------------------------------------------------

def _render_welcome() -> None:
    st.markdown(
        '<div class="welcome-hero">'
        '<div class="w-icon">🌐</div>'
        '<div class="w-title">Ready to analyze your proxies</div>'
        '<div class="w-sub">Click <strong style="color:#10b981;">▶ Run</strong> to fetch, test '
        'and enrich up to 2,000 proxy configurations with fast concurrent validation.</div>'
        '</div>',
        unsafe_allow_html=True,
    )

    c1, c2, c3, c4, c5 = st.columns(5)
    steps = [
        ("⬇️", "Fetch", "Download + Base64 decode"),
        ("🔍", "Parse", "VLESS · VMess · Trojan · SS · Hysteria2 · TUIC"),
        ("🧪", "Test", "Fast concurrent TCP probes"),
        ("🌍", "Enrich", "IP geolocation · ISP · category"),
        ("📊", "Export", "TXT · JSON · clipboard"),
    ]
    for col, (icon, title, desc) in zip([c1, c2, c3, c4, c5], steps):
        with col:
            st.markdown(
                f'<div class="step-card">'
                f'<div class="s-icon">{icon}</div>'
                f'<div class="s-title">{title}</div>'
                f'<div class="s-desc">{desc}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

# ---------------------------------------------------------------------------
# Error
# ---------------------------------------------------------------------------

def _render_error(rs: dict) -> None:
    err = rs.get("error") or st.session_state.get("_last_error")
    tb = rs.get("error_tb") or st.session_state.get("_last_error_tb")
    if not err:
        return
    st.error(f"❌  {err}")
    if tb:
        with st.expander("🔍  Full traceback"):
            st.code(tb, language="python")
    st.info(f"📄  Full logs: `{LOG_FILE}`")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    st.markdown(GLOBAL_CSS, unsafe_allow_html=True)

    if "_run_state" not in st.session_state:
        st.session_state._run_state = _init_run_state()
    if "_thread" not in st.session_state:
        st.session_state._thread = None
    if "_trigger_run" not in st.session_state:
        st.session_state._trigger_run = False

    rs = st.session_state._run_state
    source_url, text_input, concurrency, timeout = _render_sidebar()
    _render_header(rs)

    # Handle Run button
    if st.session_state._trigger_run:
        st.session_state._trigger_run = False
        st.session_state._run_state = _init_run_state()
        rs = st.session_state._run_state
        t = threading.Thread(
            target=_run_pipeline_thread,
            args=(source_url, text_input, concurrency, timeout, rs),
            daemon=True,
        )
        t.start()
        st.session_state._thread = t
        st.rerun()
        st.stop()

    # Polling: pipeline is running → show live progress and rerun
    if rs["stage"] not in ("idle", "done", "error"):
        _render_live_progress(rs)
        time.sleep(0.5)
        st.rerun()
        st.stop()  # never fall through

    # Done / error / idle states
    if rs["stage"] == "error":
        st.session_state._last_error = rs.get("error")
        st.session_state._last_error_tb = rs.get("error_tb")
        _render_error(rs)
    elif rs["stage"] == "done" and rs.get("result"):
        result: list[EnrichedResult] = rs["result"]
        _render_table(result)
        st.caption(f"⏱  Pipeline completed in {rs.get('elapsed', 0):.1f}s · {len(result)} results")
    elif rs["stage"] == "idle":
        _render_welcome()


if __name__ == "__main__":
    main()