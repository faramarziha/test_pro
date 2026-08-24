"""
ProxyDashboard: Interactive Streamlit web dashboard.

Threaded pipeline with live in-UI progress, ETA, animated status bar,
and a fully redesigned dark glassmorphism theme.
"""
from __future__ import annotations

import asyncio
import json
import logging
import queue
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
# Logging to file (not to terminal)
# ---------------------------------------------------------------------------
LOG_DIR = Path.home() / ".proxyhub" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "proxyhub.log"

# Only log to file, NOT stderr (prevents terminal spam)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8")],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_URL = (
    "https://raw.githubusercontent.com/Diversan313/"
    "apex-parser/main/alive_full.txt"
)

# Page config — MUST be first st. call
st.set_page_config(
    page_title="ProxyHub — Proxy Intelligence",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Global CSS — Dark Glassmorphism Theme
# ---------------------------------------------------------------------------

GLOBAL_CSS = """
<style>
/* ── Root theming ── */
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

/* ── Page background ── */
.stApp {
    background: linear-gradient(160deg, #0a0e14 0%, #0f1724 50%, #0a0e14 100%);
}
.stMainBlockContainer {
    padding-top: 1.5rem !important;
}

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #111827 0%, #0f1724 100%) !important;
    border-right: 1px solid rgba(255,255,255,0.05) !important;
}
[data-testid="stSidebar"] .st-emotion-cache-1cypcdb {
    background: transparent !important;
}

/* ── Headers ── */
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

/* ── Buttons ── */
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
    transform: translateY(-1px);
}
.stButton > button[kind="primary"] {
    background: var(--accent-grad) !important;
    border: none !important;
    color: #fff !important;
    box-shadow: 0 4px 20px rgba(16,185,129,0.25);
}
.stButton > button[kind="primary"]:hover {
    box-shadow: 0 6px 28px rgba(16,185,129,0.4);
    transform: translateY(-2px);
}
.stButton > button:disabled {
    opacity: 0.4 !important;
    cursor: not-allowed !important;
}

/* ── Inputs ── */
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

/* ── Sliders ── */
.stSlider > div > div > div { background: var(--accent) !important; }

/* ── Select boxes ── */
.stMultiSelect [data-baseweb="tag"] {
    background: var(--surface2) !important;
    border: 1px solid var(--border) !important;
    border-radius: 6px !important;
}

/* ── Dataframe ── */
.stDataFrame {
    border: 1px solid var(--border) !important;
    border-radius: 12px !important;
    overflow: hidden !important;
}
.stDataFrame [data-testid="stTable"] {
    background: var(--surface) !important;
}
.stDataFrame th {
    background: rgba(255,255,255,0.03) !important;
    color: var(--text-dim) !important;
    font-size: 0.72rem !important;
    text-transform: uppercase !important;
    letter-spacing: 0.05em !important;
    font-weight: 600 !important;
    padding: 0.5rem 0.75rem !important;
    border-bottom: 1px solid var(--border) !important;
}
.stDataFrame td {
    font-size: 0.78rem !important;
    padding: 0.4rem 0.75rem !important;
    color: var(--text) !important;
    border-bottom: 1px solid rgba(255,255,255,0.03) !important;
    background: transparent !important;
}
.stDataFrame tr:hover td {
    background: rgba(255,255,255,0.03) !important;
}

/* ── Metric cards ── */
.metric-card {
    background: linear-gradient(145deg, rgba(255,255,255,0.03) 0%, rgba(255,255,255,0.01) 100%);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 14px;
    padding: 1.2rem 1.5rem;
    backdrop-filter: blur(10px);
    position: relative;
    overflow: hidden;
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
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #6b7280;
    margin-bottom: 0.35rem;
    font-weight: 600;
}
.metric-card .m-value {
    font-size: 1.7rem;
    font-weight: 800;
    color: #e5e7eb;
    letter-spacing: -0.02em;
}
.metric-card .m-sub {
    font-size: 0.7rem;
    color: #4b5563;
    margin-top: 0.15rem;
}

/* ── Category badges ── */
.badge {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 20px;
    font-size: 0.68rem;
    font-weight: 600;
    letter-spacing: 0.02em;
}
.badge-residential { background: rgba(16,185,129,0.15); color: #6ee7b7; }
.badge-datacenter { background: rgba(59,130,246,0.15); color: #93c5fd; }
.badge-proxy      { background: rgba(239,68,68,0.15); color: #fca5a5; }
.badge-business   { background: rgba(139,92,246,0.15); color: #c4b5fd; }
.badge-unknown    { background: rgba(107,114,128,0.15); color: #9ca3af; }

/* ── Live log terminal ── */
.live-terminal {
    background: #0b0f16;
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 12px;
    padding: 1rem 1.25rem;
    font-family: "JetBrains Mono", "Cascadia Code", "Consolas", monospace;
    font-size: 0.75rem;
    color: #9ca3af;
    max-height: 320px;
    overflow-y: auto;
    line-height: 1.7;
}
.live-terminal .log-line { display: block; }
.live-terminal .log-stage { color: #10b981; font-weight: 600; }
.live-terminal .log-info  { color: #93c5fd; }
.live-terminal .log-warn  { color: #f59e0b; }
.live-terminal .log-error { color: #ef4444; }
.live-terminal .log-done  { color: #6ee7b7; }

/* ── Progress bar override ── */
.stProgress > div > div > div {
    background: var(--accent-grad) !important;
    border-radius: 4px !important;
}

/* ── Expander ── */
.stExpander {
    border: 1px solid var(--border) !important;
    border-radius: 10px !important;
    background: var(--surface) !important;
}

/* ── Spinner ── */
.stSpinner > div { border-color: var(--accent) !important; }

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 6px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.08); border-radius: 3px; }

/* ── Status row ── */
.status-dot {
    display: inline-block;
    width: 8px; height: 8px;
    border-radius: 50%;
    margin-right: 6px;
    animation: pulse 2s infinite;
}
.status-dot.green  { background: #10b981; }
.status-dot.yellow { background: #f59e0b; }
.status-dot.red    { background: #ef4444; }
@keyframes pulse {
    0%, 100% { opacity: 1; }
    50%      { opacity: 0.4; }
}

/* ── Hero section ── */
.hero-badge {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: rgba(16,185,129,0.1);
    border: 1px solid rgba(16,185,129,0.2);
    border-radius: 20px;
    padding: 4px 14px;
    font-size: 0.72rem;
    color: #6ee7b7;
    font-weight: 500;
    margin-bottom: 0.5rem;
}

/* ── Export buttons ── */
.stDownloadButton > button {
    border-radius: 10px !important;
    font-weight: 600 !important;
    background: var(--surface2) !important;
    border: 1px solid var(--border) !important;
    color: var(--text) !important;
    transition: all 0.2s !important;
}
.stDownloadButton > button:hover {
    background: rgba(255,255,255,0.08) !important;
    border-color: rgba(255,255,255,0.15) !important;
}
</style>
"""

# ---------------------------------------------------------------------------
# Badge helper
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

# ---------------------------------------------------------------------------
# Shared run state (written by bg thread, read by Streamlit)
# ---------------------------------------------------------------------------
import copy as _copy

def _init_run_state() -> dict:
    """Create a fresh run-state dict used by the background thread."""
    return {
        "stage": "idle",        # idle | fetching | parsing | testing | enriching | done | error
        "message": "",
        "log_lines": [],        # list of (type, text) tuples
        "tested": 0,
        "total": 0,
        "working_count": 0,
        "start_ts": 0.0,
        "elapsed": 0.0,
        "finished": False,
        "result": None,         # list[EnrichedResult] on success
        "error": None,
        "error_tb": None,
    }

def _log(run_state: dict, msg: str, kind: str = "info") -> None:
    """Add a log line to run_state (called from bg thread)."""
    logger.info(msg)
    run_state["log_lines"].append((kind, msg))
    run_state["message"] = msg

# ---------------------------------------------------------------------------
# Background pipeline runner
# ---------------------------------------------------------------------------

def _run_pipeline_thread(
    source_url: str,
    text_input: str,
    concurrency: int,
    timeout: float,
    run_state: dict,
) -> None:
    """Entry point for the background thread. Runs the async pipeline."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(
            _async_pipeline(source_url, text_input, concurrency, timeout, run_state)
        )
    except Exception as exc:
        logger.error(f"Pipeline crashed: {exc}", exc_info=True)
        run_state["error"] = str(exc)
        run_state["error_tb"] = traceback.format_exc()
        run_state["stage"] = "error"
        run_state["finished"] = True
    finally:
        loop.close()


async def _async_pipeline(
    source_url: str,
    text_input: str,
    concurrency: int,
    timeout: float,
    rs: dict,
) -> None:
    """Full async pipeline: fetch → parse → test → enrich."""
    fetcher = SubscriptionFetcher()
    parser = ProxyParser()

    # ── Stage 1: Fetch ──
    rs["stage"] = "fetching"
    rs["start_ts"] = time.time()
    _log(rs, "⬇️  Fetching subscription data...", "stage")

    if text_input.strip():
        result = fetcher.parse_text(text_input, source="manual")
    else:
        result = await fetcher.fetch_url(source_url)

    _log(rs, f"    ✅  Fetched {result.proxy_count} configs (Base64: {result.is_base64})", "info")

    if not result.raw_lines:
        raise ValueError("No proxy configurations found in the source.")

    # ── Stage 2: Parse ──
    rs["stage"] = "parsing"
    _log(rs, "🔍  Parsing protocols (VLESS, VMess, Trojan, SS, Hysteria2, TUIC)...", "stage")
    parsed = [p for line in result.raw_lines if (p := parser.parse(line))]
    _log(rs, f"    ✅  Parsed {len(parsed)} valid proxy configs", "info")

    if not parsed:
        raise ValueError("Failed to parse any proxy configurations.")

    # ── Stage 3: Test ──
    rs["stage"] = "testing"
    rs["total"] = len(parsed)
    rs["tested"] = 0

    installer = SingBoxInstaller()
    sb_path = await installer.ensure_installed()
    if sb_path:
        _log(rs, f"🔧  sing-box ready ({Path(sb_path).name})", "info")
    else:
        _log(rs, "⚠️  sing-box not found — using TCP fallback", "warn")

    tester = SingBoxTester(
        concurrency=concurrency,
        connect_timeout=timeout,
        singbox_path=sb_path,
        installer=installer,
    )

    _log(rs, f"🧪  Testing {len(parsed)} nodes with {concurrency} concurrent workers...", "stage")

    # Progress callback from tester → updates run_state
    def _on_progress(done: int, total: int, _tr) -> None:
        rs["tested"] = done
        rs["total"] = total
        rs["elapsed"] = time.time() - rs["start_ts"]

    batch = await tester.test_all(parsed, progress_callback=_on_progress)
    rs["working_count"] = batch.working

    _log(rs,
        f"    ✅  {batch.working} working, {batch.dead} dead "
        f"({batch.elapsed_seconds:.1f}s)",
        "done" if batch.working > 0 else "warn",
    )

    # ── Stage 4: Enrich ──
    rs["stage"] = "enriching"
    _log(rs, "🌍  Querying IP geolocation (ip-api.com batch)...", "stage")

    engine = IPIntelligenceEngine()
    enriched = await enrich_test_results(batch.results, engine)
    enriched.sort(key=lambda r: (not r.is_working, r.latency_ms))

    _log(rs, f"    ✅  Enrichment complete — {len(enriched)} results ready", "done")

    # ── Done ──
    rs["stage"] = "done"
    rs["result"] = enriched
    rs["finished"] = True
    rs["elapsed"] = time.time() - rs["start_ts"]

# ---------------------------------------------------------------------------
# Live progress component (the main terminal-like widget)
# ---------------------------------------------------------------------------

def _render_live_progress(run_state: dict) -> None:
    """Render the live pipeline progress widget."""
    rs = run_state
    total = max(rs["total"], 1)
    tested = rs["tested"]
    pct = min(tested / total, 1.0) if rs["stage"] == "testing" else 0.0
    elapsed = rs.get("elapsed", time.time() - rs["start_ts"]) if rs["start_ts"] else 0.0

    # ETA
    eta_str = ""
    if pct > 0.01 and rs["stage"] == "testing":
        eta = (elapsed / pct) - elapsed
        if eta < 60:
            eta_str = f"~{eta:.0f}s left"
        elif eta < 3600:
            eta_str = f"~{eta/60:.0f}m left"
        else:
            eta_str = f"~{eta/3600:.1f}h left"

    # Stage label
    stage_labels = {
        "fetching":   "⬇️  Fetching subscription",
        "parsing":    "🔍  Parsing configs",
        "testing":    f"🧪  Testing proxies ({tested}/{total})",
        "enriching":  "🌍  Enriching with IP data",
        "done":       "✅  Pipeline complete ✓",
        "error":      "❌  Pipeline failed",
    }
    stage_label = stage_labels.get(rs["stage"], "⏳  Running...")

    # Render the card
    st.markdown(f"""
    <div style="
        background: linear-gradient(145deg, rgba(16,185,129,0.06) 0%, rgba(6,182,212,0.04) 100%);
        border: 1px solid rgba(16,185,129,0.15);
        border-radius: 16px;
        padding: 1.5rem 1.75rem;
        margin-bottom: 1.5rem;
    ">
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:0.75rem;">
            <div style="
                width:10px;height:10px;border-radius:50%;
                background: {'#10b981' if rs['stage'] != 'error' else '#ef4444'};
                animation: pulse 1.8s infinite;
                box-shadow: 0 0 12px {'rgba(16,185,129,0.5)' if rs['stage'] != 'error' else 'rgba(239,68,68,0.5)'};
            "></div>
            <span style="font-size:1rem;font-weight:700;color:#e5e7eb;">{stage_label}</span>
            <span style="margin-left:auto;font-size:0.75rem;color:#6b7280;">{eta_str}</span>
        </div>
        {_progress_bar_html(pct, rs["stage"]) if rs["stage"] == "testing" else ""}
        <div style="
            background:#0b0f16;
            border:1px solid rgba(255,255,255,0.06);
            border-radius:10px;
            padding:0.75rem 1rem;
            margin-top:0.75rem;
            font-family:'JetBrains Mono','Consolas',monospace;
            font-size:0.7rem;
            color:#9ca3af;
            max-height:200px;
            overflow-y:auto;
            line-height:1.6;
        ">
            {"".join(_format_log_line(ln) for ln in rs["log_lines"][-12:])}
            <span class="log-line" style="color:#4b5560;">▊</span>
        </div>
    </div>
    """, unsafe_allow_html=True)


def _progress_bar_html(pct: float, stage: str) -> str:
    """HTML progress bar with shimmer animation."""
    pct_display = min(pct * 100, 100)
    return f"""
    <div style="
        height:6px;background:rgba(255,255,255,0.05);
        border-radius:3px;overflow:hidden;
        position:relative;
    ">
        <div style="
            height:100%;width:{pct_display:.1f}%;
            background: linear-gradient(90deg, #10b981, #06b6d4, #10b981);
            background-size: 200% 100%;
            animation: shimmer 2s linear infinite;
            border-radius:3px;
            transition: width 0.4s ease;
        "></div>
    </div>
    <style>
        @keyframes shimmer {{
            0%   {{ background-position: 200% 0; }}
            100% {{ background-position: -200% 0; }}
        }}
    </style>
    """


def _format_log_line(ln: tuple[str, str]) -> str:
    """Format a (kind, text) tuple into HTML log line."""
    kind, text = ln
    cls = {
        "stage": "log-stage",
        "info": "log-info",
        "warn": "log-warn",
        "error": "log-error",
        "done": "log-done",
    }.get(kind, "log-info")
    return f'<span class="log-line {cls}">{text}</span>\n'

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

def _render_sidebar() -> tuple[str, str, int, float]:
    st.sidebar.markdown("""
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:0.5rem;">
        <span style="font-size:1.6rem;">⚡</span>
        <span style="font-size:1.1rem;font-weight:800;background:linear-gradient(135deg,#10b981,#06b6d4);-webkit-background-clip:text;-webkit-text-fill-color:transparent;">ProxyHub</span>
    </div>
    """, unsafe_allow_html=True)

    st.sidebar.markdown('<p style="color:#6b7280;font-size:0.72rem;margin-top:-0.5rem;margin-bottom:1rem;">Proxy Intelligence Engine v1.0</p>', unsafe_allow_html=True)

    with st.sidebar.expander("📡  Subscription Source", expanded=True):
        source_url = st.text_input(
            "URL",
            value=DEFAULT_URL,
            key="source_url",
            placeholder="https://...",
            label_visibility="collapsed",
        )
        text_input = st.text_area(
            "Or paste raw configs / Base64",
            height=90,
            key="text_input",
            placeholder="vless://...\nvmess://...",
            label_visibility="collapsed",
        )

    with st.sidebar.expander("⚙️  Test Settings", expanded=True):
        concurrency = st.slider(
            "Workers",
            min_value=10, max_value=200, value=50, step=10,
            help="More = faster, but heavier on CPU",
        )
        timeout = st.slider(
            "Timeout",
            min_value=2.0, max_value=10.0, value=5.0, step=0.5,
            help="Seconds per connection attempt",
        )

    # sing-box status
    st.sidebar.markdown("---")
    path = find_singbox_sync()
    if path:
        st.sidebar.markdown(
            f'<div style="display:flex;align-items:center;gap:6px;font-size:0.72rem;color:#6ee7b7;">'
            f'<span class="status-dot green"></span> sing-box ready</div>',
            unsafe_allow_html=True,
        )
    else:
        st.sidebar.markdown(
            '<div style="display:flex;align-items:center;gap:6px;font-size:0.72rem;color:#f59e0b;">'
            '<span class="status-dot yellow"></span> sing-box: auto-download on run</div>',
            unsafe_allow_html=True,
        )

    st.sidebar.markdown(
        f'<div style="font-size:0.62rem;color:#374151;margin-top:1rem;">📄 Log: <code style="font-size:0.6rem;">{LOG_FILE}</code></div>',
        unsafe_allow_html=True,
    )

    return source_url, text_input, concurrency, timeout

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

def _render_header(run_state: dict) -> None:
    running = run_state.get("stage", "idle") not in ("idle", "done", "error")
    has_results = run_state.get("result") is not None

    c1, c2 = st.columns([3, 1])
    with c1:
        st.markdown(
            '<div class="hero-badge">⚡ PROXY INTELLIGENCE HUB</div>',
            unsafe_allow_html=True,
        )
        st.title("ProxyHub")
        st.caption("Fetch, test, enrich & categorize thousands of proxy configs — all in one place.")

    with c2:
        st.markdown("<br>", unsafe_allow_html=True)
        col_a, col_b = st.columns(2)
        with col_a:
            disabled = running or (has_results and run_state["stage"] == "done")
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
                st.rerun()

# ---------------------------------------------------------------------------
# Metric cards
# ---------------------------------------------------------------------------

def _render_metrics(enriched: list[EnrichedResult]) -> None:
    if not enriched:
        return
    working = [r for r in enriched if r.is_working]
    dead = len(enriched) - len(working)
    avg_ms = sum(r.latency_ms for r in working) / len(working) if working else 0
    cats = {}
    for r in working:
        cats[r.category] = cats.get(r.category, 0) + 1
    protocols = len({r.protocol for r in enriched})
    countries = len({r.country for r in working if r.country})

    cols = st.columns(6)
    metrics = [
        ("TOTAL", str(len(enriched)), f"{protocols} protocols"),
        ("WORKING", str(len(working)), f"{len(working)/max(len(enriched),1)*100:.0f}% success"),
        ("DEAD", str(dead), ""),
        ("AVG LATENCY", f"{avg_ms:.0f} ms", f"best: {min((r.latency_ms for r in working), default=0):.0f}ms"),
        ("RESIDENTIAL", str(cats.get("Residential / ISP", 0)), f"{countries} countries"),
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
    st.subheader("📊  Results")

    # Filters row
    cf1, cf2, cf3, cf4, cf5 = st.columns([1.5, 1.5, 1.5, 2, 1])
    with cf1:
        protocols = sorted({r.protocol for r in enriched})
        sel_proto = st.multiselect("Protocol", protocols, default=protocols, key="fp", label_visibility="collapsed", placeholder="All protocols")
    with cf2:
        categories = sorted({r.category for r in enriched})
        sel_cat = st.multiselect("Category", categories, default=categories, key="fc", label_visibility="collapsed", placeholder="All categories")
    with cf3:
        countries = sorted({r.country for r in enriched if r.country})
        sel_country = st.multiselect("Country", countries, default=[], key="fco", label_visibility="collapsed", placeholder="All countries")
    with cf4:
        search = st.text_input("Search", placeholder="IP, host, ISP...", key="fs", label_visibility="collapsed")
    with cf5:
        show_only = st.selectbox("Show", ["All", "Working only", "Dead only"], key="fshow", label_visibility="collapsed")

    # Build rows
    rows = []
    for i, r in enumerate(enriched):
        if sel_proto and r.protocol not in sel_proto:
            continue
        if sel_cat and r.category not in sel_cat:
            continue
        if sel_country and r.country not in sel_country:
            continue
        if show_only == "Working only" and not r.is_working:
            continue
        if show_only == "Dead only" and r.is_working:
            continue
        if search:
            q = search.lower()
            if not any(q in str(f).lower() for f in [r.host, r.ip, r.isp, r.country, r.city, r.proxy_raw] if f):
                continue

        icon = "🟢" if r.is_working else "🔴"
        rows.append({
            "": icon,
            "Protocol": r.protocol,
            "Host": f"{r.host}:{r.port}",
            "IP": r.ip or "—",
            "Latency": f"{r.latency_ms:.0f} ms" if r.latency_ms > 0 else "—",
            "Country": r.country or "—",
            "City": r.city or "—",
            "ISP / ASN": r.isp or "—",
            "Category": _badge(r.category),
            "_raw": r.proxy_raw,
        })

    if not rows:
        st.info("No results match the selected filters.")
        return

    df = pd.DataFrame(rows)
    display_cols = ["", "Protocol", "Host", "IP", "Latency", "Country", "City", "ISP / ASN", "Category"]

    st.dataframe(
        df[display_cols],
        use_container_width=True,
        height=520,
        hide_index=True,
        column_config={
            "": st.column_config.Column(width="small"),
            "Category": st.column_config.Column(width="medium"),
        },
    )

    # ── Per-row copy ──
    st.markdown("---")
    st.subheader("📋  Copy Configs")
    cols = st.columns(4)
    for idx, (_, row) in enumerate(df.iterrows()):
        cfg = row["_raw"]
        label = f"{row['Protocol']} · {row['Host']} ({row['Latency']})"
        with cols[idx % 4]:
            with st.expander(label, expanded=False):
                st.code(cfg, language=None)
                if st.button("📋 Copy", key=f"cpy_{idx}", use_container_width=True):
                    _do_copy(cfg)
                    st.toast("Copied ✓", icon="📋")

    # ── Bulk export ──
    st.markdown("---")
    st.subheader("💾  Export")
    working_raws = [
        r.proxy_raw for r in enriched
        if r.is_working
        and (not sel_proto or r.protocol in sel_proto)
        and (not sel_cat or r.category in sel_cat)
    ]

    ex1, ex2, ex3 = st.columns(3)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    with ex1:
        st.download_button("📄  TXT Subscription", "\n".join(working_raws), f"proxyhub_{ts}.txt", "text/plain", use_container_width=True, disabled=not working_raws)
    with ex2:
        json_out = json.dumps([
            {"protocol": r.protocol, "host": r.host, "port": r.port,
             "latency_ms": r.latency_ms, "country": r.country, "city": r.city,
             "isp": r.isp, "category": r.category, "config": r.proxy_raw}
            for r in enriched if r.is_working
        ], indent=2, ensure_ascii=False)
        st.download_button("📊  JSON Report", json_out, f"proxyhub_{ts}.json", "application/json", use_container_width=True, disabled=not working_raws)
    with ex3:
        by_cat = {}
        for r in enriched:
            if r.is_working:
                by_cat.setdefault(r.category, []).append(r.proxy_raw)
        cat_txt = "\n\n".join(f"# {c}\n" + "\n".join(cfgs) for c, cfgs in by_cat.items())
        st.download_button("📂  By Category", cat_txt, f"proxyhub_cats_{ts}.txt", "text/plain", use_container_width=True, disabled=not by_cat)


def _do_copy(text: str) -> None:
    try:
        import pyperclip
        pyperclip.copy(text)
    except Exception:
        pass

# ---------------------------------------------------------------------------
# Welcome screen
# ---------------------------------------------------------------------------

def _render_welcome() -> None:
    st.markdown("""
    <div style="text-align:center;padding:3rem 1rem;">
        <div style="font-size:3rem;margin-bottom:0.5rem;">🌐</div>
        <h2 style="margin-bottom:0.25rem;">Ready to analyze your proxies</h2>
        <p style="color:#6b7280;max-width:480px;margin:0 auto 1.5rem;">
            Click <strong style="color:#10b981;">▶ Run</strong> to fetch, test, and enrich
            up to 2,000 proxy configurations with concurrent validation.
        </p>
    </div>
    """, unsafe_allow_html=True)

    c1, c2, c3, c4, c5 = st.columns(5)
    steps = [
        ("⬇️", "Fetch", "Download + Base64 decode"),
        ("🔍", "Parse", "VLESS, VMess, Trojan, SS, Hysteria2, TUIC"),
        ("🧪", "Test", "50 concurrent, sing-box SOCKS5"),
        ("🌍", "Enrich", "IP geolocation, ISP, category"),
        ("📊", "Export", "TXT, JSON, clipboard copy"),
    ]
    for col, (icon, title, desc) in zip([c1, c2, c3, c4, c5], steps):
        with col:
            st.markdown(f"""
            <div style="
                background: var(--surface);
                border:1px solid var(--border);
                border-radius:12px;
                padding:1rem 0.75rem;
                text-align:center;
            ">
                <div style="font-size:1.5rem;">{icon}</div>
                <div style="font-weight:700;font-size:0.78rem;color:#e5e7eb;margin:0.3rem 0 0.15rem;">{title}</div>
                <div style="font-size:0.62rem;color:#6b7280;">{desc}</div>
            </div>
            """, unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Error display
# ---------------------------------------------------------------------------

def _render_error(run_state: dict) -> None:
    err = run_state.get("error") or st.session_state.get("_last_error")
    tb = run_state.get("error_tb") or st.session_state.get("_last_error_tb")
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

    # Init session state
    if "_run_state" not in st.session_state:
        st.session_state._run_state = _init_run_state()
    if "_thread" not in st.session_state:
        st.session_state._thread: Optional[threading.Thread] = None
    if "_trigger_run" not in st.session_state:
        st.session_state._trigger_run = False

    rs = st.session_state._run_state
    source_url, text_input, concurrency, timeout = _render_sidebar()
    _render_header(rs)

    # ── Handle Run button ──
    if st.session_state._trigger_run:
        st.session_state._trigger_run = False
        # Reset state
        st.session_state._run_state = _init_run_state()
        rs = st.session_state._run_state
        # Spawn thread
        t = threading.Thread(
            target=_run_pipeline_thread,
            args=(source_url, text_input, concurrency, timeout, rs),
            daemon=True,
        )
        t.start()
        st.session_state._thread = t
        st.rerun()

    # ── Polling loop: if pipeline is running, rerun every 0.4s ──
    if rs["stage"] not in ("idle", "done", "error"):
        _render_live_progress(rs)
        time.sleep(0.4)
        st.rerun()

    # ── Show results or error ──
    if rs["stage"] == "error":
        st.session_state._last_error = rs["error"]
        st.session_state._last_error_tb = rs["error_tb"]
        _render_error(rs)
    elif rs["stage"] == "done" and rs["result"]:
        result: list[EnrichedResult] = rs["result"]
        _render_metrics(result)
        _render_table(result)
        # Show brief summary of the run
        st.caption(f"Pipeline completed in {rs.get('elapsed', 0):.1f}s · {len(result)} results")
    elif rs["stage"] == "idle":
        _render_welcome()

    # Always show error if persisted from last run
    if rs["stage"] in ("idle", "done") and (st.session_state.get("_last_error")):
        _render_error(rs)


if __name__ == "__main__":
    main()