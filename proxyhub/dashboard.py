"""
ProxyDashboard — Persian-first UI with stepper progress and clean layout.
Threaded pipeline + native Streamlit widgets + REAL DELAY testing.
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
# Logging (file only)
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
    page_title="ProxyHub — تست و تحلیل کانفیگ",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Global CSS
# ---------------------------------------------------------------------------

GLOBAL_CSS = """
<style>
:root {
    --bg: #0b0f17;
    --surface: #121826;
    --surface2: #1a2234;
    --border: rgba(255,255,255,0.07);
    --text: #d6dae2;
    --dim: #7b8494;
    --green: #22c55e;
    --cyan: #22d3ee;
    --red: #f87171;
    --amber: #fbbf24;
    --grad: linear-gradient(135deg, #22c55e, #22d3ee);
}

.stApp { background: radial-gradient(ellipse at top, #101828 0%, #0b0f17 55%); }
.stMainBlockContainer { padding-top: 1.2rem !important; max-width: 1400px; }

[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0f1522 0%, #0b0f17 100%) !important;
    border-right: 1px solid var(--border) !important;
}

/* Persian-friendly font stack */
.stApp, .stMarkdown, p, span, div {
    font-family: "Vazirmatn", "Segoe UI", Tahoma, sans-serif;
}

h1 {
    font-size: 1.9rem !important; font-weight: 800 !important;
    background: var(--grad); -webkit-background-clip: text;
    -webkit-text-fill-color: transparent; background-clip: text;
    margin-bottom: 0 !important;
}

/* Buttons */
.stButton > button {
    border-radius: 12px !important; font-weight: 700 !important;
    border: 1px solid var(--border) !important;
    background: var(--surface2) !important; color: var(--text) !important;
    transition: all .18s ease !important; font-size: 0.9rem !important;
}
.stButton > button:hover {
    border-color: rgba(34,197,94,0.4) !important;
    background: rgba(34,197,94,0.08) !important;
}
.stButton > button[kind="primary"] {
    background: var(--grad) !important; border: none !important; color: #062012 !important;
    box-shadow: 0 6px 24px rgba(34,197,94,0.35);
    font-size: 1rem !important; padding: 0.55rem 1.4rem !important;
}
.stButton > button[kind="primary"]:hover {
    box-shadow: 0 8px 32px rgba(34,197,94,0.5); transform: translateY(-1px);
}
.stButton > button:disabled { opacity: .45 !important; }

/* Inputs */
.stTextInput input, .stTextArea textarea {
    background: var(--surface) !important;
    border: 1px solid var(--border) !important;
    border-radius: 10px !important; color: var(--text) !important;
    font-size: 0.85rem !important;
}
.stTextInput input:focus, .stTextArea textarea:focus {
    border-color: var(--green) !important;
    box-shadow: 0 0 0 3px rgba(34,197,94,0.12) !important;
}

/* Cards */
.ph-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 16px;
    padding: 1.3rem 1.5rem;
}
.ph-stat { text-align: center; padding: 1.1rem 0.6rem; }
.ph-stat .s-num { font-size: 2rem; font-weight: 800; line-height: 1.1; }
.ph-stat .s-lbl { font-size: 0.72rem; color: var(--dim); margin-top: 0.3rem; letter-spacing: 0.03em; }

/* Stepper */
.ph-stepper { display: flex; align-items: center; gap: 0; margin: 0.4rem 0 1rem; }
.ph-step { display: flex; align-items: center; flex: 1; }
.ph-step .dot {
    width: 30px; height: 30px; border-radius: 50%;
    display: flex; align-items: center; justify-content: center;
    font-size: 0.8rem; font-weight: 700; flex-shrink: 0;
    background: var(--surface2); color: var(--dim);
    border: 2px solid var(--border);
}
.ph-step.active .dot {
    background: var(--grad); color: #062012; border-color: transparent;
    box-shadow: 0 0 16px rgba(34,197,94,0.45);
    animation: ph-glow 1.6s ease-in-out infinite;
}
.ph-step.done .dot { background: rgba(34,197,94,0.18); color: var(--green); border-color: rgba(34,197,94,0.4); }
.ph-step .lbl { font-size: 0.74rem; color: var(--dim); margin-left: 8px; white-space: nowrap; }
.ph-step.active .lbl { color: var(--text); font-weight: 700; }
.ph-step.done .lbl { color: var(--green); }
.ph-step .bar { flex: 1; height: 2px; background: var(--border); margin: 0 10px; }
.ph-step.done .bar { background: rgba(34,197,94,0.5); }
@keyframes ph-glow {
    0%,100% { box-shadow: 0 0 10px rgba(34,197,94,0.3); }
    50%     { box-shadow: 0 0 22px rgba(34,197,94,0.6); }
}

/* Badges */
.badge {
    display: inline-block; padding: 3px 11px; border-radius: 20px;
    font-size: 0.68rem; font-weight: 700; white-space: nowrap;
}
.b-res { background: rgba(34,197,94,.16);  color: #86efac; }
.b-dc  { background: rgba(56,189,248,.16); color: #7dd3fc; }
.b-vpn { background: rgba(248,113,113,.16);color: #fca5a5; }
.b-biz { background: rgba(192,132,252,.16);color: #d8b4fe; }
.b-mob { background: rgba(251,191,36,.16); color: #fcd34d; }
.b-unk { background: rgba(148,163,184,.16);color: #cbd5e1; }

/* Table rows */
.ph-row { padding: 7px 4px; border-bottom: 1px solid rgba(255,255,255,0.04); }
.ph-row:hover { background: rgba(255,255,255,0.025); border-radius: 8px; }
.ph-head {
    font-size: 0.66rem; letter-spacing: 0.09em; color: var(--dim);
    font-weight: 700; padding: 4px 4px 8px;
    border-bottom: 1px solid rgba(255,255,255,0.09);
}
.ph-name { font-size: 0.8rem; color: #e2e8f0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.ph-sub  { font-size: 0.66rem; color: var(--dim); }
.ph-cell { font-size: 0.76rem; color: #aeb6c4; }

/* Hero / welcome */
.ph-hero { text-align: center; padding: 2.2rem 1rem 1.6rem; }
.ph-hero .ico { font-size: 3.2rem; margin-bottom: 0.4rem; }
.ph-hero .ttl { font-size: 1.35rem; font-weight: 800; color: #eef2f7; margin-bottom: 0.35rem; }
.ph-hero .sub { color: var(--dim); max-width: 520px; margin: 0 auto; font-size: 0.86rem; line-height: 1.8; }

/* Guide cards */
.ph-guide {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 14px; padding: 1.1rem 0.9rem; text-align: center; height: 100%;
}
.ph-guide .g-ico { font-size: 1.7rem; margin-bottom: 0.45rem; }
.ph-guide .g-ttl { font-weight: 800; font-size: 0.84rem; color: #e2e8f0; margin-bottom: 0.25rem; }
.ph-guide .g-dsc { font-size: 0.68rem; color: var(--dim); line-height: 1.7; }

/* Status pill */
.ph-pill {
    display: inline-flex; align-items: center; gap: 6px;
    background: rgba(34,197,94,0.1); border: 1px solid rgba(34,197,94,0.25);
    color: #86efac; border-radius: 20px; padding: 3px 13px;
    font-size: 0.7rem; font-weight: 600;
}

/* Log console */
.ph-log {
    background: #0a0e15; border: 1px solid var(--border); border-radius: 10px;
    padding: 0.8rem 1rem; font-family: "Consolas", monospace;
    font-size: 0.72rem; color: #8b95a7; line-height: 1.75;
    max-height: 190px; overflow-y: auto; direction: ltr; text-align: left;
}

::-webkit-scrollbar { width: 7px; height: 7px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.1); border-radius: 4px; }

.stDownloadButton > button {
    border-radius: 10px !important; font-weight: 700 !important;
    background: var(--surface2) !important;
    border: 1px solid var(--border) !important; color: var(--text) !important;
}
.stDownloadButton > button:hover { border-color: rgba(34,197,94,0.4) !important; }

.stExpander { border: 1px solid var(--border) !important; border-radius: 10px !important; }
.stCode { background: #0a0e15 !important; border-radius: 8px !important; }

/* Progress bar */
.stProgress > div > div > div { background: var(--grad) !important; }
</style>
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
CATEGORY_BADGE = {
    "Residential / ISP": "b-res",
    "Datacenter / Hosting": "b-dc",
    "Public Proxy / VPN": "b-vpn",
    "Business / Education": "b-biz",
    "Mobile / Cellular": "b-mob",
    "Unknown": "b-unk",
}
CATEGORY_FA = {
    "Residential / ISP": "خانگی / ISP",
    "Datacenter / Hosting": "دیتاسنتر",
    "Public Proxy / VPN": "پروکسی عمومی / VPN",
    "Business / Education": "سازمانی / آموزشی",
    "Mobile / Cellular": "موبایل / سلولی",
    "Unknown": "نامشخص",
}

def _badge(cat: str) -> str:
    cls = CATEGORY_BADGE.get(cat, "b-unk")
    label = CATEGORY_FA.get(cat, cat)
    return f'<span class="badge {cls}">{label}</span>'


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

def _run_pipeline_thread(source_url, text_input, concurrency, timeout, min_speed, rs) -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(
            _async_pipeline(source_url, text_input, concurrency, timeout, min_speed, rs)
        )
    except Exception as exc:
        logger.error(f"Pipeline crashed: {exc}", exc_info=True)
        rs["error"] = str(exc)
        rs["error_tb"] = traceback.format_exc()
        rs["stage"] = "error"
        rs["finished"] = True
    finally:
        loop.close()


async def _async_pipeline(source_url, text_input, concurrency, timeout, min_speed, rs) -> None:
    fetcher = SubscriptionFetcher()
    parser = ProxyParser()

    # 1 — Fetch
    rs["stage"] = "fetching"
    rs["start_ts"] = time.time()
    _log(rs, "[1/4] دریافت سابسکریپشن...", "stage")
    if text_input.strip():
        result = fetcher.parse_text(text_input, source="manual")
    else:
        result = await fetcher.fetch_url(source_url)
    _log(rs, f"  {result.proxy_count} کانفیگ دریافت شد (Base64: {result.is_base64})", "info")
    if not result.raw_lines:
        raise ValueError("هیچ کانفیگی در منبع پیدا نشد.")

    # 2 — Parse
    rs["stage"] = "parsing"
    _log(rs, "[2/4] پردازش کانفیگ‌ها...", "stage")
    parsed = [p for line in result.raw_lines if (p := parser.parse(line))]
    _log(rs, f"  {len(parsed)} کانفیگ معتبر", "info")
    if not parsed:
        raise ValueError("هیچ کانفیگی قابل پردازش نبود.")

    # 3 — REAL DELAY test
    rs["stage"] = "testing"
    rs["total"] = len(parsed)
    rs["tested"] = 0
    installer = SingBoxInstaller()
    sb_path = await installer.ensure_installed()
    if not sb_path:
        raise RuntimeError("نصب sing-box ناموفق بود — تست تأخیر واقعی در دسترس نیست.")
    _log(rs, f"  هسته sing-box آماده است", "info")

    tester = SingBoxTester(
        concurrency=concurrency, connect_timeout=timeout,
        singbox_path=sb_path, installer=installer, min_download_kbps=min_speed,
    )
    _log(rs, f"[3/4] تست اتصال و سرعت {len(parsed)} کانفیگ (حداقل {min_speed:.0f} KB/s)...", "stage")

    def _on_progress(done, total, _tr):
        rs["tested"] = done
        rs["total"] = total
        rs["elapsed"] = time.time() - rs["start_ts"]

    batch = await tester.test_all(parsed, progress_callback=_on_progress)
    rs["working_count"] = batch.working
    _log(rs, f"  {batch.working} سالم، {batch.dead} خراب ({batch.elapsed_seconds:.0f} ثانیه)",
         "done" if batch.working else "warn")

    # 4 — Enrich
    rs["stage"] = "enriching"
    _log(rs, "[4/4] تشخیص کشور و نوع IP (ip-api)...", "stage")
    engine = IPIntelligenceEngine()
    enriched = await enrich_test_results(batch.results, engine)
    enriched.sort(key=lambda r: (not r.is_working, r.latency_ms))
    _log(rs, f"  غنی‌سازی انجام شد — {len(enriched)} نتیجه", "done")

    rs["stage"] = "done"
    rs["result"] = enriched
    rs["finished"] = True
    rs["elapsed"] = time.time() - rs["start_ts"]

# ---------------------------------------------------------------------------
# Stepper
# ---------------------------------------------------------------------------

STEPS = [
    ("fetching",  "۱", "دریافت"),
    ("parsing",   "۲", "پردازش"),
    ("testing",   "۳", "تست تأخیر"),
    ("enriching", "۴", "تشخیص IP"),
]

def _render_stepper(rs: dict) -> None:
    order = ["fetching", "parsing", "testing", "enriching"]
    try:
        idx = order.index(rs["stage"])
    except ValueError:
        idx = -1

    html = ['<div class="ph-stepper">']
    for i, (key, num, label) in enumerate(STEPS):
        cls = "active" if i == idx else ("done" if i < idx else "")
        check = "✓" if i < idx else num
        html.append(
            f'<div class="ph-step {cls}">'
            f'<div class="dot">{check}</div>'
            f'<div class="lbl">{label}</div>'
            f'<div class="bar"></div>'
            f'</div>'
        )
    html.append("</div>")
    st.markdown("".join(html), unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Live progress
# ---------------------------------------------------------------------------

def _render_live_progress(rs: dict) -> None:
    total = max(rs.get("total", 0), 1)
    tested = rs.get("tested", 0)
    elapsed = rs.get("elapsed", time.time() - rs.get("start_ts", time.time()))
    stage = rs["stage"]

    _render_stepper(rs)

    if stage == "testing":
        pct = min(tested / total, 1.0)
        st.progress(pct, text=f"تست شده: {tested:,} از {total:,} ({pct*100:.0f}٪)")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("تست شده", f"{tested:,}")
        c2.metric("باقی‌مانده", f"{total - tested:,}")
        c3.metric("زمان سپری‌شده", f"{elapsed:.0f}s")
        if pct > 0.02:
            eta = (elapsed / pct) - elapsed
            eta_s = f"~{eta:.0f}s" if eta < 60 else (f"~{eta/60:.0f}m" if eta < 3600 else f"~{eta/3600:.1f}h")
            c4.metric("زمان باقی‌مانده", eta_s)
    else:
        msgs = {
            "fetching": "در حال دریافت سابسکریپشن...",
            "parsing": "در حال پردازش کانفیگ‌ها...",
            "enriching": "در حال تشخیص کشور و نوع IP...",
        }
        if stage in msgs:
            st.info(msgs[stage])

    logs = rs.get("log_lines", [])
    if logs:
        log_text = "\n".join(f"{msg}" for _, msg in logs[-14:])
        st.markdown(f'<div class="ph-log">{log_text}</div>', unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

def _render_sidebar():
    st.sidebar.markdown(
        '<div style="display:flex;align-items:center;gap:8px;padding:4px 0 2px;">'
        '<span style="font-size:1.7rem;">⚡</span>'
        '<span style="font-size:1.15rem;font-weight:800;color:#22c55e;">ProxyHub</span>'
        '</div>'
        '<div style="font-size:0.7rem;color:#7b8494;">تست و تحلیل کانفیگ‌های پروکسی</div>',
        unsafe_allow_html=True,
    )
    st.sidebar.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

    with st.sidebar.expander("📡 منبع کانفیگ", expanded=True):
        source_url = st.text_input("آدرس سابسکریپشن", value=DEFAULT_URL,
                                   key="source_url", label_visibility="collapsed")
        st.caption("یا کانفیگ‌ها را دستی وارد کنید:")
        text_input = st.text_area("کانفیگ دستی", height=90, key="text_input",
                                  placeholder="vless://...\nvmess://...",
                                  label_visibility="collapsed")

    with st.sidebar.expander("⚙️ تنظیمات پیشرفته", expanded=False):
        concurrency = st.slider("تست‌های همزمان", 10, 200, 50, 10,
                                help="بیشتر = سریع‌تر ولی سنگین‌تر")
        timeout = st.slider("تایم‌اوت (ثانیه)", 2.0, 20.0, 8.0, 0.5,
                            help="حداکثر انتظار برای اتصال و نمونه‌گیری سرعت")
        min_speed = st.number_input("حداقل سرعت دانلود (KB/s)", min_value=1.0,
                                    max_value=10000.0, value=100.0, step=10.0,
                                    help="کانفیگ باید حداقل این سرعت را در تست واقعی داشته باشد")

    st.sidebar.markdown("---")
    path = find_singbox_sync()
    ok = bool(path)
    bg = "rgba(34,197,94,0.1)" if ok else "rgba(251,191,36,0.1)"
    bd = "rgba(34,197,94,0.25)" if ok else "rgba(251,191,36,0.25)"
    fg = "#86efac" if ok else "#fbbf24"
    msg = "● هسته تست آماده" if ok else "● هسته در اولین اجرا نصب می‌شود"
    st.sidebar.markdown(
        f'<span class="ph-pill" style="background:{bg};'
        f'border-color:{bd};color:{fg};">{msg}</span>',
        unsafe_allow_html=True,
    )

    return source_url, text_input, concurrency, timeout, min_speed

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

def _render_header(rs: dict) -> None:
    running = rs.get("stage", "idle") not in ("idle", "done", "error")

    c1, c2 = st.columns([3, 1])
    with c1:
        st.title("⚡ ProxyHub")
        st.caption("دریافت، تست واقعی، تشخیص IP و دسته‌بندی کانفیگ‌های پروکسی — همه در یک‌جا")
    with c2:
        st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)
        ca, cb = st.columns([1.4, 1])
        with ca:
            if st.button(
                "🚀  شروع تست" if not running else "⏳  در حال اجرا...",
                type="primary", use_container_width=True,
                disabled=running, key="btn_run",
            ):
                st.session_state._trigger_run = True
        with cb:
            if st.button("↻ پاک‌سازی", use_container_width=True, key="btn_clear"):
                st.session_state._run_state = _init_run_state()
                st.session_state._thread = None
                st.session_state._last_error = None
                st.rerun()
                st.stop()

# ---------------------------------------------------------------------------
# Welcome
# ---------------------------------------------------------------------------

def _render_welcome() -> None:
    st.markdown(
        '<div class="ph-hero">'
        '<div class="ico">🌐</div>'
        '<div class="ttl">آماده تست کانفیگ‌های شما</div>'
        '<div class="sub">با دکمه «🚀 شروع تست»، همه کانفیگ‌های سابسکریپشن با روش '
        '<b>تأخیر واقعی</b> (مثل v2rayNG) بررسی می‌شوند و کانفیگ‌های سالم با '
        'کشور، نوع IP و تأخیر واقعی نمایش داده می‌شوند.</div>'
        '</div>',
        unsafe_allow_html=True,
    )

    c1, c2, c3, c4 = st.columns(4)
    guides = [
        ("📡", "۱. دریافت", "سابسکریپشن به‌صورت خودکار دانلود و دیکد می‌شود (Base64 هم پشتیبانی می‌شود)"),
        ("🧪", "۲. تست واقعی", "هر کانفیگ از داخل تونل به generate_204 وصل می‌شود — دقیقاً مثل v2rayNG"),
        ("🌍", "۳. تشخیص IP", "کشور، شهر، ISP و نوع شبکه (خانگی/دیتاسنتر/VPN) شناسایی می‌شود"),
        ("📋", "۴. کپی و خروجی", "کپی تک‌تک کانفیگ‌ها یا خروجی TXT و JSON"),
    ]
    for col, (ico, ttl, dsc) in zip([c1, c2, c3, c4], guides):
        with col:
            st.markdown(
                f'<div class="ph-guide"><div class="g-ico">{ico}</div>'
                f'<div class="g-ttl">{ttl}</div><div class="g-dsc">{dsc}</div></div>',
                unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------

def _render_table(enriched: list[EnrichedResult]) -> None:
    if not enriched:
        return

    working = [r for r in enriched if r.is_working]
    dead = len(enriched) - len(working)

    # Stats
    s1, s2, s3, s4 = st.columns(4)
    cards = [
        (len(enriched), "کل کانفیگ‌ها", "#e2e8f0"),
        (len(working), "سالم ✓", "#22c55e"),
        (dead, "خراب ✗", "#f87171"),
        (f"{len(working)/max(len(enriched),1)*100:.0f}٪", "نرخ موفقیت", "#22d3ee"),
    ]
    for col, (num, lbl, color) in zip([s1, s2, s3, s4], cards):
        with col:
            st.markdown(
                f'<div class="ph-card ph-stat">'
                f'<div class="s-num" style="color:{color}">{num}</div>'
                f'<div class="s-lbl">{lbl}</div></div>',
                unsafe_allow_html=True)

    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

    # Filters
    f1, f2 = st.columns([2, 3])
    with f1:
        search = st.text_input("جستجو", placeholder="🔍 جستجو (نام، سرور، کشور)...",
                               key="fs", label_visibility="collapsed")
    with f2:
        all_cats = sorted({r.category for r in enriched})
        cat_labels = [CATEGORY_FA.get(c, c) for c in all_cats]
        label_to_cat = {CATEGORY_FA.get(c, c): c for c in all_cats}
        sel_labels = st.pills("دسته‌بندی", cat_labels, selection_mode="multi",
                              key="fpills", label_visibility="collapsed")
        sel_cats = [label_to_cat[l] for l in (sel_labels or [])]

    working_only = st.checkbox("فقط کانفیگ‌های سالم", value=False, key="fwork")

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

    # Header
    h = st.columns([2.7, 1.3, 0.9, 1.4, 0.7, 0.8], vertical_alignment="center")
    for col, title in zip(h, ["نام کانفیگ", "کشور", "تأخیر", "نوع شبکه", "وضعیت", "کپی"]):
        col.markdown(f'<div class="ph-head">{title}</div>', unsafe_allow_html=True)

    # Pagination
    PAGE = 20
    total_pages = max(1, (len(rows) + PAGE - 1) // PAGE)
    if "table_page" not in st.session_state:
        st.session_state.table_page = 0
    st.session_state.table_page = min(st.session_state.table_page, total_pages - 1)
    page = st.session_state.table_page
    page_rows = rows[page * PAGE:(page + 1) * PAGE]

    # Rows
    for i, r in enumerate(page_rows):
        c = st.columns([2.7, 1.3, 0.9, 1.4, 0.7, 0.8], vertical_alignment="center")
        name = _config_name(r)
        proto = r.protocol.lower()
        with c[0]:
            st.markdown(
                f'<div class="ph-row"><div class="ph-name">{name}</div>'
                f'<div class="ph-sub">{proto} · {r.host}:{r.port}</div></div>',
                unsafe_allow_html=True)
        flag = _country_flag(r.country_code)
        c[1].markdown(f'<div class="ph-cell">{flag} {r.country or "—"}</div>',
                      unsafe_allow_html=True)
        if r.is_working and r.speed_verified and r.download_kbps > 0:
            ping = f"{r.download_kbps:.0f} KB/s"
        elif r.is_working and not r.speed_verified:
            ping = "؟"
        elif r.is_working and r.latency_ms > 0:
            ping = f"{r.latency_ms:.0f} ms"
        else:
            ping = "—"
        ping_color = "#22c55e" if (r.is_working and r.speed_verified) else ("#fbbf24" if r.is_working else "#7b8494")
        c[2].markdown(f'<div class="ph-cell" style="color:{ping_color};font-weight:700">{ping}</div>',
                      unsafe_allow_html=True)
        c[3].markdown(_badge(r.category), unsafe_allow_html=True)
        if r.ip_evidence:
            c[3].caption(f"اطمینان: {r.ip_confidence} · {', '.join(r.ip_evidence)}")
        status = "🟢" if r.is_working else f"🔴 {r.test_error[:18]}"
        c[4].markdown(f'<div style="font-size:0.72rem">{status}</div>', unsafe_allow_html=True)
        if r.is_working and not r.speed_verified:
            c[4].caption("سرعت اندازه‌گیری نشد")
        if c[5].button("کپی", key=f"cpy_{page}_{i}", use_container_width=True):
            _do_copy(r.proxy_raw)
            st.session_state.last_copied = r.proxy_raw
            st.toast("کپی شد ✓", icon="📋")

    # Pagination
    if total_pages > 1:
        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        p1, p2, p3 = st.columns([1, 2, 1])
        with p1:
            if st.button("▶ قبلی", disabled=page == 0, use_container_width=True):
                st.session_state.table_page -= 1
                st.rerun()
                st.stop()
        with p2:
            st.markdown(
                f'<div style="text-align:center;color:#7b8494;font-size:0.76rem;padding-top:0.55rem;">'
                f'صفحه {page + 1} از {total_pages} — {len(rows):,} نتیجه</div>',
                unsafe_allow_html=True)
        with p3:
            if st.button("بعدی ◀", disabled=page >= total_pages - 1, use_container_width=True):
                st.session_state.table_page += 1
                st.rerun()
                st.stop()

    # Last copied fallback
    last = st.session_state.get("last_copied")
    if last:
        with st.expander("📋 آخرین کانفیگ کپی‌شده"):
            st.code(last, language=None)

    # Export
    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
    st.subheader("💾 خروجی")
    working_raws = [r.proxy_raw for r in rows if r.is_working]
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    e1, e2, e3 = st.columns(3)
    with e1:
        st.download_button("📄 خروجی TXT (سابسکریپشن)", "\n".join(working_raws),
                           f"proxyhub_{ts}.txt", "text/plain",
                           use_container_width=True, disabled=not working_raws)
    with e2:
        json_out = json.dumps([
            {"protocol": r.protocol, "host": r.host, "port": r.port,
             "latency_ms": r.latency_ms, "country": r.country, "city": r.city,
             "isp": r.isp, "category": r.category, "ip_confidence": r.ip_confidence,
             "download_kbps": r.download_kbps, "quality": r.quality,
             "speed_verified": r.speed_verified, "exit_ip": r.exit_ip,
             "test_error": r.test_error, "config": r.proxy_raw}
            for r in rows if r.is_working
        ], indent=2, ensure_ascii=False)
        st.download_button("📊 خروجی JSON", json_out,
                           f"proxyhub_{ts}.json", "application/json",
                           use_container_width=True, disabled=not working_raws)
    with e3:
        by_cat: dict[str, list[str]] = {}
        for r in rows:
            if r.is_working:
                by_cat.setdefault(r.category, []).append(r.proxy_raw)
        cat_txt = "\n\n".join(f"# {CATEGORY_FA.get(c, c)}\n" + "\n".join(cfgs)
                              for c, cfgs in by_cat.items())
        st.download_button("📂 خروجی بر اساس دسته", cat_txt,
                           f"proxyhub_cats_{ts}.txt", "text/plain",
                           use_container_width=True, disabled=not by_cat)


def _config_name(r: EnrichedResult) -> str:
    raw = r.proxy_raw
    if "#" in raw:
        from urllib.parse import unquote
        name = unquote(raw.split("#", 1)[1]).strip()
        if name:
            return name[:42]
    return f"{r.host}:{r.port}"[:42]


_FLAG_OVERRIDES = {"UK": "🇬🇧", "RU": "🇷🇺"}

def _country_flag(code: str) -> str:
    if not code or len(code) != 2:
        return ""
    code = code.upper()
    if code in _FLAG_OVERRIDES:
        return _FLAG_OVERRIDES[code]
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
# Error
# ---------------------------------------------------------------------------

def _render_error(rs: dict) -> None:
    err = rs.get("error") or st.session_state.get("_last_error")
    tb = rs.get("error_tb") or st.session_state.get("_last_error_tb")
    if not err:
        return
    st.error(f"❌ خطا: {err}")
    if tb:
        with st.expander("🔍 جزئیات کامل خطا"):
            st.code(tb, language="python")
    st.info(f"📄 لاگ کامل: `{LOG_FILE}`")

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
    source_url, text_input, concurrency, timeout, min_speed = _render_sidebar()
    _render_header(rs)

    if st.session_state._trigger_run:
        st.session_state._trigger_run = False
        st.session_state._run_state = _init_run_state()
        rs = st.session_state._run_state
        t = threading.Thread(
            target=_run_pipeline_thread,
            args=(source_url, text_input, concurrency, timeout, min_speed, rs),
            daemon=True,
        )
        t.start()
        st.session_state._thread = t
        st.rerun()
        st.stop()

    if rs["stage"] not in ("idle", "done", "error"):
        _render_live_progress(rs)
        time.sleep(0.5)
        st.rerun()
        st.stop()

    if rs["stage"] == "error":
        st.session_state._last_error = rs.get("error")
        st.session_state._last_error_tb = rs.get("error_tb")
        _render_error(rs)
    elif rs["stage"] == "done" and rs.get("result"):
        result: list[EnrichedResult] = rs["result"]
        _render_table(result)
        st.caption(f"⏱ تست در {rs.get('elapsed', 0):.0f} ثانیه کامل شد · {len(result):,} کانفیگ")
    elif rs["stage"] == "idle":
        _render_welcome()


if __name__ == "__main__":
    main()
