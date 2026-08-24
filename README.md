# ProxyHub — Proxy Intelligence Dashboard

Parse, test, enrich, categorize, and display proxy configurations at scale.

### Supported Protocols
VLESS (REALITY), VMess, Trojan, Shadowsocks (SS), Hysteria2 (hy2), TUIC

---

## Quick Start

### Option 1: Run directly (any OS)
```bash
pip install -r requirements.txt
streamlit run app.py
```

### Option 2: Build standalone .exe (Windows)
```bash
build_exe.bat
```
Output: `dist/ProxyHub.exe` — double-click to run, no Python needed.

### Option 3: Quick launch (Windows)
Double-click `run.bat`

---

## Usage

1. Open `http://localhost:8501` in your browser
2. Enter a subscription URL or paste raw configs (Base64 supported)
3. Click **▶️ Run Pipeline**
4. Results show in a filterable table with copy + export options

---

## Features

- **Auto-download sing-box** — downloads the sing-box proxy core on first run, no manual install
- **Concurrent testing** — up to 200 parallel workers, configurable timeout
- **IP intelligence** — batch geolocation via ip-api.com, ISP/hosting detection
- **Network categorization** — Residential, Datacenter, Proxy/VPN, Business/Education
- **Search & filter** — by protocol, category, country, keyword
- **Copy configs** — one-click clipboard copy per row
- **Bulk export** — TXT subscription, JSON, or category-grouped

---

## Debugging

If the app doesn't work:
- Check the log file: `~/.proxyhub/logs/proxyhub.log` (Linux/macOS) or `%USERPROFILE%\.proxyhub\logs\proxyhub.log` (Windows)
- Run with the console visible to see live errors
- Send the log file when reporting issues

---

## Default Source
```
https://raw.githubusercontent.com/Diversan313/apex-parser/main/alive_full.txt
```