#!/usr/bin/env bash
# ProxyHub launcher
# Installs deps and runs the Streamlit dashboard.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Install Python requirements if needed
if [ -f requirements.txt ]; then
    pip install -r requirements.txt --quiet 2>/dev/null || pip3 install -r requirements.txt --quiet
fi

echo "🌐 Starting ProxyHub on http://localhost:8501"
exec streamlit run app.py --server.address=0.0.0.0 --server.port="${PORT:-8501}" "$@"