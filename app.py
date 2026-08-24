#!/usr/bin/env python3
"""
ProxyHub — Parse, test, enrich, categorize, and display proxy configurations.

Usage:
    streamlit run app.py
    python app.py          # also works (calls streamlit bootstrap)
"""

from proxyhub.dashboard import main

if __name__ == "__main__":
    main()