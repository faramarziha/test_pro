# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec for ProxyHub — builds a standalone Windows .exe

Build command:
    pip install pyinstaller
    pyinstaller proxyhub.spec --clean --noconfirm

Output: dist/ProxyHub.exe
"""

import os
import sys
from pathlib import Path

from PyInstaller.utils.hooks import collect_data_files, collect_submodules

block_cipher = None

# Collect streamlit's static assets
streamlit_datas = collect_data_files("streamlit")

# Collect aiohttp dependencies
aiohttp_datas = collect_data_files("aiohttp")

a = Analysis(
    ["app.py"],
    pathex=[],
    binaries=[],
    datas=streamlit_datas + aiohttp_datas + [
        ("proxyhub/*.py", "proxyhub"),
    ],
    hiddenimports=[
        "streamlit",
        "streamlit.runtime",
        "streamlit.runtime.scriptrunner",
        "streamlit.web",
        "streamlit.web.bootstrap",
        "streamlit.watcher",
        "streamlit.commands",
        "pandas",
        "pyperclip",
        "aiohttp",
        "aiohttp_socks",
        "urllib3",
        "charset_normalizer",
        "certifi",
        "idna",
        "yaml",
        "altair",
        "pyarrow",
        "numpy",
        "PIL",
        "watchdog",
        "git",
        "pydeck",
        "blinker",
        "jinja2",
        "packaging",
        "pydantic",
        "pydantic_core",
        "toml",
        "tornado",
        "urllib3",
        "websockets",
        "protobuf",
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name="ProxyHub",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,  # Keep console for debug output
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,
)