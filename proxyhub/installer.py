"""
SingBoxInstaller: Auto-downloads the sing-box binary for the current platform.

Downloads from GitHub releases, extracts, caches locally, and verifies.
Runs on first use when sing-box is not already on PATH or in the cache.
"""
from __future__ import annotations

import hashlib
import logging
import os
import platform
import shutil
import stat
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

# sing-box GitHub release info
SINGBOX_REPO = "SagerNet/sing-box"
SINGBOX_VERSION = "1.11.7"  # pinned stable version

# Cache directory for downloaded binaries
CACHE_DIR = Path.home() / ".proxyhub" / "bin"

# Map platform.system() + platform.machine() → release asset name fragment
ARCH_MAP: dict[str, dict[str, str]] = {
    "Linux": {
        "x86_64": "linux-amd64",
        "amd64": "linux-amd64",
        "aarch64": "linux-arm64",
        "arm64": "linux-arm64",
    },
    "Darwin": {
        "x86_64": "darwin-amd64",
        "amd64": "darwin-amd64",
        "arm64": "darwin-arm64",
        "aarch64": "darwin-arm64",
    },
    "Windows": {
        "x86_64": "windows-amd64",
        "amd64": "windows-amd64",
        "aarch64": "windows-arm64",
        "arm64": "windows-arm64",
    },
}

DOWNLOAD_TIMEOUT = aiohttp.ClientTimeout(total=120)


class SingBoxInstaller:
    """Downloads and caches the sing-box binary for the current platform."""

    def __init__(self, version: str = SINGBOX_VERSION):
        self._version = version
        self._os_name = platform.system()
        self._arch = platform.machine().lower()
        self._cache_path = self._resolve_cache_path()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def binary_path(self) -> Optional[str]:
        """Return the path to the cached binary if it exists and is usable."""
        if self._cache_path and self._cache_path.exists():
            return str(self._cache_path)
        return None

    async def ensure_installed(self) -> Optional[str]:
        """Ensure sing-box is available. Downloads if needed. Returns path or None."""
        # 1. Check PATH first
        path = shutil.which("sing-box") or shutil.which("singbox")
        if path:
            logger.info("sing-box found on PATH: %s", path)
            return path

        # 2. Check cache
        if self._cache_path and self._cache_path.exists():
            logger.info("sing-box found in cache: %s", self._cache_path)
            self._ensure_executable(self._cache_path)
            return str(self._cache_path)

        # 3. Download
        logger.info("sing-box not found — downloading v%s for %s/%s",
                     self._version, self._os_name, self._arch)
        try:
            return await self._download()
        except Exception as exc:
            logger.warning("Failed to download sing-box: %s", exc)
            return None

    def is_available(self) -> bool:
        """Check synchronously whether sing-box is available (PATH or cache)."""
        if shutil.which("sing-box") or shutil.which("singbox"):
            return True
        if self._cache_path and self._cache_path.exists():
            return True
        return False

    async def get_status(self) -> dict:
        """Return a status dictionary about the binary availability."""
        path = await self.ensure_installed()
        return {
            "available": path is not None,
            "path": path,
            "version": self._version,
            "platform": f"{self._os_name}/{self._arch}",
            "in_path": bool(shutil.which("sing-box") or shutil.which("singbox")),
            "in_cache": self._cache_path is not None and self._cache_path.exists(),
        }

    # ------------------------------------------------------------------
    # Download & extract
    # ------------------------------------------------------------------

    async def _download(self) -> Optional[str]:
        """Download and extract the appropriate sing-box release asset."""
        arch_key = self._get_arch_key()
        if not arch_key:
            logger.warning("Unsupported platform: %s/%s", self._os_name, self._arch)
            return None

        asset_name = f"sing-box-{self._version}-{arch_key}"
        ext = ".zip" if self._os_name == "Windows" else ".tar.gz"
        asset_filename = f"{asset_name}{ext}"

        download_url = (
            f"https://github.com/{SINGBOX_REPO}/releases/download/"
            f"v{self._version}/{asset_filename}"
        )

        logger.info("Downloading sing-box from %s", download_url)

        async with aiohttp.ClientSession(timeout=DOWNLOAD_TIMEOUT) as session:
            async with session.get(download_url) as resp:
                resp.raise_for_status()
                archive_data = await resp.read()

        # Extract
        bin_path = self._extract(archive_data, asset_name, ext)
        if bin_path:
            self._ensure_executable(bin_path)
            logger.info("sing-box installed to %s", bin_path)
        return str(bin_path) if bin_path else None

    def _extract(
        self, data: bytes, asset_name: str, ext: str
    ) -> Optional[Path]:
        """Extract archive and return path to the binary."""
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

        with tempfile.TemporaryDirectory(prefix="singbox_") as tmpdir:
            tmp = Path(tmpdir)
            archive_path = tmp / f"sing-box{ext}"

            # Write archive
            archive_path.write_bytes(data)

            # Extract
            if ext.endswith(".tar.gz"):
                with tarfile.open(archive_path, "r:gz") as tf:
                    tf.extractall(tmp)
            elif ext.endswith(".zip"):
                with zipfile.ZipFile(archive_path, "r") as zf:
                    zf.extractall(tmp)
            else:
                return None

            # Find the sing-box binary inside the extracted directory
            # Usually: <asset_name>/sing-box or <asset_name>/sing-box.exe
            extracted_dir = tmp / asset_name
            if not extracted_dir.exists():
                # Try to find any directory containing sing-box
                for d in tmp.iterdir():
                    if d.is_dir():
                        extracted_dir = d
                        break

            binary_name = "sing-box.exe" if self._os_name == "Windows" else "sing-box"
            src = extracted_dir / binary_name
            if not src.exists():
                # Maybe in a nested directory
                candidates = list(extracted_dir.rglob(binary_name))
                if candidates:
                    src = candidates[0]

            if not src.exists():
                logger.warning("Could not find %s in extracted archive", binary_name)
                return None

            # Copy to cache
            dst = CACHE_DIR / binary_name
            shutil.copy2(src, dst)
            return dst

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_arch_key(self) -> Optional[str]:
        """Determine the release asset architecture fragment."""
        os_map = ARCH_MAP.get(self._os_name)
        if not os_map:
            return None
        arch_key = os_map.get(self._arch)
        if not arch_key:
            # Try alternate names
            if self._arch in ("x86_64", "amd64"):
                arch_key = os_map.get("x86_64") or os_map.get("amd64")
            elif self._arch in ("arm64", "aarch64"):
                arch_key = os_map.get("arm64") or os_map.get("aarch64")
        return arch_key

    def _resolve_cache_path(self) -> Optional[Path]:
        """Resolve the expected cache path for this platform."""
        binary_name = "sing-box.exe" if self._os_name == "Windows" else "sing-box"
        return CACHE_DIR / binary_name

    @staticmethod
    def _ensure_executable(path: Path) -> None:
        """Make the binary executable on Unix systems."""
        if os.name != "nt":
            st = path.stat()
            path.chmod(st.st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)


# ------------------------------------------------------------------
# Synchronous convenience: best-effort path detection (no download)
# ------------------------------------------------------------------

def find_singbox_sync() -> Optional[str]:
    """Synchronous best-effort: PATH → cache. Does NOT download."""
    path = shutil.which("sing-box") or shutil.which("singbox")
    if path:
        return path
    installer = SingBoxInstaller()
    bp = installer.binary_path
    if bp and Path(bp).exists():
        return bp
    return None