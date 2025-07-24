# scrapers/common/browser.py
"""
Factory that spins up a single, headful Chrome instance
with stored cookies / local-storage so LinkedIn skips 2FA.
"""

from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, Any

from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page


def get_headful_driver(
    cookies_path: str,
    storage_path: str | None = None,
    proxy: str | None = None,
    headless: bool = False,
    viewport: Dict[str, int] | None = None,
) -> Dict[str, Any]:
    """
    Returns dict: {"browser": Browser, "context": BrowserContext, "page": Page}.
    Caller must call browser.close().
    """
    if viewport is None:
        viewport = {"width": 1366, "height": 768}

    pw = sync_playwright().start()

    browser = pw.chromium.launch(
        headless=headless,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--no-sandbox",
            "--disable-gpu",
        ],
    )

    context_kwargs = {
        "viewport": viewport,
        "locale": "en-US",
        "timezone_id": "America/New_York",
    }

    if proxy:
        context_kwargs["proxy"] = {"server": proxy}

    context: BrowserContext = browser.new_context(**context_kwargs)

    # ---------- load cookies ----------
    cookies_file = Path(cookies_path)
    if cookies_file.exists():
        with cookies_file.open() as f:
            cookies = json.load(f)
        context.add_cookies(cookies)

    # ---------- load localStorage (optional) ----------
    if storage_path and Path(storage_path).exists():
        with Path(storage_path).open() as f:
            storage = json.load(f)
        context.add_init_script(
            f"""
            const data = {json.dumps(storage)};
            for (const [k,v] of Object.entries(data))
                localStorage.setItem(k, v);
            """
        )

    page: Page = context.new_page()
    return {"browser": browser, "context": context, "page": page}