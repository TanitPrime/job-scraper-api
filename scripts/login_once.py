#!/usr/bin/env python3
# scripts/login_once.py
"""
One-off script to obtain a fresh LinkedIn cookie jar and local-storage dump.
Run it, complete the 2FA on your phone, then press <Enter> inside the terminal
when you land on the feed.  The script writes:
  secrets/cookies.json
  secrets/local_storage.json
"""

import json
import os
import sys
from pathlib import Path
from getpass import getpass

from playwright.sync_api import sync_playwright

SECRETS_DIR = Path(__file__).resolve().parent.parent / "secrets"
COOKIES_FILE = SECRETS_DIR / "cookies.json"
STORAGE_FILE = SECRETS_DIR / "local_storage.json"

SECRETS_DIR.mkdir(exist_ok=True, mode=0o700)


def prompt_env(var: str) -> str:
    value = os.getenv(var)
    if not value:
        value = getpass(f"{var}: ")
    if not value:
        sys.exit(f"{var} is required")
    return value


def main() -> None:
    username = prompt_env("LINKEDIN_USERNAME")
    password = prompt_env("LINKEDIN_PASSWORD")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=200)
        ctx = browser.new_context(
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="America/New_York",
        )
        page = ctx.new_page()

        # stealth basics
        page.add_init_script("delete navigator.webdriver;")

        # --- login flow ---
        page.goto("https://www.linkedin.com/login")
        page.fill("#username", username)
        page.fill("#password", password)
        page.click('button[type="submit"]')

        # Wait for 2FA / feed
        try:
            page.wait_for_url("https://www.linkedin.com/feed", timeout=120_000)
            print("[INFO] Successfully reached feed.")
        except Exception as exc:
            sys.exit(f"[ERROR] Never reached feed: {exc}")

        # --- export cookies ---
        cookies = ctx.cookies()
        with COOKIES_FILE.open("w") as f:
            json.dump(cookies, f, indent=2)
        print(f"[INFO] Saved cookies → {COOKIES_FILE}")

        # --- export localStorage (optional but helps)
        storage = page.evaluate("() => ({ ...localStorage })")
        with STORAGE_FILE.open("w") as f:
            json.dump(storage, f, indent=2)
        print(f"[INFO] Saved localStorage → {STORAGE_FILE}")

        browser.close()


if __name__ == "__main__":
    main()