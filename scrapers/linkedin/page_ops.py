"""Page-level helpers (scroll, next, card collection)."""
from typing import List
import time
from playwright.sync_api import Page
from scrapers.common.selectors.selectors import LinkedInSelectors


def scroll_to_load_all_jobs(page: Page, max_attempts: int = 15) -> None:
    """Scroll the sidebar until no new cards appear."""
    prev = 0
    sidebar_element = page.locator(LinkedInSelectors.sidebar).element_handle()
    for attempt in range(max_attempts):
        time.sleep(1.5)
        page.evaluate(
            "el => el.scrollTo({ top: el.scrollHeight, behavior: 'smooth' })",
            sidebar_element
        )
        time.sleep(0.5)  # Wait for the scroll to complete
        page.evaluate(
            "el => el.scrollTo({ bottom: el.scrollHeight, behavior: 'smooth' })",
            sidebar_element
        )
        #print(f"✅ Scrolled sidebar (attempt {attempt+1})")
        time.sleep(1.5)
        curr = page.locator(LinkedInSelectors.job_card_container).count()
        if curr == prev:
            break
        prev = curr


def collect_cards(page: Page, needed: int) -> List:
    """Return up to `needed` fresh job cards."""
    return page.locator(LinkedInSelectors.job_card_container).all()[:needed]


def go_next(page: Page, timeout = 30000):
        next_btn = page.locator(LinkedInSelectors.next_page)
        if next_btn.count() > 0:
            print("Clicking next page button.")
            next_btn.click()
            page.wait_for_selector(
                LinkedInSelectors.job_card_container,
                state="attached",
                timeout=timeout
                ) # Wait for job cards to load after clicking next     

        else:
            print("No next page button to click.")