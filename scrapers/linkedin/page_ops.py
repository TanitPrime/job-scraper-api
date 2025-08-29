"""Page-level helpers (scroll, next, card collection)."""
from typing import List
import time
import random
from playwright.sync_api import Page, TimeoutError as PWTimeout
from scrapers.common.selectors.selectors import LinkedInSelectors
from scrapers.common.rate_limiter import with_retry_and_backoff, rate_limit


@with_retry_and_backoff(retries=3, base_delay=2.0, max_delay=20.0, exceptions=(PWTimeout,))
def scroll_to_load_all_jobs(page: Page, max_attempts: int = 15) -> None:
    """
    Scroll the sidebar until no new cards appear.
    Includes retry logic for robustness.
    """
    prev = 0
    sidebar_element = page.locator(LinkedInSelectors.sidebar).element_handle()
    for attempt in range(max_attempts):
        # Use rate limiting between scrolls
        time.sleep(random.uniform(1.0, 2.0))
        page.evaluate(
            "el => el.scrollTo({ top: el.scrollHeight, behavior: 'smooth' })",
            sidebar_element
        )
        time.sleep(0.5)  # Wait for the scroll to complete
        page.evaluate(
            "el => el.scrollTo({ bottom: el.scrollHeight, behavior: 'smooth' })",
            sidebar_element
        )
        time.sleep(random.uniform(1.0, 2.0))
        curr = page.locator(LinkedInSelectors.job_card_container).count()
        if curr == prev:
            break
        prev = curr


def collect_cards(page: Page, needed: int) -> List:
    """Return up to `needed` fresh job cards."""
    return page.locator(LinkedInSelectors.job_card_container).all()[:needed]


@with_retry_and_backoff(retries=3, base_delay=2.0, max_delay=20.0, exceptions=(PWTimeout,))
@rate_limit(min_delay=2.0, max_delay=4.0)
def go_next(page: Page, timeout = 30000):
    """
    Navigate to the next page of job listings.
    Includes retry logic and rate limiting.
    """
    next_btn = page.locator(LinkedInSelectors.next_page)
    if next_btn.count() > 0:
        print("Clicking next page button.")
        next_btn.click()
        page.wait_for_selector(
            LinkedInSelectors.job_card_container,
            state="attached",
            timeout=timeout
        )  # Wait for job cards to load after clicking next     
    else:
        print("No next page button to click.")