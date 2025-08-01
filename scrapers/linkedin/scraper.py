# scrapers/linkedin/scraper.py
"""
LinkedIn scraper that:
- builds one boolean query per location
- walks pages in batchs of N jobs
- stops early when freshness or relevance thresholds are crossed
- returns ONLY new & relevant jobs
"""

import time
import random
from typing import List
from urllib.parse import urlencode

from playwright.sync_api import sync_playwright, Browser, Page, TimeoutError as PWTimeout

from scrapers.base import BaseScraper
from scrapers.linkedin.models import LinkedInJob
from scrapers.linkedin.query_builder import build_boolean_query
from scrapers.common.browser import get_headful_driver
from scrapers.common.selectors.selectors import LinkedInSelectors
from scrapers.common.firebase_client import get_firestore_client
from scrapers.common.search_matrix import load_matrix
from scrapers.common.scraper_control import scraper_control


class ReLoginRequired(Exception):
    """Raised when cookies are expired / login wall appears."""


class LinkedInScraper(BaseScraper):
    source = "linkedin"

    def __init__(
        self,
        cookies_path: str,
        storage_path: str,
        proxy: str | None = None,
        headless: bool = False,
        matrix: dict[str, list[str]] = load_matrix(),
    ):
        self.cookies_path = cookies_path
        self.storage_path = storage_path
        self.proxy = proxy
        self.headless = headless
        self._browser: Browser | None = None
        self._page: Page | None = None
        self._db = get_firestore_client() # Firestore client for database operations
        self.matrix = matrix
        scraper_control.set_status(self.source, "OFF") # Set initial status to OFF

    # ------------------------------------------------------------------
    # Browser lifecycle
    # ------------------------------------------------------------------

    def _start_browser(self) -> Page:
        driver = get_headful_driver(
            cookies_path=self.cookies_path,
            storage_path=self.storage_path,
            proxy=self.proxy,
            headless=self.headless,
        )
        self._browser = driver["browser"]
        self._page = driver["page"]
        return self._page

    def _close_browser(self):
        if self._browser:
            self._browser.close()

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------
    def scrape_batch(
        self,
        batch_size: int = 50,
        max_pages: int = 3,
        freshness_thresh: float = 0.8,
        relevance_thresh: float = 0.3,
        delay: float = 6.0,
    ) -> List[LinkedInJob]:
        """
        Scrape a batch of jobs from LinkedIn.
        Returns a list of new LinkedInJob objects.
        Args:
            batch_size: Number of jobs to scrape per batch.
            max_pages: Maximum number of pages to scrape.
            freshness_thresh: Minimum ratio of fresh jobs to continue scraping.
            relevance_thresh: Minimum relevance score to consider a job valid.
            delay: Delay between page loads to avoid rate limiting.
        
        Raises:
            ReLoginRequired: If cookies are expired or login wall appears.
        """
        from scrapers.linkedin.page_ops import scroll_to_load_all_jobs, collect_cards, go_next
        from scrapers.common.batch_processor import flush_batch

        # Start browser and set status
        scraper_control.set_status(self.source, "ON")
        page = self._start_browser()
        try:
            # Build the boolean query and navigate to the jobs page
            query = build_boolean_query()
            url = "https://www.linkedin.com/jobs/search?" + urlencode(
                {"geoId": "92000000", "keywords": query, "f_JT": "F"}
            )
            page.goto(url, timeout=50000)
            # Wait for the page to load
            page.wait_for_selector(LinkedInSelectors.job_card_container, timeout=15000)

            # Scraping starts here
            new_jobs, batch_buffer = [], []
            for _ in range(max_pages):
                # Scroll to load all jobs in the sidebar
                scroll_to_load_all_jobs(page)
                # Collect job cards
                cards = collect_cards(page, batch_size - len(batch_buffer))
                if not cards:
                    break

                # Extract job data from cards
                batch_buffer.extend(
                    self._extract_single_job(card) for card in cards
                )

                print("Calculating batch freshness and relevance")
                if len(batch_buffer) >= batch_size:
                    # Flush the batch to Firestore
                    print(f"Flushing batch of {len(batch_buffer)} jobs")
                    flushed = flush_batch(
                        self.source,
                        batch_buffer,
                        freshness_thresh,
                        relevance_thresh,
                    )
                    new_jobs.extend(flushed)
                    batch_buffer.clear()
                    if not flushed:  # early exit
                        break

                if not self._has_next_page(page):
                    print("No more pages to scrape.")
                    break
                go_next(page, timeout=5000)

            # flush leftover
            flushed = flush_batch(
                self.source,
                batch_buffer,
                freshness_thresh,
                relevance_thresh,
            )
            new_jobs.extend(flushed)
            return new_jobs
        finally:
            self._close_browser()
            scraper_control.set_status(self.source, "OFF")

    # ------------------------------------------------------------------
    # Page helpers
    # ------------------------------------------------------------------

    def _extract_single_job(
        self, card
    ) -> LinkedInJob:
        # Get linkedin internal ID
        linkedin_id = card.get_attribute(LinkedInSelectors.job_id_attr) or ""
        # Get title
        title = self._safe_extract(card, LinkedInSelectors.title)
        # Get company
        company = self._safe_extract(card,LinkedInSelectors.company)
        # Get location
        location = self._safe_extract(card,LinkedInSelectors.location)


        # deep fetch of description
        desc = ""
        try:
            card.click()
            card.page.wait_for_selector(
                LinkedInSelectors.description, timeout=8000
            )
            desc = (
                card.page.locator(LinkedInSelectors.description)
                .inner_text()
                .strip()
            )
            # Get category by calculating relevance from JD 
            from scrapers.common.classifier import classify_job
            category = classify_job(desc)

            # TODO  extract tags from JD and save to list
        except Exception as e:
            print("Could not fetch JD", e)
            pass

        return LinkedInJob(
            source=self.source,
            category = category,
            source_id=linkedin_id,
            company=company,
            title=title,
            description=desc,
            location=location,
            url=f"https://www.linkedin.com/jobs/view/{linkedin_id}/",
            posted_at=self._safe_extract(card, LinkedInSelectors.posted_at),
            seniority_level=self._safe_extract(card, LinkedInSelectors.seniority),
            employment_type=self._safe_extract(card, LinkedInSelectors.emp_type),
            job_function=self._safe_extract(card, LinkedInSelectors.function),
            industries=self._safe_extract(card, LinkedInSelectors.industries),
            applicant_count=self._safe_int(card, LinkedInSelectors.applicant_count),
        )

    # ------------------------------------------------------------------
    # Small utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _safe_extract(card, sel: str) -> str:
        try:
            return card.locator(sel).first.inner_text(timeout=1000).strip()
        except Exception:
            return ""

    @staticmethod
    def _safe_int(card, sel: str) -> int:
        try:
            txt = card.locator(sel).fitst.inner_text(timeout=1000)
            return int("".join(filter(str.isdigit, txt)))
        except Exception:
            return 0

    @staticmethod
    def _has_next_page(page: Page) -> bool:
        count = page.locator(LinkedInSelectors.next_page).count()
        print(f"Next page button found: {count > 0}")
        return count > 0
