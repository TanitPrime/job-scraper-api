# scrapers/linkedin/scraper.py
"""
LinkedIn scraper that:
- builds one boolean query per location
- walks pages in slices of N jobs
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
from scrapers.common.selectors import LinkedInSelectors
from scrapers.common.firebase_client import get_firestore_client
from scrapers.common.relevance import cosine_similarity


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
    ):
        self.cookies_path = cookies_path
        self.storage_path = storage_path
        self.proxy = proxy
        self.headless = headless
        self._browser: Browser | None = None
        self._page: Page | None = None
        self._db = get_firestore_client()

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
        locations: List[str],
        categories: List[str],
        slice_size: int = 50,
        max_pages: int = 3,
        freshness_thresh: float = 0.8,
        relevance_thresh: float = 0.3,
        delay: float = 6.0,
    ) -> List[LinkedInJob]:
        page = self._start_browser()
        all_new_jobs: List[LinkedInJob] = []

        for loc in locations:
            query = build_boolean_query(categories, loc)
            url = "https://www.linkedin.com/jobs/search?" + urlencode(
                {"keywords": query, "f_JT": "F"}
            )
            page.goto(url, timeout=30000)
            page.wait_for_selector(
                LinkedInSelectors.job_card_container, timeout=15000
            )

            seen_so_far = 0
            for _ in range(max_pages):
                cards = page.locator(LinkedInSelectors.job_card_container).all()[
                    seen_so_far : seen_so_far + slice_size
                ]
                if not cards:
                    break

                slice_jobs = [
                    self._extract_single_job(card, loc, categories) for card in cards
                ]
                ids = [j.id for j in slice_jobs]

                # ---- early-exit checks ----
                existing = {
                    snap.id
                    for snap in self._db.collection("jobs").get_all(
                        [self._db.collection("jobs").document(i) for i in ids]
                    )
                    if snap.exists
                }
                fresh_ratio = len(existing) / len(slice_jobs)

                rel_scores = [
                    cosine_similarity(j.description, categories) for j in slice_jobs
                ]
                avg_rel = sum(rel_scores) / len(rel_scores)

                if fresh_ratio > freshness_thresh or avg_rel < relevance_thresh:
                    # stop scanning further for this location
                    break

                # keep only truly new jobs
                new_slice = [j for j in slice_jobs if j.id not in existing]
                all_new_jobs.extend(new_slice)

                seen_so_far += len(cards)
                time.sleep(random.uniform(delay * 0.8, delay * 1.2))

                # next page
                if not self._has_next_page(page):
                    break
                self._go_next(page)

        self._close_browser()
        return all_new_jobs

    # ------------------------------------------------------------------
    # Page helpers
    # ------------------------------------------------------------------

    def _extract_single_job(
        self, card, location: str, categories: List[str]
    ) -> LinkedInJob:
        linkedin_id = card.get_attribute(LinkedInSelectors.job_id_attr) or ""
        title = card.locator(LinkedInSelectors.title).inner_text(timeout=2000).strip()
        company = (
            card.locator(LinkedInSelectors.company).inner_text(timeout=2000).strip()
        )

        # build deterministic global id
        job_id = self.build_deterministic_id([self.source, linkedin_id, company, title])

        # optional deep fetch of description
        desc = ""
        try:
            card.click()
            card.page.wait_for_selector(
                LinkedInSelectors.description, timeout=5000
            )
            desc = (
                card.page.locator(LinkedInSelectors.description)
                .inner_text()
                .strip()
            )
        except Exception:
            pass

        return LinkedInJob(
            source=self.source,
            source_id=linkedin_id,
            company=company,
            title=title,
            description=desc,
            location=location,
            url=f"https://www.linkedin.com/jobs/view/{linkedin_id}/",
            linkedin_job_id=linkedin_id,
            posted_at=self._safe_extract(card, LinkedInSelectors.posted_at),
            seniority_level=self._safe_extract(card, LinkedInSelectors.seniority),
            employment_type=self._safe_extract(card, LinkedInSelectors.emp_type),
            job_function=self._safe_extract(card, LinkedInSelectors.function),
            industries=self._safe_extract(card, LinkedInSelectors.industries),
            applicant_count=self._safe_int(card, LinkedInSelectors.applicant_count),
            id=job_id,
        )

    # ------------------------------------------------------------------
    # Small utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _safe_extract(card, sel: str) -> str:
        try:
            return card.locator(sel).inner_text(timeout=1000).strip()
        except Exception:
            return ""

    @staticmethod
    def _safe_int(card, sel: str) -> int:
        try:
            txt = card.locator(sel).inner_text(timeout=1000)
            return int("".join(filter(str.isdigit, txt)))
        except Exception:
            return 0

    @staticmethod
    def _has_next_page(page: Page) -> bool:
        return page.locator(LinkedInSelectors.next_page).count() > 0

    @staticmethod
    def _go_next(page: Page):
        page.locator(LinkedInSelectors.next_page).click()
        page.wait_for_load_state("networkidle")