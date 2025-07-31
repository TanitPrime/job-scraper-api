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
from scrapers.common.relevance import token_fuzzy
from scrapers.common.search_matrix import load_matrix



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
        self._db = get_firestore_client()
        self.matrix = matrix

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
        slice_size: int = 50,
        max_pages: int = 3,
        freshness_thresh: float = 0.8,
        relevance_thresh: float = 0.3,
        delay: float = 6.0,
    ) -> List[LinkedInJob]:
        page = self._start_browser()
        all_new_jobs: List[LinkedInJob] = []
        
        query = build_boolean_query()
        url = "https://www.linkedin.com/jobs/search?" + urlencode(
            {
                "geoId": "92000000", # Set linkedin to global to expose all jobs
                "keywords": query, "f_JT": "F" # then filter down to locations and categories according to matrix
            }
        )
        page.goto(url, timeout=30000)
        page.wait_for_selector(
            LinkedInSelectors.job_card_container, timeout=15000
        )
        # Set scraper status to ON
        self.set_status(self.source, "ON")

        seen_so_far = 0
        for _ in range(max_pages):
            # Reveal all jobs
            self._scroll_to_load_all_jobs(page)
            cards = page.locator(LinkedInSelectors.job_card_container).all()[
                seen_so_far : seen_so_far + slice_size
            ]
            if not cards:
                # Raise exception
                raise ReLoginRequired("No job cards found, likely due to login wall or expired cookies.")

            slice_jobs = [
                self._extract_single_job(card) for card in cards
            ]
            #---- checking if jobs were successfully scraped-----
            if slice_jobs:
                print(f"Successfully scraped {len(slice_jobs)} jobs")
            ids = [j.id for j in slice_jobs]

            # ---- early-exit checks ----

            # Grabbing existing jobs
            refs = [self._db.collection("linkedin_jobs").document(i) for i in ids]
            docs = self._db.get_all(refs)         
            existing = {snap.id for snap in docs if snap.exists}
            total_docs = self._db.collection("linked_jobs").count().get()[0][0].value
            if total_docs == 0:
                # fallback when db is empty
                new_slice = slice_jobs
            else:
                fresh_ratio = len(existing) / len(slice_jobs)

                rel_scores = [
                    token_fuzzy(j.description, list(self.matrix["CATEGORY_KEYWORDS"].keys())) for j in slice_jobs
                ]
                avg_rel = sum(rel_scores) / len(rel_scores)

                if fresh_ratio > freshness_thresh or avg_rel < relevance_thresh:
                    # stop scanning further for this location
                    print(f"Early exit: Freshness ratio {fresh_ratio} or relevance {avg_rel} below threshold.")
                    # Set scraper status to OFF
                    self.set_status(self.source, "OFF")
                    break

            # keep only truly new jobs
            new_slice = [j for j in slice_jobs if j.id not in existing]
            all_new_jobs.extend(new_slice)

            seen_so_far += len(cards)
            time.sleep(random.uniform(delay * 0.8, delay * 1.2))

            # next page
            if not self._has_next_page(page):
                # Set scraper status to OFF
                self.set_status(self.source, "OFF")
                break
            self._go_next(page)

        self._close_browser()
        # Set scraper status to OFF
        self.set_status(self.source, "OFF")
        return all_new_jobs

    # ------------------------------------------------------------------
    # Page helpers
    # ------------------------------------------------------------------

    def _scroll_to_load_all_jobs(self, page: Page, scroll_down_attempts: int = 15) -> None:
        """Scrolls the job cards sidebar object until all jobs are loaded."""
        previous_count = 0
        # Directly select the first child div, just like your browser code
        sidebar_element = page.locator(LinkedInSelectors.sidebar).element_handle()
        if not sidebar_element:
            print("❌ first div child not found")
            return

        for attempt in range(scroll_down_attempts):
            page.evaluate(
                "el => el.scrollTo({ top: el.scrollHeight, behavior: 'smooth' })",
                sidebar_element
            )
            print(f"✅ Scrolled sidebar (attempt {attempt+1})")
            time.sleep(1.5)
            cards = page.locator(LinkedInSelectors.job_card_container).all()
            if len(cards) == previous_count:
                print("No new jobs loaded after scrolling.")
                break
            previous_count = len(cards)


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
            rel_scores = {cat: token_fuzzy(desc, kw_list) for cat, kw_list in self.matrix["CATEGORY_KEYWORDS"].items()}
            category = max(rel_scores, key=rel_scores.get)

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

    @staticmethod
    def _go_next(page: Page):
        next_btn = page.locator(LinkedInSelectors.next_page)
        if next_btn.count() > 0:
            print("Clicking next page button.")
            next_btn.click()
            page.wait_for_load_state("networkidle")
        else:
            print("No next page button to click.")