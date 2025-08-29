# scrapers/linkedin/scraper.py
"""
LinkedIn scraper that:
- builds one boolean query per location
- walks pages in batchs of N jobs
- stops early when freshness or relevance thresholds are crossed
- returns ONLY new & relevant jobs
"""

from datetime import datetime, timedelta
import re
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
        # Initialize scraper as idle
        scraper_control.set_scraper_status(self.source, "idle")

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

        # Check if service is active
        service_status = scraper_control.get_service_status()
        if service_status["status"] != "active":
            scraper_control.set_scraper_status(self.source, "paused", "Service is paused")
            return []

        # Update scraper status to running
        scraper_control.set_scraper_status(self.source, "running")
        page = self._start_browser()
        
        try:
            # Build the boolean query and navigate to the jobs page
            query = build_boolean_query()
            url = (
                    "https://www.linkedin.com/jobs/search?" +
                    urlencode({"geoId": "92000000", "keywords": query, "f_JT": "F"}) +
                    "&sortBy=DD"
                    )
            
            page.goto(url, timeout=50000)
            # Wait for the page to load
            page.wait_for_selector(LinkedInSelectors.job_card_container, timeout=15000)

            # Scraping starts here
            new_jobs, batch_buffer = [], []
            stop_early = False
            for _ in range(max_pages): # Loop over pages, then loop over cards (in batches) and flush and check thresholds on each batch
                # Scroll to load all jobs in the sidebar
                scroll_to_load_all_jobs(page)
                # Collect job cards
                cards = page.locator(LinkedInSelectors.job_card_container).all()
                if not cards:
                    break

                # Extract jobs and flush in batches
                for card in cards:
                    flushed = []
                    # Delay to avoid rate limiting
                    page.wait_for_timeout(delay * 1000)

                    # Check for login wall
                    if page.locator(LinkedInSelectors.login_wall).count() > 0:
                        raise ReLoginRequired("Login wall detected; cookies may be expired.")

                    # Extract job data from each card
                    batch_buffer.append(self._extract_single_job(card))

                    # Flush the batch if it reaches the batch size
                    if len(batch_buffer) >= batch_size:
                        print(f"Flushing batch of {len(batch_buffer)} jobs")
                        flushed = list(flush_batch(
                            self.source,
                            batch_buffer,
                            freshness_thresh,
                            relevance_thresh,
                        ))
                        print(f"Flushed is {len(flushed)}")
                        new_jobs.extend(flushed)
                        batch_buffer.clear()

                        # Early exit if no new jobs were flushed
                        if len(flushed) == 0:
                            print("No new jobs found in this batch, exiting early.")
                            stop_early = True
                            break

                # Check if we need to stop early
                if stop_early:
                    print("No new jobs found, exiting early.")
                    break

                # Check if we have more pages to scrape
                if not self._has_next_page(page):
                    print("No more pages to scrape.")
                    break
                else:
                    go_next(page, timeout=5000)

            # flush leftover
            print(f"Flushing leftover batch of {len(batch_buffer)} jobs")
            flushed = flush_batch(
                self.source,
                batch_buffer,
                freshness_thresh,
                relevance_thresh,
            )
            if flushed:
                print(f"Flushed {len(flushed)} leftover jobs.")
                new_jobs.extend(flushed)
            
            # Update success status and job count
            scraper_control.update_jobs_scraped(self.source, len(new_jobs))
            scraper_control.set_scraper_status(self.source, "idle")
            return new_jobs

        except ReLoginRequired as e:
            error_msg = "Login required: cookies may have expired"
            print(f"Error: {error_msg}")
            scraper_control.set_scraper_status(self.source, "error", error_msg)
            raise

        except Exception as e:
            error_msg = f"Scraping failed: {str(e)}"
            print(f"Error: {error_msg}")
            scraper_control.set_scraper_status(self.source, "error", error_msg)
            raise

        finally:
            self._close_browser()

    # ------------------------------------------------------------------
    # Page helpers
    # ------------------------------------------------------------------

    def _extract_number(self,txt):
        """Return the first integer found in the string."""
        m = re.search(r'(\d+)', txt)
        if m:
            return int(m.group(1))
        return None

    def _parse_relative_time(self,txt):
        """
        Turns "7 hours ago", "3 days ago", "2 weeks ago" into a yyyy/mm/dd date.
        If nothing matches, return None.
        """
        txt = txt.lower()
        now = datetime.now()

        # seconds
        m = re.search(r'(\d+)\s+second', txt)
        if m:
            delta = timedelta(seconds=int(m.group(1)))
            return (now - delta).strftime("%Y/%m/%d")
        
        # minutes
        m = re.search(r'(\d+)\s+minute', txt)
        if m:
            delta = timedelta(minutes=int(m.group(1)))
            return (now - delta).strftime("%Y/%m/%d")

        # hours
        m = re.search(r'(\d+)\s+hour', txt)
        if m:
            delta = timedelta(hours=int(m.group(1)))
            return (now - delta).strftime("%Y/%m/%d")

        # days
        m = re.search(r'(\d+)\s+day', txt)
        if m:
            delta = timedelta(days=int(m.group(1)))
            return (now - delta).strftime("%Y/%m/%d")

        # weeks
        m = re.search(r'(\d+)\s+week', txt)
        if m:
            delta = timedelta(weeks=int(m.group(1)))
            return (now - delta).strftime("%Y/%m/%d")

        return None

    def _extract_single_job(
        self, card
    ) -> LinkedInJob:
        # Get linkedin internal ID
        linkedin_id = card.get_attribute('data-job-id') or ""

        # Get title
        title = self._safe_extract(card, LinkedInSelectors.title)

        # Get company
        company = self._safe_extract(card,LinkedInSelectors.company)
        
        # deep fetch of description
        desc = ""
        try:
            card.click()
            
            # Get description
            desc = (
                card.page.locator(LinkedInSelectors.description)
                .inner_text()
                .strip()
            )

            # Get location
            location = self._safe_extract(card,LinkedInSelectors.location)

            # Get posted time delta
            posted_at_raw = self._safe_extract(card, LinkedInSelectors.posted_at)
            posted_at = self._parse_relative_time(posted_at_raw) or posted_at_raw

            # Get applicant count
            applicant_count_raw = self._safe_extract(card, LinkedInSelectors.applicant_count)
            applicant_count = self._extract_number(applicant_count_raw) or applicant_count_raw            

        except Exception as e:
            print("Could not fetch JD", e)
            pass

        return LinkedInJob(
            source=self.source,
            source_id=linkedin_id,
            company=company,
            title=title,
            description=desc,
            location=location,
            url=f"https://www.linkedin.com/jobs/view/{linkedin_id}/",
            posted_at=posted_at,
            seniority_level=self._safe_extract(card, LinkedInSelectors.seniority),
            employment_type=self._safe_extract(card, LinkedInSelectors.emp_type),
            job_function=self._safe_extract(card, LinkedInSelectors.function),
            industries=self._safe_extract(card, LinkedInSelectors.industries),
            applicant_count=applicant_count,
        )

    # ------------------------------------------------------------------
    # Small utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _safe_extract(card, sel: str) -> str:
        try:
            card.page.wait_for_selector(sel, timeout=2000)
            return card.page.locator(sel).first.inner_text(timeout=1000).strip()
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
