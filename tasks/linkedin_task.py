# tasks/linkedin_tasks.py
"""
Celery tasks for LinkedIn scraping.
"""

from __future__ import annotations
import logging
from celery import shared_task
from scrapers.linkedin.scraper import LinkedInScraper, ReLoginRequired
from scrapers.common.scraper_control import scraper_control
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


@shared_task(name="linkedin.scraper")
def run_linkedin_scraper(
    batch_size: int = 50,
    max_pages: int = 50,
    freshness_thresh: float = 0.8,
    relevance_thresh: float = 0.3,
    delay: float = 6.0,
) -> int:
    """
    Celery task entry-point.
    Returns the number of *new* jobs actually written to Firestore.
    """
    def _sync():
        logger.info("LinkedIn scrape task started.")
        # ----- gate -----------
        scraper_control.init()
        # Check if the scraper is active via scraper_control
        if scraper_control.get_control_status("linkedin") != "active":
            logger.info("Scraper paused via scraper_control; skipping beat.")
            return 0

        # ---- normal flow ----

        try:
            scraper = LinkedInScraper(
                cookies_path="secrets/cookies.json",
                storage_path="secrets/local_storage.json",
                proxy= None,  # Use default proxy settings
            )
            jobs = scraper.scrape_batch(
                batch_size=batch_size,
                max_pages=max_pages,
                freshness_thresh=freshness_thresh,
                relevance_thresh=relevance_thresh,
                delay=delay,
            )

            logger.info("Saved %d new jobs to Firestore.", len(jobs))
            return len(jobs)

        except ReLoginRequired as exc:
            # alert & skip beat cycle
            logger.warning("Re-login required: %s", exc)
            # TODO: send_slack_alert(str(exc))
            # Celery beat will simply fire again at the next schedule
            return 0
    with ThreadPoolExecutor() as pool:
        return pool.submit(_sync).result()


# alias for easy Celery registration
#linkedin_batch = LinkedInScrapeTask()