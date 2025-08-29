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
        
        # Initialize database if needed
        scraper_control.init()
        
        # Check service status
        service_status = scraper_control.get_service_status()
        if service_status["status"] != "active":
            logger.info("Scraping service is paused; skipping beat.")
            return 0

        # Get current scraper status
        scraper_status = scraper_control.get_scraper_status("linkedin")
        if scraper_status["status"] == "running":
            logger.info("Scraper is already running; skipping beat.")
            return 0
        elif scraper_status["status"] == "paused":
            logger.info("Scraper is paused; skipping beat.")
            return 0
        elif scraper_status["status"] == "error":
            logger.warning("Previous run ended with error: %s", scraper_status["error_message"])

        try:
            scraper = LinkedInScraper(
                cookies_path="secrets/cookies.json",
                storage_path="secrets/local_storage.json",
                proxy=None,  # Use default proxy settings
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
            error_msg = f"Re-login required: {str(exc)}"
            logger.warning(error_msg)
            scraper_control.set_scraper_status("linkedin", "error", error_msg)
            # TODO: send_slack_alert(str(exc))
            return 0
            
        except Exception as exc:
            error_msg = f"Unexpected error during scraping: {str(exc)}"
            logger.error(error_msg)
            scraper_control.set_scraper_status("linkedin", "error", error_msg)
            return 0
            
    with ThreadPoolExecutor() as pool:
        return pool.submit(_sync).result()


# alias for easy Celery registration
#linkedin_batch = LinkedInScrapeTask()