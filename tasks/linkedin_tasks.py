# tasks/linkedin_tasks.py
"""
Celery tasks for LinkedIn scraping.
"""

from __future__ import annotations
import logging
from typing import List

from celery import Task
from scrapers.linkedin.scraper import LinkedInScraper, ReLoginRequired
from scrapers.common.firebase_client import get_firestore_client

logger = logging.getLogger(__name__)


class LinkedInScrapeTask(Task):
    max_retries = 0  # we handle our own retry logic (beat loop)

    def run(
        self,
        locations: List[str],
        categories: List[str],
        slice_size: int = 50,
        max_pages: int = 3,
        freshness_thresh: float = 0.8,
        relevance_thresh: float = 0.3,
        delay: float = 6.0,
    ) -> int:
        """
        Celery task entry-point.
        Returns the number of *new* jobs actually written to Firestore.
        """
        logger.info("LinkedIn scrape task started.")

        try:
            scraper = LinkedInScraper(
                cookies_path="secrets/cookies.json",
                storage_path="secrets/local_storage.json",
                proxy=None,  # override via env if needed
            )
            jobs = scraper.scrape_batch(
                locations=locations,
                categories=categories,
                slice_size=slice_size,
                max_pages=max_pages,
                freshness_thresh=freshness_thresh,
                relevance_thresh=relevance_thresh,
                delay=delay,
            )

            # Bulk write new docs
            db = get_firestore_client()
            batch = db.batch()
            for job in jobs:
                ref = db.collection("jobs").document(job.id)
                batch.set(ref, job.to_dict())
            batch.commit()

            logger.info("Saved %d new jobs to Firestore.", len(jobs))
            return len(jobs)

        except ReLoginRequired as exc:
            # alert & skip beat cycle
            logger.warning("Re-login required: %s", exc)
            # TODO: send_slack_alert(str(exc))
            # Celery beat will simply fire again at the next schedule
            return 0


# alias for easy Celery registration
linkedin_batch = LinkedInScrapeTask()