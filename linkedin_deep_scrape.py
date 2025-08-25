"""
Script to deeply scrape LinkedIn job postings to enrich fresh database.
"""
from tasks.linkedin_task import run_linkedin_scraper

new = run_linkedin_scraper(
    batch_size=20,
    max_pages=150,
    freshness_thresh=0,
    relevance_thresh=0,
    delay=3,
)
print(f"✅ Scraped + stored {new} new jobs")