from tasks.linkedin_task import run_linkedin_scraper

new = run_linkedin_scraper(
    batch_size=20,
    max_pages=50,
    freshness_thresh=0.8,
    relevance_thresh=0.3,
    delay=3,
)
print(f"✅ Scraped + stored {new} new jobs")