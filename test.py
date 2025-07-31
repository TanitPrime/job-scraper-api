from tasks.linkedin_task import run_linkedin_scraper

new = run_linkedin_scraper(
    slice_size=50,
    max_pages=10,
    freshness_thresh=0,
    relevance_thresh=0,
    delay=2.5,
)
print(f"✅ Scraped + stored {new} new jobs")