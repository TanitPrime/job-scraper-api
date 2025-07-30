from tasks.linkedin_tasks import linkedin_batch

new = linkedin_batch.run(
    slice_size=50,
    max_pages=10,
    freshness_thresh=0,
    relevance_thresh=0,
    delay=3,
)
print(f"✅ Scraped + stored {new} new jobs")