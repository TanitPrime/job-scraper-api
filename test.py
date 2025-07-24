from tasks.linkedin_tasks import linkedin_batch
new = linkedin_batch.run(
    locations=["Paris"],
    categories=["software engineer", "backend"],
    slice_size=10,
    max_pages=1,
    freshness_thresh=0.8,
    relevance_thresh=0.3,
    delay=3,
)
print(f"✅ Scraped + stored {new} new jobs")