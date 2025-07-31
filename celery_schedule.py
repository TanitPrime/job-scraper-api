from celery import Celery
import sqlite3

# Configure Celery app
celery_app = Celery(
    "job_scraper",
    broker="redis://localhost:6379/0",  # Change if using a different broker
    backend="redis://localhost:6379/0"
)

# Autodiscover tasks in the tasks/ folder
celery_app.autodiscover_tasks(["tasks"])

def is_scraper_active(name: str) -> bool:
    """
    Check if the scraper is marked as 'active' in the control database.
    """
    conn = sqlite3.connect("scraper_control.db")
    c = conn.cursor()
    c.execute("SELECT status FROM scraper_control WHERE name = ?", (name,))
    row = c.fetchone()
    conn.close()
    return row is None or row[0] == "active"

@celery_app.task(name="tasks.linkedin_tasks.run_linkedin_scraper")
def run_linkedin_scraper(
    slice_size: int = 50,
    max_pages: int = 3,
    freshness_thresh: float = 0.8,
    relevance_thresh: float = 0.3,
    delay: float = 6.0,
) -> int:
    """
    Celery task for LinkedIn scraping. Skips if paused in SQLite.
    """
    if not is_scraper_active("linkedin"):
        print("LinkedIn scraper is paused. Skipping task.")
        return 0

    # Import here to avoid circular imports
    from tasks.linkedin_tasks import linkedin_batch
    return linkedin_batch.run(
        slice_size=slice_size,
        max_pages=max_pages,
        freshness_thresh=freshness_thresh,
        relevance_thresh=relevance_thresh,
        delay=delay,
    )

# Optional: Celery beat schedule for periodic scraping
celery_app.conf.beat_schedule = {
    "run-linkedin-scraper-every-5-hours": {
        "task": "tasks.linkedin_tasks.run_linkedin_scraper",
        "schedule": 5 * 60 * 60,  # every 5 hours
        "args": []
    }
}