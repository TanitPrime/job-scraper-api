from celery import Celery
from celery.schedules import crontab
import sqlite3

# Configure Celery app
app = Celery(
    "job_scraper",
    broker="redis://localhost:6379/0",  # Change if using a different broker
    backend="redis://localhost:6379/0"
)

app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)

# # Autodiscover tasks in the tasks/ folder
# celery_app.autodiscover_tasks(["tasks"])
from tasks.linkedin_task import run_linkedin_scraper

def is_scraper_active(source: str, path = "scraper_control.db") -> bool:
    """
    Check if the scraper is marked as 'ON' in the control database.
    """
    conn = sqlite3.connect(path)
    c = conn.cursor()
    c.execute("SELECT status FROM scraper_control WHERE source = ?", (source))
    row = c.fetchone()
    conn.close()
    return row is None or row[0] == "ON"

# Optional: Celery beat schedule for periodic scraping
app.conf.beat_schedule = {
    "run-linkedin-scraper-every-5-hours": {
        "task": 'linkedin.scraper',
        "schedule": 5*60,  # every 5 minutes for testing change to 5*60 for production
        "args": [10, 1, 0.8 , 0.3 , 3.5] # args: batch_size, max_pages, freshness_thresh, relevance_thresh, delay
    }
}