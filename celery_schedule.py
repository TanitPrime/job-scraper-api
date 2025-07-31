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
celery_app.conf.beat_schedule = {
    "run-linkedin-scraper-every-5-hours": {
        "task": "linkedin.scraper",
        "schedule": 5 * 60 * 60,  # every 5 hours
        "args": []
    }
}