"""
Scraper control module for managing scraper states and service status.
"""
import sqlite3
from datetime import datetime
from typing import Dict, List
from contextlib import contextmanager

class ScraperControl:
    def __init__(self, db_path: str = "scraper_control.db"):
        self.db_path = db_path

    @contextmanager
    def _get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    def init(self) -> None:
        """Initialize the database by creating necessary tables."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            # Table for individual scraper status
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS scraper_status (
                    name TEXT PRIMARY KEY,
                    status TEXT CHECK(status IN ('running', 'paused', 'error', 'idle')) NOT NULL DEFAULT 'idle',
                    last_run TIMESTAMP,
                    last_success TIMESTAMP,
                    error_message TEXT,
                    jobs_scraped INTEGER DEFAULT 0
                )
            """)
            # Table for overall service status
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS service_status (
                    id INTEGER PRIMARY KEY CHECK(id = 1),
                    status TEXT CHECK(status IN ('active', 'paused')) NOT NULL DEFAULT 'active',
                    last_check TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    active_scrapers INTEGER DEFAULT 0
                )
            """)
            # Insert default service status if not exists
            cursor.execute("""
                INSERT OR IGNORE INTO service_status (id, status, last_check)
                VALUES (1, 'active', CURRENT_TIMESTAMP)
            """)
            conn.commit()

    def get_scraper_status(self, name: str) -> Dict:
        """Get detailed status of a specific scraper."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT status, last_run, last_success, error_message, jobs_scraped
                FROM scraper_status WHERE name = ?
            """, (name,))
            row = cursor.fetchone()
            if row:
                return {
                    "name": name,
                    "status": row[0],
                    "last_run": row[1],
                    "last_success": row[2],
                    "error_message": row[3],
                    "jobs_scraped": row[4]
                }
            return {
                "name": name,
                "status": "idle",
                "last_run": None,
                "last_success": None,
                "error_message": None,
                "jobs_scraped": 0
            }

    def set_scraper_status(self, name: str, status: str, error_message: str = None) -> None:
        """Update a scraper's status and related information."""
        now = datetime.utcnow().isoformat()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO scraper_status (name, status, last_run, error_message)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET 
                    status = ?,
                    last_run = ?,
                    error_message = ?
            """, (name, status, now, error_message, status, now, error_message))
            
            if status == "running":
                # Update active scrapers count
                cursor.execute("""
                    UPDATE service_status 
                    SET active_scrapers = (
                        SELECT COUNT(*) FROM scraper_status WHERE status = 'running'
                    )
                    WHERE id = 1
                """)
            conn.commit()

    def update_jobs_scraped(self, name: str, count: int) -> None:
        """Update the number of jobs scraped by a scraper."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE scraper_status 
                SET jobs_scraped = jobs_scraped + ?,
                    last_success = CURRENT_TIMESTAMP
                WHERE name = ?
            """, (count, name))
            conn.commit()

    def get_service_status(self) -> Dict:
        """Get overall service status including active scrapers."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT status, last_check, active_scrapers
                FROM service_status WHERE id = 1
            """)
            row = cursor.fetchone()
            return {
                "status": row[0],
                "last_check": row[1],
                "active_scrapers": row[2]
            }

    def set_service_status(self, status: str) -> None:
        """Update the overall service status."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE service_status 
                SET status = ?,
                    last_check = CURRENT_TIMESTAMP
                WHERE id = 1
            """, (status,))
            conn.commit()

    def get_all_scrapers_status(self) -> List[Dict]:
        """Get status of all scrapers."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name, status, last_run, last_success, error_message, jobs_scraped FROM scraper_status")
            return [{
                "name": row[0],
                "status": row[1],
                "last_run": row[2],
                "last_success": row[3],
                "error_message": row[4],
                "jobs_scraped": row[5]
            } for row in cursor.fetchall()]

# Create a singleton instance
scraper_control = ScraperControl()

