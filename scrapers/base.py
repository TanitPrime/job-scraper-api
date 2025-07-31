# scrapers/base.py
from abc import ABC, abstractmethod
from typing import List, Dict, Any
from dataclasses import dataclass, field
import hashlib
import re


@dataclass
class Job:
    """Universal job posting representation."""
    source: str
    category: str  # New field for job category
    source_id: str
    company: str
    title: str
    description: str
    location: str
    url: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    id: str = field(init=False)  # deterministic global ID


    # Post init to automatically generate ID
    def __post_init__(self) -> None:
        # Generate a deterministic ID based on source, source_id, company, and title
        from scrapers.base import BaseScraper
        self.id = BaseScraper.build_deterministic_id(
            [self.source, self.source_id, self.company, self.title]
        )
        # Create scraper_control.db
        BaseScraper.make_status()
        # Set status to OFF
        BaseScraper.set_status(self.source, "OFF")


class BaseScraper(ABC):
    """Abstract scraper every concrete scraper must implement."""

    @property
    @abstractmethod
    def source(self) -> str:
        """Return the lowercase source name (e.g. 'linkedin')."""

    @abstractmethod
    def scrape_batch(self, **kwargs) -> List[Job]:
        """
        Scrape a batch of job postings.
        Concrete classes decide how to paginate, filter, etc.
        """

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    @staticmethod
    def build_deterministic_id(raw_parts: List[str]) -> str:
        """
        Create a 16-byte deterministic ID from any ordered list of strings.
        Guarantees consistent length across all scrapers.
        """
        payload = "|".join(str(p).strip().lower() for p in raw_parts)
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        return digest[:16]

    @staticmethod
    def normalize_text(text: str) -> str:
        """Lightweight text normalizer for relevance scoring."""
        if not text:
            return ""
        text = re.sub(r"\s+", " ", text)
        return text.strip().lower()
    
    @staticmethod
    def make_status(db_path: str = "scraper_control.db") -> None:
        """
        Create a status table in the SQLite database if it doesn't exist.
        This is used to track scraper activity.
        """
        import sqlite3
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS scraper_status (
                source TEXT PRIMARY KEY,
                status TEXT CHECK( pType IN ('ON','OFF') )   NOT NULL DEFAULT 'OFF',
                last_run TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()
    
    @staticmethod
    def set_status(source: str, status: str, db_path: str = "scraper_control.db") -> None:
        """
        Set the status of a scraper in the SQLite database.
        """
        import sqlite3
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO scraper_status (source, status)
            VALUES (?, ?)
            ON CONFLICT(source) DO UPDATE SET status = ?, last_run = CURRENT_TIMESTAMP
        """, (source, status))
        conn.commit()
        conn.close()