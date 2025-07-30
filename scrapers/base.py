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
        from scrapers.base import BaseScraper
        self.id = BaseScraper.build_deterministic_id(
            [self.source, self.source_id, self.company, self.title]
        )


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