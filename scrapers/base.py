# scrapers/base.py
from abc import ABC, abstractmethod
from typing import List, Dict, Any
from dataclasses import dataclass
import hashlib
import re


@dataclass
class Job:
    """Universal job posting representation."""
    source: str               # e.g. "linkedin"
    source_id: str            # raw numeric or slug from the source
    company: str
    title: str
    description: str
    location: str
    url: str
    metadata: Dict[str, Any]  # flexible key/value extras


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