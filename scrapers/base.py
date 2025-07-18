from abc import ABC, abstractmethod
from dataclasses import dataclass
import hashlib


@dataclass
class JobCard:
    # Metadata
    job_id: str
    title: str
    company: str
    location: str
    detail_url: str
    proximity: enumerate["on-site", "hybrid", "remote"]
    snippet: str = ""           
    # JD
    description: str = ""       


class BaseScraper(ABC):
    name: str

    @abstractmethod
    def search_single(self, keyword: str, region: str = "") -> JobCard:
        """Return exactly one JobCard for the first hit."""
        return None
    
    def _normalize_text(self, text: list[str]) -> str:
        """Normalize headers like job title and company name"""
        return None

    @abstractmethod
    def hash_job(self, title: str, company_name: str, location: JobCard.location, **kwargs: str) -> str:
        """
        Make determinestic job ID using job data
        args:
            title: Job title,
            company_name: Name of the job poster,
            location: Job location if specified,
            **kwarfs: Any site specific identifying data
        """
        # Normalize text
        title = self._normalize_text(title)
        company_name = self._normalize_text(company_name)
        # Make a string to hash
        text = title + company_name + location + kwargs
        # Hash the string and return
        return hashlib.sha256(text.encode("utf-8")).hexdigest()
