"""
LinkedIn-specific dataclasses that extend the universal Job model.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Any

from scrapers.base import Job


@dataclass
class LinkedInJob(Job):
    """
    Concrete Job subclass holding extra LinkedIn fields.
    """
    posted_at: str = ""            # ISO-8601 or "2 days ago"
    seniority_level: str = ""
    employment_type: str = ""
    job_function: str = ""
    industries: str = ""
    applicant_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Return flattened dict ready for Firestore."""
        base = self.__dict__.copy()
        base["source"] = self.source
        return base