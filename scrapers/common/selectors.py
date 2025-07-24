"""
Central registry for LinkedIn DOM selectors.
If LinkedIn changes its markup, edit only this file (or update the
optional config.json and reload at runtime).
"""

from __future__ import annotations
import json
from pathlib import Path

# ---------- hard-coded defaults ----------
DEFAULT_SELECTORS = {
    # containers
    "job_card_container": '[data-job-id]',
    # attributes
    "job_id_attr": "data-job-id",
    # text nodes inside a card
    "title": '[data-job-id] .job-card-list__title',
    "company": '[data-job-id] .job-card-container__company-name',
    "posted_at": '[data-job-id] time',
    "seniority": '[data-job-id] .job-card-container__metadata-item:nth-child(1)',
    "emp_type": '[data-job-id] .job-card-container__metadata-item:nth-child(2)',
    "function": '[data-job-id] .job-card-container__metadata-item:nth-child(3)',
    "industries": '[data-job-id] .job-card-container__metadata-item:nth-child(4)',
    "applicant_count": '[data-job-id] .job-card-container__applicant-count',
    # right-pane description (after click)
    "description": '.jobs-description-content__text',
    # pagination
    "next_page": 'button[aria-label="Next"]',
}

# ---------- optional runtime override via config.json ----------
_config_path = Path(__file__).with_name("selectors_config.json")
if _config_path.exists():
    with _config_path.open() as f:
        DEFAULT_SELECTORS.update(json.load(f))


class LinkedInSelectors:
    """
    Simple namespace so the rest of the codebase can do:
        LinkedInSelectors.job_card_container
    """
    __slots__ = ()  # prevent accidental attribute creation

    def __getattr__(self, name: str) -> str:
        try:
            return DEFAULT_SELECTORS[name]
        except KeyError:
            raise AttributeError(f"Unknown selector {name!r}") from None


# create singleton
LinkedInSelectors = LinkedInSelectors()