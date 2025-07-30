"""
Central registry for LinkedIn DOM selectors.
If LinkedIn changes its markup, edit only this file (or update the
optional config.json and reload at runtime).
"""

from __future__ import annotations
import json
from pathlib import Path


class LinkedInSelectors:
    """
    Simple namespace so the rest of the codebase can do:
        LinkedInSelectors.job_card_container
    """
    __slots__ = ()  # prevent accidental attribute creation

    def __getattr__(self, name: str) -> str:
        try:
            # ----------  import linkedin_selectrs_config.json ----------
            _LINKEDIN_SELECTORS = {}
            _config_path = Path(__file__).with_name("linkedin_selectors_config.json")
            if _config_path.exists():
                with _config_path.open() as f:
                    _LINKEDIN_SELECTORS.update(json.load(f))
            return _LINKEDIN_SELECTORS[name]
        except KeyError:
            raise AttributeError(f"Unknown selector {name!r}") from None


# create singleton
LinkedInSelectors = LinkedInSelectors()