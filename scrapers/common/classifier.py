"""Infer category and location from card data."""
from typing import Dict, List

from scrapers.common.relevance import token_fuzzy
from scrapers.common.search_matrix import load_matrix


def classify_job(description: str) -> str:
    """Return the category with highest fuzzy score."""
    matrix = load_matrix()
    CATEGORY_KEYWORDS: Dict[str, List[str]] = matrix["CATEGORY_KEYWORDS"]
    best_cat, best_score = "", 0.0
    for cat, kw_list in CATEGORY_KEYWORDS.items():
        score = token_fuzzy(description, kw_list)
        if score > best_score:
            best_cat, best_score = cat, score
    return best_cat