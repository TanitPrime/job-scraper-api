"""
Turn a list of locations and a list of category keywords
into a single boolean search query that LinkedIn’s /jobs
endpoint accepts in its `keywords=` parameter.
"""
import json
from pathlib import Path
from typing import List
from scrapers.common.search_matrix import load_matrix

search_matrix = load_matrix()

def build_boolean_query(categories: List[str] = list(search_matrix["CATEGORY_KEYWORDS"].keys()), locations: str = search_matrix["LOCATIONS"]) -> str:
    """
    Example:
        categories = ["software engineer", "backend"]
        locations   = ["United States", "Remote"]
    Returns:
        'software engineer OR backend location: "United States" OR "Remote"'
    """
    # Escape internal quotes in category strings
    escaped_cats = [f'{cat.strip()} ' for cat in categories if cat.strip()]
    categories_part = " OR ".join(escaped_cats)
    locations = [f'"{l.strip()}"' for l in locations]
    locations_part = " OR ".join(locations)
    query = f'({categories_part}) location: ({locations_part})'

    return query
