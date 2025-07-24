"""
Turn a list of locations and a list of category keywords
into a single boolean search query that LinkedIn’s /jobs
endpoint accepts in its `keywords=` parameter.
"""

from typing import List


def build_boolean_query(categories: List[str], location: str) -> str:
    """
    Example:
        categories = ["software engineer", "backend"]
        location   = "United States"
    Returns:
        '("software engineer" OR backend) AND "United States"'
    """
    # Escape internal quotes in category strings
    escaped_cats = [f'"{cat.strip()}"' for cat in categories if cat.strip()]
    categories_part = " OR ".join(escaped_cats)

    escaped_loc = f'"{location.strip()}"'
    query = f"({categories_part}) AND {escaped_loc}"
    return query