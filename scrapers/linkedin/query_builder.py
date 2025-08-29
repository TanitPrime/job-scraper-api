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
        categories = ["dev", "design"]
        locations   = ["Tunisia"]
    Returns:
        A boolean query combining keywords from both languages for each category
    """
    all_keywords = []
    
    # For each category, get both English and French keywords
    for category in categories:
        if category in search_matrix["CATEGORY_KEYWORDS"]:
            category_keywords = []
            # Add English keywords
            category_keywords.extend(search_matrix["CATEGORY_KEYWORDS"][category]["en"])
            # Add French keywords
            category_keywords.extend(search_matrix["CATEGORY_KEYWORDS"][category]["fr"])
            # Add any other keywords
            if "other" in search_matrix["CATEGORY_KEYWORDS"][category]:
                category_keywords.extend(search_matrix["CATEGORY_KEYWORDS"][category]["other"])
            # Add unique keywords to the final list
            all_keywords.extend(set(category_keywords))
    
    # Create the query parts
    keyword_part = " OR ".join([f'"{kw.strip()}"' for kw in all_keywords if kw.strip()])
    locations_part = " OR ".join([f'"{l.strip()}"' for l in locations])
    
    query = f'({keyword_part}) location: ({locations_part})'

    return query
