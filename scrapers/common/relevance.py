# scrapers/common/relevance.py
"""
Cosine similarity with NLTK stop-words (English + French + …).
"""

from __future__ import annotations
import re
import math
from typing import List, Dict

# lazy import so nltk is optional until first run
try:
    import nltk
    from nltk.corpus import stopwords
    _STOP = set(stopwords.words("english") + stopwords.words("french"))
except LookupError:
    # auto-download once if not present
    nltk.download("stopwords")
    from nltk.corpus import stopwords
    _STOP = set(stopwords.words("english") + stopwords.words("french"))


def _tokenize(text: str) -> Dict[str, int]:
    tokens = re.findall(r"\b\w+\b", text.lower())
    counts: Dict[str, int] = {}
    for t in tokens:
        if t not in _STOP:
            counts[t] = counts.get(t, 0) + 1
    return counts


def _norm(vec: Dict[str, int]) -> float:
    return math.sqrt(sum(v * v for v in vec.values()))


def cosine_similarity(text: str, category_keywords: List[str]) -> float:
    if not text or not category_keywords:
        return 0.0

    cat_text = " ".join(category_keywords)
    cat_vec, cat_norm = _tokenize(cat_text), _norm(_tokenize(cat_text))
    doc_vec, doc_norm = _tokenize(text), _norm(_tokenize(text))

    if cat_norm == 0 or doc_norm == 0:
        return 0.0

    dot = sum(doc_vec.get(t, 0) * cat_vec.get(t, 0) for t in doc_vec)
    return dot / (doc_norm * cat_norm)