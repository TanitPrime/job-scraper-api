from rapidfuzz import fuzz 
from typing import List
import numpy as np
import re

# Calculates similarity between tokens and keywords
def token_fuzzy(text: str, kw_list: List[str]) -> float:
    text_tok   = set(re.findall(r'\w+', text.lower()))
    scores = [
        fuzz.token_set_ratio(' '.join(text_tok), k.lower()) / 100.0
        for k in kw_list
    ]
    return np.max(scores) if scores else 0.0