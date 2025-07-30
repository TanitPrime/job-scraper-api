import json
from pathlib import Path
from typing import Dict, List

_FILE = Path(__file__).with_name("search_matrix.json")

def load_matrix() -> Dict:
    with open(_FILE, encoding="utf-8") as f:
        return json.load(f)