# scrapers/common/firebase_client.py
"""
Singleton Firestore client that loads
service-account credentials from a JSON file
sitting in the same directory as this file.
"""

from __future__ import annotations
import os
from pathlib import Path

import firebase_admin
from firebase_admin import credentials, firestore

_CERT_FILE = Path(__file__).with_name("smartech-108ae-firebase-adminsdk-fbsvc-34e0e4da66.json")

# ------------------------------------------------------------------
# Lazy singleton
# ------------------------------------------------------------------
_db = None


def get_firestore_client() -> firestore.Client:
    global _db
    if _db is None:
        if not _CERT_FILE.exists():
            raise FileNotFoundError(
                f"Firebase service-account key not found: {_CERT_FILE}"
            )
        cred = credentials.Certificate(str(_CERT_FILE))
        # allow only one init
        try:
            firebase_admin.initialize_app(cred)
        except ValueError:
            # app already initialized
            pass
        _db = firestore.client()
    return _db