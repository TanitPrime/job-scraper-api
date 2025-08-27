"""Flush / early-exit logic for one batch."""
from typing import List
from scrapers.common.firebase_client import get_firestore_client
from scrapers.common.relevance import token_fuzzy
from scrapers.common.search_matrix import load_matrix


def flush_batch(
    source: str,
    batch_buffer: List,
    freshness_thresh: float,
    relevance_thresh: float,
    db =  get_firestore_client(),
    matrix = load_matrix()
) -> List:
    """Send new jobs to Firestore and return them."""
    print(f"Received batch of {len(batch_buffer)} jobs to flush")
    if not batch_buffer:
        return []
    # Prepare to check existing jobs
    # Use source-specific collection
    CATEGORY_KEYWORDS = list(matrix["CATEGORY_KEYWORDS"].keys())
    ids = [j.id for j in batch_buffer]
    refs = [db.collection(f"{source}_jobs").document(i) for i in ids]
    docs = db.get_all(refs)
    existing = {snap.id for snap in docs if snap.exists}
    total_docs = db.collection(f"{source}_jobs").count().get()[0][0].value

    if total_docs == 0:
        # If no jobs exist, write all as new
        new_batch = batch_buffer
    else:
        # Calculate freshness and relevance
        # Relevance: average fuzzy score against category keywords
        rel_scores = [
            token_fuzzy(j.description, CATEGORY_KEYWORDS)
            for j in batch_buffer
        ]
        avg_rel = sum(rel_scores) / len(rel_scores)
        # Freshness: ratio of existing jobs to total in batch
        fresh_ratio = len(existing) / len(batch_buffer)

        # Early exit if batch is not fresh or relevant enough
        if fresh_ratio > freshness_thresh or avg_rel < relevance_thresh:
            return []  # early exit signal
        new_batch = [j for j in batch_buffer if j.id not in existing]
    print(f"🔍 Found {len(existing)} existing jobs, {len(new_batch)} new jobs to write.")
    # Write new jobs to Firestore
    if new_batch:
        batch = db.batch()
        for job in new_batch:
            batch.set(db.collection(f"jobs").document(job.id), job.to_dict())
        batch.commit()
        print(f"✅ Wrote {len(new_batch)} new jobs to Firestore.")
    return new_batch