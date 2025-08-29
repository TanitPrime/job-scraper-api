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
    """
    Send new jobs to Firestore and return them.
    
    Args:
        source: Source of the jobs (e.g., 'linkedin')
        batch_buffer: List of job objects to process
        freshness_thresh: Minimum ratio of new jobs required (0-1)
        relevance_thresh: Minimum average relevance score required (0-1)
        db: Firestore client
        matrix: Search matrix with keywords
    
    Returns:
        List of new jobs that were written to Firestore
    """
    #print(f"Received batch of {len(batch_buffer)} jobs to flush")
    if not batch_buffer:
        return []
    # Prepare to check existing jobs
    # Use source-specific collection
    CATEGORY_KEYWORDS = list(matrix["CATEGORY_KEYWORDS"].keys())
    ids = [j.id for j in batch_buffer]
    refs = [db.collection(f"jobs").document(i) for i in ids]
    docs = db.get_all(refs)
    existing = {snap.id for snap in docs if snap.exists}
    new_jobs = [j for j in batch_buffer if j.id not in existing]
    total_docs = db.collection(f"jobs").count().get()[0][0].value

    # Calculate freshness ratio (new jobs / total jobs in batch)
    fresh_ratio = len(new_jobs) / len(batch_buffer) if batch_buffer else 0
    print(f"Freshness ratio: {fresh_ratio:.2f} ({len(new_jobs)} new out of {len(batch_buffer)} total)")

    # If no jobs exist yet, or we're doing a deep scrape (thresholds = 0), write all new jobs
    if total_docs == 0 or freshness_thresh == 0 or relevance_thresh == 0:
        print("No existing jobs or deeply scraping; writing all new jobs.")
        new_batch = batch_buffer
    else:
        # Calculate freshness and relevance
        # Relevance: average fuzzy score against category keywords
        rel_scores = [
            token_fuzzy(j.description, CATEGORY_KEYWORDS)
            for j in batch_buffer
        ]
        avg_rel = sum(rel_scores) / len(rel_scores) if rel_scores else 0
        print(f"Average relevance: {avg_rel:.2f}")

        # Check if the batch meets our quality thresholds
        if fresh_ratio < freshness_thresh or avg_rel < relevance_thresh:
            print(f"Batch rejected: Not fresh or relevant enough ({fresh_ratio:.2f}, {avg_rel:.2f})")
            return []
        
        new_batch = new_jobs
    print(f"🔍 Found {len(existing)} existing jobs, {len(new_batch)} new jobs to write.")
    # Write new jobs to Firestore
    if new_batch:
        batch = db.batch()
        for job in new_batch:
            batch.set(db.collection(f"jobs").document(job.id), job.to_dict())
        batch.commit()
        print(f"✅ Wrote {len(new_batch)} new jobs to Firestore.")
    return new_batch