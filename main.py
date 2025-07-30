from typing import Optional
import sqlite3
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from scrapers.common.search_matrix import load_matrix
from pathlib import Path

app = FastAPI()

SEARCH_MATRIX_PATH = Path("scrapers/common/search_matrix.json")
DB_PATH = "scraper_control.db"

def init_db() -> None:
    """
    Initialize the SQLite database and create the scraper_control table if it doesn't exist.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS scraper_control (
            name TEXT PRIMARY KEY,
            status TEXT NOT NULL CHECK(status IN ('active', 'paused'))
        )
    """)
    conn.commit()
    conn.close()

init_db()

def set_scraper_status(name: str, status: str) -> None:
    """
    Set the status ('active' or 'paused') for a given scraper in the control table.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO scraper_control (name, status) VALUES (?, ?)", (name, status))
    conn.commit()
    conn.close()

def get_scraper_status(name: str) -> str:
    """
    Get the current status of a scraper. Returns 'active' if not set.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT status FROM scraper_control WHERE name = ?", (name,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else "active"

class SearchMatrixModel(BaseModel):
    CATEGORY_KEYWORDS: dict
    LOCATIONS: list

@app.get("/health")
def health() -> JSONResponse:
    """
    Health check endpoint.
    Returns status 'ok' if the API is running.
    """
    return JSONResponse(content={"status": "ok"}, status_code=200)

@app.get("/")
def root() -> dict:
    """
    Root endpoint.
    Returns a simple status message.
    """
    return {"status": "linkedin-scraper running"}

@app.get("/search-matrix")
def get_search_matrix() -> dict:
    """
    Get the current search matrix.
    """
    try:
        matrix = load_matrix()
        return matrix
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/search-matrix")
def create_search_matrix(matrix: SearchMatrixModel) -> dict:
    """
    Create a new search matrix.
    Fails if one already exists.
    """
    if SEARCH_MATRIX_PATH.exists():
        raise HTTPException(status_code=409, detail="Search matrix already exists. Use PUT to update.")
    try:
        SEARCH_MATRIX_PATH.write_text(matrix.json(), encoding="utf-8")
        return {"status": "created"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/search-matrix")
def update_search_matrix(matrix: SearchMatrixModel) -> dict:
    """
    Update the search matrix.
    """
    try:
        SEARCH_MATRIX_PATH.write_text(matrix.json(), encoding="utf-8")
        return {"status": "updated"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/search-matrix")
def delete_search_matrix() -> dict:
    """
    Delete the search matrix file.
    """
    try:
        if SEARCH_MATRIX_PATH.exists():
            SEARCH_MATRIX_PATH.unlink()
            return {"status": "deleted"}
        else:
            raise HTTPException(status_code=404, detail="Search matrix not found.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/scraper/{name}/pause")
def pause_scraper(name: str) -> dict:
    """
    Pause a scraper by setting its status to 'paused'.
    """
    set_scraper_status(name, "paused")
    return {"status": "paused", "scraper": name}

@app.post("/scraper/{name}/start")
def start_scraper(name: str) -> dict:
    """
    Start a scraper by setting its status to 'active'.
    """
    set_scraper_status(name, "active")
    return {"status": "active", "scraper": name}

@app.get("/scraper/{name}/status")
def scraper_status(name: str) -> dict:
    """
    Get the current status of a scraper.
    """
    status = get_scraper_status(name)
    return {"scraper": name, "status": status}