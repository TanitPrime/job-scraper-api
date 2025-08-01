from typing import Optional
import sqlite3
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from scrapers.common.search_matrix import load_matrix
from pathlib import Path
from scrapers.common.scraper_control import scraper_control
app = FastAPI()

SEARCH_MATRIX_PATH = Path("scrapers/common/search_matrix.json")
DB_PATH = "scraper_control.db"

# Initialize the database if it doesn't exist
scraper_control.init(DB_PATH)

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

@app.post("/scraper/pause")
def pause_scraper(name: str) -> dict:
    """
    Pause a scraper by setting its status to 'paused'.
    """
    scraper_control.set_control_status(name, "paused")
    return {"status": "paused"}

@app.post("/scraper/start")
def start_scraper(name: str) -> dict:
    """
    Start a scraper by setting its status to 'active'.
    """
    scraper_control(name, "active")
    return {"status": "active", "scraper": name}

@app.get("/scraper/{name}/status")
def scraper_status(name: str) -> dict:
    """
    Get the current status of a scraper.
    """
    status = scraper_control.set_scraper_status(name)
    return {"scraper": name, "status": status}