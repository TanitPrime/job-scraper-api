from typing import Optional
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, field_validator
from scrapers.common.search_matrix import load_matrix
from pathlib import Path
from scrapers.common.scraper_control import ScraperControl
app = FastAPI()

SEARCH_MATRIX_PATH = Path("scrapers/common/search_matrix.json")
DB_PATH = "scraper_control.db"

# Initialize the database if it doesn't exist
scraper_control = ScraperControl(DB_PATH)

from typing import Dict, List, Literal

class CategoryLanguageModel(BaseModel):
    en: List[str]
    fr: List[str]

class CategoryKeywordsModel(BaseModel):
    root: Dict[str, CategoryLanguageModel]

class SearchMatrixModel(BaseModel):
    CATEGORY_KEYWORDS: CategoryKeywordsModel
    LOCATIONS: List[str]

    @field_validator('CATEGORY_KEYWORDS')
    def validate_categories(cls, v):
        """Validate that all required categories are present with both language versions"""
        required_categories = {'dev', 'design', 'languages'}
        if not all(cat in v.root for cat in required_categories):
            raise ValueError(f"Missing required categories. Must include: {required_categories}")
        return v

    @field_validator('LOCATIONS')
    def validate_locations(cls, v):
        """Validate that locations list is not empty"""
        if not v:
            raise ValueError("Locations list cannot be empty")
        return v

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

@app.get("/service/status")
def get_service_status() -> dict:
    """
    Get the overall service status and active scrapers count.
    """
    return scraper_control.get_service_status()

@app.post("/service/pause")
def pause_service() -> dict:
    """
    Pause the entire scraping service.
    """
    scraper_control.set_service_status("paused")
    return {"status": "paused"}

@app.post("/service/start")
def start_service() -> dict:
    """
    Start/resume the entire scraping service.
    """
    scraper_control.set_service_status("active")
    return {"status": "active"}

@app.get("/scrapers")
def list_scrapers() -> List[dict]:
    """
    Get status information for all scrapers.
    """
    return scraper_control.get_all_scrapers_status()

@app.post("/scraper/{name}/pause")
def pause_scraper(name: str) -> dict:
    """
    Pause a specific scraper.
    """
    scraper_control.set_scraper_status(name, "paused")
    return scraper_control.get_scraper_status(name)

@app.post("/scraper/{name}/start")
def start_scraper(name: str) -> dict:
    """
    Start a specific scraper.
    """
    scraper_control.set_scraper_status(name, "running")
    return scraper_control.get_scraper_status(name)

@app.get("/scraper/{name}/status")
def get_scraper_status(name: str) -> dict:
    """
    Get detailed status information for a specific scraper.
    """
    return scraper_control.get_scraper_status(name)