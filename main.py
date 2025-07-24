from fastapi import FastAPI
app = FastAPI()

@app.get("/health")
def health():
    # Todo
    # Check api health
    return None

@app.get("/")
def root():
    return {"status": "linkedin-scraper running"}