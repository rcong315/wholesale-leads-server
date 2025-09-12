from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from scraper.scraper import scrape_by_zip

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/scrape/{zip_code}")
async def scrape_leads(
    zip_code: str, 
    headless: bool = Query(None, description="Override headless mode"),
    use_cache: bool = Query(True, description="Use cached data if available")
):
    result = await scrape_by_zip(zip_code, headless=headless, use_cache=use_cache)
    return result