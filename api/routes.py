from fastapi import FastAPI, Query
from scraper.scraper import scrape_by_zip

app = FastAPI()

@app.post("/scrape/{zip_code}")
async def scrape_leads(zip_code: str, headless: bool = Query(None, description="Override headless mode")):
    result = await scrape_by_zip(zip_code, headless=headless)
    return result