from fastapi import FastAPI
from scraper.scraper import scrape_by_zip

app = FastAPI()

@app.post("/scrape/{zip_code}")
async def scrape_leads(zip_code: str):
    result = scrape_by_zip(zip_code)
    return result