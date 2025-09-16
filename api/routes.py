from fastapi import FastAPI, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from scraper.scraper import scrape
from google_drive.api import GoogleDriveAPI
import asyncio
import json
import logging

logger = logging.getLogger(__name__)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global dict to track scraping progress
scraping_status = {}


@app.get("/status/{zip_code}")
async def check_zip_code_status(zip_code: str):
    drive_api = GoogleDriveAPI()

    # Check if file exists in cache
    json_exists = drive_api.file_exists(zip_code, "json")
    csv_exists = drive_api.file_exists(zip_code, "csv")

    # Check if currently being scraped
    is_scraping = zip_code in scraping_status and scraping_status[zip_code]["status"] == "in_progress"

    return {
        "zip_code": zip_code,
        "cached": json_exists,
        "csv_available": csv_exists,
        "is_scraping": is_scraping,
        "scraping_progress": scraping_status.get(zip_code, {}).get("message", "") if is_scraping else ""
    }


@app.get("/progress/{zip_code}")
async def get_scraping_progress(zip_code: str):
    if zip_code in scraping_status:
        return scraping_status[zip_code]
    else:
        return {"status": "not_found", "message": "No scraping job found for this zip code"}


@app.post("/scrape/{zip_code}")
async def scrape_leads(
    zip_code: str,
    background_tasks: BackgroundTasks,
    headless: bool = Query(None, description="Override headless mode"),
    use_cache: bool = Query(True, description="Use cached data if available"),
):
    # Check if already being scraped
    if zip_code in scraping_status and scraping_status[zip_code]["status"] == "in_progress":
        return {"error": "Scraping already in progress for this zip code", "status": "in_progress"}

    # Check cache first
    if use_cache:
        drive_api = GoogleDriveAPI()
        cached_data = drive_api.load_cache(zip_code)
        if cached_data:
            return cached_data

    # Start background scraping task
    background_tasks.add_task(background_scrape, zip_code, headless, use_cache)

    # Initialize status
    scraping_status[zip_code] = {
        "status": "in_progress",
        "message": "Starting scrape...",
        "progress": 0
    }

    return {
        "status": "started",
        "message": "Scraping started. Check /progress/{zip_code} for updates or /status/{zip_code} for completion status.",
        "zip_code": zip_code
    }


async def background_scrape(zip_code: str, headless=None, use_cache=True):
    def progress_callback(message):
        if zip_code in scraping_status:
            scraping_status[zip_code]["message"] = message
            logger.info(f"Progress for {zip_code}: {message}")

    try:
        result = await scrape(zip_code, headless=headless, use_cache=use_cache, progress_callback=progress_callback)

        if "error" in result:
            scraping_status[zip_code] = {
                "status": "error",
                "message": result["error"],
                "progress": 0
            }
        else:
            scraping_status[zip_code] = {
                "status": "completed",
                "message": f"Found {result['total_leads']} leads",
                "progress": 100,
                "result": result
            }

    except Exception as e:
        logger.error(f"Background scrape error for {zip_code}: {e}")
        scraping_status[zip_code] = {
            "status": "error",
            "message": str(e),
            "progress": 0
        }
