from fastapi import FastAPI, Query, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from scraper.scraper import scrape
from google_drive.api import GoogleDriveAPI
from street_view.api import StreetViewAPI
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
    is_scraping = (
        zip_code in scraping_status
        and scraping_status[zip_code]["status"] == "in_progress"
    )

    return {
        "zip_code": zip_code,
        "cached": json_exists,
        "csv_available": csv_exists,
        "is_scraping": is_scraping,
        "scraping_progress": (
            scraping_status.get(zip_code, {}).get("message", "") if is_scraping else ""
        ),
    }


@app.get("/progress/{zip_code}")
async def get_scraping_progress(zip_code: str):
    if zip_code in scraping_status:
        return scraping_status[zip_code]
    else:
        return {
            "status": "not_found",
            "message": "No scraping job found for this zip code",
        }


@app.post("/scrape/{zip_code}")
async def scrape_leads(
    zip_code: str,
    background_tasks: BackgroundTasks,
    headless: bool = Query(None, description="Override headless mode"),
    use_cache: bool = Query(True, description="Use cached data if available"),
):
    # Check if already being scraped
    if (
        zip_code in scraping_status
        and scraping_status[zip_code]["status"] == "in_progress"
    ):
        return {
            "error": "Scraping already in progress for this zip code",
            "status": "in_progress",
        }

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
        "progress": 0,
    }

    return {
        "status": "started",
        "message": "Scraping started. Check /progress/{zip_code} for updates or /status/{zip_code} for completion status.",
        "zip_code": zip_code,
    }


async def background_scrape(zip_code: str, headless=None, use_cache=True):
    def progress_callback(message):
        if zip_code in scraping_status:
            scraping_status[zip_code]["message"] = message
            logger.info(f"Progress for {zip_code}: {message}")

    try:
        result = await scrape(
            zip_code,
            headless=headless,
            use_cache=use_cache,
            progress_callback=progress_callback,
        )

        if "error" in result:
            scraping_status[zip_code] = {
                "status": "error",
                "message": result["error"],
                "progress": 0,
            }
        else:
            scraping_status[zip_code] = {
                "status": "completed",
                "message": f"Found {result['total_leads']} leads",
                "progress": 100,
                "result": result,
            }

    except Exception as e:
        logger.error(f"Background scrape error for {zip_code}: {e}")
        scraping_status[zip_code] = {
            "status": "error",
            "message": str(e),
            "progress": 0,
        }


@app.get("/street-view/image")
async def get_street_view_image_bytes(
    address: str = Query(..., description="Street address of the house"),
    size: str = Query("640x640", description="Image size (e.g., '640x640')"),
    heading: int = Query(
        None, description="Camera heading in degrees (0-360, optional)"
    ),
    pitch: int = Query(0, description="Camera pitch in degrees (-90 to 90)"),
    fov: int = Query(90, description="Field of view in degrees (10-120)"),
):
    try:
        street_view_api = StreetViewAPI()

        # First geocode the address to get coordinates
        coords = street_view_api.geocode_address(address.strip())
        if not coords:
            raise HTTPException(
                status_code=404, detail=f"Could not geocode address: {address}"
            )

        lat, lng = coords["lat"], coords["lng"]

        # Get the image data
        image_data = street_view_api.get_street_view_image_data(
            lat=lat,
            lng=lng,
            size=size,
            heading=heading,
            pitch=pitch,
            fov=fov,
            return_base64=False,
        )

        if not image_data:
            raise HTTPException(
                status_code=404,
                detail="Could not fetch Street View image for the given address",
            )

        # Return raw image bytes with metadata in headers
        headers = {
            "X-Address": address.strip(),
            "X-Coordinates": f"{lat},{lng}",
            "X-Image-Size": size,
            "X-Pitch": str(pitch),
            "X-FOV": str(fov),
            "Content-Disposition": f'inline; filename="streetview_{address.replace(" ", "_").replace(",", "")}.jpg"',
        }

        if heading is not None:
            headers["X-Heading"] = str(heading)

        return Response(content=image_data, media_type="image/jpeg", headers=headers)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Error getting street view image bytes for address '{address}': {e}"
        )
        raise HTTPException(
            status_code=500,
            detail="Internal server error while fetching street view image",
        )
