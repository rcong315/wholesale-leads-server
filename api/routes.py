from fastapi import FastAPI, Query, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel
from typing import List, Optional
from scraper.scraper import scrape
from google_drive.api import GoogleDriveAPI
from street_view.api import StreetViewAPI
import logging
import asyncio
import zipfile
import io
import json

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


# Pydantic models for request/response
class BatchStreetViewRequest(BaseModel):
    addresses: List[str]
    size: Optional[str] = "640x640"
    heading: Optional[int] = None
    pitch: Optional[int] = 0
    fov: Optional[int] = 90
    return_format: Optional[str] = "zip"  # "zip" or "json"


class StreetViewImageResult(BaseModel):
    address: str
    success: bool
    coordinates: Optional[dict] = None
    error_message: Optional[str] = None
    image_filename: Optional[str] = None
    image_size_bytes: Optional[int] = None


class BatchStreetViewResponse(BaseModel):
    total_requested: int
    successful: int
    failed: int
    results: List[StreetViewImageResult]


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


@app.get("/street-view")
async def get_street_view_image(
    address: str = Query(..., description="Street address of the house"),
    size: str = Query("640x640", description="Image size (e.g., '640x640')"),
    format: str = Query("url", description="Return format: 'url', 'base64', or 'bytes'")
):
    """
    Get a Street View image of a house given its address.

    Args:
        address: Full street address
        size: Image dimensions in format 'widthxheight' (max 640x640 for free tier)
        format: Return format - 'url' for image URL, 'base64' for base64 encoded data, 'bytes' for raw image data

    Returns:
        Street View image data and metadata
    """
    try:
        street_view_api = StreetViewAPI()

        if format not in ["url", "base64", "bytes"]:
            raise HTTPException(status_code=400, detail="Format must be 'url', 'base64', or 'bytes'")

        result = street_view_api.get_house_image(
            address=address,
            size=size,
            return_format=format
        )

        if not result:
            raise HTTPException(status_code=404, detail="Could not find Street View image for the given address")

        if format == "bytes" and "image_data" in result:
            # Return raw image data as response
            return Response(
                content=result["image_data"],
                media_type="image/jpeg",
                headers={
                    "X-Address": result["address"],
                    "X-Coordinates": f"{result['coordinates']['lat']},{result['coordinates']['lng']}"
                }
            )

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting street view image for address '{address}': {e}")
        raise HTTPException(status_code=500, detail="Internal server error while fetching street view image")


@app.get("/street-view/image")
async def get_street_view_image_bytes(
    address: str = Query(..., description="Street address of the house"),
    size: str = Query("640x640", description="Image size (e.g., '640x640')"),
    heading: int = Query(None, description="Camera heading in degrees (0-360, optional)"),
    pitch: int = Query(0, description="Camera pitch in degrees (-90 to 90)"),
    fov: int = Query(90, description="Field of view in degrees (10-120)")
):
    """
    Get a Street View image as raw bytes for a given address.

    This endpoint always returns the image as JPEG bytes, suitable for direct display or saving.

    Args:
        address: Full street address of the house
        size: Image dimensions in format 'widthxheight' (max 640x640 for free tier)
        heading: Camera heading direction in degrees (optional, auto if not specified)
        pitch: Camera pitch angle in degrees (-90 to 90)
        fov: Field of view in degrees (10-120)

    Returns:
        Raw JPEG image bytes with address and coordinates in headers
    """
    try:
        street_view_api = StreetViewAPI()

        # First geocode the address to get coordinates
        coords = street_view_api.geocode_address(address.strip())
        if not coords:
            raise HTTPException(status_code=404, detail=f"Could not geocode address: {address}")

        lat, lng = coords["lat"], coords["lng"]

        # Get the image data
        image_data = street_view_api.get_street_view_image_data(
            lat=lat,
            lng=lng,
            size=size,
            heading=heading,
            pitch=pitch,
            fov=fov,
            return_base64=False
        )

        if not image_data:
            raise HTTPException(status_code=404, detail="Could not fetch Street View image for the given address")

        # Return raw image bytes with metadata in headers
        headers = {
            "X-Address": address.strip(),
            "X-Coordinates": f"{lat},{lng}",
            "X-Image-Size": size,
            "X-Pitch": str(pitch),
            "X-FOV": str(fov),
            "Content-Disposition": f'inline; filename="streetview_{address.replace(" ", "_").replace(",", "")}.jpg"'
        }

        if heading is not None:
            headers["X-Heading"] = str(heading)

        return Response(
            content=image_data,
            media_type="image/jpeg",
            headers=headers
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting street view image bytes for address '{address}': {e}")
        raise HTTPException(status_code=500, detail="Internal server error while fetching street view image")


async def process_single_address(street_view_api: StreetViewAPI, address: str, size: str, heading: Optional[int], pitch: int, fov: int) -> tuple:
    """
    Process a single address and return image data and metadata.

    Returns:
        tuple: (success: bool, result_data: dict, image_data: bytes or None)
    """
    try:
        # Geocode the address
        coords = street_view_api.geocode_address(address.strip())
        if not coords:
            return False, {
                "address": address.strip(),
                "success": False,
                "error_message": "Could not geocode address"
            }, None

        lat, lng = coords["lat"], coords["lng"]

        # Get the image data
        image_data = street_view_api.get_street_view_image_data(
            lat=lat,
            lng=lng,
            size=size,
            heading=heading,
            pitch=pitch,
            fov=fov,
            return_base64=False
        )

        if not image_data:
            return False, {
                "address": address.strip(),
                "success": False,
                "error_message": "Could not fetch Street View image data"
            }, None

        # Generate filename
        safe_address = address.strip().replace(" ", "_").replace(",", "").replace("/", "_")
        filename = f"streetview_{safe_address}.jpg"

        return True, {
            "address": address.strip(),
            "success": True,
            "coordinates": coords,
            "image_filename": filename,
            "image_size_bytes": len(image_data)
        }, image_data

    except Exception as e:
        logger.error(f"Error processing address '{address}': {e}")
        return False, {
            "address": address.strip(),
            "success": False,
            "error_message": str(e)
        }, None


@app.post("/street-view/batch")
async def get_batch_street_view_images(request: BatchStreetViewRequest):
    """
    Get Street View images for multiple addresses.

    Args:
        request: BatchStreetViewRequest containing addresses and image parameters

    Returns:
        ZIP file containing images and metadata, or JSON response with results
    """
    try:
        if not request.addresses:
            raise HTTPException(status_code=400, detail="No addresses provided")

        if len(request.addresses) > 50:  # Limit to prevent abuse
            raise HTTPException(status_code=400, detail="Maximum 50 addresses allowed per request")

        street_view_api = StreetViewAPI()
        results = []
        images_data = {}

        logger.info(f"Processing batch request for {len(request.addresses)} addresses")

        # Process addresses concurrently (but with some limit to avoid rate limiting)
        semaphore = asyncio.Semaphore(5)  # Limit concurrent requests

        async def process_with_semaphore(address):
            async with semaphore:
                return await asyncio.to_thread(
                    process_single_address,
                    street_view_api,
                    address,
                    request.size,
                    request.heading,
                    request.pitch,
                    request.fov
                )

        # Process all addresses
        tasks = [process_with_semaphore(address) for address in request.addresses]
        address_results = await asyncio.gather(*tasks, return_exceptions=True)

        successful = 0
        failed = 0

        for i, result in enumerate(address_results):
            if isinstance(result, Exception):
                logger.error(f"Exception processing address {request.addresses[i]}: {result}")
                results.append(StreetViewImageResult(
                    address=request.addresses[i],
                    success=False,
                    error_message=str(result)
                ))
                failed += 1
            else:
                success, result_data, image_data = result
                results.append(StreetViewImageResult(**result_data))

                if success and image_data:
                    images_data[result_data["image_filename"]] = image_data
                    successful += 1
                else:
                    failed += 1

        # Return format based on request
        if request.return_format == "json":
            # Return JSON response with metadata
            return BatchStreetViewResponse(
                total_requested=len(request.addresses),
                successful=successful,
                failed=failed,
                results=results
            )

        else:  # return_format == "zip" (default)
            # Create ZIP file with images and metadata
            if not images_data:
                raise HTTPException(status_code=404, detail="No images could be retrieved for any of the addresses")

            zip_buffer = io.BytesIO()

            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                # Add images to zip
                for filename, image_data in images_data.items():
                    zip_file.writestr(filename, image_data)

                # Add metadata JSON
                metadata = {
                    "total_requested": len(request.addresses),
                    "successful": successful,
                    "failed": failed,
                    "request_parameters": {
                        "size": request.size,
                        "heading": request.heading,
                        "pitch": request.pitch,
                        "fov": request.fov
                    },
                    "results": [result.dict() for result in results]
                }
                zip_file.writestr("metadata.json", json.dumps(metadata, indent=2))

            zip_buffer.seek(0)

            return Response(
                content=zip_buffer.getvalue(),
                media_type="application/zip",
                headers={
                    "Content-Disposition": f"attachment; filename=street_view_images_{successful}_of_{len(request.addresses)}.zip",
                    "X-Total-Requested": str(len(request.addresses)),
                    "X-Successful": str(successful),
                    "X-Failed": str(failed)
                }
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing batch street view request: {e}")
        raise HTTPException(status_code=500, detail="Internal server error while processing batch request")


@app.get("/street-view/coordinates")
async def get_street_view_by_coordinates(
    lat: float = Query(..., description="Latitude coordinate"),
    lng: float = Query(..., description="Longitude coordinate"),
    size: str = Query("640x640", description="Image size (e.g., '640x640')"),
    heading: int = Query(None, description="Camera heading in degrees (0-360)"),
    pitch: int = Query(0, description="Camera pitch in degrees (-90 to 90)"),
    fov: int = Query(90, description="Field of view in degrees (10-120)"),
    format: str = Query("url", description="Return format: 'url', 'base64', or 'bytes'")
):
    """
    Get a Street View image using latitude/longitude coordinates.

    Args:
        lat: Latitude coordinate
        lng: Longitude coordinate
        size: Image dimensions in format 'widthxheight' (max 640x640 for free tier)
        heading: Camera heading direction in degrees (optional)
        pitch: Camera pitch angle in degrees
        fov: Field of view in degrees
        format: Return format - 'url' for image URL, 'base64' for base64 encoded data, 'bytes' for raw image data

    Returns:
        Street View image data and metadata
    """
    try:
        street_view_api = StreetViewAPI()

        if format not in ["url", "base64", "bytes"]:
            raise HTTPException(status_code=400, detail="Format must be 'url', 'base64', or 'bytes'")

        if format == "url":
            image_url = street_view_api.get_street_view_image_url(
                lat=lat,
                lng=lng,
                size=size,
                heading=heading,
                pitch=pitch,
                fov=fov
            )

            if not image_url:
                raise HTTPException(status_code=404, detail="Could not generate Street View image URL")

            return {
                "coordinates": {"lat": lat, "lng": lng},
                "size": size,
                "heading": heading,
                "pitch": pitch,
                "fov": fov,
                "image_url": image_url
            }

        else:
            image_data = street_view_api.get_street_view_image_data(
                lat=lat,
                lng=lng,
                size=size,
                heading=heading,
                pitch=pitch,
                fov=fov,
                return_base64=(format == "base64")
            )

            if not image_data:
                raise HTTPException(status_code=404, detail="Could not fetch Street View image data")

            if format == "bytes":
                return Response(
                    content=image_data,
                    media_type="image/jpeg",
                    headers={
                        "X-Coordinates": f"{lat},{lng}",
                        "X-Heading": str(heading) if heading is not None else "",
                        "X-Pitch": str(pitch),
                        "X-FOV": str(fov)
                    }
                )

            return {
                "coordinates": {"lat": lat, "lng": lng},
                "size": size,
                "heading": heading,
                "pitch": pitch,
                "fov": fov,
                "image_data": image_data
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting street view image for coordinates ({lat}, {lng}): {e}")
        raise HTTPException(status_code=500, detail="Internal server error while fetching street view image")
