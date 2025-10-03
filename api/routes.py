from fastapi import FastAPI, Query, BackgroundTasks, HTTPException, Body, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from db.database import Database
from street_view.api import StreetViewAPI
from typing import Optional, Dict
import logging

logger = logging.getLogger(__name__)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=[
        "X-Image-Date",
        "X-Address",
        "X-Coordinates",
        "X-Image-Size",
        "X-Pitch",
        "X-FOV",
        "X-Heading",
    ],
)


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


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

        # Get metadata including image date
        metadata = street_view_api.get_street_view_metadata(lat, lng, heading)

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

        if metadata and metadata.get("date"):
            headers["X-Image-Date"] = metadata["date"]

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


@app.get("/locations")
async def get_all_locations():
    """Get all cached locations"""
    db = Database()
    locations = db.get_locations()
    return {"locations": locations, "count": len(locations)}


@app.get("/filter-options")
async def get_filter_options():
    """Get distinct values for filter dropdowns"""
    db = Database()
    try:
        options = db.get_filter_options()
        return options
    except Exception as e:
        logger.error(f"Error fetching filter options: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/leads")
async def get_leads(
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    body: Optional[Dict] = Body(None),
):
    """Get paginated leads with optional filters and sorting"""
    db = Database()

    try:
        filters = body.get("filters") if body else None
        sort_by_param = body.get("sortBy", "") if body else ""

        # Parse sortBy parameter (e.g., "value_desc" -> column="est_value", order="desc")
        sort_column = "id"
        sort_order = "asc"

        if sort_by_param:
            # Map frontend sort values to database columns
            sort_mapping = {
                "value": "est_value",
                "city": "city",
                "last_sale_date": "last_sale_date",
                "last_sale_amount": "last_sale_amount",
                "loan_balance": "total_loan_balance",
                "interest_rate": "loan_interest_rate",
            }

            # Split the sort parameter (e.g., "value_desc" -> ["value", "desc"])
            parts = sort_by_param.rsplit("_", 1)
            if len(parts) == 2:
                field, order = parts
                if field in sort_mapping and order in ["asc", "desc"]:
                    sort_column = sort_mapping[field]
                    sort_order = order

        result = db.get_leads_paginated(
            offset=offset,
            limit=limit,
            filters=filters,
            sort_by=sort_column,
            sort_order=sort_order,
        )
        return result
    except Exception as e:
        logger.error(f"Error fetching paginated leads: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/leads/{lead_id}")
async def update_lead(
    lead_id: int = Path(..., description="Lead ID to update"),
    updates: Dict = Body(..., description="Fields to update"),
):
    """Update a lead by ID and automatically mark as favorite"""
    db = Database()

    try:
        # Verify lead exists
        lead = db.get_lead_by_id(lead_id)
        if not lead:
            raise HTTPException(
                status_code=404, detail=f"Lead with ID {lead_id} not found"
            )

        # Update the lead
        success = db.update_lead(lead_id, updates)
        if success:
            updated_lead = db.get_lead_by_id(lead_id)
            return {"success": True, "lead": updated_lead}
        else:
            raise HTTPException(status_code=500, detail="Failed to update lead")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating lead {lead_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/leads/{lead_id}/favorite")
async def toggle_favorite(
    lead_id: int = Path(..., description="Lead ID to favorite/unfavorite"),
    is_favorite: bool = Body(..., description="Favorite status"),
):
    """Toggle favorite status for a lead"""
    db = Database()

    try:
        # Verify lead exists
        lead = db.get_lead_by_id(lead_id)
        if not lead:
            raise HTTPException(
                status_code=404, detail=f"Lead with ID {lead_id} not found"
            )

        # Update only the favorite status
        success = db.toggle_favorite(lead_id, is_favorite)
        if success:
            updated_lead = db.get_lead_by_id(lead_id)
            return {"success": True, "lead": updated_lead}
        else:
            raise HTTPException(status_code=500, detail="Failed to toggle favorite")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error toggling favorite for lead {lead_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Global dict to track scraping progress
# scraping_status = {}


# @app.get("/status/{location}")
# async def check_location_status(location: str):
#     db = Database()

#     # Check if location exists in database
#     cached = db.location_exists(location)

#     # Check if currently being scraped
#     is_scraping = (
#         location in scraping_status
#         and scraping_status[location]["status"] == "in_progress"
#     )

#     return {
#         "location": location,
#         "cached": cached,
#         "is_scraping": is_scraping,
#         "scraping_progress": (
#             scraping_status.get(location, {}).get("message", "") if is_scraping else ""
#         ),
#     }


# @app.get("/progress/{location}")
# async def get_scraping_progress(location: str):
#     if location in scraping_status:
#         return scraping_status[location]
#     else:
#         return {
#             "status": "not_found",
#             "message": "No scraping job found for this location",
#         }


# @app.post("/scrape/{location}")
# async def scrape_leads(
#     location: str,
#     background_tasks: BackgroundTasks,
#     headless: bool = Query(None, description="Override headless mode"),
#     use_cache: bool = Query(True, description="Use cached data if available"),
# ):
#     # Check if already being scraped
#     if (
#         location in scraping_status
#         and scraping_status[location]["status"] == "in_progress"
#     ):
#         return {
#             "error": "Scraping already in progress for this location",
#             "status": "in_progress",
#         }

#     # Check cache first
#     if use_cache:
#         db = Database()
#         cached_data = db.get_leads(location)
#         if cached_data:
#             return cached_data

#     # Start background scraping task
#     background_tasks.add_task(background_scrape, location, headless, use_cache)

#     # Initialize status
#     scraping_status[location] = {
#         "status": "in_progress",
#         "message": "Starting scrape...",
#         "progress": 0,
#     }

#     return {
#         "status": "started",
#         "message": "Scraping started. Check /progress/{location} for updates or /status/{location} for completion status.",
#         "location": location,
#     }


# async def background_scrape(location: str, headless=None, use_cache=True):
#     def progress_callback(message):
#         if location in scraping_status:
#             scraping_status[location]["message"] = message
#             logger.info(f"Progress for {location}: {message}")

#     try:
#         result = await scrape(
#             location,
#             headless=headless,
#             use_cache=use_cache,
#             progress_callback=progress_callback,
#         )

#         if "error" in result:
#             scraping_status[location] = {
#                 "status": "error",
#                 "message": result["error"],
#                 "progress": 0,
#             }
#         else:
#             scraping_status[location] = {
#                 "status": "completed",
#                 "message": f"Found {result['total_leads']} leads",
#                 "progress": 100,
#                 "result": result,
#             }

#     except Exception as e:
#         logger.error(f"Background scrape error for {location}: {e}")
#         scraping_status[location] = {
#             "status": "error",
#             "message": str(e),
#             "progress": 0,
#         }
