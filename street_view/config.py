import os
from dotenv import load_dotenv

load_dotenv()


class Config:

    GOOGLE_STREETVIEW_API_KEY = os.getenv("GOOGLE_STREETVIEW_API_KEY", "XXX")

    # Default image size (max 640x640 for free tier)
    DEFAULT_IMAGE_SIZE = "640x640"

    # Default field of view (10-120 degrees)
    DEFAULT_FOV = 120

    # Default pitch (-90 to 90 degrees)
    DEFAULT_PITCH = 0

    # Street View Static API endpoint
    STREETVIEW_STATIC_API_URL = "https://maps.googleapis.com/maps/api/streetview"

    # Street View Metadata API endpoint
    STREETVIEW_METADATA_API_URL = "https://maps.googleapis.com/maps/api/streetview/metadata"

    # Geocoding API endpoint for address validation
    GEOCODING_API_URL = "https://maps.googleapis.com/maps/api/geocode/json"
