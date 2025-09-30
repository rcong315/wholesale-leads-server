import requests
import logging
import base64
from typing import Optional
from urllib.parse import urlencode

from street_view.config import Config

logger = logging.getLogger(__name__)


class StreetViewAPI:

    def __init__(self):
        self.config = Config()
        if not self.config.GOOGLE_STREETVIEW_API_KEY:
            logger.warning("GOOGLE_STREETVIEW_API_KEY not set in environment variables")

    def geocode_address(self, address: str) -> Optional[dict[str, float]]:
        if not self.config.GOOGLE_STREETVIEW_API_KEY:
            logger.error("Google API key not configured")
            return None

        try:
            params = {"address": address, "key": self.config.GOOGLE_STREETVIEW_API_KEY}

            url = f"{self.config.GEOCODING_API_URL}?{urlencode(params)}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            data = response.json()

            if data.get("status") == "OK" and data.get("results"):
                location = data["results"][0]["geometry"]["location"]
                return {"lat": location["lat"], "lng": location["lng"]}
            else:
                logger.warning(
                    f"Geocoding failed for address '{address}': {data.get('status')}"
                )
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Network error during geocoding: {e}")
            return None
        except Exception as e:
            logger.error(f"Error geocoding address '{address}': {e}")
            return None

    def get_street_view_metadata(
        self, lat: float, lng: float, heading: int = None
    ) -> Optional[dict]:
        """Get Street View metadata including image date"""
        if not self.config.GOOGLE_STREETVIEW_API_KEY:
            logger.error("Google API key not configured")
            return None

        try:
            params = {
                "location": f"{lat},{lng}",
                "key": self.config.GOOGLE_STREETVIEW_API_KEY,
            }

            if heading is not None:
                params["heading"] = heading

            url = f"{self.config.STREETVIEW_METADATA_API_URL}?{urlencode(params)}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            data = response.json()

            if data.get("status") == "OK":
                return {
                    "date": data.get("date"),
                    "pano_id": data.get("pano_id"),
                    "location": data.get("location"),
                }
            else:
                logger.warning(f"Metadata request failed: {data.get('status')}")
                return None

        except Exception as e:
            logger.error(f"Error fetching Street View metadata: {e}")
            return None

    def get_street_view_image_data(
        self,
        address: str = None,
        lat: float = None,
        lng: float = None,
        size: str = None,
        fov: int = None,
        pitch: int = None,
        heading: int = None,
        return_base64: bool = False,
    ) -> Optional[bytes | str]:
        if not self.config.GOOGLE_STREETVIEW_API_KEY:
            logger.error("Google API key not configured")
            return None

        # If coordinates not provided, geocode the address
        if lat is None or lng is None:
            if not address:
                logger.error("Either address or lat/lng coordinates must be provided")
                return None

            coords = self.geocode_address(address)
            if not coords:
                logger.error(f"Could not geocode address: {address}")
                return None
            lat, lng = coords["lat"], coords["lng"]

        # Use default values if not specified
        size = size or self.config.DEFAULT_IMAGE_SIZE
        fov = fov or self.config.DEFAULT_FOV
        pitch = pitch or self.config.DEFAULT_PITCH

        # Build parameters
        params = {
            "location": f"{lat},{lng}",
            "size": size,
            "fov": fov,
            "pitch": pitch,
            "key": self.config.GOOGLE_STREETVIEW_API_KEY,
        }

        # Add heading if specified
        if heading is not None:
            params["heading"] = heading

        # Generate URL
        url = f"{self.config.STREETVIEW_STATIC_API_URL}?{urlencode(params)}"

        logger.info(f"Generated Street View URL for coordinates ({lat}, {lng})")

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            # Check if we got a valid image (Google returns error images for invalid locations)
            content_type = response.headers.get("content-type", "")
            if not content_type.startswith("image/"):
                logger.warning("Street View API returned non-image content")
                return None

            image_data = response.content

            if return_base64:
                return base64.b64encode(image_data).decode("utf-8")
            else:
                return image_data

        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading Street View image: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error downloading Street View image: {e}")
            return None
