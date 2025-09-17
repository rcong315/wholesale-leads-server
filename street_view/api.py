import requests
import logging
import base64
from typing import Optional, Dict, Any, Union
from urllib.parse import urlencode

from street_view.config import Config

logger = logging.getLogger(__name__)


class StreetViewAPI:

    def __init__(self):
        self.config = Config()
        if not self.config.GOOGLE_STREETVIEW_API_KEY:
            logger.warning("GOOGLE_STREETVIEW_API_KEY not set in environment variables")

    def geocode_address(self, address: str) -> Optional[Dict[str, float]]:
        """
        Convert address to latitude/longitude coordinates using Google Geocoding API.

        Args:
            address: Street address to geocode

        Returns:
            Dictionary with 'lat' and 'lng' keys, or None if geocoding fails
        """
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

    def get_street_view_image_url(
        self,
        address: str = None,
        lat: float = None,
        lng: float = None,
        size: str = None,
        fov: int = None,
        pitch: int = None,
        heading: int = None,
    ) -> Optional[str]:
        """
        Generate Street View image URL for the given address or coordinates.

        Args:
            address: Street address (will be geocoded if lat/lng not provided)
            lat: Latitude coordinate
            lng: Longitude coordinate
            size: Image size in format "widthxheight" (max 640x640 for free tier)
            fov: Field of view in degrees (10-120)
            pitch: Camera pitch in degrees (-90 to 90)
            heading: Camera heading in degrees (0-360)

        Returns:
            Street View image URL string, or None if parameters are invalid
        """
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
        return url

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
    ) -> Optional[Union[bytes, str]]:
        """
        Download Street View image data for the given address or coordinates.

        Args:
            address: Street address (will be geocoded if lat/lng not provided)
            lat: Latitude coordinate
            lng: Longitude coordinate
            size: Image size in format "widthxheight" (max 640x640 for free tier)
            fov: Field of view in degrees (10-120)
            pitch: Camera pitch in degrees (-90 to 90)
            heading: Camera heading in degrees (0-360)
            return_base64: If True, return base64 encoded string instead of raw bytes

        Returns:
            Image data as bytes or base64 string, or None if request fails
        """
        url = self.get_street_view_image_url(
            address=address,
            lat=lat,
            lng=lng,
            size=size,
            fov=fov,
            pitch=pitch,
            heading=heading,
        )

        if not url:
            return None

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

    def get_house_image(
        self, address: str, size: str = "640x640", return_format: str = "url"
    ) -> Optional[Dict[str, Any]]:
        """
        Get a house image from Street View given an address.

        Args:
            address: Full street address of the house
            size: Image size in format "widthxheight" (max 640x640 for free tier)
            return_format: "url", "bytes", or "base64"

        Returns:
            Dictionary containing image data and metadata, or None if request fails
        """
        if not address or not address.strip():
            logger.error("Address cannot be empty")
            return None

        # First geocode the address to get coordinates
        coords = self.geocode_address(address.strip())
        if not coords:
            return None

        lat, lng = coords["lat"], coords["lng"]

        try:
            result = {"address": address.strip(), "coordinates": coords, "size": size}

            if return_format == "url":
                image_url = self.get_street_view_image_url(lat=lat, lng=lng, size=size)
                result["image_url"] = image_url

            elif return_format in ["bytes", "base64"]:
                image_data = self.get_street_view_image_data(
                    lat=lat,
                    lng=lng,
                    size=size,
                    return_base64=(return_format == "base64"),
                )
                if image_data:
                    result["image_data"] = image_data
                else:
                    logger.error("Failed to download image data")
                    return None
            else:
                logger.error(f"Invalid return_format: {return_format}")
                return None

            logger.info(f"Successfully generated house image for address: {address}")
            return result

        except Exception as e:
            logger.error(f"Error getting house image for address '{address}': {e}")
            return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Example usage
    api = StreetViewAPI()

    # Test with a sample address
    test_address = "312 Joann St, Costa Mesa, CA"
    result = api.get_house_image(test_address, return_format="bytes", size="640x640")

    if result:
        print(f"Success! Image URL: {result.get('image_url')}")
        print(f"Coordinates: {result.get('coordinates')}")
    else:
        print("Failed to get house image")

    import os

    if result and "image_data" in result:
        # Save image to root directory
        filename = "street_view_test_image.jpg"
        filepath = os.path.join(os.getcwd(), filename)

        try:
            with open(filepath, "wb") as f:
                f.write(result["image_data"])

            file_size = os.path.getsize(filepath)
            logger.info(f"✓ Image saved successfully to: {filepath}")
            logger.info(f"  File size: {file_size:,} bytes")
            logger.info(f"  Address: {result['address']}")
            logger.info(f"  Coordinates: {result['coordinates']}")

        except Exception as e:
            logger.error(f"✗ Failed to save image: {e}")
    else:
        logger.error("✗ Failed to download image data")
