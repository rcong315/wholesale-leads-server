#!/usr/bin/env python3
"""
Test script for Google Street View API.
Downloads a Street View image and saves it to the root directory.
"""

import logging
import os
from street_view.api import StreetViewAPI

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def test_street_view_api():
    """Test the Street View API by downloading an image and saving it locally."""

    # Test address - Google headquarters
    test_address = "1600 Amphitheatre Parkway, Mountain View, CA"

    logger.info(f"Testing Street View API with address: {test_address}")

    # Initialize the API
    api = StreetViewAPI()

    # Test 1: Get image URL
    logger.info("Test 1: Getting image URL...")
    result_url = api.get_house_image(test_address, return_format="url")

    if result_url:
        logger.info(f"✓ Image URL generated successfully: {result_url['image_url']}")
        logger.info(f"  Coordinates: {result_url['coordinates']}")
    else:
        logger.error("✗ Failed to generate image URL")
        return False

    # Test 2: Download image data and save to file
    logger.info("Test 2: Downloading image data...")
    result_bytes = api.get_house_image(test_address, return_format="bytes", size="640x640")

    if result_bytes and 'image_data' in result_bytes:
        # Save image to root directory
        filename = "street_view_test_image.jpg"
        filepath = os.path.join(os.getcwd(), filename)

        try:
            with open(filepath, 'wb') as f:
                f.write(result_bytes['image_data'])

            file_size = os.path.getsize(filepath)
            logger.info(f"✓ Image saved successfully to: {filepath}")
            logger.info(f"  File size: {file_size:,} bytes")
            logger.info(f"  Address: {result_bytes['address']}")
            logger.info(f"  Coordinates: {result_bytes['coordinates']}")

        except Exception as e:
            logger.error(f"✗ Failed to save image: {e}")
            return False
    else:
        logger.error("✗ Failed to download image data")
        return False

    # Test 3: Test with coordinates directly
    logger.info("Test 3: Testing with coordinates...")
    lat, lng = 37.4219983, -122.084

    coord_result = api.get_street_view_image_data(
        lat=lat,
        lng=lng,
        size="400x400",
        heading=45,
        pitch=10,
        return_base64=False
    )

    if coord_result:
        coord_filename = "street_view_coordinates_test.jpg"
        coord_filepath = os.path.join(os.getcwd(), coord_filename)

        try:
            with open(coord_filepath, 'wb') as f:
                f.write(coord_result)

            file_size = os.path.getsize(coord_filepath)
            logger.info(f"✓ Coordinate-based image saved to: {coord_filepath}")
            logger.info(f"  File size: {file_size:,} bytes")
            logger.info(f"  Coordinates: ({lat}, {lng})")

        except Exception as e:
            logger.error(f"✗ Failed to save coordinate-based image: {e}")
            return False
    else:
        logger.error("✗ Failed to download coordinate-based image")
        return False

    logger.info("🎉 All tests completed successfully!")
    return True


def test_error_cases():
    """Test error handling with invalid inputs."""
    logger.info("Testing error cases...")

    api = StreetViewAPI()

    # Test with invalid address
    result = api.get_house_image("Invalid Address That Doesn't Exist 12345")
    if result is None:
        logger.info("✓ Invalid address properly handled")
    else:
        logger.warning("⚠ Invalid address test didn't fail as expected")

    # Test with empty address
    result = api.get_house_image("")
    if result is None:
        logger.info("✓ Empty address properly handled")
    else:
        logger.warning("⚠ Empty address test didn't fail as expected")


if __name__ == "__main__":
    logger.info("Starting Google Street View API tests...")

    try:
        success = test_street_view_api()
        test_error_cases()

        if success:
            logger.info("🎉 Street View API test completed successfully!")
            logger.info("Check the root directory for saved test images.")
        else:
            logger.error("❌ Street View API test failed!")

    except Exception as e:
        logger.error(f"❌ Test script failed with error: {e}")
        raise