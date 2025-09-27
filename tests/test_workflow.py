#!/usr/bin/env python3
"""
Simple test to verify the key components of the wholesale leads system work together.
This test checks imports and basic functionality without requiring external services.
"""

import sys
import os

# Add the server directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'server'))

def test_imports():
    """Test that all main modules can be imported"""
    print("Testing imports...")

    try:
        # Test Google Drive API module
        print("  - Testing Google Drive API import...")
        from google_drive.api import GoogleDriveAPI
        print("    ✓ GoogleDriveAPI imported successfully")

        # Test API routes
        print("  - Testing API routes import...")
        from api.routes import app
        print("    ✓ FastAPI routes imported successfully")

        # Test scraper module
        print("  - Testing scraper import...")
        from scraper.scraper import scrape, BatchLeadsScraper
        print("    ✓ Scraper modules imported successfully")

        # Test config
        print("  - Testing config import...")
        from scraper.config import Config
        print("    ✓ Config imported successfully")

        print("✓ All imports successful!")
        return True

    except ImportError as e:
        print(f"✗ Import error: {e}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False

def test_google_drive_utilities():
    """Test Google Drive utility functions that don't require authentication"""
    print("\nTesting Google Drive utilities...")

    try:
        from google_drive.api import GoogleDriveAPI

        # Test CSV conversion function
        test_data = [
            {"name": "John Doe", "address": "123 Main St", "city": "Anytown"},
            {"name": "Jane Smith", "address": "456 Oak Ave", "city": "Somewhere"}
        ]

        # Create a mock GoogleDriveAPI instance (won't authenticate)
        # We'll just test the CSV conversion method
        api = GoogleDriveAPI.__new__(GoogleDriveAPI)  # Create without calling __init__
        csv_content = api.convert_leads_to_csv(test_data)

        if csv_content and "John Doe" in csv_content and "123 Main St" in csv_content:
            print("  ✓ CSV conversion working correctly")
            return True
        else:
            print("  ✗ CSV conversion failed")
            return False

    except Exception as e:
        print(f"  ✗ Google Drive utilities test failed: {e}")
        return False

def test_api_structure():
    """Test that API routes are properly structured"""
    print("\nTesting API structure...")

    try:
        from api.routes import app

        # Check that key routes exist
        routes = [route.path for route in app.routes]
        expected_routes = ["/status/{location}", "/progress/{location}", "/scrape/{location}"]

        for expected_route in expected_routes:
            if any(expected_route in route for route in routes):
                print(f"  ✓ Route {expected_route} found")
            else:
                print(f"  ✗ Route {expected_route} missing")
                return False

        print("  ✓ All expected routes present")
        return True

    except Exception as e:
        print(f"  ✗ API structure test failed: {e}")
        return False

def main():
    """Run all tests"""
    print("Wholesale Leads System - Basic Workflow Test")
    print("=" * 50)

    all_tests_passed = True

    # Run tests
    all_tests_passed &= test_imports()
    all_tests_passed &= test_google_drive_utilities()
    all_tests_passed &= test_api_structure()

    print("\n" + "=" * 50)
    if all_tests_passed:
        print("✓ All tests passed! Basic workflow structure is correct.")
        print("\nNext steps:")
        print("1. Install dependencies: pip install -r server/requirements.txt")
        print("2. Set up Google Drive credentials")
        print("3. Configure environment variables")
        print("4. Start the FastAPI server: uvicorn main:app --reload")
        print("5. Start the React frontend: npm start")
    else:
        print("✗ Some tests failed. Please check the errors above.")

    return all_tests_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)