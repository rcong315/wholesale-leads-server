#!/usr/bin/env python3
"""
Simple test to verify the key components of the wholesale leads system work together.
This test checks imports and basic functionality without requiring external services.
"""

import sys
import os

# Add the server directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "server"))


def test_imports():
    """Test that all main modules can be imported"""
    print("Testing imports...")

    try:
        # Test Database module
        print("  - Testing Database import...")
        from db.database import Database

        print("    ✓ Database imported successfully")

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


def test_database_utilities():
    """Test Database utility functions"""
    print("\nTesting Database utilities...")

    try:
        from db.database import Database
        import tempfile
        import os

        # Create a temporary database for testing
        with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as tmp_file:
            tmp_db_path = tmp_file.name

        try:
            # Test database initialization
            db = Database(tmp_db_path)
            print("  ✓ Database initialization working correctly")

            # Test saving and retrieving leads
            test_data = [
                {
                    "Property Address": "123 Main St",
                    "City": "Anytown",
                    "Owner First Name": "John",
                    "Owner Last Name": "Doe",
                },
                {
                    "Property Address": "456 Oak Ave",
                    "City": "Somewhere",
                    "Owner First Name": "Jane",
                    "Owner Last Name": "Smith",
                },
            ]

            saved_count = db.save_leads("test_location", test_data)
            if saved_count == 2:
                print("  ✓ Lead saving working correctly")
            else:
                print("  ✗ Lead saving failed")
                return False

            # Test retrieving leads
            leads_data = db.get_leads("test_location")
            if leads_data and leads_data["total_leads"] == 2:
                print("  ✓ Lead retrieval working correctly")
                return True
            else:
                print("  ✗ Lead retrieval failed")
                return False

        finally:
            # Clean up temporary database
            if os.path.exists(tmp_db_path):
                os.unlink(tmp_db_path)

    except Exception as e:
        print(f"  ✗ Database utilities test failed: {e}")
        return False


def test_api_structure():
    """Test that API routes are properly structured"""
    print("\nTesting API structure...")

    try:
        from api.routes import app

        # Check that key routes exist
        routes = [route.path for route in app.routes]
        expected_routes = [
            "/status/{location}",
            "/progress/{location}",
            "/scrape/{location}",
        ]

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
    all_tests_passed &= test_database_utilities()
    all_tests_passed &= test_api_structure()

    print("\n" + "=" * 50)
    if all_tests_passed:
        print("✓ All tests passed! Basic workflow structure is correct.")
        print("\nNext steps:")
        print("1. Install dependencies: pip install -r server/requirements.txt")
        print("2. Configure environment variables")
        print("3. Start the FastAPI server: uvicorn main:app --reload")
        print("4. Start the React frontend: npm start")
    else:
        print("✗ Some tests failed. Please check the errors above.")

    return all_tests_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
