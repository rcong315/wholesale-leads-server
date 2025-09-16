#!/usr/bin/env python3
"""
Test script to verify the pagination info extraction functionality.
"""

import sys
import os
from bs4 import BeautifulSoup

# Add the parent directory to Python path to import scraper
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from scraper.scraper import BatchLeadsScraper

def test_pagination_extraction():
    """Test the pagination extraction logic with the provided HTML"""

    # Sample HTML from the user
    html_content = '''
    <div _ngcontent-cvq-c506="" class="ng-star-inserted">
        <app-paginator _ngcontent-cvq-c506="" _nghost-cvq-c495="">
            <div _ngcontent-cvq-c495="" class="ctableview_pagination position-right row">
                <p _ngcontent-cvq-c495=""> Rows per page
                    <select _ngcontent-cvq-c495="" class="ng-untouched ng-pristine ng-valid">
                        <option _ngcontent-cvq-c495="" value="15">15</option>
                        <option _ngcontent-cvq-c495="" value="25">25</option>
                        <option _ngcontent-cvq-c495="" value="50">50</option>
                        <option _ngcontent-cvq-c495="" value="100">100</option>
                    </select>
                    <span _ngcontent-cvq-c495=""> 351 - 374 of 374 </span>
                </p>
            </div>
        </app-paginator>
    </div>
    '''

    print("Testing pagination info extraction...")
    print("HTML content:", html_content[:100] + "...")

    soup = BeautifulSoup(html_content, "html.parser")
    scraper = BatchLeadsScraper()

    result = scraper.extract_pagination_info(soup)

    if result:
        print(f"✓ Successfully extracted pagination info:")
        print(f"  Total leads: {result['total_leads']}")
        print(f"  Current range: {result['current_start']} - {result['current_end']}")

        # Test progress message generation
        total_leads = result['total_leads']
        page_size = result['current_end'] - result['current_start'] + 1
        total_pages = (total_leads + page_size - 1) // page_size
        current_page = (result['current_start'] - 1) // page_size + 1

        print(f"  Calculated page size: {page_size}")
        print(f"  Calculated total pages: {total_pages}")
        print(f"  Current page: {current_page}")

        # Example progress messages
        print(f"\nExample progress messages:")
        print(f"  Initial: 'Found {total_leads} total leads across approximately {total_pages} pages'")
        print(f"  Page progress: 'Scraping page {current_page} of {total_pages} ({result['current_end']}/{total_leads} leads)'")

        return True
    else:
        print("✗ Failed to extract pagination info")
        return False

def test_edge_cases():
    """Test edge cases for pagination extraction"""
    print("\nTesting edge cases...")

    # Test different HTML structures
    test_cases = [
        # Case 1: Different spacing
        '<span> 1 - 25 of 100 </span>',
        # Case 2: No spaces
        '<span>26-50 of 100</span>',
        # Case 3: Large numbers
        '<span> 1975 - 2000 of 2000 </span>',
        # Case 4: Numbers with commas
        '<span> 1,001 - 1,025 of 5,234 </span>',
        # Case 5: Large numbers with commas
        '<span> 9,976 - 10,000 of 12,345 </span>',
        # Case 6: Invalid format
        '<span> invalid format </span>',
    ]

    scraper = BatchLeadsScraper()

    for i, test_html in enumerate(test_cases, 1):
        soup = BeautifulSoup(test_html, "html.parser")
        result = scraper.extract_pagination_info(soup)

        if result:
            print(f"  Test {i}: ✓ Extracted - Total: {result['total_leads']}, Range: {result['current_start']}-{result['current_end']}")
        else:
            print(f"  Test {i}: ✗ Could not extract from: {test_html}")

def main():
    """Run all tests"""
    print("Enhanced Progress Tracking - Pagination Extraction Test")
    print("=" * 60)

    success1 = test_pagination_extraction()
    test_edge_cases()

    print("\n" + "=" * 60)
    if success1:
        print("✓ Main pagination extraction test passed!")
        print("\nThe enhanced progress tracking should now show:")
        print("- Total leads discovered on first page")
        print("- Estimated total pages")
        print("- Current page progress (e.g., 'Page 3 of 15')")
        print("- Lead count progress (e.g., '45/374 leads')")
    else:
        print("✗ Main test failed. Check the pagination extraction logic.")

if __name__ == "__main__":
    main()