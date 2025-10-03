#!/usr/bin/env python3

import asyncio
import logging
import time
from datetime import datetime
from ca_locations import get_locations
from scraper.scraper import BatchLeadsScraper
from database import Database

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("batch_scraper.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


class BatchScraper:
    def __init__(self, max_retries=3, delay_seconds=5, skip_existing=True):
        self.max_retries = max_retries
        self.delay_seconds = delay_seconds
        self.skip_existing = skip_existing
        self.processed = 0
        self.failed = 0
        self.skipped = 0
        self.start_time = None

        self.scraper = BatchLeadsScraper()
        self.database = Database(chunk_size=500)  # Use chunked processing
        self.existing_locations = set()

    async def init_browser(self, headless=True):
        await self.scraper.init_browser(headless=headless)

    async def login(self):
        await self.scraper.login()

    def load_existing_locations(self):
        if self.skip_existing:
            logger.info("Checking SQLite database for existing locations...")
            cached_locations = self.database.get_locations()
            self.existing_locations = set(cached_locations)
            logger.info(
                f"Found {len(self.existing_locations)} existing locations that will be skipped"
            )
        else:
            logger.info("Skip existing disabled - will process all locations")

    async def scrape_all_california(self, start_index=0, limit=None):
        locations = get_locations()

        if limit:
            locations = locations[start_index : start_index + limit]
        else:
            locations = locations[start_index:]

        # Load existing locations to skip
        self.load_existing_locations()

        # Filter out existing locations if skip_existing is enabled
        if self.skip_existing:
            original_count = len(locations)
            locations = [loc for loc in locations if loc not in self.existing_locations]
            skipped_count = original_count - len(locations)
            if skipped_count > 0:
                logger.info(
                    f"Skipping {skipped_count} locations that already exist in SQLite database"
                )
                self.skipped = skipped_count

        total_locations = len(locations)
        if total_locations == 0:
            logger.info("No new locations to process - all are already cached!")
            return

        logger.info(
            f"Starting batch scrape of {total_locations} California locations..."
        )
        logger.info(f"Max retries per location: {self.max_retries}")

        self.start_time = time.time()

        for i, location in enumerate(locations, 1):
            logger.info(
                f"Progress: {i}/{total_locations} ({(i/total_locations)*100:.1f}%) - Processing location: {location}"
            )

            try:
                # Check if location already exists in database (if skip_existing is enabled)
                if self.skip_existing and self.database.location_exists(location):
                    logger.info(
                        f"Location {location} already exists in database, skipping..."
                    )
                    self.skipped += 1
                    continue

                # Scrape the location (using chunked processing - data is saved during scraping)
                leads = await self.scraper.scrape_location(location)

                # Check results - chunked processing saves data during scraping
                if leads and len(leads) > 0:
                    logger.info(
                        f"Successfully processed location: {location} ({len(leads)} leads)"
                    )
                    self.processed += 1
                else:
                    # Check if location has any leads in database (might have been saved via chunked processing)
                    db_result = self.database.get_leads(location)
                    if db_result and db_result.get("total_leads", 0) > 0:
                        logger.info(
                            f"Successfully processed location: {location} ({db_result['total_leads']} leads saved via chunked processing)"
                        )
                        self.processed += 1
                    else:
                        logger.info(f"No leads found for location: {location}")
                        self.processed += 1

            except Exception as e:
                self.failed += 1
                logger.error(f"Failed to process location {location}: {str(e)}")

            # Progress update every 10 locations
            if i % 10 == 0:
                elapsed_time = time.time() - self.start_time
                avg_time_per_location = elapsed_time / i
                estimated_remaining = avg_time_per_location * (total_locations - i)

                logger.info(f"--- Progress Report ---")
                logger.info(
                    f"Processed: {i}/{total_locations} ({(i/total_locations)*100:.1f}%)"
                )
                logger.info(f"Fresh scrapes: {self.processed}")
                logger.info(f"Cached results: {self.skipped}")
                logger.info(f"Failed: {self.failed}")
                logger.info(f"Elapsed time: {elapsed_time/60:.1f} minutes")
                logger.info(
                    f"Estimated remaining: {estimated_remaining/60:.1f} minutes"
                )
                logger.info(
                    f"Average time per location: {avg_time_per_location:.2f} seconds"
                )
                logger.info("---------------------")

            # Add delay between requests to be respectful to the server
            if i < total_locations:
                await asyncio.sleep(self.delay_seconds)

        self.print_final_summary(total_locations)

    def print_final_summary(self, total_locations):
        """Print final summary statistics"""
        elapsed_time = time.time() - self.start_time

        logger.info("=" * 50)
        logger.info("BATCH SCRAPING COMPLETED")
        logger.info("=" * 50)
        logger.info(f"Total locations processed: {total_locations}")
        logger.info(f"Fresh scrapes: {self.processed}")
        logger.info(f"Cached results: {self.skipped}")
        logger.info(f"Failed: {self.failed}")
        logger.info(
            f"Success rate: {((self.processed + self.skipped) / total_locations) * 100:.1f}%"
        )
        logger.info(
            f"Total time: {elapsed_time/60:.1f} minutes ({elapsed_time/3600:.1f} hours)"
        )
        logger.info(
            f"Average time per location: {elapsed_time/total_locations:.2f} seconds"
        )
        logger.info("=" * 50)


async def main():
    """Main function with command line options"""
    import argparse

    parser = argparse.ArgumentParser(description="Batch scrape California locations")
    parser.add_argument("--start", type=int, default=0, help="Start index (default: 0)")
    parser.add_argument(
        "--limit", type=int, help="Limit number of locations to process"
    )
    parser.add_argument(
        "--delay",
        type=int,
        default=5,
        help="Delay between requests in seconds (default: 5)",
    )
    parser.add_argument(
        "--retries", type=int, default=3, help="Max retries per location (default: 3)"
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Process all locations, even if already cached",
    )

    args = parser.parse_args()

    skip_existing = not args.no_skip_existing
    scraper = BatchScraper(
        max_retries=args.retries, delay_seconds=args.delay, skip_existing=skip_existing
    )
    await scraper.init_browser(headless=False)
    await scraper.login()

    logger.info(f"Batch scraper started at {datetime.now()}")
    logger.info(
        f"Configuration: start={args.start}, limit={args.limit}, delay={args.delay}s, retries={args.retries}, skip_existing={skip_existing}"
    )

    try:
        await scraper.scrape_all_california(start_index=args.start, limit=args.limit)
    except KeyboardInterrupt:
        logger.info("Batch scraping interrupted by user")
        scraper.print_final_summary(args.limit or len(get_locations()))
    except Exception as e:
        logger.error(f"Batch scraping failed with error: {str(e)}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
