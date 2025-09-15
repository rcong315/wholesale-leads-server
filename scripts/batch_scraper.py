#!/usr/bin/env python3

import asyncio
import logging
import time
from datetime import datetime
from ca_zip_codes import get_zip_codes
from scraper.scraper import BatchLeadsScraper
from google_drive.api import GoogleDriveAPI

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
        self.drive_api = GoogleDriveAPI() if skip_existing else None
        self.existing_zip_codes = set()

    async def init_browser(self, headless=True):
        await self.scraper.init_browser(headless=headless)

    async def login(self):
        await self.scraper.login()

    def load_existing_zip_codes(self):
        if self.drive_api:
            logger.info("Checking Google Drive for existing zip code files...")
            self.existing_zip_codes = self.drive_api.get_existing_zip_codes()
            logger.info(
                f"Found {len(self.existing_zip_codes)} existing zip codes that will be skipped"
            )
        else:
            logger.info("Skip existing files disabled - will process all zip codes")

    async def scrape_all_california(self, start_index=0, limit=None):
        zip_codes = get_zip_codes()

        if limit:
            zip_codes = zip_codes[start_index : start_index + limit]
        else:
            zip_codes = zip_codes[start_index:]

        # Load existing zip codes to skip
        self.load_existing_zip_codes()

        # Filter out existing zip codes if skip_existing is enabled
        if self.skip_existing:
            original_count = len(zip_codes)
            zip_codes = [zc for zc in zip_codes if zc not in self.existing_zip_codes]
            skipped_count = original_count - len(zip_codes)
            if skipped_count > 0:
                logger.info(
                    f"Skipping {skipped_count} zip codes that already exist in Google Drive"
                )
                self.skipped = skipped_count

        total_zips = len(zip_codes)
        if total_zips == 0:
            logger.info("No new zip codes to process - all are already cached!")
            return

        logger.info(f"Starting batch scrape of {total_zips} ZIP codes...")
        logger.info(f"Max retries per ZIP: {self.max_retries}")

        self.start_time = time.time()

        for i, zip_code in enumerate(zip_codes, 1):
            logger.info(
                f"Progress: {i}/{total_zips} ({(i/total_zips)*100:.1f}%) - Processing ZIP: {zip_code}"
            )

            try:
                await self.scraper.scrape_zip_code(zip_code)
                self.processed += 1
                logger.info(f"Successfully processed ZIP code: {zip_code}")
            except Exception as e:
                self.failed += 1
                logger.error(f"Failed to process ZIP code {zip_code}: {str(e)}")

            # Progress update every 10 ZIP codes
            if i % 10 == 0:
                elapsed_time = time.time() - self.start_time
                avg_time_per_zip = elapsed_time / i
                estimated_remaining = avg_time_per_zip * (total_zips - i)

                logger.info(f"--- Progress Report ---")
                logger.info(f"Processed: {i}/{total_zips} ({(i/total_zips)*100:.1f}%)")
                logger.info(f"Fresh scrapes: {self.processed}")
                logger.info(f"Cached results: {self.skipped}")
                logger.info(f"Failed: {self.failed}")
                logger.info(f"Elapsed time: {elapsed_time/60:.1f} minutes")
                logger.info(
                    f"Estimated remaining: {estimated_remaining/60:.1f} minutes"
                )
                logger.info(f"Average time per ZIP: {avg_time_per_zip:.2f} seconds")
                logger.info("---------------------")

            # Add delay between requests to be respectful to the server
            if i < total_zips:
                await asyncio.sleep(self.delay_seconds)

        self.print_final_summary(total_zips)

    def print_final_summary(self, total_zips):
        """Print final summary statistics"""
        elapsed_time = time.time() - self.start_time

        logger.info("=" * 50)
        logger.info("BATCH SCRAPING COMPLETED")
        logger.info("=" * 50)
        logger.info(f"Total ZIP codes processed: {total_zips}")
        logger.info(f"Fresh scrapes: {self.processed}")
        logger.info(f"Cached results: {self.skipped}")
        logger.info(f"Failed: {self.failed}")
        logger.info(
            f"Success rate: {((self.processed + self.skipped) / total_zips) * 100:.1f}%"
        )
        logger.info(
            f"Total time: {elapsed_time/60:.1f} minutes ({elapsed_time/3600:.1f} hours)"
        )
        logger.info(f"Average time per ZIP: {elapsed_time/total_zips:.2f} seconds")
        logger.info("=" * 50)


async def main():
    """Main function with command line options"""
    import argparse

    parser = argparse.ArgumentParser(description="Batch scrape California ZIP codes")
    parser.add_argument("--start", type=int, default=0, help="Start index (default: 0)")
    parser.add_argument(
        "--limit", type=int, help="Limit number of ZIP codes to process"
    )
    parser.add_argument(
        "--delay",
        type=int,
        default=5,
        help="Delay between requests in seconds (default: 5)",
    )
    parser.add_argument(
        "--retries", type=int, default=3, help="Max retries per ZIP code (default: 3)"
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Process all ZIP codes, even if already cached",
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
        scraper.print_final_summary(args.limit or len(get_zip_codes()))
    except Exception as e:
        logger.error(f"Batch scraping failed with error: {str(e)}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
