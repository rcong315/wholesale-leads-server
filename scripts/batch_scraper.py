#!/usr/bin/env python3

import asyncio
import logging
import time
from datetime import datetime
from ca_zip_codes import get_zip_codes
from scraper.scraper import BatchLeadsScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('batch_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BatchScraper:
    def __init__(self, max_retries=3, delay_seconds=5):
        self.max_retries = max_retries
        self.delay_seconds = delay_seconds
        self.processed = 0
        self.failed = 0
        self.skipped = 0
        self.start_time = None

        self.scraper = BatchLeadsScraper()

    async def init_browser(self, headless=True):
        await self.scraper.init_browser(headless=headless)

    async def login(self):
        await self.scraper.login()
    
    async def scrape_all_california(self, start_index=0, limit=None):
        """Scrape all California ZIP codes"""
        zip_codes = get_zip_codes()
        
        if limit:
            zip_codes = zip_codes[start_index:start_index + limit]
        else:
            zip_codes = zip_codes[start_index:]
            
        total_zips = len(zip_codes)
        logger.info(f"Starting batch scrape of {total_zips} ZIP codes...")
        logger.info(f"Max retries per ZIP: {self.max_retries}")
        
        self.start_time = time.time()
        
        for i, zip_code in enumerate(zip_codes, 1):
            logger.info(f"Progress: {i}/{total_zips} ({(i/total_zips)*100:.1f}%)")
            
            await self.scraper.scrape_zip_code(zip_code)

            
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
                logger.info(f"Estimated remaining: {estimated_remaining/60:.1f} minutes")
                logger.info(f"Average time per ZIP: {avg_time_per_zip:.2f} seconds")
                logger.info("---------------------")
            
            # Add delay between requests to be respectful to the server
            if i < total_zips:
                await asyncio.sleep(self.delay_seconds)
                
        self.print_final_summary(total_zips)

    def print_final_summary(self, total_zips):
        """Print final summary statistics"""
        elapsed_time = time.time() - self.start_time
        
        logger.info("="*50)
        logger.info("BATCH SCRAPING COMPLETED")
        logger.info("="*50)
        logger.info(f"Total ZIP codes processed: {total_zips}")
        logger.info(f"Fresh scrapes: {self.processed}")
        logger.info(f"Cached results: {self.skipped}")
        logger.info(f"Failed: {self.failed}")
        logger.info(f"Success rate: {((self.processed + self.skipped) / total_zips) * 100:.1f}%")
        logger.info(f"Total time: {elapsed_time/60:.1f} minutes ({elapsed_time/3600:.1f} hours)")
        logger.info(f"Average time per ZIP: {elapsed_time/total_zips:.2f} seconds")
        logger.info("="*50)

async def main():
    """Main function with command line options"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Batch scrape California ZIP codes')
    parser.add_argument('--start', type=int, default=0, help='Start index (default: 0)')
    parser.add_argument('--limit', type=int, help='Limit number of ZIP codes to process')
    parser.add_argument('--delay', type=int, default=5, help='Delay between requests in seconds (default: 5)')
    parser.add_argument('--retries', type=int, default=3, help='Max retries per ZIP code (default: 3)')
    
    args = parser.parse_args()
    
    scraper = BatchScraper(max_retries=args.retries, delay_seconds=args.delay)
    await scraper.init_browser(headless=False)
    await scraper.login()
    
    logger.info(f"Batch scraper started at {datetime.now()}")
    logger.info(f"Configuration: start={args.start}, limit={args.limit}, delay={args.delay}s, retries={args.retries}")
    
    try:
        await scraper.scrape_all_california(
            start_index=args.start,
            limit=args.limit
        )
    except KeyboardInterrupt:
        logger.info("Batch scraping interrupted by user")
        scraper.print_final_summary(args.limit or len(get_zip_codes()))
    except Exception as e:
        logger.error(f"Batch scraping failed with error: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())