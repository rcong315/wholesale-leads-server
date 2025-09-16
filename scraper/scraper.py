import logging
import asyncio
import sys
import json
import time
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

from scraper.config import Config
from google_drive.api import GoogleDriveAPI

logger = logging.getLogger(__name__)


class BatchLeadsScraper:
    def __init__(self, config=None):
        self.config = config or Config()
        self.browser = None
        self.context = None
        self.drive_api = GoogleDriveAPI()

        # Buffering for periodic writes
        self.leads_buffer = []
        self.total_leads_written = 0
        self.current_zip_code = None

        # Performance tracking
        self.scrape_start_time = None
        self.write_operations = 0
        self.total_write_time = 0.0
        self.pages_scraped = 0

    async def init_browser(self, headless=None):
        try:
            playwright = await async_playwright().start()
            use_headless = headless if headless is not None else self.config.HEADLESS
            self.browser = await playwright.chromium.launch(
                headless=use_headless,
                args=(
                    [
                        "--no-sandbox",
                        "--disable-setuid-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-accelerated-2d-canvas",
                        "--no-first-run",
                        "--no-zygote",
                        "--disable-gpu",
                    ]
                    if use_headless
                    else []
                ),
            )
            self.context = await self.browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
            )

        except Exception as e:
            logger.error(f"Failed to launch browser: {e}")
            raise (e)

    async def login(self):
        try:
            page = await self.context.new_page()

            await page.goto(f"{self.config.BATCHLEADS_BASE_URL}login")
            await page.wait_for_timeout(3000)

            try:
                await page.wait_for_selector(
                    'input[formcontrolname="email"]', timeout=3000
                )
                await page.fill(
                    'input[formcontrolname="email"]', self.config.BATCHLEADS_EMAIL
                )
                logger.info(
                    'Email filled using selector: input[formcontrolname="email"]'
                )
            except:
                logger.error("Could not find email input field")
                return False

            try:
                await page.wait_for_selector(
                    'input[formcontrolname="password"]', timeout=3000
                )
                await page.fill(
                    'input[formcontrolname="password"]', self.config.BATCHLEADS_PASSWORD
                )
                logger.info(
                    'Password filled using selector: input[formcontrolname="password"]'
                )
            except:
                logger.error("Could not find password input field")
                return False

            try:
                await page.wait_for_selector('button[type="submit"]', timeout=3000)
                await page.click('button[type="submit"]')
                logger.info('Submit clicked using selector: button[type="submit"]')
            except:
                logger.error("Could not find or click submit button")
                return False

            try:
                await page.wait_for_url(lambda url: "login" not in url, timeout=10000)
            except:
                await page.wait_for_load_state("networkidle", timeout=10000)

            return True

        except Exception as e:
            logger.error(f"Login error: {e}")
            return False

    def extract_pagination_info(self, soup):
        try:
            # Look for pagination text like " 351 - 374 of 374 "
            pagination_spans = soup.find_all("span")
            for span in pagination_spans:
                text = span.get_text().strip()
                # Pattern: "X - Y of Z" where Z is total leads
                if " of " in text and " - " in text:
                    parts = text.split(" of ")
                    if len(parts) == 2:
                        try:
                            total_leads = int(parts[1].strip().replace(",", ""))
                            # Extract current range
                            range_part = parts[0].strip()
                            if " - " in range_part:
                                range_parts = range_part.split(" - ")
                                start_lead = int(range_parts[0].strip().replace(",", ""))
                                end_lead = int(range_parts[1].strip().replace(",", ""))
                                return {
                                    "total_leads": total_leads,
                                    "current_start": start_lead,
                                    "current_end": end_lead,
                                }
                        except ValueError:
                            continue

            logger.warning("Could not extract pagination info from HTML")
            return None

        except Exception as e:
            logger.error(f"Error extracting pagination info: {e}")
            return None

    def get_buffer_size_mb(self):
        """Calculate buffer size in MB"""
        buffer_str = json.dumps(self.leads_buffer)
        return sys.getsizeof(buffer_str) / (1024 * 1024)

    async def flush_leads_to_drive(self, is_final=False):
        """Flush current buffer to Google Drive"""
        if not self.leads_buffer or not self.current_zip_code:
            return

        try:
            flush_start_time = time.time()
            buffer_size_mb = self.get_buffer_size_mb()
            leads_count = len(self.leads_buffer)

            flush_type = "FINAL" if is_final else "PARTIAL"
            logger.info(f"[{flush_type}] Starting flush of {leads_count} leads to drive (buffer: {buffer_size_mb:.2f}MB)")

            # Use append for partial writes, save_cache for final write
            if is_final:
                # For final write, append remaining buffer and finalize
                if hasattr(self.drive_api, 'append_to_cache'):
                    success = await asyncio.get_event_loop().run_in_executor(
                        None,
                        self.drive_api.append_to_cache,
                        self.current_zip_code,
                        self.leads_buffer,
                        True  # is_final
                    )
                else:
                    # Fallback to traditional save_cache if append not available
                    all_leads = self.leads_buffer
                    success = await asyncio.get_event_loop().run_in_executor(
                        None,
                        self.drive_api.save_cache,
                        self.current_zip_code,
                        all_leads
                    )
            else:
                # Partial write
                if hasattr(self.drive_api, 'append_to_cache'):
                    success = await asyncio.get_event_loop().run_in_executor(
                        None,
                        self.drive_api.append_to_cache,
                        self.current_zip_code,
                        self.leads_buffer,
                        False  # is_final
                    )
                else:
                    # If append not available, just accumulate until final
                    logger.warning("append_to_cache not available, accumulating in memory")
                    return

            flush_duration = time.time() - flush_start_time
            self.total_write_time += flush_duration
            self.write_operations += 1

            if success:
                self.total_leads_written += leads_count

                # Calculate performance metrics
                leads_per_sec = leads_count / flush_duration if flush_duration > 0 else 0
                mb_per_sec = buffer_size_mb / flush_duration if flush_duration > 0 else 0

                logger.info(f"[{flush_type}] ✓ Flush completed in {flush_duration:.2f}s")
                logger.info(f"Performance: {leads_per_sec:.1f} leads/sec, {mb_per_sec:.2f} MB/sec")
                logger.info(f"Total progress: {self.total_leads_written} leads written in {self.write_operations} operations")

                self.leads_buffer = []  # Clear buffer after successful write
            else:
                logger.error(f"[{flush_type}] ✗ Flush failed after {flush_duration:.2f}s")

        except Exception as e:
            flush_duration = time.time() - flush_start_time
            logger.error(f"[{flush_type}] ✗ Flush error after {flush_duration:.2f}s: {e}")

    async def scrape_leads_table(self, page, page_num=1):
        try:
            html_content = await page.content()
            soup = BeautifulSoup(html_content, "html.parser")
            leads_data = []

            tables = soup.find_all("table")
            for table in tables:
                headers = []

                # Get headers
                header_row = table.find("thead")
                if header_row:
                    headers = [
                        th.get_text().strip()
                        for th in header_row.find_all(["th", "td"])
                    ][3:]

                # Get data rows
                tbody = table.find("tbody") or table
                data_rows = tbody.find_all("tr")

                for row in data_rows:
                    cells = [
                        td.get_text().strip() for td in row.find_all(["td", "th"])
                    ][2:]
                    if cells and len(cells) > 1:
                        lead = dict(zip(headers, cells))
                        leads_data.append(lead)
                        logger.debug(f"Extracted lead: {lead['Property Address']}")

            logger.info(f"Found {len(leads_data)} leads on page {page_num}")
            return leads_data, soup

        except Exception as e:
            logger.error(f"Scraping error: {e}")
            return [], None

    async def scrape_zip_code(self, zip_code, progress_callback=None):
        try:
            # Initialize for this zip code
            self.current_zip_code = zip_code
            self.leads_buffer = []
            self.total_leads_written = 0
            self.scrape_start_time = time.time()
            self.write_operations = 0
            self.total_write_time = 0.0
            self.pages_scraped = 0

            # Log scraping session initialization
            logger.info(f"=== Starting scrape session for ZIP {zip_code} ===")
            logger.info(f"Configuration: BATCH_SIZE={self.config.BATCH_SIZE}, WRITE_THRESHOLD_MB={self.config.WRITE_THRESHOLD_MB}")
            logger.info(f"Max pages limit: {self.config.MAX_PAGES}")

            page_num = 1
            max_pages = self.config.MAX_PAGES
            total_leads = None
            total_pages = None
            pending_writes = []

            page = (
                self.context.pages[0]
                if self.context.pages
                else await self.context.new_page()
            )
            search_url = f"{self.config.BATCHLEADS_BASE_URL}app/mylist-new"
            await page.goto(search_url)
            await page.wait_for_timeout(3000)

            if progress_callback:
                progress_callback(f"Searching for zip code {zip_code}...")

            try:
                zip_input = await page.query_selector('input[id="placeInput"]')
                if zip_input:
                    await zip_input.fill(str(zip_code))
                    await zip_input.press("Enter")
                    await page.wait_for_timeout(3000)
            except Exception:
                pass

            while page_num <= max_pages:
                leads, soup = await self.scrape_leads_table(page, page_num)

                # Extract pagination info on first page
                if page_num == 1 and soup:
                    pagination_info = self.extract_pagination_info(soup)
                    if pagination_info:
                        total_leads = pagination_info["total_leads"]
                        # Estimate total pages (assuming consistent page size)
                        if len(leads) > 0:
                            total_pages = (total_leads + len(leads) - 1) // len(leads)
                        if progress_callback:
                            progress_callback(
                                f"Found {total_leads} total leads across approximately {total_pages} pages"
                            )

                if not leads:
                    break

                # Add leads to buffer
                self.leads_buffer.extend(leads)
                self.pages_scraped += 1

                current_buffer_size = len(self.leads_buffer)
                current_buffer_mb = self.get_buffer_size_mb()
                logger.info(f"Page {page_num}: Added {len(leads)} leads to buffer (total: {current_buffer_size} leads, {current_buffer_mb:.2f}MB)")

                # Check threshold proximity for warnings
                batch_percentage = (current_buffer_size / self.config.BATCH_SIZE) * 100
                memory_percentage = (current_buffer_mb / self.config.WRITE_THRESHOLD_MB) * 100

                if batch_percentage >= 75 or memory_percentage >= 75:
                    logger.info(f"Buffer approaching thresholds: {batch_percentage:.1f}% batch size, {memory_percentage:.1f}% memory limit")

                # Check if we need to flush based on batch size or memory threshold
                batch_threshold_hit = current_buffer_size >= self.config.BATCH_SIZE
                memory_threshold_hit = current_buffer_mb >= self.config.WRITE_THRESHOLD_MB
                should_flush = batch_threshold_hit or memory_threshold_hit

                if should_flush:
                    trigger_reason = []
                    if batch_threshold_hit:
                        trigger_reason.append(f"batch size ({current_buffer_size}/{self.config.BATCH_SIZE})")
                    if memory_threshold_hit:
                        trigger_reason.append(f"memory threshold ({current_buffer_mb:.2f}/{self.config.WRITE_THRESHOLD_MB}MB)")

                    logger.info(f"Flush triggered by: {', '.join(trigger_reason)}")

                if should_flush:
                    # Start async write in background
                    write_task = asyncio.create_task(self.flush_leads_to_drive(is_final=False))
                    pending_writes.append(write_task)

                    logger.info(f"Started background write operation #{self.write_operations + 1} (pending writes: {len(pending_writes)})")

                    if progress_callback:
                        progress_callback(f"Writing batch to drive (buffer: {current_buffer_size} leads, {current_buffer_mb:.2f}MB)")

                # Enhanced progress message
                total_scraped = self.total_leads_written + len(self.leads_buffer)
                if progress_callback:
                    if total_leads and total_pages:
                        progress_callback(
                            f"Scraping page {page_num} of {total_pages} ({total_scraped}/{total_leads} leads)"
                        )
                    else:
                        progress_callback(
                            f"Scraping page {page_num} ({total_scraped} leads so far)"
                        )

                page_num += 1

                next_button = await page.query_selector('a[aria-label="Next"]')
                if next_button and await next_button.is_enabled():
                    await next_button.click()
                    await page.wait_for_timeout(3000)
                else:
                    break

            # Wait for any pending writes to complete
            if pending_writes:
                if progress_callback:
                    progress_callback("Waiting for background writes to complete...")
                await asyncio.gather(*pending_writes, return_exceptions=True)

            # Final flush of remaining buffer
            if self.leads_buffer:
                await self.flush_leads_to_drive(is_final=True)

            # Calculate comprehensive statistics
            total_scrape_time = time.time() - self.scrape_start_time
            total_final = self.total_leads_written
            pages_final = self.pages_scraped

            # Log comprehensive final statistics
            logger.info(f"=== Scrape session completed for ZIP {zip_code} ===")
            logger.info(f"📊 SCRAPING SUMMARY:")
            logger.info(f"   • Total leads scraped: {total_final}")
            logger.info(f"   • Pages processed: {pages_final}")
            logger.info(f"   • Total scraping time: {total_scrape_time:.2f}s ({total_scrape_time/60:.1f}m)")

            if pages_final > 0:
                logger.info(f"   • Average leads per page: {total_final/pages_final:.1f}")
                logger.info(f"   • Average time per page: {total_scrape_time/pages_final:.1f}s")

            if total_final > 0:
                logger.info(f"   • Scraping rate: {total_final/total_scrape_time:.1f} leads/sec")

            logger.info(f"💾 WRITING PERFORMANCE:")
            logger.info(f"   • Write operations: {self.write_operations}")
            logger.info(f"   • Total write time: {self.total_write_time:.2f}s")

            if self.write_operations > 0:
                avg_write_time = self.total_write_time / self.write_operations
                logger.info(f"   • Average write time: {avg_write_time:.2f}s")

                if total_final > 0:
                    logger.info(f"   • Average leads per write: {total_final/self.write_operations:.1f}")

            write_overhead_pct = (self.total_write_time / total_scrape_time) * 100 if total_scrape_time > 0 else 0
            logger.info(f"   • Write overhead: {write_overhead_pct:.1f}% of total time")

            logger.info(f"🎯 EFFICIENCY METRICS:")
            if total_pages:
                completion_rate = (pages_final / total_pages) * 100
                logger.info(f"   • Page completion rate: {completion_rate:.1f}% ({pages_final}/{total_pages})")

            if total_leads and total_final > 0:
                capture_rate = (total_final / total_leads) * 100
                logger.info(f"   • Lead capture rate: {capture_rate:.1f}% ({total_final}/{total_leads})")

            logger.info(f"=" * 50)

            if progress_callback:
                progress_callback(
                    f"Completed: Scraped {total_final} leads from {pages_final} pages in {total_scrape_time:.1f}s"
                )

            return total_final

        except Exception as e:
            # Enhanced error logging with context
            error_time = time.time() - self.scrape_start_time if self.scrape_start_time else 0
            buffer_state = f"{len(self.leads_buffer)} leads, {self.get_buffer_size_mb():.2f}MB" if hasattr(self, 'leads_buffer') else "unknown"

            logger.error(f"=== SCRAPE ERROR for ZIP {zip_code} ===")
            logger.error(f"Error: {str(e)}")
            logger.error(f"Error occurred after: {error_time:.1f}s")
            logger.error(f"Pages completed: {getattr(self, 'pages_scraped', 0)}")
            logger.error(f"Leads written so far: {getattr(self, 'total_leads_written', 0)}")
            logger.error(f"Buffer state: {buffer_state}")
            logger.error(f"Write operations completed: {getattr(self, 'write_operations', 0)}")
            logger.error(f"=" * 40)

            return getattr(self, 'total_leads_written', 0)

    async def close(self):
        try:
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
        except Exception as e:
            logger.error(f"Error closing browser: {e}")


async def scrape(zip_code, headless=None, use_cache=True, progress_callback=None):
    config = Config()
    drive_api = GoogleDriveAPI()

    if progress_callback:
        progress_callback("Initializing browser...")

    scraper = BatchLeadsScraper(config)
    await scraper.init_browser(headless=headless)

    if progress_callback:
        progress_callback("Logging in...")

    await scraper.login()

    try:
        cached_data = None
        if use_cache:
            if progress_callback:
                progress_callback("Checking cache...")
            cached_data = drive_api.load_cache(zip_code)

        if cached_data:
            logger.info(f"Using cached data for zip code {zip_code}")
            if progress_callback:
                progress_callback("Found cached data")
            return cached_data

        if progress_callback:
            progress_callback(f"Scraping data for zip code {zip_code}...")

        total_leads_count = await scraper.scrape_zip_code(zip_code, progress_callback)

        # Data is now automatically written to drive during scraping
        if total_leads_count > 0:
            # Load the complete data from drive to return in the expected format
            final_cached_data = drive_api.load_cache(zip_code)
            if final_cached_data:
                final_cached_data["cached"] = False  # This was freshly scraped
                final_cached_data["cache_age_days"] = 0
                if progress_callback:
                    progress_callback("Scraping completed successfully")
                return final_cached_data

            # Fallback if load fails
            result = {
                "zip_code": zip_code,
                "total_leads": total_leads_count,
                "leads": [],  # Not available in memory anymore
                "cached": False,
                "cache_age_days": 0,
            }

            if progress_callback:
                progress_callback("Scraping completed successfully")

            return result
        else:
            result = {
                "zip_code": zip_code,
                "total_leads": 0,
                "leads": [],
                "cached": False,
                "cache_age_days": 0,
            }

            if progress_callback:
                progress_callback("No leads found for this zip code")

            return result

    except Exception as e:
        logger.error(f"Error in scrape: {e}")
        if progress_callback:
            progress_callback(f"Error: {str(e)}")
        return {"error": str(e)}
    finally:
        await scraper.close()


if __name__ == "__main__":
    zip_code = "94588"

    async def main():
        result = await scrape(zip_code, use_cache=False)
        if "error" in result:
            print(f"Error: {result['error']}")
        else:
            print(
                f"Zip {result['zip_code']}: Found {result.get('total_leads', 0)} leads (cached: {result.get('cached', False)})"
            )

    asyncio.run(main())
