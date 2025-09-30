import logging
import asyncio

# from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

from scraper.config import Config
from database import Database

logger = logging.getLogger(__name__)


class BatchLeadsScraper:
    def __init__(self, config=None):
        self.config = config or Config()
        self.all_data = []
        self.browser = None
        self.context = None
        self.database = Database(chunk_size=self.config.DB_CHUNK_SIZE)

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
                                start_lead = int(
                                    range_parts[0].strip().replace(",", "")
                                )
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

    async def scrape_location(
        self, location, progress_callback=None, use_chunked_processing=True
    ):
        try:
            if use_chunked_processing:
                return await self._scrape_location_chunked(location, progress_callback)
            else:
                return await self._scrape_location_legacy(location, progress_callback)

        except Exception as e:
            logger.error(f"Scraper error: {e}")
            return []

    async def _scrape_location_chunked(self, location, progress_callback=None):
        """Memory-efficient scraping that processes and saves leads in chunks"""
        try:
            total_saved = 0
            page_num = 1
            max_pages = self.config.MAX_PAGES
            total_leads = None
            total_pages = None
            chunk_leads = []

            page = (
                self.context.pages[0]
                if self.context.pages
                else await self.context.new_page()
            )
            search_url = f"{self.config.BATCHLEADS_BASE_URL}app/mylist-new"
            await page.goto(search_url)
            await page.wait_for_timeout(3000)

            if progress_callback:
                progress_callback(f"Searching for location {location}...")

            try:
                location_input = await page.query_selector('input[id="placeInput"]')
                if location_input:
                    await location_input.fill(str(location))
                    await location_input.press("Enter")
                    await page.wait_for_timeout(3000)
            except Exception:
                pass

            # Set rows per page to 100 for more efficient scraping
            try:
                if progress_callback:
                    progress_callback("Setting pagination to 100 rows per page...")

                # Look for the rows per page dropdown
                rows_dropdown = await page.query_selector("select")
                if rows_dropdown:
                    await rows_dropdown.select_option(value="100")
                    await page.wait_for_timeout(2000)
                    logger.info("Set pagination to 100 rows per page")
                else:
                    logger.debug("Rows per page dropdown not found")
            except Exception as e:
                logger.debug(f"Could not set rows per page: {e}")

            # Clear existing data for this location first
            if page_num == 1:
                self.database._log_memory_usage(f"before scraping {location}")
                # Clear existing leads to prepare for new data
                import sqlite3

                with sqlite3.connect(self.database.db_path) as conn:
                    conn.execute("DELETE FROM leads WHERE location = ?", (location,))
                    conn.commit()
                logger.info(f"Cleared existing leads for location {location}")

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

                chunk_leads.extend(leads)
                logger.info(f"Page {page_num}: Added {len(leads)} leads to chunk")

                # Save chunk when it reaches configured size or we're at the last page
                should_save_chunk = (
                    len(chunk_leads)
                    >= self.config.SCRAPER_CHUNK_SIZE
                    * 100  # Estimate 100 leads per page
                    or page_num >= max_pages
                    or (total_pages and page_num >= total_pages)
                )

                if should_save_chunk and chunk_leads:
                    saved_count = await self._save_chunk_to_db(
                        location, chunk_leads, page_num, total_pages
                    )
                    total_saved += saved_count
                    chunk_leads = []  # Clear chunk to free memory

                # Enhanced progress message
                if progress_callback:
                    if total_leads and total_pages:
                        progress_callback(
                            f"Scraping page {page_num} of {total_pages} ({total_saved} leads saved, {len(chunk_leads)} in current chunk)"
                        )
                    else:
                        progress_callback(
                            f"Scraping page {page_num} ({total_saved} leads saved, {len(chunk_leads)} in current chunk)"
                        )

                page_num += 1

                next_button = await page.query_selector('a[aria-label="Next"]')
                if next_button and await next_button.is_enabled():
                    await next_button.click()
                    await page.wait_for_timeout(3000)
                else:
                    break

            # Save any remaining leads in the final chunk
            if chunk_leads:
                saved_count = await self._save_chunk_to_db(
                    location, chunk_leads, page_num, total_pages, is_final=True
                )
                total_saved += saved_count

            if progress_callback:
                progress_callback(
                    f"Completed: Scraped and saved {total_saved} leads from {page_num - 1} pages"
                )

            self.database._log_memory_usage(f"after scraping {location}")

            # Return leads in the expected format for compatibility
            all_leads = self.database.get_leads(location)
            if all_leads and "leads" in all_leads:
                return all_leads["leads"]
            return []

        except Exception as e:
            logger.error(f"Chunked scraper error: {e}")
            return []

    async def _save_chunk_to_db(
        self,
        location: str,
        chunk_leads: list,
        current_page: int,
        total_pages: int,
        is_final: bool = False,
    ):
        """Save a chunk of leads to database"""
        if not chunk_leads:
            return 0

        try:
            chunk_label = "final chunk" if is_final else f"chunk at page {current_page}"
            logger.info(
                f"Saving {chunk_label} with {len(chunk_leads)} leads for {location}"
            )

            # Use database's chunked save but don't clear existing data (we already did that)
            # Save directly without clearing
            import sqlite3
            from datetime import datetime

            with sqlite3.connect(self.database.db_path) as conn:
                current_time = datetime.now().isoformat()
                insert_data = []

                for lead in chunk_leads:
                    # Convert CSV headers to database columns
                    db_lead = {}
                    for key, value in lead.items():
                        db_key = (
                            key.lower()
                            .replace(" ", "_")
                            .replace(".", "")
                            .replace("?", "")
                            .replace("%", "pct")
                        )
                        from database import CSV_COLUMNS

                        if db_key in CSV_COLUMNS:
                            # Handle numeric fields
                            if db_key in [
                                "list_count",
                                "tag_count",
                                "bedrooms",
                                "bathrooms",
                                "year_build",
                                "lead_score",
                            ]:
                                try:
                                    db_lead[db_key] = (
                                        int(value) if value and value != "-" else None
                                    )
                                except ValueError:
                                    db_lead[db_key] = None
                            else:
                                db_lead[db_key] = value if value != "-" else None

                    # Ensure location and timestamps are properly set
                    db_lead["location"] = location
                    db_lead["created_at"] = current_time
                    insert_data.append(db_lead)

                # Build dynamic insert query
                if insert_data:
                    sample_lead = insert_data[0]
                    columns = list(sample_lead.keys())
                    placeholders = ", ".join(["?" for _ in columns])
                    insert_query = f"INSERT INTO leads ({', '.join(columns)}) VALUES ({placeholders})"

                    # Execute batch insert
                    values_list = [
                        [lead.get(col) for col in columns] for lead in insert_data
                    ]
                    conn.executemany(insert_query, values_list)
                    conn.commit()

                    logger.info(f"Saved {len(insert_data)} leads to database")
                    return len(insert_data)

            return 0

        except Exception as e:
            logger.error(f"Failed to save chunk for location {location}: {e}")
            return 0

    async def _scrape_location_legacy(self, location, progress_callback=None):
        """Legacy scraping method that loads all leads into memory (for backward compatibility)"""
        try:
            all_leads = []
            page_num = 1
            max_pages = self.config.MAX_PAGES
            total_leads = None
            total_pages = None

            page = (
                self.context.pages[0]
                if self.context.pages
                else await self.context.new_page()
            )
            search_url = f"{self.config.BATCHLEADS_BASE_URL}app/mylist-new"
            await page.goto(search_url)
            await page.wait_for_timeout(3000)

            if progress_callback:
                progress_callback(f"Searching for location {location}...")

            try:
                location_input = await page.query_selector('input[id="placeInput"]')
                if location_input:
                    await location_input.fill(str(location))
                    await location_input.press("Enter")
                    await page.wait_for_timeout(3000)
            except Exception:
                pass

            # Set rows per page to 100 for more efficient scraping
            try:
                if progress_callback:
                    progress_callback("Setting pagination to 100 rows per page...")

                # Look for the rows per page dropdown
                rows_dropdown = await page.query_selector("select")
                if rows_dropdown:
                    await rows_dropdown.select_option(value="100")
                    await page.wait_for_timeout(2000)
                    logger.info("Set pagination to 100 rows per page")
                else:
                    logger.debug("Rows per page dropdown not found")
            except Exception as e:
                logger.debug(f"Could not set rows per page: {e}")

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

                all_leads.extend(leads)
                logger.info(f"Page {page_num}: Added {len(leads)} leads")

                # Enhanced progress message
                if progress_callback:
                    if total_leads and total_pages:
                        progress_callback(
                            f"Scraping page {page_num} of {total_pages} ({len(all_leads)}/{total_leads} leads)"
                        )
                    else:
                        progress_callback(
                            f"Scraping page {page_num} ({len(all_leads)} leads so far)"
                        )

                page_num += 1

                next_button = await page.query_selector('a[aria-label="Next"]')
                if next_button and await next_button.is_enabled():
                    await next_button.click()
                    await page.wait_for_timeout(3000)
                else:
                    break

            if progress_callback:
                progress_callback(
                    f"Completed: Scraped {len(all_leads)} leads from {page_num - 1} pages"
                )

            self.all_data = all_leads
            return all_leads

        except Exception as e:
            logger.error(f"Legacy scraper error: {e}")
            return []

    async def close(self):
        try:
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
        except Exception as e:
            logger.error(f"Error closing browser: {e}")


async def scrape(location, headless=None, use_cache=True, progress_callback=None):
    config = Config()
    db = Database()

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
            cached_data = db.get_leads(location)

        if cached_data:
            logger.info(f"Using cached data for location {location}")
            if progress_callback:
                progress_callback("Found cached data")
            return cached_data

        if progress_callback:
            progress_callback(f"Scraping data for location {location}...")

        leads = await scraper.scrape_location(location, progress_callback)

        if len(leads) > 0:
            if progress_callback:
                progress_callback("Saving data to database...")

            db.save_leads(location, leads)

            result = {
                "location": location,
                "total_leads": len(leads),
                "leads": leads,
                "cached": False,
                "cache_age_days": 0,
            }

            if progress_callback:
                progress_callback("Scraping completed successfully")

            return result
        else:
            result = {
                "location": location,
                "total_leads": 0,
                "leads": [],
                "cached": False,
                "cache_age_days": 0,
            }

            if progress_callback:
                progress_callback("No leads found for this location")

            return result

    except Exception as e:
        logger.error(f"Error in scrape: {e}")
        if progress_callback:
            progress_callback(f"Error: {str(e)}")
        return {"error": str(e)}
    finally:
        await scraper.close()


if __name__ == "__main__":
    location = "94588"

    async def main():
        result = await scrape(location, use_cache=False)
        if "error" in result:
            print(f"Error: {result['error']}")
        else:
            print(
                f"Location {result['location']}: Found {result.get('total_leads', 0)} leads (cached: {result.get('cached', False)})"
            )

    asyncio.run(main())
