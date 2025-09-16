import logging
import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

from scraper.config import Config
from google_drive.api import GoogleDriveAPI

logger = logging.getLogger(__name__)


class BatchLeadsScraper:
    def __init__(self, config=None):
        self.config = config or Config()
        self.all_data = []
        self.browser = None
        self.context = None

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
                            total_leads = int(parts[1].strip())
                            # Extract current range
                            range_part = parts[0].strip()
                            if " - " in range_part:
                                range_parts = range_part.split(" - ")
                                start_lead = int(range_parts[0].strip())
                                end_lead = int(range_parts[1].strip())
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

    async def scrape_zip_code(self, zip_code, progress_callback=None):
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
            logger.error(f"Scraper error: {e}")
            return []

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

        leads = await scraper.scrape_zip_code(zip_code, progress_callback)

        if len(leads) > 0:
            if progress_callback:
                progress_callback("Saving data to cache...")

            if drive_api:
                drive_api.save_cache(zip_code, leads)

            result = {
                "zip_code": zip_code,
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
