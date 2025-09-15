import time
import logging
import pandas as pd
import json
import os
import asyncio
from datetime import datetime
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

from scraper.config import Config

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
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-accelerated-2d-canvas',
                    '--no-first-run',
                    '--no-zygote',
                    '--disable-gpu'
                ] if use_headless else []
            )
            self.context = await self.browser.new_context(
                user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080}
            )

        except Exception as e:
            logger.error(f"Failed to launch browser: {e}")
            raise(e)
    
    async def login(self):
        try:
            page = await self.context.new_page()

            await page.goto(f"{self.config.BASE_URL}login")
            await page.wait_for_timeout(3000)
            
            try:
                await page.wait_for_selector('input[formcontrolname="email"]', timeout=3000)
                await page.fill('input[formcontrolname="email"]', self.config.BATCHLEADS_EMAIL)
                logger.info("Email filled using selector: input[formcontrolname=\"email\"]")
            except:
                logger.error("Could not find email input field")
                return False
            
            try:
                await page.wait_for_selector('input[formcontrolname="password"]', timeout=3000)
                await page.fill('input[formcontrolname="password"]', self.config.BATCHLEADS_PASSWORD)
                logger.info("Password filled using selector: input[formcontrolname=\"password\"]")
            except:
                logger.error("Could not find password input field")
                return False
            
            try:
                await page.wait_for_selector('button[type="submit"]', timeout=3000)
                await page.click('button[type="submit"]')
                logger.info("Submit clicked using selector: button[type=\"submit\"]")
            except:
                logger.error("Could not find or click submit button")
                return False
            
            try:
                await page.wait_for_url(lambda url: 'login' not in url, timeout=10000)
            except:
                await page.wait_for_load_state('networkidle', timeout=10000)
            
            return True
            
        except Exception as e:
            logger.error(f"Login error: {e}")
            return False
    
    async def scrape_leads_table(self, page, page_num=1):
        try:
            html_content = await page.content()
            soup = BeautifulSoup(html_content, 'html.parser')
            leads_data = []
            
            tables = soup.find_all('table')
            for table in tables:
                headers = []
                
                # Get headers
                header_row = table.find('thead')
                if header_row:
                    headers = [th.get_text().strip() for th in header_row.find_all(['th', 'td'])][3:]
                
                # Get data rows
                tbody = table.find('tbody') or table
                data_rows = tbody.find_all('tr')
                
                for row in data_rows:
                    cells = [td.get_text().strip() for td in row.find_all(['td', 'th'])][2:]
                    if cells and len(cells) > 1:
                        lead = dict(zip(headers, cells))
                        leads_data.append(lead)
                        logger.debug(f"Extracted lead: {lead['Property Address']}")
            
            logger.info(f"Found {len(leads_data)} leads on page {page_num}")
            return leads_data
            
        except Exception as e:
            logger.error(f"Scraping error: {e}")
            return []
    
    async def scrape_zip_code(self, zip_code):
        try:
            all_leads = []
            page_num = 1
            max_pages = self.config.MAX_PAGES

            page = await self.context.new_page()
            search_url = f"{self.config.BASE_URL}app/mylist-new"
            await page.goto(search_url)
            await page.wait_for_timeout(3000)
            
            try:
                zip_input = await page.query_selector('input[id="placeInput"]')
                if zip_input:
                    await zip_input.fill(str(zip_code))
                    await zip_input.press("Enter")
                    await page.wait_for_timeout(3000)
            except Exception:
                pass
            
            while page_num <= max_pages:
                leads = await self.scrape_leads_table(page, page_num)
                
                if not leads:
                    break
                    
                all_leads.extend(leads)
                logger.info(f"Page {page_num}: Added {len(leads)} leads")
                page_num += 1

                next_button = await page.query_selector('a[aria-label="Next"]')
                if next_button and await next_button.is_enabled():
                    await next_button.click()
                    await page.wait_for_timeout(3000)
                else:
                    break
            
            self.all_data = all_leads
            return all_leads
            
        except Exception as e:
            logger.error(f"Scraper error: {e}")
            return []
    
    def save_to_json(self, filename="leads_data.json"):
        if not self.all_data:
            logger.warning("No data to save")
            return False
        
        try:
            cache_data = {
                "timestamp": datetime.now().isoformat(),
                "leads": self.all_data
            }
            with open(filename, 'w') as f:
                json.dump(cache_data, f, indent=2)
            logger.info(f"Data saved to {filename}")
            return True
        except Exception as e:
            logger.error(f"Failed to save data to JSON: {e}")
            return False
    
    def save_to_csv(self, filename="batchleads_data.csv"):
        if not self.all_data:
            logger.warning("No data to save")
            return False
        
        try:
            df = pd.DataFrame(self.all_data)
            df.to_csv(filename, index=False)
            logger.info(f"Data saved to {filename}")
            return True
        except Exception as e:
            logger.error(f"Failed to save data to CSV: {e}")
            return False
        
    async def close(self):
        if self.browser:
            await self.browser.close()
        
def load_cache(zip_code):
    config = Config()

    data_dir = "scraped_data"
    filename = os.path.join(data_dir, f"batchleads_data_{zip_code}.json")

    if os.path.exists(filename):
        try:
            with open(filename, 'r') as f:
                cached_file = json.load(f)
            cache_timestamp = datetime.fromisoformat(cached_file["timestamp"])
            cached_data = cached_file["leads"]

            # Check if cache is still valid
            if cached_data is not None:
                cache_age_days = (datetime.now() - cache_timestamp).days
                if cache_age_days < config.CACHE_EXPIRATION_DAYS:
                    logger.info(f"Returning cached data for zip code {zip_code} (age: {cache_age_days} days)")
                    return {
                        "zip_code": zip_code,
                        "total_leads": len(cached_data),
                        "leads": cached_data,
                        "cached": True,
                        "cache_age_days": cache_age_days
                    }
                else:
                    logger.info(f"Cache expired for zip code {zip_code} (age: {cache_age_days} days)")
        except (KeyError, TypeError, ValueError):
            logger.warning(f"Malformed cache file for zip code {zip_code}, ignoring cache")
    return None

async def scrape(zip_codes, headless=None, use_cache=True):
    config = Config()
    
    # Create data directory if it doesn't exist
    data_dir = "scraped_data"
    os.makedirs(data_dir, exist_ok=True)

    scraper = BatchLeadsScraper(config)
    await scraper.init_browser(headless=headless)
    await scraper.login()

    for zip_code in zip_codes:
        if use_cache:
            cached_data = load_cache(zip_code)
        if cached_data:
            logger.info(f"Using cached data for zip code {zip_code}")
            yield cached_data
            continue

        else:        
            # If no cache or cache loading failed, scrape fresh data
            try:
                leads = await scraper.scrape_zip_code(zip_code)
                if len(leads) > 0:
                    csv_filename = os.path.join(data_dir, f"batchleads_data_{zip_code}.csv")
                    scraper.save_to_csv(csv_filename)
                    json_filename = os.path.join(data_dir, f"batchleads_data_{zip_code}.json")
                    scraper.save_to_json(json_filename)
                yield {
                    "zip_code": zip_code,
                    "total_leads": len(leads),
                    "leads": leads,
                    "cached": False,
                    "cache_age_days": 0
                }
            except Exception as e:
                logger.error(f"Error in scrape: {e}")
                yield {"error": str(e)}

    await scraper.close()

if __name__ == "__main__":
    zip_codes = ["92618", "94588", "90001"]
    async def main():
        async for result in scrape(zip_codes, use_cache=False):
            if "error" in result:
                print(f"Error: {result['error']}")
            else:
                print(f"Zip {result['zip_code']}: Found {result.get('total_leads', 0)} leads (cached: {result.get('cached', False)})")
    asyncio.run(main())
