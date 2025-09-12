import time
import logging
import pandas as pd
import json
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

from scraper.config import Config

logger = logging.getLogger(__name__)

class BatchLeadsScraper:
    def __init__(self, config):
        self.config = config
        self.all_data = []
    
    async def login(self, page):
        try:
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
    
    async def scrape_leads_by_zip(self, page, zip_code, page_num=1):
        try:
            # Get the page HTML content
            html_content = await page.content()
            
            # Parse HTML with BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            leads_data = []
            
            # Method 1: Look for table structures
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
    
    async def scrape_all_pages(self, zip_code, headless=None):
        browser = None
        try:
            playwright = await async_playwright().start()
            use_headless = headless if headless is not None else self.config.HEADLESS
            browser = await playwright.chromium.launch(
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
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            page = await context.new_page()
            
            if not await self.login(page):
                return []
            
            all_leads = []
            page_num = 1
            max_pages = self.config.MAX_PAGES

            search_url = f"{self.config.BASE_URL}app/mylist-new"
            await page.goto(search_url)
            await page.wait_for_timeout(3000)
            
            # Fill in the ZIP code filter if available
            try:
                zip_input = await page.query_selector('input[id="placeInput"]')
                if zip_input:
                    await zip_input.fill(str(zip_code))
                    await page.wait_for_timeout(500)
                    await zip_input.press("Enter")
                    await page.wait_for_timeout(3000)
            except Exception:
                pass
            
            while page_num <= max_pages:
                leads = await self.scrape_leads_by_zip(page, zip_code, page_num)
                
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
        finally:
            if browser:
                await browser.close()
    
    def save_to_json(self, filename="leads_data.json"):
        if not self.all_data:
            logger.warning("No data to save")
            return False
        
        try:
            with open(filename, 'w') as f:
                json.dump(self.all_data, f, indent=2)
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

async def scrape_by_zip(zip_code, headless=None):
    config = Config()
    scraper = BatchLeadsScraper(config)
    
    try:
        leads = await scraper.scrape_all_pages(zip_code, headless=headless)
        return {
            "zip_code": zip_code,
            "total_leads": len(leads),
            "leads": leads
        }
    except Exception as e:
        logger.error(f"Error in scrape_by_zip: {e}")
        return {"error": str(e)}
    
if __name__ == "__main__":
    import asyncio
    zip_code = "92618"
    result = asyncio.run(scrape_by_zip(zip_code))
    print(f"Found {result.get('total_leads', 0)} leads")
