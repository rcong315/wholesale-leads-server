import time
import logging
import pandas as pd
import json
from playwright.async_api import async_playwright

from scraper.config import Config

logger = logging.getLogger(__name__)

class BatchLeadsScraper:
    def __init__(self, config):
        self.config = config
        self.all_data = []
    
    async def login(self, page):
        try:
            await page.goto(f"{self.config.BASE_URL}login")
            await page.wait_for_load_state('networkidle')
            
            # Try multiple common selectors for email input
            email_selectors = [
                'input[type="email"]',
                'input[name="email"]',
                'input[id="email"]',
                'input[placeholder*="email" i]',
                'input[placeholder*="Email" i]',
                '#email',
                '.email-input',
                'input:first-of-type'
            ]
            
            email_filled = False
            for selector in email_selectors:
                try:
                    await page.wait_for_selector(selector, timeout=3000)
                    await page.fill(selector, self.config.BATCHLEADS_EMAIL)
                    email_filled = True
                    logger.info(f"Email filled using selector: {selector}")
                    break
                except:
                    continue
            
            if not email_filled:
                logger.error("Could not find email input field")
                return False
            
            # Try multiple common selectors for password input
            password_selectors = [
                'input[type="password"]',
                'input[name="password"]',
                'input[id="password"]',
                '#password',
                '.password-input'
            ]
            
            password_filled = False
            for selector in password_selectors:
                try:
                    await page.wait_for_selector(selector, timeout=3000)
                    await page.fill(selector, self.config.BATCHLEADS_PASSWORD)
                    password_filled = True
                    logger.info(f"Password filled using selector: {selector}")
                    break
                except:
                    continue
            
            if not password_filled:
                logger.error("Could not find password input field")
                return False
            
            # Try multiple common selectors for submit button
            submit_selectors = [
                'button[type="submit"]',
                'input[type="submit"]',
                'button:has-text("Log in")',
                'button:has-text("Login")',
                'button:has-text("Sign in")',
                '.login-button',
                '.submit-button'
            ]
            
            submit_clicked = False
            for selector in submit_selectors:
                try:
                    await page.wait_for_selector(selector, timeout=3000)
                    await page.click(selector)
                    submit_clicked = True
                    logger.info(f"Submit clicked using selector: {selector}")
                    break
                except:
                    continue
            
            if not submit_clicked:
                logger.error("Could not find submit button")
                return False
            
            await page.wait_for_load_state('networkidle')
            
            if 'login' in page.url:
                logger.error("Login failed - still on login page")
                return False
                
            logger.info("Login successful")
            return True
            
        except Exception as e:
            logger.error(f"Login error: {e}")
            return False
    
    async def scrape_leads_by_zip(self, page, zip_code, page_num=1):
        try:
            search_url = f"{self.config.BASE_URL}leads?zip={zip_code}&page={page_num}&limit=50"
            await page.goto(search_url)
            await page.wait_for_load_state('networkidle')
            
            await page.wait_for_timeout(3000)
            
            leads_data = await page.evaluate("""
                () => {
                    const leads = [];
                    
                    const leadRows = document.querySelectorAll('[data-testid="lead-row"], .lead-item, .property-row, tr[data-lead-id]');
                    if (leadRows.length > 0) {
                        leadRows.forEach(row => {
                            const lead = {};
                            
                            const addressEl = row.querySelector('[data-field="address"], .address, .property-address');
                            if (addressEl) lead.property_address = addressEl.textContent.trim();
                            
                            const cityEl = row.querySelector('[data-field="city"], .city');
                            if (cityEl) lead.city = cityEl.textContent.trim();
                            
                            const stateEl = row.querySelector('[data-field="state"], .state');
                            if (stateEl) lead.state = stateEl.textContent.trim();
                            
                            const zipEl = row.querySelector('[data-field="zip"], .zip');
                            if (zipEl) lead.zip = zipEl.textContent.trim();
                            
                            const ownerEl = row.querySelector('[data-field="owner"], .owner, .owner-name');
                            if (ownerEl) lead.owner = ownerEl.textContent.trim();
                            
                            const valueEl = row.querySelector('[data-field="value"], .value, .property-value');
                            if (valueEl) lead.value = valueEl.textContent.trim();
                            
                            const phoneEl = row.querySelector('[data-field="phone"], .phone');
                            if (phoneEl) lead.phone = phoneEl.textContent.trim();
                            
                            const emailEl = row.querySelector('[data-field="email"], .email');
                            if (emailEl) lead.email = emailEl.textContent.trim();
                            
                            if (Object.keys(lead).length > 0) {
                                leads.push(lead);
                            }
                        });
                    } else {
                        const tableRows = document.querySelectorAll('table tbody tr');
                        if (tableRows.length > 0) {
                            const headers = Array.from(document.querySelectorAll('table thead th')).map(th => th.textContent.trim().toLowerCase());
                            
                            tableRows.forEach(row => {
                                const cells = Array.from(row.querySelectorAll('td'));
                                if (cells.length === headers.length) {
                                    const lead = {};
                                    cells.forEach((cell, index) => {
                                        const header = headers[index];
                                        if (header) {
                                            lead[header] = cell.textContent.trim();
                                        }
                                    });
                                    if (Object.keys(lead).length > 0) {
                                        leads.push(lead);
                                    }
                                }
                            });
                        }
                    }
                    
                    return leads;
                }
            """)
            
            logger.info(f"Found {len(leads_data)} leads on page {page_num}")
            return leads_data
            
        except Exception as e:
            logger.error(f"Scraping error: {e}")
            return []
    
    async def scrape_all_pages(self, zip_code):
        browser = None
        try:
            playwright = await async_playwright().start()
            browser = await playwright.chromium.launch(headless=self.config.HEADLESS)
            context = await browser.new_context()
            page = await context.new_page()
            
            if not await self.login(page):
                return []
            
            all_leads = []
            page_num = 1
            max_pages = self.config.MAX_PAGES
            
            while page_num <= max_pages:
                leads = await self.scrape_leads_by_zip(page, zip_code, page_num)
                
                if not leads:
                    break
                    
                all_leads.extend(leads)
                logger.info(f"Page {page_num}: Added {len(leads)} leads")
                page_num += 1
            
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

async def scrape_by_zip(zip_code):
    config = Config()
    scraper = BatchLeadsScraper(config)
    
    try:
        leads = await scraper.scrape_all_pages(zip_code)
        return {
            "zip_code": zip_code,
            "total_leads": len(leads),
            "leads": leads
        }
    except Exception as e:
        logger.error(f"Error in scrape_by_zip: {e}")
        return {"error": str(e)}