import time
import logging
import pandas as pd
import requests
import json
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from scraper.config import Config

logger = logging.getLogger(__name__)

class BatchLeadsScraper:
    def __init__(self, config):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.all_data = []
    
    def login(self):
        try:
            login_data = {
                'email': self.config.BATCHLEADS_EMAIL,
                'password': self.config.BATCHLEADS_PASSWORD
            }
            
            response = self.session.post(
                urljoin(self.config.BASE_URL, 'api/auth/login'),
                json=login_data
            )
            
            if response.status_code == 200:
                logger.info("Login successful")
                return True
            else:
                logger.error(f"Login failed: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Login error: {e}")
            return False
    
    def scrape_leads_by_zip(self, zip_code, page=1):
        try:
            search_params = {
                'zip': zip_code,
                'page': page,
                'limit': 50
            }
            
            response = self.session.get(
                urljoin(self.config.BASE_URL, 'api/leads'),
                params=search_params
            )
            
            if response.status_code == 200:
                data = response.json()
                leads = data.get('leads', [])
                logger.info(f"Found {len(leads)} leads on page {page}")
                return leads
            else:
                logger.error(f"API request failed: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"Scraping error: {e}")
            return []
    
    def scrape_all_pages(self, zip_code):
        all_leads = []
        page = 1
        max_pages = self.config.MAX_PAGES
        
        while page <= max_pages:
            leads = self.scrape_leads_by_zip(zip_code, page)
            
            if not leads:
                break
                
            all_leads.extend(leads)
            logger.info(f"Page {page}: Added {len(leads)} leads")
            page += 1
        
        self.all_data = all_leads
        return all_leads
    
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

def scrape_by_zip(zip_code):
    config = Config()
    scraper = BatchLeadsScraper(config)
    
    if not scraper.login():
        return {"error": "Login failed"}
    
    leads = scraper.scrape_all_pages(zip_code)
    
    return {
        "zip_code": zip_code,
        "total_leads": len(leads),
        "leads": leads
    }