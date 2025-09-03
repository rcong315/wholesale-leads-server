#!/usr/bin/env python3
import logging
import sys
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from config import Config
from auth import BatchLeadsAuth
from batchleads_scraper import BatchLeadsScraper

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('batchleads_scraper.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def setup_driver(headless=False):
    """Setup Chrome WebDriver with options"""
    chrome_options = Options()
    
    if headless:
        chrome_options.add_argument("--headless")
    
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.implicitly_wait(Config.IMPLICIT_WAIT)
    
    return driver

def main():
    """Main scraping function"""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        # Validate configuration
        logger.info("Validating configuration...")
        Config.validate()
        logger.info("Configuration validated successfully")
        
        # Setup WebDriver
        logger.info("Setting up WebDriver...")
        driver = setup_driver(headless=False)  # Set to True for headless mode
        
        try:
            # Initialize modules
            auth = BatchLeadsAuth(driver, Config)
            scraper = BatchLeadsScraper(driver, Config)
            
            # Step 1: Login
            logger.info("Step 1: Logging into BatchLeads...")
            if not auth.login():
                logger.error("Login failed. Please check your credentials.")
                return False
            
            # Step 2: Navigate to My Lists
            logger.info("Step 2: Navigating to My Lists...")
            if not auth.navigate_to_my_lists():
                logger.error("Failed to navigate to My Lists")
                return False
            
            # Step 3: Apply city filter
            logger.info("Step 3: Applying city filter...")
            if not scraper.apply_city_filter():
                logger.error("Failed to apply city filter")
                return False
            
            # Step 4: Scrape all data
            logger.info("Step 4: Scraping data from all pages...")
            all_data, headers = scraper.scrape_all_pages()
            
            if not all_data:
                logger.warning("No data was scraped")
                return False
            
            # Step 5: Save to CSV
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"batchleads_data_{Config.FILTER_ZIP}_{timestamp}.csv"
            
            logger.info("Step 5: Saving data to CSV...")
            if scraper.save_to_csv(filename):
                logger.info(f"✅ Scraping completed successfully!")
                logger.info(f"📊 Total records scraped: {len(all_data)}")
                logger.info(f"📁 Data saved to: {filename}")
                return True
            else:
                logger.error("Failed to save data to CSV")
                return False
                
        finally:
            logger.info("Closing WebDriver...")
            driver.quit()
            
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)