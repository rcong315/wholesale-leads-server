import time
import logging
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException, NoSuchElementException

logger = logging.getLogger(__name__)

class BatchLeadsScraper:
    def __init__(self, driver, config):
        self.driver = driver
        self.config = config
        self.wait = WebDriverWait(driver, config.IMPLICIT_WAIT)
        self.all_data = []
    
    def apply_city_filter(self):
        """Apply city filter to the data table"""
        try:
            logger.info(f"Applying zip filter: {self.config.FILTER_ZIP}")
            
            # Updated with actual search input filter based on HTML structure
            from selenium.webdriver.common.keys import Keys
            
            city_filter_input = self.wait.until(
                EC.presence_of_element_located((By.ID, "placeInput"))
            )
            city_filter_input.clear()
            city_filter_input.send_keys(self.config.FILTER_ZIP)
            city_filter_input.send_keys(Keys.ENTER)
            
            # Wait for filter to be applied
            time.sleep(self.config.PAGE_LOAD_WAIT)
            logger.info("City filter applied successfully")
            return True
            
        except TimeoutException:
            logger.error("City filter elements not found")
            return False
        except Exception as e:
            logger.error(f"Failed to apply city filter: {e}")
            return False
    
    def scrape_table_data(self):
        """Scrape data from the current page table"""
        try:
            # Updated table selector based on actual HTML structure
            table = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.row-border.hover"))
            )
            
            # Get table headers
            headers = []
            header_elements = table.find_elements(By.CSS_SELECTOR, "thead th")
            for header in header_elements:
                header_text = header.text.strip()
                # Skip lead status column as it contains icons
                if header_text.lower() in ['', 'lead status', 'status', 'lead']:
                    headers.append(None)  # Placeholder to maintain column alignment
                else:
                    headers.append(header_text)
            headers.pop(0)
            
            # Get table rows
            rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
            page_data = []
            
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                row_data = {}
                
                for i, cell in enumerate(cells):
                    if i < len(headers) and headers[i] is not None:  # Skip None placeholders (icon columns)
                        row_data[headers[i]] = cell.text.strip()
                
                if row_data:  # Only add non-empty rows
                    # Log important columns for each row
                    important_cols = ['Address']
                    log_data = {}
                    for col in important_cols:
                        if col in row_data and row_data[col]:
                            log_data[col] = row_data[col]
                    
                    if log_data:
                        logger.info(f"Row {len(page_data)+1}: {log_data}")
                    else:
                        # If no important columns found, log first few available columns
                        first_few = dict(list(row_data.items())[:1])
                        logger.info(f"Row {len(page_data)+1}: {first_few}")
                    
                    page_data.append(row_data)
            
            logger.info(f"Scraped {len(page_data)} records from current page")
            return page_data, headers
            
        except TimeoutException:
            logger.error("Table not found on page")
            return [], []
        except Exception as e:
            logger.error(f"Failed to scrape table data: {e}")
            return [], []
    
    def has_next_page(self):
        """Check if there's a next page available"""
        try:
            # Updated with actual pagination selectors for ngb-pagination
            next_button = self.driver.find_element(By.CSS_SELECTOR, "ngb-pagination a[aria-label='Next']")
            return "disabled" not in next_button.get_attribute("class")
        except NoSuchElementException:
            # Try alternative pagination selector
            try:
                # Try alternative selector - looking for next button in pagination controls
                next_link = self.driver.find_element(By.CSS_SELECTOR, ".ctableview_pagination .pagination .page-item:last-child a")
                parent_element = next_link.find_element(By.XPATH, "..")
                return "disabled" not in parent_element.get_attribute("class")
            except NoSuchElementException:
                return False
    
    def go_to_next_page(self):
        """Navigate to the next page"""
        try:
            # Updated with actual next page button selector for ngb-pagination
            next_button = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "ngb-pagination a[aria-label='Next']"))
            )
            next_button.click()
            
            # Wait for page to load
            time.sleep(self.config.PAGE_LOAD_WAIT)
            
            # Wait for table to be present on new page
            self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.row-border.hover"))
            )
            
            logger.info("Successfully navigated to next page")
            return True
            
        except TimeoutException:
            logger.error("Failed to navigate to next page")
            return False
        except Exception as e:
            logger.error(f"Navigation to next page failed: {e}")
            return False
    
    def scrape_all_pages(self):
        """Scrape data from limited number of pages"""
        try:
            page_number = 1
            headers = []
            max_pages = self.config.MAX_PAGES
            
            while page_number <= max_pages:
                logger.info(f"Scraping page {page_number}/{max_pages}...")
                
                page_data, page_headers = self.scrape_table_data()
                
                if not headers and page_headers:
                    headers = page_headers
                
                if page_data:
                    self.all_data.extend(page_data)
                    logger.info(f"Added {len(page_data)} records from page {page_number}")
                
                # Stop if we've reached the max pages
                if page_number >= max_pages:
                    logger.info(f"Reached maximum pages limit ({max_pages}), stopping")
                    break
                
                # Check if there's a next page
                if not self.has_next_page():
                    logger.info("No more pages available")
                    break
                
                # Navigate to next page
                if not self.go_to_next_page():
                    logger.warning("Failed to navigate to next page, stopping")
                    break
                
                page_number += 1
            
            logger.info(f"Scraping complete. Total records: {len(self.all_data)}")
            return self.all_data, headers
            
        except Exception as e:
            logger.error(f"Failed to scrape all pages: {e}")
            return self.all_data, headers
    
    def save_to_csv(self, filename="batchleads_data.csv"):
        """Save scraped data to CSV file"""
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