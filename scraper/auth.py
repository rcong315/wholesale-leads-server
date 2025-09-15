import time
import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

logger = logging.getLogger(__name__)


class BatchLeadsAuth:
    def __init__(self, driver, config):
        self.driver = driver
        self.driver.set_page_load_timeout(config.PAGE_LOAD_WAIT)
        self.config = config
        self.wait = WebDriverWait(driver, config.IMPLICIT_WAIT)

    def login(self):
        """Login to BatchLeads with credentials from config"""
        try:
            logger.info("Navigating to BatchLeads login page...")
            self.driver.get(self.config.BATCHLEADS_BASE_URL)

        except TimeoutException:
            pass
        except NoSuchElementException as e:
            logger.error(f"Login element not found: {e}")
            return False
        except Exception as e:
            logger.error(f"Login failed: {e}")
            return False

        # Updated selectors based on actual login form HTML
        email_field = self.wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "input[formcontrolname='email']")
            )
        )
        password_field = self.driver.find_element(
            By.CSS_SELECTOR, "input[formcontrolname='password']"
        )
        login_button = self.driver.find_element(
            By.CSS_SELECTOR, "button[type='submit']"
        )

        logger.info("Entering login credentials...")
        email_field.clear()
        email_field.send_keys(self.config.BATCHLEADS_EMAIL)

        password_field.clear()
        password_field.send_keys(self.config.BATCHLEADS_PASSWORD)

        logger.info("Clicking login button...")
        login_button.click()

        # Wait for login to complete - look for dashboard sidebar element
        self.wait.until(EC.presence_of_element_located((By.ID, "app-side-bar")))

        logger.info("Login successful!")
        return True

    def navigate_to_my_lists(self):
        """Navigate to My Lists page"""
        try:
            logger.info("Navigating to My Lists...")

            # Updated navigation to My Lists using the sidebar
            my_lists_link = self.wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//span[@tooltip='My Lists']//parent::a")
                )
            )
            my_lists_link.click()

            # Wait for My Lists page to load - look for the main content area
            self.wait.until(
                EC.presence_of_element_located((By.CLASS_NAME, "main_content"))
            )

            time.sleep(self.config.PAGE_LOAD_WAIT)
            logger.info("Successfully navigated to My Lists")
            return True

        except TimeoutException:
            logger.error("Navigation to My Lists timeout")
            return False
        except Exception as e:
            logger.error(f"Navigation to My Lists failed: {e}")
            return False
