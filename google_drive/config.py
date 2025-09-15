import os
from dotenv import load_dotenv

load_dotenv()


class Config:

    # Scraped data directory (default: scraped_data)
    SCRAPED_DATA_DIR = os.getenv("SCRAPED_DATA_DIR", "scraped_data")

    GOOGLE_DRIVE_DIR_ID = os.getenv("GOOGLE_DRIVE_DIR_ID", "XXX")
