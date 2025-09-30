import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    BATCHLEADS_EMAIL = os.getenv("BATCHLEADS_EMAIL")
    BATCHLEADS_PASSWORD = os.getenv("BATCHLEADS_PASSWORD")
    MAX_PAGES = int(os.getenv("MAX_PAGES", 9999))

    BATCHLEADS_BASE_URL = os.getenv("BATCHLEADS_BASE_URL", "https://app.batchleads.io")

    HEADLESS = os.getenv("HEADLESS", "true").lower() in ("true", "1", "t", "yes")

    # Memory management settings
    SCRAPER_CHUNK_SIZE = int(os.getenv("SCRAPER_CHUNK_SIZE", 10))  # Process 10 pages at a time
    DB_CHUNK_SIZE = int(os.getenv("DB_CHUNK_SIZE", 500))  # Save 500 leads at a time

    @classmethod
    def validate(cls):
        required_vars = ["BATCHLEADS_EMAIL", "BATCHLEADS_PASSWORD"]
        missing_vars = [var for var in required_vars if not getattr(cls, var)]

        if missing_vars:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )

        return True
