import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    BATCHLEADS_EMAIL = os.getenv("BATCHLEADS_EMAIL")
    BATCHLEADS_PASSWORD = os.getenv("BATCHLEADS_PASSWORD")
    MAX_PAGES = int(os.getenv("MAX_PAGES", 999))

    BATCHLEADS_BASE_URL = os.getenv("BATCHLEADS_BASE_URL", "https://app.batchleads.io")

    HEADLESS = os.getenv("HEADLESS", "true").lower() in ("true", "1", "t", "yes")

    # Periodic writing configuration
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", 500))  # Number of leads before writing
    WRITE_THRESHOLD_MB = float(
        os.getenv("WRITE_THRESHOLD_MB", 2.0)
    )  # Memory threshold in MB

    @classmethod
    def validate(cls):
        import logging

        logger = logging.getLogger(__name__)

        required_vars = ["BATCHLEADS_EMAIL", "BATCHLEADS_PASSWORD"]
        missing_vars = [var for var in required_vars if not getattr(cls, var)]

        if missing_vars:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )

        # Log configuration for visibility
        logger.info("📋 SCRAPER CONFIGURATION:")
        logger.info(f"   • Batch size: {cls.BATCH_SIZE} leads")
        logger.info(f"   • Memory threshold: {cls.WRITE_THRESHOLD_MB} MB")
        logger.info(f"   • Max pages: {cls.MAX_PAGES}")
        logger.info(f"   • Headless mode: {cls.HEADLESS}")
        logger.info(f"   • Base URL: {cls.BATCHLEADS_BASE_URL}")

        return True
