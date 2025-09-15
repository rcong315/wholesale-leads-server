import os
from dotenv import load_dotenv

load_dotenv()


class Config:

    GOOGLE_DRIVE_DIR_ID = os.getenv("GOOGLE_DRIVE_DIR_ID", "XXX")

    # Cache expiration in days (default: 7 days)
    CACHE_EXPIRATION_DAYS = int(os.getenv("CACHE_EXPIRATION_DAYS", 7))
