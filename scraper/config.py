import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    BATCHLEADS_EMAIL = os.getenv('BATCHLEADS_EMAIL')
    BATCHLEADS_PASSWORD = os.getenv('BATCHLEADS_PASSWORD')
    MAX_PAGES = int(os.getenv('MAX_PAGES', 99))
    
    BASE_URL = 'https://app.batchleads.io/'

    HEADLESS = os.getenv('HEADLESS', 'true').lower() in ('true', '1', 't', 'yes')
    
    @classmethod
    def validate(cls):
        required_vars = ['BATCHLEADS_EMAIL', 'BATCHLEADS_PASSWORD']
        missing_vars = [var for var in required_vars if not getattr(cls, var)]
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        return True