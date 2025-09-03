import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    BATCHLEADS_EMAIL = os.getenv('BATCHLEADS_EMAIL')
    BATCHLEADS_PASSWORD = os.getenv('BATCHLEADS_PASSWORD')
    FILTER_ZIP = os.getenv('FILTER_ZIP')
    IMPLICIT_WAIT = int(os.getenv('IMPLICIT_WAIT', 10))
    PAGE_LOAD_WAIT = int(os.getenv('PAGE_LOAD_WAIT', 5))
    MAX_PAGES = int(os.getenv('MAX_PAGES', 3))
    
    BASE_URL = 'https://app.batchleads.io/'
    
    @classmethod
    def validate(cls):
        required_vars = ['BATCHLEADS_EMAIL', 'BATCHLEADS_PASSWORD', 'FILTER_ZIP']
        missing_vars = [var for var in required_vars if not getattr(cls, var)]
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        return True