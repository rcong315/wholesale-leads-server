from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from datetime import datetime, date
import os
from dotenv import load_dotenv
import random
import string

# Load environment variables
load_dotenv()

# Initialize FastAPI app
app = FastAPI(
    title="Wholesale Leads API",
    description="API for managing wholesale leads data",
    version="1.0.0"
)

# Configure CORS
allowed_origins = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration from environment
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
AUTH_TOKEN = os.getenv("AUTH_TOKEN")
DROPDOWN_OPTIONS = os.getenv("DROPDOWN_OPTIONS", "Option 1,Option 2,Option 3").split(",")
TABLE_COLUMNS_CONFIG = os.getenv("TABLE_COLUMNS", "id:int,name:str,value:float").split(",")

# Parse table columns configuration
def parse_table_columns():
    columns = {}
    for col_config in TABLE_COLUMNS_CONFIG:
        if ":" in col_config:
            name, col_type = col_config.split(":")
            columns[name.strip()] = col_type.strip()
    return columns

TABLE_COLUMNS = parse_table_columns()

# Request/Response Models
class DropdownOptionsResponse(BaseModel):
    options: List[str]

class TableDataRequest(BaseModel):
    selection: str

class TableDataResponse(BaseModel):
    data: List[Dict[str, Any]]

# Authentication dependency
async def verify_authentication(
    x_api_key: Optional[str] = Header(None),
    x_api_secret: Optional[str] = Header(None),
    authorization: Optional[str] = Header(None)
):
    """Verify API authentication if configured"""
    # If no authentication is configured, allow access
    if not API_KEY and not API_SECRET and not AUTH_TOKEN:
        return True
    
    # Check API Key
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")
    
    # Check API Secret
    if API_SECRET and x_api_secret != API_SECRET:
        raise HTTPException(status_code=401, detail="Invalid API Secret")
    
    # Check Auth Token
    if AUTH_TOKEN:
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Missing or invalid authorization token")
        token = authorization.replace("Bearer ", "")
        if token != AUTH_TOKEN:
            raise HTTPException(status_code=401, detail="Invalid authentication token")
    
    return True

# Helper function to generate sample data
def generate_sample_data(selection: str, num_records: int = 10) -> List[Dict[str, Any]]:
    """Generate sample data based on the table columns configuration"""
    data = []
    
    for i in range(num_records):
        record = {}
        for col_name, col_type in TABLE_COLUMNS.items():
            if col_type == "int":
                record[col_name] = i + 1 if col_name == "id" else random.randint(1, 100)
            elif col_type == "float":
                record[col_name] = round(random.uniform(0, 100), 2)
            elif col_type == "bool":
                record[col_name] = random.choice([True, False])
            elif col_type == "date":
                # Generate random date in 2024
                month = random.randint(1, 12)
                day = random.randint(1, 28)  # Safe for all months
                record[col_name] = f"2024-{month:02d}-{day:02d}"
            else:  # Default to string
                if col_name == "company_name":
                    record[col_name] = f"{selection} Company {i+1}"
                elif col_name == "contact_name":
                    first_names = ["John", "Jane", "Bob", "Alice", "Charlie", "Diana"]
                    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Davis"]
                    record[col_name] = f"{random.choice(first_names)} {random.choice(last_names)}"
                elif col_name == "email":
                    record[col_name] = f"contact{i+1}@{selection.lower().replace(' ', '')}.com"
                elif col_name == "phone":
                    record[col_name] = f"({random.randint(100, 999)}) {random.randint(100, 999)}-{random.randint(1000, 9999)}"
                elif col_name == "address":
                    record[col_name] = f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Elm', 'Market', 'First'])} Street"
                elif col_name == "city":
                    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia"]
                    record[col_name] = random.choice(cities)
                elif col_name == "state":
                    states = ["NY", "CA", "IL", "TX", "AZ", "PA"]
                    record[col_name] = random.choice(states)
                elif col_name == "zip_code":
                    record[col_name] = f"{random.randint(10000, 99999)}"
                elif col_name == "status":
                    statuses = ["New", "Contacted", "Qualified", "Proposal", "Closed"]
                    record[col_name] = random.choice(statuses)
                elif col_name == "notes":
                    record[col_name] = f"Lead from {selection}. Follow up required."
                else:
                    record[col_name] = f"{col_name}_{i+1}"
        
        data.append(record)
    
    return data

# API Endpoints
@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Wholesale Leads API",
        "version": "1.0.0",
        "endpoints": [
            "/api/dropdown-options",
            "/api/table-data"
        ]
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/api/dropdown-options", response_model=DropdownOptionsResponse)
async def get_dropdown_options(auth: bool = Depends(verify_authentication)):
    """
    Get the list of options for the dropdown menu.
    The options are configured in the .env file.
    """
    # Clean up the options (remove extra whitespace)
    options = [option.strip() for option in DROPDOWN_OPTIONS]
    return DropdownOptionsResponse(options=options)

@app.post("/api/table-data", response_model=TableDataResponse)
async def get_table_data(
    request: TableDataRequest,
    auth: bool = Depends(verify_authentication)
):
    """
    Get table data based on the selected dropdown option.
    Returns a list of records with configurable columns.
    """
    # Validate that the selection is a valid option
    valid_options = [option.strip() for option in DROPDOWN_OPTIONS]
    if request.selection not in valid_options:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid selection. Must be one of: {', '.join(valid_options)}"
        )
    
    # Generate sample data based on selection
    # In a real application, this would query a database
    data = generate_sample_data(request.selection, num_records=15)
    
    return TableDataResponse(data=data)

@app.get("/api/config")
async def get_configuration(auth: bool = Depends(verify_authentication)):
    """
    Get the current table configuration.
    This endpoint is useful for debugging and understanding the data structure.
    """
    return {
        "dropdown_options": [option.strip() for option in DROPDOWN_OPTIONS],
        "table_columns": TABLE_COLUMNS,
        "authentication_required": bool(API_KEY or API_SECRET or AUTH_TOKEN)
    }

# Run the application
if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("PORT", 8000))
    host = os.getenv("HOST", "0.0.0.0")
    
    print(f"Starting server on {host}:{port}")
    print(f"Configured dropdown options: {DROPDOWN_OPTIONS}")
    print(f"Configured table columns: {list(TABLE_COLUMNS.keys())}")
    
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=True
    )
