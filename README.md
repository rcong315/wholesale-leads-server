# Wholesale Leads Server

A FastAPI backend server for managing wholesale leads data with configurable columns and dropdown options.

## Features

- RESTful API endpoints
- Configurable dropdown options via environment variables
- Dynamic table column configuration
- Sample data generation
- CORS support
- Optional authentication (API Key, Secret, Bearer Token)
- Health check endpoint
- Configuration endpoint for debugging

## Prerequisites

- Python 3.8 or higher
- pip

## Installation

1. Navigate to the server directory:
```bash
cd server
```

2. Create a virtual environment (recommended):
```bash
python -m venv venv
```

3. Activate the virtual environment:
- On Windows:
  ```bash
  venv\Scripts\activate
  ```
- On macOS/Linux:
  ```bash
  source venv/bin/activate
  ```

4. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

Edit the `.env` file to configure your server:

```env
# Server Configuration
PORT=8000
HOST=0.0.0.0

# CORS Configuration
ALLOWED_ORIGINS=http://localhost:3000,http://127.0.0.1:3000

# Dropdown Menu Options (comma-separated)
DROPDOWN_OPTIONS=Wholesaler A,Wholesaler B,Wholesaler C,Wholesaler D,Wholesaler E

# API Authentication (optional - leave empty to disable)
API_KEY=your-api-key-here
API_SECRET=your-api-secret-here
AUTH_TOKEN=your-auth-token-here

# Table Columns Configuration
# Format: column_name:type,column_name:type
# Supported types: str, int, float, bool, date
TABLE_COLUMNS=id:int,company_name:str,contact_name:str,email:str,phone:str,address:str,city:str,state:str,zip_code:str,lead_score:float,status:str,created_date:date,last_contact:date,notes:str
```

## Running the Server

Start the FastAPI server:
```bash
python main.py
```

Or use uvicorn directly:
```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

The server will be available at `http://localhost:8000`

## API Documentation

Once the server is running, you can access:
- Interactive API documentation (Swagger UI): `http://localhost:8000/docs`
- Alternative API documentation (ReDoc): `http://localhost:8000/redoc`

## API Endpoints

### `GET /`
Root endpoint - returns API information

### `GET /health`
Health check endpoint

### `GET /api/dropdown-options`
Returns the list of dropdown options configured in `.env`

**Response:**
```json
{
  "options": ["Wholesaler A", "Wholesaler B", "Wholesaler C"]
}
```

### `POST /api/table-data`
Returns table data based on the selected dropdown option

**Request Body:**
```json
{
  "selection": "Wholesaler A"
}
```

**Response:**
```json
{
  "data": [
    {
      "id": 1,
      "company_name": "Wholesaler A Company 1",
      "contact_name": "John Smith",
      "email": "contact1@wholesalera.com",
      ...
    }
  ]
}
```

### `GET /api/config`
Returns the current configuration (for debugging)

## Authentication

The server supports three types of authentication (all optional):

1. **API Key**: Send in `X-API-Key` header
2. **API Secret**: Send in `X-API-Secret` header
3. **Bearer Token**: Send in `Authorization` header as `Bearer <token>`

If authentication is configured in `.env`, all endpoints (except `/` and `/health`) will require the appropriate headers.

## Customizing Table Columns

To customize the table columns, modify the `TABLE_COLUMNS` in the `.env` file:

```env
TABLE_COLUMNS=column1:type,column2:type,column3:type
```

Supported types:
- `str`: String values
- `int`: Integer values
- `float`: Floating-point values
- `bool`: Boolean values (true/false)
- `date`: Date strings (YYYY-MM-DD format)

The server will automatically generate appropriate sample data based on the column names and types.

## Development

The server runs with auto-reload enabled by default, so any changes to the code will automatically restart the server.

## Production Deployment

For production deployment:

1. Set appropriate environment variables
2. Use a production ASGI server like Gunicorn:
   ```bash
   gunicorn main:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
   ```
3. Consider using a reverse proxy like Nginx
4. Enable HTTPS/SSL certificates
5. Set up proper logging and monitoring

## Technologies Used

- FastAPI - Modern web framework
- Uvicorn - ASGI server
- Pydantic - Data validation
- python-dotenv - Environment variable management
- CORS middleware - Cross-origin resource sharing
