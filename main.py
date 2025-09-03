from fastapi import FastAPI
from api.routes import app as scrape_app

app = FastAPI()

app.mount("/api", scrape_app)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)