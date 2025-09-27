import os.path
import logging
import json
import csv
from datetime import datetime

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
import io

from google_drive.config import Config

SCOPES = ["https://www.googleapis.com/auth/drive"]

logger = logging.getLogger(__name__)


class GoogleDriveAPI:

    def __init__(self):
        self.config = Config()
        self.creds = None
        self.authenticate()
        self.service = build("drive", "v3", credentials=self.creds)

    def authenticate(self):
        if os.path.exists("token.json"):
            self.creds = Credentials.from_authorized_user_file("token.json", SCOPES)

        # If there are no (valid) credentials available, let the user log in.
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    "credentials.json", SCOPES
                )
                self.creds = flow.run_local_server(port=8888)
            # Save the credentials for the next run
            with open("token.json", "w") as token:
                token.write(self.creds.to_json())

    def list_files(self):
        try:
            query = f"'{self.config.GOOGLE_DRIVE_DIR_ID}' in parents"

            results = (
                self.service.files()
                .list(q=query, pageSize=1000, fields="nextPageToken, files(id, name)")
                .execute()
            )
            items = results.get("files", [])

            if not items:
                logger.info("No files found in Google Drive.")
                return []

            logger.info(f"Found {len(items)} files in Google Drive")
            return items
        except HttpError as error:
            logger.error(f"Error listing files: {error}")
            return []

    def get_existing_locations(self):
        try:
            files = self.list_files()
            locations = set()

            for file in files:
                name = file["name"]
                # Extract location from filename pattern: batchleads_data_{location}.json
                if name.startswith("batchleads_data_") and name.endswith(".json"):
                    location = name.replace("batchleads_data_", "").replace(".json", "")
                    locations.add(location)

            logger.info(f"Found {len(locations)} existing locations in cache")
            return locations
        except Exception as error:
            logger.error(f"Error getting existing locations: {error}")
            return set()

    def file_exists(self, location, file_type="json"):
        try:
            extension = "json" if file_type == "json" else "csv"
            file_name = f"batchleads_data_{location}.{extension}"
            query = f"name = '{file_name}' and '{self.config.GOOGLE_DRIVE_DIR_ID}' in parents"

            results = self.service.files().list(q=query, fields="files(id)").execute()
            items = results.get("files", [])

            return len(items) > 0
        except Exception as error:
            logger.error(f"Error checking if file exists: {error}")
            return False

    def convert_leads_to_csv(self, leads_data):
        if not leads_data or len(leads_data) == 0:
            return ""

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=leads_data[0].keys())
        writer.writeheader()
        writer.writerows(leads_data)

        return output.getvalue()

    def download(self, location):
        try:
            file_name = f"batchleads_data_{location}.json"
            logger.info(f"Downloading file: {file_name}")
            query = f"name = '{file_name}' and '{self.config.GOOGLE_DRIVE_DIR_ID}' in parents"

            results = self.service.files().list(q=query, fields="files(id)").execute()
            items = results.get("files", [])

            if not items:
                logger.warning(f"File not found: {file_name}")
                return None

            file_id = items[0]["id"]
            request = self.service.files().get_media(fileId=file_id)

            file_content = io.BytesIO()
            downloader = MediaIoBaseDownload(file_content, request)

            done = False
            while not done:
                _, done = downloader.next_chunk()

            file_content.seek(0)
            content = file_content.read().decode("utf-8")
            logger.info(
                f"Successfully downloaded {file_name} ({len(content)} characters)"
            )
            return content

        except HttpError as error:
            logger.error(f"Error downloading {file_name}: {error}")
            return None

    def upload(self, file_name, data, file_type="json"):
        try:
            logger.info(f"Uploading file: {file_name}")
            file_metadata = {
                "name": file_name,
                "parents": [self.config.GOOGLE_DRIVE_DIR_ID],
            }

            if file_type == "json":
                mimetype = "application/json"
            else:
                mimetype = "text/csv"

            media = MediaIoBaseUpload(
                io.BytesIO(data.encode("utf-8")), mimetype=mimetype
            )

            query = f"name = '{file_name}' and '{self.config.GOOGLE_DRIVE_DIR_ID}' in parents"
            results = self.service.files().list(q=query, fields="files(id)").execute()
            items = results.get("files", [])

            if items:
                # Update existing file
                file_id = items[0]["id"]
                logger.info(f"Updating existing file: {file_name}")
                updated_file = (
                    self.service.files()
                    .update(
                        fileId=file_id,
                        media_body=media,
                    )
                    .execute()
                )
                logger.info(f"Successfully updated file: {file_name}")
                return updated_file
            else:
                # Create new file
                logger.info(f"Creating new file: {file_name}")
                new_file = (
                    self.service.files()
                    .create(
                        body=file_metadata,
                        media_body=media,
                        fields="id",
                    )
                    .execute()
                )
                logger.info(f"Successfully created file: {file_name}")
                return new_file
        except (HttpError, Exception) as error:
            logger.error(f"Error uploading {file_name}: {error}")
            return None

    def load_cache(self, location):
        try:
            content = self.download(location)
            if not content:
                return None

            cached_file = json.loads(content)
            cache_timestamp = datetime.fromisoformat(cached_file["timestamp"])
            cached_data = cached_file["leads"]

            if cached_data is not None:
                cache_age_days = (datetime.now() - cache_timestamp).days
                if cache_age_days < self.config.CACHE_EXPIRATION_DAYS:
                    logger.info(
                        f"Returning cached data for location {location} (age: {cache_age_days} days)"
                    )
                    return {
                        "location": location,
                        "total_leads": len(cached_data),
                        "leads": cached_data,
                        "cached": True,
                        "cache_age_days": cache_age_days,
                    }
                else:
                    logger.info(
                        f"Cache expired for location {location} (age: {cache_age_days} days)"
                    )

        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
            logger.warning(f"Malformed cache file for location {location}: {error}")
        except Exception as error:
            logger.error(f"Error loading cache for location {location}: {error}")

        return None

    def save_cache(self, location, leads_data):
        try:
            # Save JSON format
            json_file_name = f"batchleads_data_{location}.json"
            cache_data = {
                "timestamp": datetime.now().isoformat(),
                "leads": leads_data,
            }
            data_json = json.dumps(cache_data, indent=2)

            json_result = self.upload(json_file_name, data_json, "json")

            # Save CSV format
            csv_file_name = f"batchleads_data_{location}.csv"
            data_csv = self.convert_leads_to_csv(leads_data)

            csv_result = None
            if data_csv:
                csv_result = self.upload(csv_file_name, data_csv, "csv")

            if json_result:
                logger.info(f"Successfully cached JSON data for location {location}")
                if csv_result:
                    logger.info(f"Successfully cached CSV data for location {location}")
                return True
            else:
                logger.error(f"Failed to cache data for location {location}")
                return False

        except Exception as error:
            logger.error(f"Error saving cache for location {location}: {error}")
            return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    api = GoogleDriveAPI()
    result = api.download("90001")
    if result:
        logger.info(f"Downloaded content length: {len(result)}")
    else:
        logger.info("No content downloaded")
