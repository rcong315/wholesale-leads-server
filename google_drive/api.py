import os.path
import logging
import json
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

    def get_existing_zip_codes(self):
        try:
            files = self.list_files()
            zip_codes = set()

            for file in files:
                name = file["name"]
                # Extract zip code from filename pattern: batchleads_data_{zip_code}.json
                if name.startswith("batchleads_data_") and name.endswith(".json"):
                    zip_code = name.replace("batchleads_data_", "").replace(".json", "")
                    zip_codes.add(zip_code)

            logger.info(f"Found {len(zip_codes)} existing zip codes in cache")
            return zip_codes
        except Exception as error:
            logger.error(f"Error getting existing zip codes: {error}")
            return set()

    def download(self, zip_code):
        try:
            file_name = f"batchleads_data_{zip_code}.json"
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

    def upload(self, file_name, data):
        try:
            logger.info(f"Uploading file: {file_name}")
            file_metadata = {
                "name": file_name,
                "parents": [self.config.GOOGLE_DRIVE_DIR_ID],
            }

            media = MediaIoBaseUpload(
                io.BytesIO(data.encode("utf-8")), mimetype="application/json"
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

    def load_cache(self, zip_code):
        try:
            content = self.download(zip_code)
            if not content:
                return None

            cached_file = json.loads(content)
            cache_timestamp = datetime.fromisoformat(cached_file["timestamp"])
            cached_data = cached_file["leads"]

            if cached_data is not None:
                cache_age_days = (datetime.now() - cache_timestamp).days
                if cache_age_days < self.config.CACHE_EXPIRATION_DAYS:
                    logger.info(
                        f"Returning cached data for zip code {zip_code} (age: {cache_age_days} days)"
                    )
                    return {
                        "zip_code": zip_code,
                        "total_leads": len(cached_data),
                        "leads": cached_data,
                        "cached": True,
                        "cache_age_days": cache_age_days,
                    }
                else:
                    logger.info(
                        f"Cache expired for zip code {zip_code} (age: {cache_age_days} days)"
                    )

        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
            logger.warning(f"Malformed cache file for zip code {zip_code}: {error}")
        except Exception as error:
            logger.error(f"Error loading cache for zip code {zip_code}: {error}")

        return None

    def save_cache(self, zip_code, leads_data):
        try:
            file_name = f"batchleads_data_{zip_code}.json"
            cache_data = {
                "timestamp": datetime.now().isoformat(),
                "leads": leads_data,
            }
            data_json = json.dumps(cache_data, indent=2)

            result = self.upload(file_name, data_json)
            if result:
                logger.info(f"Successfully cached data for zip code {zip_code}")
                return True
            else:
                logger.error(f"Failed to cache data for zip code {zip_code}")
                return False

        except Exception as error:
            logger.error(f"Error saving cache for zip code {zip_code}: {error}")
            return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    api = GoogleDriveAPI()
    result = api.download("90001")
    if result:
        logger.info(f"Downloaded content length: {len(result)}")
    else:
        logger.info("No content downloaded")
