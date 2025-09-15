import os.path
import logging

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
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
                logger.info("No files found.")
                return
            logger.info("Files:")
            for item in items:
                logger.info(f"{item['name']} ({item['id']})")
        except HttpError as error:
            logger.error(f"An error occurred: {error}")

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
            logger.info(f"Successfully downloaded {file_name} ({len(content)} characters)")
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
            media = io.BytesIO(data.encode("utf-8"))
            media.seek(0)

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
        except HttpError as error:
            logger.error(f"Error uploading {file_name}: {error}")
            return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    api = GoogleDriveAPI()
    result = api.download("90001")
    if result:
        logger.info(f"Downloaded content length: {len(result)}")
    else:
        logger.info("No content downloaded")
