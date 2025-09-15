import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from google_drive.config import Config

SCOPES = ["https://www.googleapis.com/auth/drive"]


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
                print("No files found.")
                return
            print("Files:")
            for item in items:
                print(f"{item['name']} ({item['id']})")
        except HttpError as error:
            # TODO(developer) - Handle errors from drive API.
            print(f"An error occurred: {error}")

    def download(self, zip_code):
        try:
            file_name = f"batchleads_data_{zip_code}.json"
            query = f"name = '{file_name}' and '{self.config.GOOGLE_DRIVE_DIR_ID}' in parents"

            results = (
                self.service.files()
                .list(q=query, pageSize=10, fields="nextPageToken, files(id, name)")
                .execute()
            )
            items = results.get("files", [])

            if not items:
                print("No files found.")
                return
            print("Files:")
            for item in items:
                print(f"{item['name']} ({item['id']})")
        except HttpError as error:
            # TODO(developer) - Handle errors from drive API.
            print(f"An error occurred: {error}")

    def upload(self, zip_code, data):
        pass


if __name__ == "__main__":
    api = GoogleDriveAPI()
    api.download("92618")
