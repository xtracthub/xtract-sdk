
# import pickle
import json
import os
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

from xtract_sdk.downloaders.google_drive import GoogleDriveDownloader

SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly', 'https://www.googleapis.com/auth/drive']


base_creds_path = "/Users/tylerskluzacek/.xtract/creds"


# TODO: integrate this as a client tool for Xtract.
def do_login_flow():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.

    token_path = os.path.join(base_creds_path, "GDRIVE")

    if os.path.exists(token_path):
        with open(token_path, 'r') as token:
            creds = json.load(token)
            creds = Credentials.from_authorized_user_info(creds)

            # Refresh the refresh_tokens here.
            if creds.refresh_token:
                creds.refresh(Request())

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        print("IN HERE IF CREDS NOT EXPIRED.")
        flow = InstalledAppFlow.from_client_secrets_file(
            '../credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)

        # Save the credentials for the next run
        with open(token_path, 'w') as token:
            json.dump(json.loads(creds.to_json()), token)

    return creds


creds = do_login_flow()

# Show that we can go to JSON and back...
creds = json.loads(creds.to_json())
creds = Credentials.from_authorized_user_info(creds)

file_id = "1XCS2Xqu35TiQgCpI8J8uu4Mss9FNnp1-AuHo-pMujb4"
file_id2 = "0B5nDSpS9a_3kUFdiTXRFdS12QUk"

gdd = GoogleDriveDownloader(creds)

file_path = gdd._export_file(file_id, "text/csv", stage_path='.')
file_path2 = gdd._get_media(file_id2, stage_path='.')
