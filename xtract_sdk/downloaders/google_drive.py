

# TODO: Add something that can create temporary directories if needed.
import io
import os
import tempfile

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# TODO: Test and see if any of this works


class GoogleDriveDownloader:
    def __init__(self, auth_creds, download_type="file", mkdir=False):

        if type(auth_creds) == bytes:
            self.auth_creds = pickle.loads(auth_creds)

        self.auth_creds = auth_creds
        self.download_type = download_type
        self.orig_dir = os.getcwd()

        self.service = self.generate_drive_connection()

        if mkdir:
            self.new_dir = tempfile.mkdtemp()
        else:
            self.new_dir = self.orig_dir

    def export_file(self, file_id, mimeType):
        try:
            request = self.service.files().export(fileId=file_id, mimeType=mimeType)
            os.chdir(self.new_dir)
            self.fetch(file_id, request)
            return os.getcwd() + file_id
        except Exception as e:
            raise ValueError(f"[Google Drive] Unable to make download! Caught: {e}")
        finally:
            os.chdir(self.orig_dir)

    def get_media(self, file_id):
        try:
            request = self.service.files().get_media(fileId=file_id)
            self.fetch(file_id, request)
            return os.getcwd() + file_id
        except Exception as e:
            raise ValueError(f"[Google Drive] Unable to make download! Caught: {e}")
        finally:
            os.chdir(self.orig_dir)

    def generate_drive_connection(self):
        service = build('drive', 'v3', credentials=self.auth_creds)
        return service

    def fetch(self, file_id, request):
        fh = io.FileIO(file_id, 'wb')
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while done is False:
            status, done = downloader.next_chunk()
            # print("Download %d%%." % int(status.progress() * 100))

    def success_response(self, file_id):
        resp_info = {"status": "SUCCESS", "download_type": "file", "path": f"{self.new_dir}/{file_id}"}
        return resp_info


import pickle
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly', 'https://www.googleapis.com/auth/drive']

def do_login_flow():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return creds

# creds = do_login_flow()
# file_id = "1XCS2Xqu35TiQgCpI8J8uu4Mss9FNnp1-AuHo-pMujb4"
# file_id2 = "0B5nDSpS9a_3kUFdiTXRFdS12QUk"
#
# gdd = GoogleDriveDownloader(creds)
#
# file_path = gdd.export_file(file_id, "text/csv")
# file_path2 = gdd.get_media(file_id2)
#
# print(file_path2)