
import io
import os
import pickle
import tempfile

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


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

        self.success_files = set()
        self.fail_files = set()

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
        self.success_files.add(file_id)

    def success_response(self, file_id):
        resp_info = {"status": "SUCCESS", "download_type": "file", "path": f"{self.new_dir}/{file_id}"}
        return resp_info



