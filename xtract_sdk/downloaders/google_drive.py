
import io
import os
import pickle

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from xtract_sdk.downloaders.base import DownloaderInterface
from xtract_sdk.packagers.family_batch import FamilyBatch
from xtract_sdk.packagers.family import Family

class GoogleDriveDownloader(DownloaderInterface):
    def __init__(self, auth_creds, **kwargs):

        super().__init__(**kwargs)

        if type(auth_creds) == bytes:
            self.auth_creds = pickle.loads(auth_creds)
        else:
            self.auth_creds = auth_creds

        self.service = self._generate_drive_connection()

    def _export_file(self, file_id, mimeType):
        try:
            request = self.service.files().export(fileId=file_id, mimeType=mimeType)
            os.chdir(self.new_dir)
            self.fetch_one(file_id, request)
            return os.getcwd() + file_id
        except Exception as e:
            raise ValueError(f"[Google Drive] Unable to make download! Caught: {e}")
        finally:
            os.chdir(self.orig_dir)

    def _get_media(self, file_id):
        try:
            request = self.service.files().get_media(fileId=file_id)
            self.fetch_one(file_id, request)
            return os.getcwd() + file_id
        except Exception as e:
            raise ValueError(f"[Google Drive] Unable to make download! Caught: {e}")
        finally:
            os.chdir(self.orig_dir)

    def _generate_drive_connection(self):
        service = build('drive', 'v3', credentials=self.auth_creds)
        return service

    def fetch_one(self, file_id, request):
        fh = io.FileIO(file_id, 'wb')
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while done is False:
            status, done = downloader.next_chunk()
        self.success_files.add(file_id)

    def success_response(self, file_id):
        resp_info = {"status": "SUCCESS", "download_type": "file", "path": f"{self.new_dir}/{file_id}"}
        return resp_info

    def fetch(self, fid, download_type="export", mimeType=None):
        assert(download_type in ["media", "export"], "Download type must be 'media' or 'export' (with mimeType)!")

        if download_type == "media":
            self._get_media(fid)
        else:
            assert(mimeType is not None, "Local mimeType required for media downloads")
            self._export_file(fid, mimeType)

    def batch_fetch(self, family_batch, mode='serial'):
        """ This needs to input list of triples (file_id, download_type, mimeType) """
        files_to_download = family_batch.file_ls

        assert isinstance(family_batch, FamilyBatch), "Google Drive's batch_fetch requires FamilyBatch object as input"

        if mode == 'serial':
            for f_obj in files_to_download:
                fid = f_obj["path"]
                is_gdoc = f_obj["is_gdoc"]
                mimeType = f_obj["mimeType"]

                if is_gdoc:
                    download_type = "export"
                else:
                    download_type = "media"

                self.fetch(fid, download_type, mimeType=mimeType)
