
import io
import os
import pickle

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from xtract_sdk.downloaders.base import DownloaderInterface


class GoogleDriveDownloader(DownloaderInterface):
    def __init__(self, auth_creds, **kwargs):

        super().__init__(**kwargs)

        if type(auth_creds) == bytes:
            self.auth_creds = pickle.loads(auth_creds)
        else:
            self.auth_creds = auth_creds

        self.service = self._generate_drive_connection()

    def _export_file(self, file_id, mime_type, stage_path):
        path, filename = os.path.split(stage_path)

        try:
            request = self.service.files().export(fileId=file_id, mimeType=mime_type)
            os.chdir(path)
            self.fetch_one(file_id, request, stage_path)

            return stage_path
        except Exception as e:
            raise ValueError(f"[Google Drive] Unable to make download! Caught: {e}")
        finally:
            os.chdir(self.orig_dir)

    def _get_media(self, file_id, stage_path):
        path, filename = os.path.split(stage_path)
        try:
            request = self.service.files().get_media(fileId=file_id)
            os.chdir(path)
            self.fetch_one(file_id, request, stage_path)
            return stage_path
        except Exception as e:
            raise ValueError(f"[Google Drive] Unable to make download! Caught: {e}")
        finally:
            os.chdir(self.orig_dir)

    def _generate_drive_connection(self):
        service = build('drive', 'v3', credentials=self.auth_creds)
        return service

    def fetch_one(self, file_id, request, stage_path):
        fh = io.FileIO(file_id, 'wb')
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while done is False:
            status, done = downloader.next_chunk()
        self.success_files.append({'remote_path': file_id, 'local_path': stage_path})

    def success_response(self, file_id):
        resp_info = {"status": "SUCCESS", "download_type": "file", "path": f"{self.new_dir}/{file_id}"}
        return resp_info

    def fetch(self, fid, download_type="export", mime_type=None, stage_path=None):
        assert(download_type in ["media", "export"], "Download type must be 'media' or 'export' (with mimeType)!")

        if download_type == "media":
            self._get_media(fid, stage_path)
        else:
            assert(mime_type is not None, "Local mimeType required for media downloads")
            self._export_file(fid, mime_type, stage_path)

    def batch_fetch(self, file_maps, mode='serial'):
        """ This needs to input list of triples (file_id, download_type, mimeType) """

        # assert isinstance(family_batch, FamilyBatch), "Google Drive's batch_fetch requires FamilyBatch object input"
        assert isinstance(file_maps, list), "Google Drive's batch requires thruple objects "

        if mode == 'serial':
            for f_obj in file_maps:

                fid = f_obj[0]
                tmp_path = f_obj[1]

                path, file = os.path.split(tmp_path)
                if not os.path.isdir(path):
                    os.makedirs(path)

                # TODO: Can switch to using maps instead of tuples (cleaner)
                is_gdoc = False
                mime_type = None
                if len(f_obj) > 3:
                    is_gdoc = f_obj[3]
                if len(f_obj) > 4:
                    mime_type = f_obj[4]

                if is_gdoc:
                    download_type = "export"
                else:
                    download_type = "media"

                self.fetch(fid, download_type, mime_type=mime_type, stage_path=tmp_path)
