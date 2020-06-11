
import os
import requests
import tempfile
import threading


# TODO: os.path.join() everywhere.
class GlobusHttpsDownloader:
    def __init__(self, download_type="file", mk_tmpdir=False):

        self.download_type = download_type
        self.orig_dir = os.getcwd()

        if mk_tmpdir:
            self.new_dir = tempfile.mkdtemp()
        else:
            self.new_dir = self.orig_dir

        self.success_files = set()
        self.fail_files = set()

    # TODO: Make multithreaded.
    def fetch(self, remote_filepath, headers, local_filename=None, nested_folder_id=None):

        print(f"Fetching from remote file path: {remote_filepath}")

        try:
            req = requests.get(remote_filepath, headers=headers)
        except Exception as e:
            print(e)
            return e

        print(req)

        if nested_folder_id:
            dir_path = f"{self.new_dir}/{nested_folder_id}"
            os.makedirs(f"{self.new_dir}/{nested_folder_id}/{local_filename}")

        else:
            dir_path = self.new_dir
        os.chdir(dir_path)

        local_file_path = f"{dir_path}/{local_filename}"

        with open(local_file_path, 'wb') as f:
            f.write(req.content)
        os.chdir(self.orig_dir)
        self.success_files.add(local_filename)

    def batch_fetch(self, *args, **kwargs):
        print("BATCH DOWNLOADS FOR GLOBUS HTTPS NOT YET IMPLEMENTED.")

