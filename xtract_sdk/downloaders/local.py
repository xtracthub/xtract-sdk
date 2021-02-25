
from xtract_sdk.downloaders.base import DownloaderInterface


# TODO: docs should state that this is just a passthrough class.
class LocalDownloader(DownloaderInterface):
    def __init__(self, auth_creds=None, **kwargs):

        super().__init__(**kwargs)

    def fetch(self):
        pass

    def batch_fetch(self, tups):

        # TODO: check to see if path actually exists at location.

        for tup in tups:
            file_path = tup[1]
            self.success_files.append({'remote_path': file_path, 'local_path': file_path})