
from xtract_sdk.downloaders.base import DownloaderInterface


class GlobusTransferDownloader(DownloaderInterface):

    def __init__(self, download_type="file", mkdir=False):
        super().__init__(download_type, mkdir)

    def fetch(self):
        raise NotImplementedError("GlobusTransferDownloader Not yet initialized...")

    def batch_fetch(self):
        raise NotImplementedError("GlobusTransferDownloader Not yet initialized...")
