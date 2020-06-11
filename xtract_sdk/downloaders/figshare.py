
import requests
import os

from xtract_sdk.downloaders.base import DownloaderInterface


class FigshareDownloader(DownloaderInterface):

    def __init__(self, download_type="file", mkdir=False):
        super().__init__(download_type, mkdir)

    def fetch(self, fid, path):

        """Download a file from figshare

        Stolen (with credit) from Logan Ward (@wardlt)

        Args:
            fid (int): ID number of figshare article
            path (str): Download path
        """

        # Get the article details
        art_details = requests.get('https://api.figshare.com/v2/articles/{}/files'.format(fid)).json()

        # Loop over each file
        for detail in art_details:
            # Make the download path
            filename = detail['name']
            data_path = os.path.join(path, filename)

            # Download the file
            req = requests.get(art_details[0]['download_url'], stream=True)
            print(data_path)
            with open(data_path, 'wb') as fp:
                for chunk in req.iter_content(chunk_size=1024 ** 2):
                    fp.write(chunk)

    def batch_fetch(self):
        raise NotImplementedError("Batch fetching not yet implemented for FigShare")


