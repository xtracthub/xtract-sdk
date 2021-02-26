
from .packagers import FamilyBatch, Family
from .downloaders import GlobusHttpsDownloader, GlobusTransferDownloader, GoogleDriveDownloader, LocalDownloader
import os
import json
import shutil
from queue import Queue
from .utils.xtract_utils import get_dl_thruples_from_fam
from google.oauth2.credentials import Credentials


class XtractAgent:
    def __init__(self, ep_name, xtract_dir):
        """ To init, we go find the config object located at .xtract directory.

            Additionally, load any cached creds.
        """

        self.phase = "INIT"
        self.loaded = False
        self.creds = dict()
        self.filename_to_path_map = dict()
        self.fid_to_rm_loc_map = dict()

        self.ready_families = []

        # Step 1: Load the 'self-aware' config data (Globus and funcX endpoint IDs so we can initiate transfers)
        base_path = xtract_dir
        if not os.path.isdir(base_path):
            raise NotADirectoryError(f"No directory at {base_path}. "
                                     f"Please configure before creating Xtract() object!")
        xtr_ep_path = os.path.join(base_path, ep_name)
        if not os.path.isdir(xtr_ep_path):
            raise NotADirectoryError(f"No directory at {xtr_ep_path}. "
                                     f"Please configure before creating Xtract() object!")
        config_path = os.path.join(xtr_ep_path, 'config.json')
        if not os.path.isfile(config_path):
            raise FileNotFoundError(f"config.json not found in {xtr_ep_path} directory!")

        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
        except json.JSONDecodeError:
            raise ValueError("Caught json.JSONDecodeError -- please ensure config is proper JSON.")

        # These are the 'self-aware' variables of the data and compute endpoints.
        self.funcx_eid = config['funcx_eid']
        self.globus_eid = config['globus_eid']
        self.local_download_path = config['local_download_path']

        # Make the folder at which we'll do the extractions if it doesn't already exist.
        if not os.path.isdir(self.local_download_path):
            os.makedirs(self.local_download_path)

        # Now we want to load any cached credentials (give them name in dict that is same as filename).
        creds_dir_path = os.path.join(xtr_ep_path, 'creds')
        if os.path.isdir(creds_dir_path):

            all_creds = os.listdir(creds_dir_path)
            for cred in all_creds:
                cred_path = os.path.join(creds_dir_path, cred)

                with open(cred_path, 'r') as f:
                    self.creds[cred] = json.load(f)
        self.loaded = True

        # TODO: turn Globus into a queue.
        # leave as queue in case we don't want to pull down EVERYTHING in future
        self.families_to_download = {"GLOBUS_HTTPS": Queue(), "GLOBUS": None, "LOCAL": Queue(), "GDRIVE": Queue()}

        # TODO: if someone overwrites a not-None downloader, should throw an error.
        self.downloaders = {"GLOBUS_HTTPS": None, "GLOBUS": None, "LOCAL": None, "GDRIVE": None}
        self.phase = "LOAD_FAMILIES"

        self.success_files = []
        self.fail_files = []

        self.folders_to_delete = []

    def load_family(self, family):

        assert self.phase == 'LOAD_FAMILIES', "LOAD_FAMILIES stage not invocable after download. " \
                                            "Please load all families before downloading!"

        # TODO: debug the weird type changes in here.
        if isinstance(family, Family):  # probably need 'isinstanceof' here.
            pass
        elif isinstance(family, dict):
            fam = Family()
            fam.from_dict(family)
        else:
            fam_type = type(family)
            raise ValueError(f"Invalid type for family... Should be `Family` object, not: {fam_type}")

        downloader_type = family['download_type']
        self.families_to_download[downloader_type].put(family)
        fid = family['family_id']
        self.folders_to_delete.append(os.path.join(self.local_download_path, fid))

    def load_family_batch(self, family_batch):
        for item in family_batch.families_to_download:
            self.load_family(item)

    def download_batch(self, downloader):
        # TODO: test all four of these.

        self.phase = "DOWNLOADING"
        is_local = False
        if downloader == "GLOBUS":
            # creds = self.creds['GLOBUS']
            # dl = GlobusHttpsDownloader()
            # TODO: Will want this for 0.1.0. Not urgent because we can pre-fetch via Globus. Much more elegant.
            raise NotImplementedError("Need to do full Globus transfers at endpoint.")
        elif downloader == "GLOBUS_HTTPS":
            creds = self.creds['GLOBUS_HTTPS']
            self.downloaders[downloader] = GlobusHttpsDownloader()
        elif downloader == "GDRIVE":
            creds = self.creds['GDRIVE']
            # Transform the dictionary creds BACK into a Credentials object.
            creds = Credentials.from_authorized_user_info(creds)
            self.downloaders[downloader] = GoogleDriveDownloader(auth_creds=creds)
        elif downloader == "LOCAL":
            # In this case, it means the file is already there and no actual download needs to happen.
            is_local = True  # We set this here to tell get_dl_tuples_from_fam to not change the file paths.
            creds = None
            self.downloaders[downloader] = LocalDownloader()
        else:
            raise ValueError(f"Unknown downloader type: {downloader}")

        thruples = []

        dl = self.downloaders[downloader]

        # This means we didn't load any of this data.
        if dl is None:
            raise NotImplementedError("GLOBUS not implemented yet")

        while not self.families_to_download[downloader].empty():
            fam = self.families_to_download[downloader].get()
            base_url = fam['base_url']

            thrups_to_proc = get_dl_thruples_from_fam(family=fam,
                                                      headers=creds,
                                                      base_url=base_url,
                                                      base_store_path=self.local_download_path,
                                                      is_local=is_local)
            for thrup in thrups_to_proc:
                filename = thrup[0]
                new_path = thrup[1]
                self.filename_to_path_map[filename] = new_path
                thruples.append(thrup)

            dl.batch_fetch(thruples)
            self.success_files.extend(dl.success_files)
            self.fail_files.extend(dl.fail_files)

            # TODO: will want to add these to Family objects
            # fam['success_files'] = dl.success_files
            fam['fail_files'] = dl.fail_files
            fid = fam['family_id']
            self.fid_to_rm_loc_map[fid] = dl.success_files

            # TODO: will want to put actual family objects here.
            self.ready_families.append(fam)

    def fetch_all_files(self):
        # Iterate over all different types of downloaders.
        assert self.phase == "LOAD_FAMILIES", "XtractAgent() cannot fetch_all_files multiple times. " \
                                              "Please create and run fetch_all_files from a new Xtract agent."

        for dl_key in self.families_to_download:

            # If not implemented downloader or the queue is empty, then do nothing.
            if self.families_to_download[dl_key] is None or self.families_to_download[dl_key].qsize() == 0:
                continue

            downloader = dl_key

            # Download all files in FamilyBatch.
            self.download_batch(downloader)
            self.post_process()

    def delete_downloaded_files(self):
        # TODO: find a way to mute success_files to account for fact that it's been deleted.
        for fid_folder in self.folders_to_delete:

            if os.path.isdir(fid_folder):
                shutil.rmtree(fid_folder)

    def post_process(self):
        for fam in self.ready_families:

            fid = fam['family_id']
            new_paths = self.fid_to_rm_loc_map[fid]

            remote_local_map = dict()

            for path_dict in new_paths:
                rm_p = path_dict['remote_path']
                lc_p = path_dict['local_path']

                remote_local_map[rm_p] = lc_p

            fam['remote_local_map'] = remote_local_map
            # fam.pop('success_files', None)
