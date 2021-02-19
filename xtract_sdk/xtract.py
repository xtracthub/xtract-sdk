
from .packagers import FamilyBatch, Family
from .downloaders import GlobusHttpsDownloader, GlobusTransferDownloader, GoogleDriveDownloader
import os
import json
from queue import Queue
from .utils.xtract_utils import get_dl_thruples_from_fam


class XtractAgent:
    def __init__(self, ep_name, xtract_dir):
        """ To init, we go find the config object located at .xtract directory.

            Additionally, load any cached creds.
            # TODO: Should we delete afterwards?
        """
        self.loaded = False
        self.creds = dict()
        self.filename_to_path_map = dict()

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

        # Now we want to load any cached credentials (give them name in dict that is same as filename).
        creds_dir_path = os.path.join(xtr_ep_path, 'creds')
        if os.path.isdir(creds_dir_path):

            all_creds = os.listdir(creds_dir_path)
            for cred in all_creds:
                cred_path = os.path.join(creds_dir_path, cred)

                with open(cred_path, 'r') as f:
                    self.creds[cred] = json.load(f)
        self.loaded = True

        self.families = Queue()  # leave as queue in case we don't want to pull down EVERYTHING in future.

    def load_family(self, family):
        if isinstance(family, Family):  # probably need 'isinstanceof' here.
            pass
        elif isinstance(family, dict):
            fam = Family()
            fam.from_dict(family)
        else:
            fam_type = type(family)
            raise ValueError(f"Invalid type for family... Should be `Family` object, not: {fam_type}")
        self.families.put(family)

    def load_family_batch(self, family_batch):
        for item in family_batch.families:
            self.load_family(item)

    def download_batch(self, downloader):
        # TODO: test all four of these.
        if downloader == "GLOBUS":
            creds = self.creds['GLOBUS']
            dl = GlobusHttpsDownloader()
            raise NotImplementedError("Need to do full Globus transfers at endpoint.")
        elif downloader == "GLOBUS_HTTPS":
            creds = self.creds['GLOBUS_HTTPS']
            dl = GlobusHttpsDownloader()
        elif downloader == "GDRIVE":
            creds = self.creds['gdrive']
            dl = GoogleDriveDownloader(auth_creds=self.creds['gdrive'])
        elif downloader == "LOCAL":
            # In this case, it means the file is already there and no actual download needs to happen.
            creds = None
            dl = None
        else:
            raise ValueError(f"Unknown downloader type: {downloader}")

        if dl is not None:
            thruples = []
            while not self.families.empty():
                fam = self.families.get()
                base_url = fam['base_url']
                thrups_to_proc = get_dl_thruples_from_fam(family=fam, headers=creds, base_url=base_url)
                for thrup in thrups_to_proc:
                    filename = thrup[0]
                    new_path = thrup[1]
                    self.filename_to_path_map[filename] = new_path
                    thruples.append(thrup)

            dl.batch_fetch(thruples)
            return {'success': dl.success_files, 'fail': dl.fail_files}
        else:
            raise NotImplementedError("We need to pass these through for local!")

    def process_next_batch(self):
        fam = self.families.get()
        downloader = fam.download_type

        self.download_batch(downloader)





