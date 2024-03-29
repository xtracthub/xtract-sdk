
from xtract_sdk.packagers import FamilyBatch, Family
from xtract_sdk.downloaders import GlobusHttpsDownloader, GlobusTransferDownloader, GoogleDriveDownloader, LocalDownloader
import os
import sys
import json
import time
import shutil
import importlib
from queue import Queue
from xtract_sdk.utils.xtract_utils import get_dl_thruples_from_fam, is_metadata_nonempty
from google.oauth2.credentials import Credentials

# curl -X POST https://search.api.globus.org/beta/index -H "Authorization: Bearer AgWmEzmoy3N438Ewlkq5mJdlvMmwX5869O28OrBO2oGMrX8rvbUgCOpMkM2Vkm6Pa79VGWk2xg2yvBu4vomGJHobXXI9gwo" -H 'Content-Type: application/json' -d '{"display_name": "Xtract Demo Index", "description": "An index for interfacing with Xtract demo metadata"}'

# curl -X GET -H "Authorization: Bearer AgWmEzmoy3N438Ewlkq5mJdlvMmwX5869O28OrBO2oGMrX8rvbUgCOpMkM2Vkm6Pa79VGWk2xg2yvBu4vomGJHobXXI9gwo" 'https://search.api.globus.org/v1/index_list'
# Delete 34ff9481-f593-446c-8ce2-f6d410fecd9d
# Delete f30dbc1f-c9a8-431c-8e59-a1fbbdbbaa1d
# Delete cba4cfc9-435e-4e88-90ab-cc17ef488d6f

# curl -X DELETE -H "Authorization: Bearer AgWmEzmoy3N438Ewlkq5mJdlvMmwX5869O28OrBO2oGMrX8rvbUgCOpMkM2Vkm6Pa79VGWk2xg2yvBu4vomGJHobXXI9gwo" 'https://search.api.globus.org/v1/index/f30dbc1f-c9a8-431c-8e59-a1fbbdbbaa1d'


class XtractAgent:
    def __init__(self, ep_name, xtract_dir,
                 recursion_depth=1000,
                 sys_path_add='/',
                 module_path=None,
                 family_batch=None,
                 metadata_write_path=None
                 ):
        """ To init, we go find the config object located at .xtract directory.

            Additionally, load any cached creds.
        """

        self.completion_stats = {
            'batch_start_time': time.time(),
            'batch_end_time': None,
            'families_processed': 0,
            'n_groups_extracted': 0,
            'n_groups_nonempty': 0,
            'n_groups_empty': 0,
            'n_exceptions': 0
        }

        self.phase = "INIT"
        self.loaded = False
        self.creds = dict()
        self.filename_to_path_map = dict()
        self.fid_to_rm_loc_map = dict()

        self.metadata_write_path = metadata_write_path

        self.module_path = module_path

        # Step 1: import 'old' family batch
        self.family_batch = family_batch

        # Step 2: We'll later feed
        self.updated_family_objects = []

        # use this to configure the python file on the container
        sys.setrecursionlimit(recursion_depth)

        assert os.path.isdir(sys_path_add), f"Cannot add non-existent sys path to PATH: {sys_path_add}"
        sys.path.insert(1, sys_path_add)

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

    def get_completion_stats(self):
        """
        Getter method to calculate completion time and return the completion_stats.
        :return: (dict) all completion stats regarding groups + families extracted (and start/end times)
        """
        self.completion_stats['batch_end_time'] = time.time()
        self.completion_stats['total_elapsed_s'] = self.completion_stats['batch_end_time'] \
            - self.completion_stats['batch_start_time']
        return self.completion_stats

    def execute_extractions(self, family_batch, input_type=str):
        assert input_type in [str, list], "Please enter a valid input_type (str or list)"
        families = family_batch.families

        my_module = importlib.import_module(self.module_path)

        for family in families:
            for gid in family.groups:
                group = family.groups[gid]
                # print(f"Group contents: {group}")
                # If MatIO, then the group will have a parser.
                if group.parser is not None:
                    parser = group.parser
                # Otherwise, there is no parser, and business as usual.
                else:
                    parser = None

                # This means that family contains one file, and extractor inputs one file.
                if input_type is str:
                    ext_start_time = time.time()
                    if parser is None:
                        # Automatically try to hit the 'execute_extractor' function
                        try:
                            mdata = my_module.execute_extractor(group.files[0]['path'])
                        except Exception as e:
                            print(f"Caught error in extractor application to file group: {e}")
                            mdata = {"error": f"Caught: {e}"}  # Here we just leave the empty metadata

                    else:  # If MatIO, then add the parser and ALL the files.
                        group_files = []
                        for file in group.files:
                            group_files.append(file['path'])

                        ext_start_time = time.time()
                        try:
                            mdata = my_module.execute_extractor(group_files, parser)
                        except Exception as e:
                            print(f"Caught error in extractor application to file group: {e}")
                            mdata = {"error": f"Caught: {e}"}  # Here we just leave the empty metadata
                    ext_end_time = time.time()

                    # Regardless of whether we fail or succeed, want extraction time for group.
                    mdata['extraction_time'] = ext_end_time - ext_start_time

                    # TODO: We definitely want to change this to 'error OR empty'.
                    if is_metadata_nonempty(mdata):
                        self.completion_stats['n_groups_nonempty'] += 1
                    else:
                        self.completion_stats['n_groups_empty'] += 1

                    self.completion_stats['n_groups_extracted'] += 1

                    # Pack the metadata back into the family object.
                    family.groups[gid].update_metadata(mdata)

                elif input_type is list:
                    raise NotImplementedError("Does not yet support groups.")
            self.updated_family_objects.append(family)
            self.completion_stats['families_processed'] += 1

    def flush_metadata_to_files(self, writer='json'):
        from xtract_sdk.agent.encoders import NumpyEncoder
        # TODO: explore using the funcX serialization methods for this.
        assert writer in ['json', 'json-np', 'pkl'], "Invalid writer: must be 'json', 'json-np' or 'pkl'"
        assert self.metadata_write_path is not None, "metadata_write_path is None. Nowhere to write!"

        # TODO: if we find something here, we should probably combine metadata objects??
        file_paths = []

        if writer == 'json':
            for family in self.updated_family_objects:

                # Step f1: transform family into dictionary
                fam_dict = family.to_dict()

                # Step f2: get the total extraction time in case we have a write error.
                # Note the slightly different group structure between Group() and dict() group objects.
                # TODO: this is only fetching from first group.
                all_groups = fam_dict['groups']
                first_group = all_groups[0]
                first_group_ext_time = first_group['metadata']['extraction_time']

                writable_file_path = os.path.join(self.metadata_write_path, family.family_id)
                with open(writable_file_path, 'w') as f:
                    try:
                        json.dump(fam_dict, f)
                    except TypeError as e:
                        json.dump({'fatal_json_dump_exception': str(e), 'extraction_time': first_group_ext_time}, f)

                # Temporary -- for bookkeeping whether paths are written
                file_paths.append(writable_file_path)
                
        elif writer == 'json-np':
            for family in self.updated_family_objects:
                fam_dict = family.to_dict()
                # print(f"Dict family: {fam_dict}")
                writable_file_path = os.path.join(self.metadata_write_path, family.family_id)
                with open(writable_file_path, 'w') as f:
                    json.dump(fam_dict, f, cls=NumpyEncoder)

                # Temporary -- for bookkeeping whether paths are written
                file_paths.append(writable_file_path)

        elif writer == 'pickle':
            # TODO: 12/20 -- I'm tempted to not support straight-up pickles.
            raise NotImplementedError("Come back and support this.")

        else:
            raise ValueError(f"Improper writer type: {writer}")
        return file_paths

    def load_family(self, family):

        assert self.phase == 'LOAD_FAMILIES', "LOAD_FAMILIES stage not invocable after download. " \
                                            "Please load all families before downloading!"

        # Make sure we are in DICT or FAMILY format
        if isinstance(family, Family):  # probably need 'isinstanceof' here.
            pass
        elif isinstance(family, dict):
            fam = Family()
            fam.from_dict(family)
        else:
            fam_type = type(family)
            raise ValueError(f"Invalid type for family... Should be `Family` object, not: {fam_type}")

        downloader_type = family.download_type
        self.families_to_download[downloader_type].put(family)
        fid = family.family_id
        self.folders_to_delete.append(os.path.join(self.local_download_path, fid))

    def _load_family_batch(self, family_batch):
        for item in family_batch.families:
            self.load_family(item)

    def _download_batch(self, downloader):

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
            creds = None  # Shouldn't need creds to access on local machine -- would just be a perms issue.
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

            base_url = fam.base_url

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
            fam.fail_files = dl.fail_files
            fid = fam.family_id
            self.fid_to_rm_loc_map[fid] = dl.success_files

            # TODO: will want to put actual family objects here.
            self.ready_families.append(fam)

    def _fetch_all_files(self):
        # Iterate over all different types of downloaders.
        assert self.phase == "LOAD_FAMILIES", "XtractAgent() cannot fetch_all_files multiple times. " \
                                              "Please create and run fetch_all_files from a new Xtract agent."

        for dl_key in self.families_to_download:

            # If not implemented downloader or the queue is empty, then do nothing.
            if self.families_to_download[dl_key] is None or self.families_to_download[dl_key].qsize() == 0:
                continue

            downloader = dl_key

            # Download all files in FamilyBatch.
            self._download_batch(downloader)
            self.post_process()

    def load_and_fetch_families(self, family_batch):

        if type(family_batch) is dict:
            print(family_batch['families'])
            fam_batch = FamilyBatch()
            fam_batch.from_dict(family_batch)

            family_batch = fam_batch

        for item in family_batch.families:
            print(item)

        self._load_family_batch(family_batch)
        self._fetch_all_files()

    def delete_downloaded_files(self):
        # TODO: find a way to mute success_files to account for fact that it's been deleted.
        for fid_folder in self.folders_to_delete:

            if os.path.isdir(fid_folder):
                shutil.rmtree(fid_folder)

    def post_process(self):
        for fam in self.ready_families:

            fid = fam.family_id
            new_paths = self.fid_to_rm_loc_map[fid]

            remote_local_map = dict()

            for path_dict in new_paths:
                rm_p = path_dict['remote_path']
                lc_p = path_dict['local_path']

                remote_local_map[rm_p] = lc_p

            fam.remote_local_map = remote_local_map
