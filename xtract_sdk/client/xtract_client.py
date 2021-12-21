import time
import json
import requests
import mdf_toolbox
import globus_sdk
from xtract_sdk.client import XTRACT_CRAWLER, XTRACT_CRAWLER_DEV, XTRACT_SERVICE, XTRACT_SERVICE_DEV


class XtractClient:

    def __init__(self, auth_scopes=None, force_login=False):

        if force_login:
            self.base_url = XTRACT_CRAWLER_DEV
            self.extract_url = XTRACT_SERVICE_DEV
        else:
            self.base_url = XTRACT_CRAWLER
            self.extract_url = XTRACT_SERVICE

        self.funcx_scope = "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all"
        scopes = [
            "openid",
            "search",
            "petrel",
            "transfer",
            self.funcx_scope
        ]

        if auth_scopes is not None:
            for scope in auth_scopes:
                scopes.append(scope)

        self.auths = mdf_toolbox.login(
            services=scopes,
            app_name="Foundry",
            make_clients=True,
            no_browser=False,
            no_local_server=False
        )

        self.crawl_ids = []
        self.cid_to_xep_map = dict()

    def crawl(self, xeps):
        """Initiates a Globus directory crawl, returning a crawl ID for each endpoint.

        Returns
        -------
        crawl_ids: list
            A crawl ID for each endpoint. Also saved to self.crawl_ids
        """

        crawl_ids = []

        crawl_url = f'{self.base_url}crawl'

        for xep in xeps:
            ep_dicts = []
            if xep.repo_type.lower() == "globus":
                ep_dicts.append({'repo_type': xep.repo_type,
                                 'eid': xep.globus_ep_id,
                                 'dir_paths': xep.dirs,
                                 'grouper': xep.grouper})

                crawl_tokens = {'Transfer': self.auths['transfer'].authorizer.access_token,
                                'Authorization': f"Bearer {self.auths['transfer'].authorizer.access_token}",
                                'FuncX': self.auths[self.funcx_scope].access_token}

                crawl_req = requests.post(crawl_url,
                                          json={'endpoints': ep_dicts,
                                                'tokens': crawl_tokens})
            elif xep.repo_type == "GDRIVE":
                raise NotImplementedError('GDRIVE is not implemented as repo type yet')
            else:
                raise Exception(f"repo_type {xep.repo_type} is invalid")

            try:
                crawl_id = json.loads(crawl_req.content)["crawl_id"]
            except json.decoder.JSONDecodeError:  # TODO: Add a better catch based on status codes.
                raise Exception(f"Crawl request failed with status {crawl_req.status_code}")

            crawl_ids.append(crawl_id)
            self.cid_to_xep_map[crawl_id] = xep  # TODO: just made this change

        self.crawl_ids = crawl_ids
        return crawl_ids

    def get_crawl_status(self, crawl_ids=None):
        """Retrieves the crawl status of a job.

        Returns
        -------
        payload: list
            For each crawl job, a dict with crawl ID, status, message, and data.
        """

        if crawl_ids is None and self.crawl_ids is None:
            raise Exception("Missing crawl ID. A crawl ID must be provided or the .crawl() method must be run")
        elif crawl_ids is None:
            crawl_ids = self.crawl_ids

        status_url = f"{self.base_url}get_crawl_status"

        statuses = []

        for cid in crawl_ids:
            crawl_status = requests.get(status_url, json={'crawl_id': cid})
            try:
                crawl_content = json.loads(crawl_status.content)
            except json.decoder.JSONDecodeError:  # TODO: Add a better catch based on status codes. IDK what the default status code is.
                raise Exception(f"Crawl status retrieval failed with status {crawl_status.status_code}")
            statuses.append(crawl_content)

        payload = []
        for status in statuses:
            if 'error' in status.keys():
                stat_dict = {'crawl_id': status['crawl_id'],
                             'status': 'ERROR',
                             'message': status['error'],
                             'data': []}
            else:
                stat_dict = {'crawl_id': status['crawl_id'],
                             'status': status['crawl_status'],
                             'message': 'OK'}
                restof_keys = set(status.keys()) - {'crawl_id', 'crawl_status'}
                stat_dict['data'] = dict((k, status[k]) for k in restof_keys)
            payload.append(stat_dict)

        return payload

    def flush_crawl_metadata(self, crawl_ids=None, n=100):
        """Returns a list of all metadata from the crawl.

        Returns
        -------
        payload: list
            The metadata for each crawl job.
        """

        if crawl_ids is None and self.crawl_ids is None:
            raise Exception("Missing crawl ID. A crawl ID must be provided or the .crawl() method must be run")
        elif crawl_ids is None:
            crawl_ids = self.crawl_ids

        flush_url = f'{self.base_url}fetch_crawl_mdata'

        payload = []
        for cid in crawl_ids:
            req = requests.get(flush_url,
                               json={'crawl_id': cid,
                                     'n': n})
            payload.append(req.content)

        return payload

    def xtract(self):
        """Initiates metadata extraction workflow, extracting/sending metadata to central Xtract service.

        Returns
        -------
        payload: list
            The xtract response code.
        """

        if self.crawl_ids is None:
            raise Exception("Missing crawl ID, the .crawl() method must be run")

        fx_headers = {'Authorization': f"Bearer {self.auths[self.funcx_scope].access_token}",
                      'Search': self.auths['search'].authorizer.access_token,
                      'Openid': self.auths['openid'].access_token}

        payload = []

        for cid in self.crawl_ids:
            post = requests.post(f'{self.extract_url}extract',
                                 json={'crawl_id': cid,
                                       'fx_ep_ids': self.cid_to_xep_map[cid].funcx_ep_id,
                                       'tokens': fx_headers,
                                       'local_mdata_path': self.cid_to_xep_map[cid].local_mdata_path,
                                       'remote_mdata_path': self.cid_to_xep_map[cid].remote_mdata_path})
            payload.append(post)

        return payload

    def get_xtract_status(self):
        """Returns the status of the metadata extraction.

        Returns
        -------
        payload: list
            For each endpoint, a dict of status and counters.
        """

        if self.crawl_ids is None:
            raise Exception("Missing crawl ID, the .crawl() method must be run")

        status_url = f'{self.extract_url}get_extract_status'

        payload = []

        for cid in self.crawl_ids:
            xtract_status = requests.get(status_url,
                                         json={'crawl_id': cid})
            payload.append({'xtract_status': json.loads(xtract_status.content)['status'],
                            'xtract_counters': json.loads(xtract_status.content)['counters']})

        return payload

    def offload_metadata(self, delete_source=False):
        """Transfers metadata from Xtraction to a timestamped folder in remote_mdata_path TODO: alt naming scheme

        Returns
        -------
        path: string
            The path where the metadata was transferred to.
        """

        if self.crawl_ids is None:
            raise Exception("Missing crawl ID, the .crawl() and .xtract() methods must be run")

        tokens = {"Transfer": self.auths["petrel"].access_token}
        transfer_authorizer = globus_sdk.AccessTokenAuthorizer(tokens['Transfer'])
        tc = globus_sdk.TransferClient(authorizer=transfer_authorizer)

        for cid in self.crawl_ids:

            source_id = self.cid_to_xep_map[cid].funcx_ep_id
            dest_id = self.cid_to_xep_map[cid].globus_ep_id
            tc.endpoint_autoactivate(source_id)
            tc.endpoint_autoactivate(dest_id)

            source_path = self.cid_to_xep_map[cid].remote_mdata_path
            dest_path = self.cid_to_xep_map[cid].local_mdata_path
            timestamped_dest_path = (dest_path
                                     + time.strftime("%Y-%m-%d-%H.%M.%S", time.gmtime())
                                     + "/")
            tc.operation_mkdir(dest_id, path=timestamped_dest_path)

            tdata = globus_sdk.TransferData(tc, source_id, dest_id)
            submit_result = tc.submit_transfer(tdata)
            print(f"Task ID: {submit_result['task_id']}")

            if delete_source:
                ddata = globus_sdk.DeleteData(tc, source_id, recursive=True)
                ddata.add_item(source_path)
                delete_result = tc.submit_delete(ddata)
                print("task_id =", delete_result["task_id"])
