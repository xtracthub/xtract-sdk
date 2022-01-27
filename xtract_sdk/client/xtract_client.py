import time
import json
import requests
import mdf_toolbox
import globus_sdk
from xtract_sdk.client import XTRACT_CRAWLER, XTRACT_CRAWLER_DEV, XTRACT_SERVICE, XTRACT_SERVICE_DEV
from ..xtract_validator import rc_validator, c_validator, gcs_validator, fcm_validator, om_validator


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
        self.cid_to_endpoint_map = dict()

    def register_containers(self, endpoint, container_path):
        """ Function to register containers with the central service. """

        rc_validator(endpoint=endpoint,
                     container_path=container_path)

        fx_headers = {'Authorization': f"Bearer {self.auths[self.funcx_scope].access_token}",
                      'Search': self.auths['search'].authorizer.access_token,
                      'Openid': self.auths['openid'].access_token}

        payload = {'fx_eid': endpoint.funcx_ep_id,
                   'headers': fx_headers,
                   'container_path': container_path}

        # TODO: make this configurable for dev + otherwise.
        route = XTRACT_SERVICE

        resp = requests.post(f"{route}config_containers", json=payload)
        return f"Register containers status (should be 200): {json.loads(resp.content)['status']}"

    def crawl(self, endpoints):
        """Initiates a Globus directory crawl, returning a crawl ID for each endpoint.

        Returns
        -------
        crawl_ids: list
            A crawl ID for each endpoint. Also saved to self.crawl_ids
        """

        c_validator(endpoints = endpoints)

        crawl_ids = []

        crawl_url = f'{self.base_url}crawl'

        for endpoint in endpoints:
            ep_dicts = []
            if endpoint.repo_type.lower() == "globus":
                ep_dicts.append({'repo_type': endpoint.repo_type,
                                 'eid': endpoint.globus_ep_id,
                                 'dir_paths': endpoint.dirs,
                                 'grouper': endpoint.grouper})

                crawl_tokens = {'Transfer': self.auths['transfer'].authorizer.access_token,
                                'Authorization': f"Bearer {self.auths['transfer'].authorizer.access_token}",
                                'FuncX': self.auths[self.funcx_scope].access_token}

                crawl_req = requests.post(crawl_url,
                                          json={'endpoints': ep_dicts,
                                                'tokens': crawl_tokens})
            elif endpoint.repo_type == "GDRIVE":
                raise NotImplementedError('GDRIVE is not implemented as repo type yet')
            else:
                raise Exception(f"repo_type {endpoint.repo_type} is invalid")

            try:
                crawl_id = json.loads(crawl_req.content)["crawl_id"]
            except json.decoder.JSONDecodeError:  # TODO: Add a better catch based on status codes.
                raise Exception(f"Crawl request failed with status {crawl_req.status_code}")

            crawl_ids.append(crawl_id)
            self.cid_to_endpoint_map[crawl_id] = endpoint

        self.crawl_ids = crawl_ids
        return crawl_ids

    def get_crawl_status(self, crawl_ids=None):
        """Retrieves the crawl status of a job.

        Returns
        -------
        payload: list
            For each crawl job, a dict with crawl ID, status, message, and data.
        """

        gcs_validator(crawl_ids=crawl_ids)

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

    def crawl_and_wait(self, endpoints):

        c_validator(endpoints=endpoints)

        self.crawl(endpoints)

        while True:

            crawl_statuses = self.get_crawl_status()
            for resp in crawl_statuses:
                print(resp)

            sub_statuses = [d['status'] for d in crawl_statuses]
            if all(s == 'complete' for s in sub_statuses):
                break

            time.sleep(2)

    def flush_crawl_metadata(self, crawl_ids=None, next_n_files=100):
        """Returns a list of all metadata from the crawl.

        Returns
        -------
        payload: list
            The metadata for each crawl job.
        """

        fcm_validator(crawl_ids=crawl_ids,
                      next_n_files=next_n_files)

        if crawl_ids is None and self.crawl_ids is None:
            raise Exception("Missing crawl ID. A crawl ID must be provided or the .crawl() method must be run")
        elif crawl_ids is None:
            crawl_ids = self.crawl_ids

        flush_url = f'{self.base_url}fetch_crawl_mdata'

        payload = []
        for cid in crawl_ids:
            req = requests.get(flush_url,
                               json={'crawl_id': cid,
                                     'n': next_n_files})
            payload.append(req.content)

        return payload

    def xtract(self):
        """Initiates metadata extraction workflow, extracting/sending metadata to central Xtract service.

        Returns
        -------
        payload: list
            The xtract response code.
        """

        if not self.crawl_ids:
            raise Exception("Missing crawl ID, the .crawl() method must be run")

        fx_headers = {'Authorization': f"Bearer {self.auths[self.funcx_scope].access_token}",
                      'Search': self.auths['search'].authorizer.access_token,
                      'Openid': self.auths['openid'].access_token}

        payload = []

        for cid in self.crawl_ids:
            if not self.cid_to_endpoint_map[cid].metadata_directory:
                raise Exception("Missing metadata directory for one of your endpoints, this is required for Xtraction.")
            post = requests.post(f'{self.extract_url}extract',
                                 json={'crawl_id': cid,
                                       'fx_ep_ids': [self.cid_to_endpoint_map[cid].funcx_ep_id],
                                       'tokens': fx_headers,
                                       'local_mdata_path': self.cid_to_endpoint_map[cid].metadata_directory,
                                       'remote_mdata_path': ''})  # TODO: let us remove this altogether
            payload.append(post)

        return payload

    def get_xtract_status(self):
        """Returns the status of the metadata extraction.

        Returns
        -------
        payload: list
            For each endpoint, a dict of status and counters.
        """

        if not self.crawl_ids:
            raise Exception("Missing crawl ID, the .crawl() method must be run")

        status_url = f'{self.extract_url}get_extract_status'

        payload = []

        for cid in self.crawl_ids:
            xtract_status = requests.get(status_url,
                                         json={'crawl_id': cid})
            payload.append({'xtract_status': json.loads(xtract_status.content)['status'],
                            'xtract_counters': json.loads(xtract_status.content)['counters']})

        return payload

    def offload_metadata(self, dest_ep_id, dest_path="", timeout=600, delete_source=False):
        """Transfers metadata from Xtraction to a timestamped folder in dest_ep_id TODO: alt naming scheme

        Returns
        -------
        path: string
            The path where the metadata was transferred to.
        """

        om_validator(dest_ep_id=dest_ep_id,
                     dest_path=dest_path,
                     timeout=timeout,
                     delete_source=delete_source)

        if not self.crawl_ids:
            raise Exception("Missing crawl ID, the .crawl() method must be run")

        tc = self.auths["transfer"]

        for cid in self.crawl_ids:

            source_id = self.cid_to_endpoint_map[cid].globus_ep_id
            tc.endpoint_autoactivate(source_id)
            tc.endpoint_autoactivate(dest_ep_id)

            source_path = self.cid_to_endpoint_map[cid].metadata_directory
            timestamped_dest_path = (dest_path
                                     + time.strftime("%Y-%m-%d-%H.%M.%S", time.gmtime())
                                     + "/")
            tc.operation_mkdir(dest_ep_id, path=timestamped_dest_path)

            tdata = globus_sdk.TransferData(tc, source_id, dest_ep_id)
            for item in tc.operation_ls(source_id, path=source_path):
                tdata.add_item(source_path + '/' + item['name'], timestamped_dest_path + item['name'])

            submit_result = tc.submit_transfer(tdata)
            print(f"Task ID: {submit_result['task_id']}")

            if delete_source:
                tc.task_wait(submit_result['task_id'],  # TODO: decide if we want this inside or outside the if
                             timeout=timeout)
                ddata = globus_sdk.DeleteData(tc, source_id, recursive=True)
                ddata.add_item(source_path)
                delete_result = tc.submit_delete(ddata)
                print("task_id =", delete_result["task_id"])

            return timestamped_dest_path

    def search_ingest(self, search_index_id, dataset_mdata):
        """ A demo (read: future-poor) function that enables people to take dataset metadata, append it to a bunch
            of entries, and push it all to an (already-existing) Globus Search index. """
        pass