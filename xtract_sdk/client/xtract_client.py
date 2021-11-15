import json
import requests
import mdf_toolbox
from xtract_sdk.client import XTRACT_CRAWLER, XTRACT_CRAWLER_DEV, XTRACT_SERVICE, XTRACT_SERVICE_DEV


class XtractClient:

    def __init__(self, auth_scopes=None, dev=False):

        if dev:
            self.base_url = XTRACT_CRAWLER_DEV
            self.extract_url = XTRACT_SERVICE_DEV
        else:
            self.base_url = XTRACT_CRAWLER
            self.extract_url = XTRACT_SERVICE

        self.funcx_scope = "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all"
        scopes = [
            "openid",
            "data_mdf",
            "search",
            "petrel",
            "transfer",
            "dlhub",
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
            no_local_server=False)

        self.crawl_ids = None

    def crawl(self, xeps):

        crawl_ids = []

        crawl_url = f'{self.base_url}crawl'

        for xep in xeps:
            ep_dicts = []
            if xep.repo_type.lower() == "globus":
                ep_dicts.append({'repo_type': xep.repo_type,
                                 'eid': xep.globus_ep_id,
                                 'dir_paths': xep.dirs,
                                 'grouper': xep.grouper})
                # crawl_params = ["eid", "dir_path", "grouper", "https_info"]
                # payload = {"repo_type": repo_type}
                # for param in crawl_params:
                #    try:
                #        payload[param] = kwargs.get(param)
                #    except KeyError:
                #        raise Exception(f"Missing parameter {param} for {repo_type} crawl")

                crawl_tokens = {'Transfer': self.auths['transfer'].authorizer.access_token,
                                'Authorization': f"Bearer {self.auths['transfer'].authorizer.access_token}",
                                'FuncX': self.auths[self.funcx_scope].access_token}

                crawl_req = requests.post(crawl_url, json={'endpoints': ep_dicts,
                                                           'tokens': crawl_tokens})
            elif repo_type == "GDRIVE":
                raise NotImplementedError('GDRIVE is not implemented as repo type yet')
            #     payload = {"auth_creds": self.gdrive_auth_creds, "repo_type": repo_type}
            #     crawl_req = requests.post(self.base_url, data=pickle.dumps(payload))
            else:
                raise Exception(f"repo_type {xep.repo_type} is invalid")

            try:
                crawl_id = json.loads(crawl_req.content)["crawl_id"]
            except:  # TODO: Add a better catch based on status codes.
                raise Exception(f"Crawl request failed with status {crawl_req.status_code}")

            crawl_ids.append(crawl_id)

        self.crawl_ids = crawl_ids
        return crawl_ids

    def get_crawl_status(self, crawl_ids=None):
        """Retrieves the crawl status of a job. .crawl() method must be run first.

        Returns
        -------
        crawl_status_content: dict
            Status of crawl job.
        """

        # if (self.crawl_ids is None) and (crawl_ids is not None):
        #    self.crawl_ids = crawl_ids
        if crawl_ids is None and self.crawl_ids is None:
            raise Exception("Missing crawl ID. A crawl ID must be provided or the .crawl() method must be run")
        elif crawl_ids is None:
            crawl_ids = self.crawl_ids

        status_url = f"{self.base_url}/get_crawl_status"

        statuses = []

        for cid in crawl_ids:
            crawl_status = requests.get(status_url, json={'crawl_id': cid})
            try:
                crawl_content = json.loads(crawl_status.content)
            except:  # TODO: Add a better catch based on status codes. IDK what the default status code is.
                raise Exception(f"Crawl status retrieval failed with status {crawl_status.status_code}")
            statuses.append(crawl_content)

        payload = []
        for status in statuses:
            stat_dict = {'crawl_id': status['crawl_id'],
                         'status': status['crawl_status'],
                         'message': 'OK or error'}  # TODO: Fix to be 'OK' when okay and an error when there's an error
            restof_keys = set(status.keys()) - set(['crawl_id',
                                                    'crawl_status'])
            stat_dict['data'] = dict((k, status[k]) for k in restof_keys)
            payload.append(stat_dict)

        return payload

    def flush_crawl_metadata(self, crawl_ids=None, n=1):

        if crawl_ids is None and self.crawl_ids is None:
            raise Exception("Missing crawl ID. A crawl ID must be provided or the .crawl() method must be run")
        elif crawl_ids is None:
            crawl_ids = self.crawl_ids

        flush_url = f'{self.base_url}fetch_crawl_mdata'

        payload = []
        for id in crawl_ids:
            req = requests.get(flush_url, json={'crawl_id': id,
                                                'n': n})
            payload.append(req.content)

        return payload

    # def extract(self, **kwargs):
    #     """Sends extract request to Xtract.
    #
    #     Parameters
    #     ----------
    #     kwargs : dict
    #         Additional keyword arguments for extracting. GLOBUS extraction supports "repo_type", "funcx_eid",
    #         "source_eid", "dest_eid", "mdata_store_path". GDRIVE extraction supports "gdrive_pkl", "funcx_eid",
    #         "source_eid", "dest_eid", "mdata_store_path". The type of extraction (GLOBUS or GDRIVE) is dependant
    #         on the repo_type parameter passed to the .crawl() method.
    #
    #     Returns
    #     -------
    #     extract_status_code : int
    #         Status code of extract request.
    #     """
    #     extract_url = f"{self.base_url}/extract"
    #     headers = {"Authorization": f"Bearer {self.petrel_token}",
    #                "Transfer": self.transfer_token,
    #                "FuncX": self.funcx_token, "Petrel": self.petrel_token}
    #     payload = {"headers": json.dump(headers), "crawl_id": self.crawl_id}
    #
    #     if self.repo_type == "GLOBUS":
    #         extract_params = ["repo_type", "funcx_eid", "source_eid", "dest_eid", "mdata_store_path"]
    #     elif self.repo_type == "GDRIVE":
    #         extract_params = ["gdrive_pkl", "funcx_eid", "source_eid", "dest_eid", "mdata_store_path"]
    #
    #     for param in extract_params:
    #         try:
    #             payload[param] = kwargs.get(param)
    #         except KeyError:
    #             raise Exception(f"Missing parameter {param} for {self.repo_type} extraction")
    #
    #     if self.repo_type == "GLOBUS":
    #         extract_req = requests.post(extract_url, json=payload)
    #     elif self.repo_type == "GDRIVE":
    #         extract_req = requests.post(extract_url, data=pickle.dumps(payload))
    #
    #     extract_status_code = extract_req.status_code
    #
    #     if extract_status_code != 200:
    #         raise Exception(f"Extraction failed with status code {extract_status_code}")
    #
    #     return extract_status_code
    #
    # def get_extract_status(self):
    #     """Retrieves the extract status of a job. .crawl() method must be run first.
    #
    #     Returns
    #     -------
    #     extract_status_content : dict
    #         Status of extract job.
    #     """
    #     extract_status_url = f"{self.base_url}/get_extract_status"
    #     payload = {"crawl_id": self.crawl_id}
    #
    #     extract_status_req = requests.get(extract_status_url, json=payload)
    #
    #     try:
    #         extract_status_content = json.loads(extract_status_req.content)
    #     except: # TODO: Add a better catch based on status codes. IDK what the default status code is.
    #         raise Exception(f"Extract status retrieval failed with status {extract_status_req.status_code}")
    #
    #     return extract_status_content
