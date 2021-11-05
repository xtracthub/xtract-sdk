
import json
import requests
import mdf_toolbox
from xtract_sdk.client.routes import XTRACT_CRAWLER, XTRACT_CRAWLER_DEV, XTRACT_SERVICE, XTRACT_SERVICE_DEV

#test
class XtractClient:

    def __init__(self, auth_scopes=None, dev=False):

        if dev:
            self.crawl_url = XTRACT_CRAWLER_DEV
            self.extract_url = XTRACT_SERVICE_DEV
        else:
            self.crawl_url = XTRACT_CRAWLER
            self.extract_url = XTRACT_SERVICE

        scopes = [
            "openid",
            "data_mdf",
            "search",
            "petrel",
            "transfer",
            "dlhub",
            "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all",
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

    def crawl(self, repo_type, **kwargs):

        # TODO: GET THE CRAWL WORKING!

        if repo_type == "GLOBUS":
            crawl_params = ["eid", "dir_path", "grouper", "https_info"]
            payload = {"repo_type": repo_type}

            for param in crawl_params:
                try:
                    payload[param] = kwargs.get(param)
                except KeyError:
                    raise Exception(f"Missing parameter {param} for {repo_type} crawl")

            payload["Transfer"] = self.transfer_token
            payload["Authorization"] = self.funcx_token

            crawl_req = requests.post(self.crawl_url, json=payload)

        # elif repo_type == "GDRIVE":
        #     payload = {"auth_creds": self.gdrive_auth_creds, "repo_type": repo_type}
        #     crawl_req = requests.post(self.crawl_url, data=pickle.dumps(payload))
        else:
            raise Exception(f"repo_type {repo_type} is invalid")

        try:
            crawl_id = json.loads(crawl_req.content)["crawl_id"]
        except:  # TODO: Add a better catch based on status codes.
            raise Exception(f"Crawl request failed with status {crawl_req.status_code}")

        self.crawl_id = crawl_id
        self.repo_type = repo_type

        return crawl_id

    def get_crawl_status(self):
        """Retrieves the crawl status of a job. .crawl() method must be run first.

        Returns
        -------
        crawl_status_content: dict
            Status of crawl job.
        """
        crawl_status_url = f"{self.base_url}/get_crawl_status"

        if self.crawl_id is None:
            raise Exception("Missing crawl ID. A crawl ID must be provided or the .crawl() method must be run")

        payload = {"crawl_id": self.crawl_id}
        crawl_status_req = requests.get(crawl_status_url, json=payload)

        try:
            crawl_status_content = json.loads(crawl_status_req.content)
        except:  # TODO: Add a better catch based on status codes. IDK what the default status code is.
            raise Exception(f"Crawl status retrieval failed with status {crawl_status_req.status_code}")

        return crawl_status_content

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
