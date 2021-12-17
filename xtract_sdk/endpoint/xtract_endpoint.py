
import json
import requests

from xtract_sdk.client.routes import XTRACT_SERVICE, XTRACT_SERVICE_DEV


class XtractEndpoint:

    def __init__(self, repo_type, globus_ep_id, dirs, grouper, local_mdata_path,
                 remote_mdata_path=None, funcx_ep_id=None):
        self.repo_type = repo_type
        self.globus_ep_id = globus_ep_id
        self.funcx_ep_id = funcx_ep_id
        self.dirs = dirs
        self.grouper = grouper
        self.local_mdata_path = local_mdata_path
        self.remote_mdata_path = remote_mdata_path

    def register_containers(self, container_path, headers):
        """ Function to register containers with the central service. """
        payload = {'fx_eid': self.funcx_ep_id,
                   'headers': headers,
                   'container_path': container_path}

        # TODO: make this configurable for dev + otherwise.
        route = XTRACT_SERVICE

        resp = requests.post(f"{route}config_containers", json=payload)
        print(f"Register containers status: {json.loads(resp.content)['status']}")
