
import json
import requests
import mdf_toolbox
from xtract_sdk.client import XTRACT_SERVICE


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

    def register_containers(self, container_path, auth_scopes=None):
        """ Function to register containers with the central service. """
        funcx_scope = "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all"
        scopes = [
            "openid",
            "search",
            "petrel",
            "transfer",
            funcx_scope
        ]

        if auth_scopes is not None:
            for scope in auth_scopes:
                scopes.append(scope)

        auths = mdf_toolbox.login(
            services=scopes,
            app_name="Foundry",
            make_clients=True,
            no_browser=False,
            no_local_server=False
        )

        fx_headers = {'Authorization': f"Bearer {auths[funcx_scope].access_token}",
                      'Search': auths['search'].authorizer.access_token,
                      'Openid': auths['openid'].access_token}

        payload = {'fx_eid': self.funcx_ep_id,
                   'headers': fx_headers,
                   'container_path': container_path}

        # TODO: make this configurable for dev + otherwise.
        route = XTRACT_SERVICE

        resp = requests.post(f"{route}config_containers", json=payload)
        print(f"Register containers status: {json.loads(resp.content)['status']}")
