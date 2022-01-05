
class XtractEndpoint:

    def __init__(self, repo_type, globus_ep_id, dirs, grouper, local_mdata_path, funcx_ep_id=None):
        self.repo_type = repo_type
        self.dirs = dirs
        self.grouper = grouper
        self.local_mdata_path = local_mdata_path

        if isinstance(globus_ep_id, str):
            self.globus_ep_id = globus_ep_id
        else:
            raise Exception('Globus endpoint ID must be a string')

        if isinstance(funcx_ep_id, str) or (not funcx_ep_id):
            self.funcx_ep_id = funcx_ep_id
        else:
            raise Exception('FuncX endpoint ID must be a string')
