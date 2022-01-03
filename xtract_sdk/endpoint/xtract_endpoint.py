
class XtractEndpoint:

    def __init__(self, repo_type, globus_ep_id, dirs, grouper, local_mdata_path, funcx_ep_id=None):
        self.repo_type = repo_type
        self.globus_ep_id = globus_ep_id
        self.funcx_ep_id = funcx_ep_id
        self.dirs = dirs
        self.grouper = grouper
        self.local_mdata_path = local_mdata_path
