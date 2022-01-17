
from ..xtract_validator import ep_validator


class XtractEndpoint:

    def __init__(self, repo_type, globus_ep_id, dirs, grouper,
                 metadata_directory=None, funcx_ep_id=None):

        if ep_validator(repo_type=repo_type,
                        globus_ep_id=globus_ep_id,
                        dirs=dirs,
                        grouper=grouper,
                        metadata_directory=metadata_directory,
                        funcx_ep_id=funcx_ep_id):
            self.repo_type = repo_type
            self.dirs = dirs
            self.grouper = grouper
            self.metadata_directory = metadata_directory
            self.globus_ep_id = globus_ep_id
            self.funcx_ep_id = funcx_ep_id
