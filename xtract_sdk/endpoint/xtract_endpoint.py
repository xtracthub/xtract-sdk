
class XtractEndpoint:

    def __init__(self, repo_type, globus_ep_id, dirs, grouper,
                 metadata_directory=None, funcx_ep_id=None):

        if not isinstance(repo_type, str):
            raise Exception('Repo type must be a string')
        elif not isinstance(globus_ep_id, str):
            raise Exception('Globus endpoint ID must be a string')
        elif not all(isinstance(elem, str) for elem in dirs):
            raise Exception('Directory object must be a list of strings')
        elif not isinstance(grouper, str):
            raise Exception('Grouper must be a string')
        elif metadata_directory and (not isinstance(metadata_directory, str)):
            raise Exception('Metadata directory must be a string (or None)')
        elif funcx_ep_id and (not isinstance(funcx_ep_id, str)):
            raise Exception('Funcx endpoint ID must be a string (or None)')
        else:
            self.repo_type = repo_type
            self.dirs = dirs
            self.grouper = grouper
            self.metadata_directory = metadata_directory
            self.globus_ep_id = globus_ep_id
            self.funcx_ep_id = funcx_ep_id
