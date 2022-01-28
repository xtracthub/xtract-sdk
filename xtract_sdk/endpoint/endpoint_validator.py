
def ep_validator(repo_type=None,
                 globus_ep_id=None,
                 dirs=None,
                 grouper=None,
                 metadata_directory=None,
                 funcx_ep_id=None):

    if repo_type and not isinstance(repo_type, str):
        raise Exception('Repo type must be a string')
    elif globus_ep_id and not isinstance(globus_ep_id, str):
        raise Exception('Globus endpoint ID must be a string')
    elif dirs and ((not isinstance(dirs, list))
                   or (not all(isinstance(elem, str) for elem in dirs))):
        raise Exception('Directory must be a list of strings')
    elif grouper and (not isinstance(grouper, str)):
        raise Exception('Grouper must be a string')
    elif metadata_directory and (not isinstance(metadata_directory, str)):
        raise Exception('Metadata directory must be a string (or None)')
    elif funcx_ep_id and (not isinstance(funcx_ep_id, str)):
        raise Exception('Funcx endpoint ID must be a string (or None)')
    else:
        return
