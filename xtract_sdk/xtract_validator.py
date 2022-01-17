
from endpoint import XtractEndpoint


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


def rc_validator(endpoint=None,
                 container_path=None):

    if endpoint and not isinstance(endpoint, XtractEndpoint):
        raise Exception('Endpoint given must be an XtractEndpoint object')
    elif container_path and not isinstance(container_path, str):
        raise Exception('Container path must be a string')
    else:
        return


def c_validator(endpoints=None):

    if endpoints and ((not isinstance(endpoints, list))
                      or (not all(isinstance(elem, XtractEndpoint) for elem in endpoints))):
        raise Exception('Endpoints must be a list of XtractEndpoints objects')
    else:
        return


def gcs_validator(crawl_ids=None):

    if crawl_ids and ((not isinstance(crawl_ids, list))
                      or (not all(isinstance(elem, str) for elem in crawl_ids))):
        raise Exception('Crawl IDs must be a list of strings')
    else:
        return


def fcm_validator(crawl_ids=None,
                  next_n_files=None):

    if crawl_ids and ((not isinstance(crawl_ids, list))
                      or (not all(isinstance(elem, str) for elem in crawl_ids))):
        raise Exception('Crawl IDs must be a list of strings')
    elif next_n_files and not isinstance(next_n_files, int):
        raise Exception('next_n_files must be a positive integer (or None)')
    elif next_n_files and next_n_files <= 0:
        raise Exception('next_n_files must be a positive integer (or None)')
    else:
        return


def om_validator(dest_ep_id=None,
                 dest_path=None,
                 timeout=None,
                 delete_source=None):

    if dest_ep_id and not isinstance(dest_ep_id, str):
        raise Exception('Destination endpoint ID must be a string')
    elif dest_path and not isinstance(dest_path, str):
        raise Exception('Destination path must be a string')
    elif timeout and not isinstance(timeout, int):
        raise Exception('Timeout must be a positive integer')
    elif timeout and timeout <= 0:
        raise Exception('Timeout must be a positive integer')
    elif not isinstance(delete_source, bool):
        raise Exception('Delete_source must be a boolean')
    else:
        return
