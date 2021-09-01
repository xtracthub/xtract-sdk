
import os


def is_metadata_nonempty(metadata):
    """
    Helper function to acknowledge metadata that are 'empty' by multiple measures.

    While empty metadata is not necessarily incorrect, it could be a heavy indicator of those that are 'incomplete' or
    actually not processing.

    :param metadata: (Union[None, set, dict]) -- object containing metadata
    :return: bool: whether the metadata are non-empty by all measures.
    """
    if metadata not in [None, {}, dict()]:
        return True
    else:
        return False

def get_dl_thruples_from_fam(family, headers, base_url, base_store_path, is_local):

    thruples = []

    family_id = family.family_id
    fam_files = family.files

    for file_obj in fam_files:
        filename = base_url + file_obj["path"]

        if not is_local:
            local_filename = filename.split('/')[-1]
        else:
            local_filename = filename

        if not is_local:
            new_path = os.path.join(family_id, local_filename)
            new_path = os.path.join(base_store_path, new_path)
        else:
            new_path = local_filename

        batch_thruple = [filename, new_path, headers]

        if 'is_gdoc' in file_obj:
            batch_thruple.append(file_obj['is_gdoc'])
        if 'mimeType' in file_obj:
            batch_thruple.append(file_obj['mimeType'])

        thruples.append(batch_thruple)

    return thruples
