
import os


def get_dl_thruples_from_fam(family, headers, base_url):

    thruples = []

    family_id = family["family_id"]
    fam_files = family["files"]

    for file_obj in fam_files:
        # base_url = file_obj['base_url']
        filename = base_url + file_obj["path"]

        # if not is_local:
        local_filename = filename.split('/')[-1]
        # else:
        #     local_filename = filename

        new_path = os.path.join(family_id, local_filename)
        batch_thruple = (filename, new_path, headers)

        thruples.append(batch_thruple)
    return thruples
