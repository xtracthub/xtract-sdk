
print("Beginning the Family packaging TO Google Drive downloading tests...")

from xtract_sdk.downloaders.google_drive import GoogleDriveDownloader
from xtract_sdk.packagers.family import Family
from xtract_sdk.packagers.family_batch import FamilyBatch

import pickle, os
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly', 'https://www.googleapis.com/auth/drive']


def do_login_flow():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                '../xtract_sdk/downloaders/credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return creds


gdr = GoogleDriveDownloader(auth_creds=do_login_flow())

file_1 = "1RbSdH_nI0EHvxFswpl1Qss7CyWXBHo-o"  # JPG image!
file_2 = "1ecjFs55sNxBiwoAtztHcoA450Gh7ak0m9VqK0Wrm1Ms"  # free text document

fam_1 = Family()
# TODO: Put the Google Drive arguments into a "gdrive_cfg" sub-dicitonary.
fam_1.add_group(files=[{'path': file_1, 'is_gdoc': False, 'metadata': {}, 'mimeType': 'image/jpg'}], parser='image')

fam_2 = Family()
fam_2.add_group(files=[{'path': file_2, 'is_gdoc': True, 'metadata': {}, 'mimeType': 'text/plain'}], parser='keyword')


fam_batch = FamilyBatch()
fam_batch.add_family(fam_1)
fam_batch.add_family(fam_2)

gdr.batch_fetch(family_batch=fam_batch)
print(gdr.success_files)
