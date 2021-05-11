
from xtract_sdk.agent.xtract import XtractAgent
from xtract_sdk.downloaders.tests.test_families import family_3
from xtract_sdk.packagers.family_batch import FamilyBatch

# TODO: TEST that .success_files is placing files with the new paths (not old).
# xtra = XtractAgent(ep_name="tyler_test", xtract_dir="/Users/tylerskluzacek/.xtract")
#
# fam_1 = family_1.to_dict()
#
# xtra.load_family(fam_1)
#
# fam_2 = family_2.to_dict()
# xtra.load_family(fam_2)
#
# fam_3 = family_1.to_dict()
# fam_3['download_type'] = "LOCAL"
# xtra.load_family(fam_3)
#
# xtra.fetch_all_files()
#
# xtra.delete_downloaded_files()

# TODO: maybe just have remote:local map instead of success/failure.

# TODO: Turn the following into unittests
# print(xtra.success_files)
# print(xtra.ready_families)

# TODO: Now we'll test a file that's stored locally.
xtra_2 = XtractAgent(ep_name="tyler_test", xtract_dir="/Users/tylerskluzacek/.xtract")

fambatch = FamilyBatch()
fambatch.add_family(family_3)


xtra_2.load_and_fetch_families(fambatch)

# xtra_2._fetch_all_files()
print(xtra_2.ready_families[0].to_dict())