
from xtract_sdk.xtract import XtractAgent
from xtract_sdk.downloaders.tests.test_families import family_1, family_2

xtra = XtractAgent(ep_name="tyler_test", xtract_dir="/Users/tylerskluzacek/.xtract")

fam_1 = family_1.to_dict()

xtra.load_family(fam_1)

print(f"# items in Xtract queue: {xtra.families.qsize()}")

xtra.download_batch(downloader="GLOBUS_HTTPS")

xtra_2 = XtractAgent(ep_name="tyler_test", xtract_dir="/Users/tylerskluzacek/.xtract")
fam_2 = family_2.to_dict()
xtra_2.load_family(fam_2)

xtra_2.download_batch(downloader="GDRIVE")

