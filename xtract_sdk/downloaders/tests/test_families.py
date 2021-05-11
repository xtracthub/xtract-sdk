
from xtract_sdk.packagers.family import Family

# GLOBUS HTTPS FAMILIES
base_url = "https://data.materialsdatafacility.org"
family_1 = Family()
family_1.add_group(files=[
    {'path': '/thurston_selfassembled_peptide_spectra_v1.1/DFT/MoleculeConfigs/di_30_-50.xyz/INCAR'},
    {'path': '/thurston_selfassembled_peptide_spectra_v1.1/DFT/MoleculeConfigs/di_30_-50.xyz/OUTCAR'},
    {'path': '/thurston_selfassembled_peptide_spectra_v1.1/DFT/MoleculeConfigs/di_30_-50.xyz/POSCAR'}],
    parser='ase')
family_1.base_url = base_url
family_1.download_type = "GLOBUS_HTTPS"

# family_2 = Family
# family.add_group(files={'path'}, parser='')

# GOOGLE DRIVE FAMILIES
base_url = ""

file_id = "1XCS2Xqu35TiQgCpI8J8uu4Mss9FNnp1-AuHo-pMujb4"
file_id2 = "0B5nDSpS9a_3kUFdiTXRFdS12QUk"

family_2 = Family()
family_2.add_group(files=[
    {'path': file_id, 'is_gdoc': True, 'mimeType': "text/csv"}],
    parser='xtract-tabular')
family_2.base_url = ""

family_2.add_group(files=[
    {'path': file_id2, 'is_gdoc': False}],
    parser='xtract-tabular')
family_2.download_type = "GDRIVE"

family_3 = Family()
family_3.add_group(files=[{'path': '/projects/CSC249ADCD01/skluzacek/containers/comma_delim'}], parser=None)
family_3.download_type = "LOCAL"
# family_3.base_url = ""

# fam_batch = FamilyBatch()
# fam_batch.add_family(fam_1)
