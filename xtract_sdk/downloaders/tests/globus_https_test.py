
from fair_research_login import NativeClient
from xtract_sdk.downloaders.globus_https import GlobusHttpsDownloader

# Get the Headers....
client = NativeClient(client_id='7414f0b4-7d05-4bb6-bb00-076fa3f17cf5')
tokens = client.login(
    requested_scopes=['https://auth.globus.org/scopes/56ceac29-e98a-440a-a594-b41e7a084b62/all',
                      'urn:globus:auth:scope:transfer.api.globus.org:all',
                     "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all",
                    "urn:globus:auth:scope:data.materialsdatafacility.org:all",
                     'email', 'openid'],
    no_local_server=True,
    no_browser=True)

auth_token = tokens["petrel_https_server"]['access_token']
transfer_token = tokens['transfer.api.globus.org']['access_token']
mdf_token = tokens["data.materialsdatafacility.org"]['access_token']
funcx_token = tokens['funcx_service']['access_token']

headers = {'Authorization': f"Bearer {funcx_token}", 'Transfer': transfer_token, 'FuncX': funcx_token, 'Petrel': mdf_token}

import json
with open("/Users/tylerskluzacek/.xtract/tyler_test/creds/GLOBUS_HTTPS", 'w') as f:
    json.dump(headers, f)

print(f"Headers: {headers}")

# NCSA file
https_file = "/mdf_open/kearns_biofilm_rupture_location_v1.1/Biofilm Images/Paper Images/Isofluence Images (79.4)/THY+75mM AA/1.jpg"
base_url = "https://data.materialsdatafacility.org"

# thruple = ('https://data.materialsdatafacility.org/thurston_selfassembled_peptide_spectra_v1.1/DFT/MoleculeConfigs/di_30_-50.xyz/INCAR', str(0), {'Authorization': 'Bearer AgbMDqq3MBoGP2Q80pe2MgXyJrKzVpgOmEK6m80rp40kJeWJbC5CE7Gya4VVdmrz7z5kajKQbXQwvIDaXwOzhNVBo', 'Transfer': 'AgVGq0egpP6qQdg52nMEM216V1MdGYgdXMjwmm5d0WQdpmlwDjSbCXOJWbvOVPdXa1GYVkXEpwk1gdUlK1jnwi9w0r', 'FuncX': 'AgbMDqq3MBoGP2Q80pe2MgXyJrKzVpgOmEK6m80rp40kJeWJbC5CE7Gya4VVdmrz7z5kajKQbXQwvIDaXwOzhNVBo', 'Petrel': 'Ag5V6erx25KBPkMbMjJ1mGdWxWjXr396jooe66bke7dOyeKGn5tnCPKB0keDm4W8DjKdjg3320yWbvsKn8YpBTVwBe'})
thruples = [('https://data.materialsdatafacility.org/thurston_selfassembled_peptide_spectra_v1.1/DFT/MoleculeConfigs/di_30_-50.xyz/INCAR', '0', {'Authorization': 'Bearer AgbMDqq3MBoGP2Q80pe2MgXyJrKzVpgOmEK6m80rp40kJeWJbC5CE7Gya4VVdmrz7z5kajKQbXQwvIDaXwOzhNVBo', 'Transfer': 'AgVGq0egpP6qQdg52nMEM216V1MdGYgdXMjwmm5d0WQdpmlwDjSbCXOJWbvOVPdXa1GYVkXEpwk1gdUlK1jnwi9w0r', 'FuncX': 'AgbMDqq3MBoGP2Q80pe2MgXyJrKzVpgOmEK6m80rp40kJeWJbC5CE7Gya4VVdmrz7z5kajKQbXQwvIDaXwOzhNVBo', 'Petrel': 'Ag5V6erx25KBPkMbMjJ1mGdWxWjXr396jooe66bke7dOyeKGn5tnCPKB0keDm4W8DjKdjg3320yWbvsKn8YpBTVwBe'}), ('https://data.materialsdatafacility.org/thurston_selfassembled_peptide_spectra_v1.1/DFT/MoleculeConfigs/di_30_-50.xyz/OUTCAR', '1', {'Authorization': 'Bearer AgbMDqq3MBoGP2Q80pe2MgXyJrKzVpgOmEK6m80rp40kJeWJbC5CE7Gya4VVdmrz7z5kajKQbXQwvIDaXwOzhNVBo', 'Transfer': 'AgVGq0egpP6qQdg52nMEM216V1MdGYgdXMjwmm5d0WQdpmlwDjSbCXOJWbvOVPdXa1GYVkXEpwk1gdUlK1jnwi9w0r', 'FuncX': 'AgbMDqq3MBoGP2Q80pe2MgXyJrKzVpgOmEK6m80rp40kJeWJbC5CE7Gya4VVdmrz7z5kajKQbXQwvIDaXwOzhNVBo', 'Petrel': 'Ag5V6erx25KBPkMbMjJ1mGdWxWjXr396jooe66bke7dOyeKGn5tnCPKB0keDm4W8DjKdjg3320yWbvsKn8YpBTVwBe'}), ('https://data.materialsdatafacility.org/thurston_selfassembled_peptide_spectra_v1.1/DFT/MoleculeConfigs/di_30_-50.xyz/POSCAR', '2', {'Authorization': 'Bearer AgbMDqq3MBoGP2Q80pe2MgXyJrKzVpgOmEK6m80rp40kJeWJbC5CE7Gya4VVdmrz7z5kajKQbXQwvIDaXwOzhNVBo', 'Transfer': 'AgVGq0egpP6qQdg52nMEM216V1MdGYgdXMjwmm5d0WQdpmlwDjSbCXOJWbvOVPdXa1GYVkXEpwk1gdUlK1jnwi9w0r', 'FuncX': 'AgbMDqq3MBoGP2Q80pe2MgXyJrKzVpgOmEK6m80rp40kJeWJbC5CE7Gya4VVdmrz7z5kajKQbXQwvIDaXwOzhNVBo', 'Petrel': 'Ag5V6erx25KBPkMbMjJ1mGdWxWjXr396jooe66bke7dOyeKGn5tnCPKB0keDm4W8DjKdjg3320yWbvsKn8YpBTVwBe'})]



from urllib.parse import urljoin

full_path = urljoin(base_url, https_file)
print(full_path)

ghd = GlobusHttpsDownloader()
# ghd.fetch(full_path, headers, "potato/sushi.file1")
# ghd.batch_fetch([(full_path, "potato/sushi.file1", headers), (full_path, "potato/sushi.file2", headers)])
ghd.batch_fetch(thruples)
print(ghd.success_files)