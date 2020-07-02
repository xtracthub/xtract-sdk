
from xtract_sdk.packagers.family import Family
from xtract_sdk.packagers.family_batch import FamilyBatch
import time

# TODO: extract from a list of families
fam = Family(str(0), headers={'potato': 'tomato'}, metadata=None)
fam2 = Family(str(1), headers={'potato': 'tomato'}, metadata=None)

group_id = fam.add_group(files=[{'path': 'a', 'metadata': {}}, {'path': 'b', 'metadata': {}},
                                {'path': 'c', 'metadata': {}}], parser="camel")
group_id2 = fam.add_group(files=[{'path': 'c', 'metadata': {}}, {'path': 'd', 'metadata': {}},
                                 {'path': 'e', 'metadata': {}}], parser="potato")
print(f"Added group with ID: {group_id}")

group_id3 = fam2.add_group(files=[{'path': 'z', 'metadata': {}}, {'path': 'y', 'metadata': {}},
                                  {'path': 'x', 'metadata': {}}], parser="camel")
group_id4 = fam2.add_group(files=[{'path': 'x', 'metadata': {}}, {'path': 'w', 'metadata': {}},
                                  {'path': 'v', 'metadata': {}}], parser="potato")

assert type(group_id) is str, "fam.add_group is not returning an id of type str"
print(type(fam.files))

assert sorted([item["path"] for item in fam.files]) == ['a', 'b', 'c', 'd', 'e'], \
    "fam.files not properly inheriting group.files"
assert sorted([item["path"] for item in fam2.files]) == ['v', 'w', 'x', 'y', 'z'], \
    "fam.files not properly inheriting group.files"

# Here we test if going to_dict and from_dict leads us to our original family object.
dict_fam = fam.to_dict()
back_to_reg_fam = Family(download_type="gdrive")
back_to_reg_fam.from_dict(dict_fam)

assert fam.family_id == back_to_reg_fam.family_id, "to_dict -> from_dict family_ids do not match"
assert fam.download_type == back_to_reg_fam.download_type, "to_dict -> from_dict family_ids do not match"

print(fam.files)
print(back_to_reg_fam.files)
assert fam.files == back_to_reg_fam.files

for group in back_to_reg_fam.groups:
    assert group in fam.groups, "to_dict -> from_dic group_ids do not map"
    assert fam.groups[group].metadata == back_to_reg_fam.groups[group].metadata
    assert fam.groups[group].parser == back_to_reg_fam.groups[group].parser
    assert fam.groups[group].files == back_to_reg_fam.groups[group].files

print("Passed all family packaging tests!")
time.sleep(1)

family_batch = FamilyBatch()

family_batch.add_family(back_to_reg_fam)
family_batch.add_family(fam2)

print(family_batch.families)
print(family_batch.file_ls)

desc_batch_files = sorted([item["path"] for item in family_batch.file_ls])
assert desc_batch_files == ['a', 'b', 'c', 'd', 'e', 'v', 'w', 'x', 'y', 'z'], \
    "family_batch not correctly getting files from families"

dict_batch = family_batch.to_dict()

print(dict_batch)

updated_fambatch = FamilyBatch()
updated_fambatch.from_dict(dict_batch)
print(updated_fambatch)

assert family_batch.file_ls == updated_fambatch.file_ls, "to_dict -> from_dict did not preserve family batch files"

print("Passed all batching tests!")
