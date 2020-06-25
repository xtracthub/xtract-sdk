
from xtract_sdk.packagers.family import Family

fam = Family(str(0), headers={'potato': 'tomato'}, metadata=None)

group_id = fam.add_group(files=[{'path': 'a', 'metadata': {}}, {'path': 'b', 'metadata': {}},
                                {'path': 'c', 'metadata': {}}], parser="camel")
group_id2 = fam.add_group(files=[{'path': 'c', 'metadata': {}}, {'path': 'd', 'metadata': {}},
                                 {'path': 'e', 'metadata': {}}], parser="potato")
print(f"Added group with ID: {group_id}")

assert type(group_id) is str, "fam.add_group is not returning an id of type str"
print(type(fam.files))

assert sorted([item["path"] for item in fam.files]) == ['a', 'b', 'c', 'd', 'e'], \
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
