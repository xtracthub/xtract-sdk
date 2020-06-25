
from xtract_sdk.packagers.family import Family

fam = Family(str(0), headers={'potato': 'tomato'}, metadata=None)

group_id = fam.add_group(files=['a', 'b', 'c'], parser="camel")
group_id2 = fam.add_group(files=['b', 'c', 'd', 'e'], parser="potato")
print(f"Added group with ID: {group_id}")

assert type(group_id) is str, "fam.add_group is not returning an id of type str"
assert fam.files == {'a', 'b', 'c', 'd', 'e'}, "fam.files not properly inheriting group.files"


print(fam.to_dict())