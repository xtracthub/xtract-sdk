# Xtract SDK v0.0.5

## Packaging: Creating file group objects for downstream data packaging. 

Here we have a number of data elements that need to be defined.

*Group*: a collection of files and extractor to be executed on the collection of files. Groups need not have mutually exclusive
sets of files. A group is also intitialized with an empty metadata dictionary. 

`from xtract_sdk.packagers import Group, Family, FamilyBatch`

`group1 = Group(files=[{'path': 12388550508, 'mdata': {}}], parser='image'}`

`group2 = Group(files=[{'path': 14546663235, 'mdata': {}}], parser='image'}`

*Family*: a collection of groups such that all files in the group are mutually exclusive. This grouping is useful for 
parallelizing the download and extraction of separate groups that may contain the  same files. You can easily add Group
objects to a family as follows: 

`fam = Family()`

`fam.add_group(group1)`
`fam.add_group(group2)`


*FamilyBatch*: If the number of groups in a family is generally small, you may want to batch the families to increase application 
performance. In order to do this, you may add any number of families to a FamilyBatch that enables users to iterate 
over families. The family_batch can be directly read by the downloader that will batch-fetch all files therein. 

`fam_batch=FamilyBatch()`

`fam_batch.add_family(fam)`

## Downloaders: coming soon! 