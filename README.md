# Xtract SDK v0.0.?

## Login: Creating an XtractClient object

First, we import the XtractClient class from the Xtract SDK

`from xtract_sdk.client import XtractClient`

Here we create an XtractClient object to request tokens from Globus Auth.

`xtr = XtractClient(auth_scopes=[scope_1, ..., scope_n], dev=False)`

When fresh tokens are needed, users will authenticate with their Globus ID by following the directions in the STDOUT. Default auth scopes are as follow:

* **openid**: provides username for identity.
* **data_mdf**: FILL IN
* **search**: interact with Globus Search
* **petrel**: read or write data on Petrel. Not needed if no data going to Petrel.
* **transfer**: needed to crawl the Globus endpoint and transfer metadata to its final location.
* **dlhub**: FILL IN
* **funcx_scope**: needed to orchestrate the metadata exraction at the given funcX endpoint.

Additional auth scopes can be added with the `auth_scopes` argument.

When true, `dev` makes you go through the full authorization flow again.

## Defining endpoints: Creating an XtractEndpoint object

First, we import the XtractEndpoint class from the Xtract SDK

`from xtract_sdk.endpoint import XtractEndpoint`

Here we create two XtractEndpoint objects to be used later in a crawl, etc.
```
xep1 = XtractEndpoint(repo_type='globus',
                      globus_ep_id='aaaa-0000-3333',
                      funcx_ep_id='aaaa-0000-3333',
                      dirs=['str1', 'str2', ..., 'strn'], 
                      grouper='file_is_group')`

xep2 = XtractEndpoint(repo_type='globus',
                      globus_ep_id='aaaa-0000-3333',
                      dirs=['str1', 'str2', ..., 'strn'], 
                      grouper='file_is_group')
```


Required arguments are as follow:
* **repo_type**: at this point, only Globus is accepted. GDrive and others to be implemented at a later date.
* **globus_ep_id**: the source endpoint ID (?), at this point assumed to be a Globus ID (see previous bullet point)
* **dirs**: directory paths for where the data resides
* **grouper**: grouping strategy we want to use for grouping.

The XtractEndpoint can also be given a `funcx_ep_id`.

## Crawling

`xtr.crawl([xep_1, ..., xep_n])`

Where `[xep_1, ..., xep_n]` is a list of XtractEndpoint objects.

The crawl ID for each endpoint will be stored in the XtractClient object as a list `xtr.crawl_ids`.

Behind the scenes, this will scan a Globus directory breadth-first (using globus_ls), first extracting physical metadata such as path, size, and extension. Next, since the *grouper* we selected is 'file_is_group', the crawler will simply create `n` single-file groups. 

The crawl is **non-blocking**, and the crawl_id here will be used to execute and monitor downstream extraction processes. 

### Getting Crawl status

`crawl_statuses = xtr.get_crawl_status(crawl_ids=None)`

We can get crawl status, seeing how many groups have been identified in the crawl. If `xtr.crawl()` has already been run, then `xtr.get_crawl_status()` will get the status of the IDs stored in `xtr.crawl_ids`. Otherwise, a list of `crawl_ids` may be given to `xtr.get_crawl_status()`.

This will return a dictionary resembling: 
```
{‘crawl_id’: String,
 ‘status’: String, 
 ‘message’: “OK” if everything is fine otherwise describes error,
 ‘data’: {'bytes_crawled': Integer, ..., 'files_crawled': Integer}}
```

Note that measuring the total files yet to crawl is impossible, as the BFS may not have discovered all files yet, and Globus does not yet have a file counting feature for all directories and subdirectories. I.e., we know when we're done, but we do not know until we get there. 

**Warning:** it currently takes up to 30 seconds for a crawl to start. *Why?* Container warming time. 

### Flushing Crawl metadata

`xtr.flush_crawl_metadata(crawl_ids=None)`

After running a crawl, we can use `xtr.flush_crawl_metadata()` to return a list of all metadata from the crawl. 

Similarly with `.get_crawl_status()`, if `xtr.crawl()` has already been run, then `xtr.flush_crawl_metadata()` will get the status of the IDs stored in `xtr.crawl_ids`. Otherwise, a list of `crawl_ids` may be given to `xtr.flush_crawl_metadata()`.

Flushing crawl metadata will return a dictionary resembling:
```
{"crawl_id": String,
 "file_ls": List,
 "num_files": Integer,
 "queue_empty": Boolean}
```

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