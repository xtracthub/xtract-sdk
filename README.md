# Xtract SDK v0.0.7a6

## Login: Creating an XtractClient object

`from xtract_sdk.client import XtractClient`

First, we import the XtractClient class from the Xtract SDK

`xtr = XtractClient(auth_scopes=None, force_login=False)`

Here we create an XtractClient object to request tokens from Globus Auth.

The **auth_scopes** argument accepts an optional list of strings which correspond to authorization scopes. While additional auth scopes may be added with the **auth_scopes** argument, there are a number of 
default scopes automatically requested within the system. These are: 

* **openid**: provides username for identity.
* **search**: interact with Globus Search
* **petrel**: read or write data on Petrel. Not needed if no data going to Petrel.
* **transfer**: needed to crawl the Globus endpoint and transfer metadata to its final location.
* **funcx_scope**: needed to orchestrate the metadata exraction at the given funcX endpoint.

When true, **force_login** makes you go through the full authorization flow again.

## Defining endpoints: Creating an XtractEndpoint object
Endpoints in Xtract are the computing fabric that enable us to move files and apply extractors to files. To this end, 
an Xtract endpoint is the combination of the following two software endpoints: 
* **Globus endpoints** [required] enable us to access all file system metadata about files stored on an endpoint, and enables us to transfer files between machines for more-efficient processing.
* **FuncX endpoints** [optional] are capable of remotely receiving extraction functions that can be applied to files on the Globus endpoint. Note that the absence of a funcX endpoint on an Xtract endpoint means that a file must be transferred to an endpoint *with* a valid funcX endpoint in able to have its metadata extracted.

`from xtract_sdk.endpoint import XtractEndpoint`

In order to create an Xtract endpoint, we first import the XtractEndpoint class from the Xtract SDK.

```
endpoint_1 = XtractEndpoint(repo_type,
                            globus_ep_id,                            
                            dirs, 
                            grouper,
                            funcx_ep_id=None,
                            metadata_directory=None)
```

Then we can actually create an endpoint object to be used later in a crawl, xtraction, etc. The arguments are as follow:
* **repo_type**: (str) at this point, only Globus is accepted. Google Drive and others will be made available at a later date. 
* **globus_ep_id**: (uuid str) the Globus endpoint ID.
* **dirs**: (list of str) directory paths on Globus endpoint for where the data reside.
* **grouper**: (str) grouping strategy for files.
* **funcx_ep_id**: (optional uuid str) funcX endpoint ID.
* **metadata_directory** (optional str) directory path on Globus endpoint for where xtraction metadata should go.

## Crawling

`xtr.crawl(endpoints)`

Where **endpoints** is a list of XtractEndpoint objects.

The crawl ID for each endpoint will be stored in the XtractClient object as a list `xtr.crawl_ids`. Furthermore, each endpoint will be stored in the XtractClient object in a dictionary `cid_to_endpoint_map`, where each crawl id key maps to the corresponding endpoint as a value.

Behind the scenes, this will scan a Globus directory breadth-first (using globus_ls), first extracting physical metadata such as path, size, and extension. Next, since the *grouper* we selected is 'file_is_group', the crawler will create a single-file group for every endpoint given. 

The crawl is **non-blocking**, and the crawl_id here will be used to execute and monitor downstream extraction processes. 

### Getting Crawl status

`xtr.get_crawl_status(crawl_ids=None)`

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

### Crawl and wait

`xtr.crawl_and_wait(endpoints)`

Where **endpoints** is a list of XtractEndpoint objects.

For ease of testing, we've implemented a **crawl_and_wait** functionality, which will crawl the given endpoints and then print the crawl status of all given endpoints every two seconds until all have completed crawling.

### Flushing Crawl metadata

`xtr.flush_crawl_metadata(crawl_ids=None, next_n_files=100)`

After running a crawl, we can use `xtr.flush_crawl_metadata()` to return a list of all metadata from the crawl. 

Similarly with `.get_crawl_status()`, if `xtr.crawl()` has already been run, then `xtr.flush_crawl_metadata()` will get the status of the IDs stored in `xtr.crawl_ids`. Otherwise, a list of **crawl_ids** may be given to `xtr.flush_crawl_metadata()`.

Each time metadata is flushed, the number of files for which metadata is returned will be equal to **next_n_files**, and then that metadata will not be able to be flushed again.  

Flushing crawl metadata will return a dictionary resembling:
```
{"crawl_id": String,
 "file_ls": List,
 "num_files": Integer,
 "queue_empty": Boolean}
```

## Xtract-ing

### Registering containers for Xtraction

`xtr.register_containers(endpoint, container_path)`

Where **endpoint** argument should be an XtractEndpoint object, and **container_path** (str) argument should be the path to the xtraction containers on the Globus endpoint.


In order to perform an xtraction, we must have the requisite containers for each extractor that is to be used. After creating client and endpoint instances, containers must be registered for each endpoint, using `.register_containers()`. 

This can be executed regardless of **crawl** completion status.

### Xtract

`xtr.xtract()`

The **crawl** method must have already been run, and an **xtract**ion will be run for each endpoint that was given to **crawl**. **xtract** will return the HTTP status response code, which should be 200.

### Getting Xtract status

`xtr.get_xtract_status()`

The **xtract** method must have already been run, and this call will return a list of **xtract statuses**, one for each endpoint given to **crawl**.

This will return a dictionary resembling:

```
{'xtract_status': String,
 'xtract_counters': {'cumu_orch_enter': Integer, 
                     'cumu_pulled': Integer, 
                     'cumu_scheduled': Integer, 
                     'cumu_to_schedule': Integer, 
                     'flagged_unknown': Integer, 
                     'fx': {'failed': Integer, 
                            'pending': Integer, 
                            'success': Integer}}
```

## Offload metadata

`xtr.offload_metadata(dest_ep_id, dest_path="", timeout=600, delete_source=False)`

The **offload_metadata** method can be used to transfer files between two endpoints, and is included in this SDK for the purpose of transferring metadata from **xtract**ion. It takes the following arguments:
* **dest_ep_id**: (str) the ID of the endpoint to which the files are being transferred.
* **dest_path**: (optional str) the path on the destination endpoint where the files should go
* **timeout**: (optional int, default 600) how long the transfer should wait until giving up if unsuccessful
* **delete_source**: (optional boolean, default False) set to True if the source files should be deleted after metadata completion

This method will transfer the metadata to a new folder (in the destination path, if supplied) which is named in the convention **YYYY-MM-DD-hh:mm:ss**. Calling the function will return the path to this folder on the destination endpoint.

## Search: coming soon! 

## Downloaders: coming soon! 
