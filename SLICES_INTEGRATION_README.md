# About
This file provides guide to deploy ModelarDB and SLICES MRS (MRS) in a docker cluster mode. They comprise the core part of the Data Management Block.
One way is to do by using provided archive files for both components.
1. Extract each archive files.
2. Change to `./mrs_dmi_upload` directory
3. Follow `./mrs_dmi_upload/README.md` to start Traefik and MRS.   
4. After you start Traefik and MRS, change to `../ModelarDB-DMMAI` directory.
5. Follow `ModelarDB-DMMAI/README.md` to start ModelarDB edge (should be replaced by AICs in the future) and cloud node.

At this point you have the following services running: __Traefik__ (used for networking), __MRS__ (used as Abstract Data Repository), __ModelarDB Cloud node__ (used as Network Data Repository) and __2 ModelarDB edge nodes__ (used for replacing AICs in this current setup). Now we start ingesting data to ModelarDB and then to MRS.

1. Change to `./ModelarDB-DMMAI/` directory.
2. Follow `./ModelarDB-DMMAI/README.md` 's __Data Curation__ section to ingest data into ModelarDB Cloud node using ModelarDB edge nodes.
3. To upload data to MRS change to `./ModelarDB-DMMAI/curate_data/`
4. Run `docker compose -f docker-compose.yml up`

## Source code
1. SLICES repository is available under this [link]([link](https://gitlab.noc.onelab.eu/TMZaidi/mrs_dmi_upload/-/tree/upload_local)). Note that you should use `upload_local` branch.
2. ModelarDB-DMMAI repository is available under this [link](https://github.com/CGodiksen/ModelarDB-DMMAI).

## Limitations
At the moment the data itself, its metadata for MRS and the subsequent table/MinIO bucket names are hard coded in the scripts. We will make this customizable once we have more clear idea about the data being used for the demo. Once you have all containers running, you can check them through MinIO UI: `127.0.0.1:9000`