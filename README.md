# ModelarDB DMMAI Proof of Concept
Proof of Concept for how to use ModelarDB for data storage in the DMMAI framework.

**Insert figure of the DMMAI framework architecture here**

ModelarDB provides the functionality for ingesting data in multiple AI controllers (AIC), storing it on the AIC, 
transferring the stored data to a cloud object store that represents the Data Management Block (DMB), and making the 
data available for data curation.

The data can be curated using the ModelarDB Python library and saved in a single file, for example using CSV. The 
SLICES Metadata Registry System (MRS) can then be used to store metadata about the curated data and make the datasets 
searchable and accessible.

This repository contains a Docker Compose setup that includes ModelarDB, MinIO (as a cloud object store), and the 
SLICES MRS. It also includes a Python script to demonstrate data ingestion, storage, and transfer. Finally, a Python
script is provided to demonstrate curating data, saving it, and registering it in the SLICES MRS.
