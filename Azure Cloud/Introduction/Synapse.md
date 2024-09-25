## Data Warehouse:
- A data warehouse is a repository that can be used to store structured data
- Use SQL query language to work with data in warehouse

## SQL Database vs SQL Datawarehouse
- Database: OLTP, current transactional data
- Datawarehouse: OLAP, historical

## Azure Synapse Analytics
- Enterprise Analytical Service
 - Synapse SQL: host your sql datawarehouse
 - Apache Spark: Get access to spark for data processing
 - Data Integration: ADF like features to ingest data

![Synapse-workspace](https://github.com/ShauryaRawat10/Data-Engineering/blob/d49db852fa5bc61c6ce536a47dc7e8ab26c56e69/Azure%20Cloud/Introduction/Storage/Synapse-workspace-setup.png)

#### Serverless SQL pool
- compute available to analyse data 
- Built-in Serverless sql pool to query data in Azure-Data-Lake-Storage account
- Underlying compute is managed for us
- we are charged for data that is processed by the queries, not for compute

#### Synapse Linked Service - External ADLS Gen 2 storage connectivity
![Synapse-external-linked-service](https://github.com/ShauryaRawat10/Data-Engineering/blob/c166465f9ed8b1ad1d387624893ca884b1865e9b/Azure%20Cloud/Introduction/Storage/external-storage-connect-synapse.png)

