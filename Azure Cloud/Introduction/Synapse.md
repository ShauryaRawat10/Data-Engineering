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

#### Azure Entra Id
Azure BLOB Data Reader role: To access blob data

```
 SELECT
    TOP 100 *
 FROM
     OPENROWSET(
         BULK 'https://mue10pocadls01.dfs.core.windows.net/datalake/EDW/Asset_Book_Dim/Internal/part-00000-74066bde-5425-4ca0-a187-1db9a37ddc7d.c000.snappy.parquet',
         FORMAT = 'PARQUET'
     ) AS [result]
```

> Error: File 'https://mue10pocadls01.dfs.core.windows.net/shauryarawat/Input/EmployeeADF.csv' cannot be opened because it does not exist or it is used by another > process.

This happens due to permission issue. Give Azure BLOB reader role in Storage account

```
CREATE DATABASE [DEV-Shaurya];


CREATE EXTERNAL DATA SOURCE srcActivityLog
WITH (
    LOCATION = 'https://mue10dadls01.blob.core.windows.net/datalake/ShauryaRawat/Azure Course'
)



CREATE EXTERNAL FILE FORMAT delimitedTextFileFormat
WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS(
        FIELD_TERMINATOR = ',',
        FIRST_ROW = 2
    )
)


CREATE EXTERNAL TABLE ActivityLog
(
    [Correlationid] varchar(255),
    [Operationname] varchar(255),
    [Status] varchar(255),
    [Eventcategory] varchar(255),
    [Level] varchar(255),
    [Time] varchar(255),
    [Subscription] varchar(255),
    [Eventinitiatedby] varchar(255),
    [Resourcetype] varchar(255),
    [Resourcegroup] varchar(255),
    [Resource] varchar(255)
)
WITH (
    LOCATION = '/ActivityLog-01.csv',
    DATA_SOURCE = srcActivityLog,
    FILE_FORMAT = delimitedTextFileFormat
)


Select * from ActivityLog
```

## External tables Parquet

```
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Password@123'


CREATE DATABASE SCOPED CREDENTIAL SasToken
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = 'sp=rawlmep&st=2024-09-28T14:02:02Z&se=2024-09-28T22:02:02Z&spr=https&sv=2022-11-02&sr=c&sig=U3l9lYDypTZ1C%2FhuTuJMWFpwqlfjWdgFGAeegEC964g%3D'


CREATE EXTERNAL DATA SOURCE srcActivityLogParquet
WITH (
    LOCATION = 'https://mue10pocadls01.blob.core.windows.net/shauryarawat/SynapseFiles',
    CREDENTIAL = SasToken
)


CREATE EXTERNAL FILE FORMAT parquetfileformat
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)


CREATE EXTERNAL TABLE ActivityLogParquet
(
    [Correlationid] varchar(255),
    [Operationname] varchar(255),
    [Status] varchar(255),
    [Eventcategory] varchar(255),
    [Level] varchar(255),
    [Time] varchar(255),
    [Subscription] varchar(255),
    [Eventinitiatedby] varchar(255),
    [Resourcetype] varchar(255),
    [Resourcegroup] varchar(255),
    [Resource] varchar(255)
)
WITH (
    LOCATION = '/ActivityLog01.parquet',
    DATA_SOURCE = srcActivityLogParquet,
    FILE_FORMAT = parquetfileformat
)


SELECT * FROM ActivityLogParquet

```

#### Shared Access Signature (SAS) Token
This can be used without need for BLOB data reader role

![SAS Token](https://github.com/ShauryaRawat10/Data-Engineering/blob/aeb70835fbaaa4fa93b1991ea141fe7b001cdbf3/Azure%20Cloud/Introduction/Storage/SAS_TOKEN.png)










