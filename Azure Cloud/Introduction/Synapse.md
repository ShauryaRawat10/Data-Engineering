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

#### External tables - multiple Parquet files

```
LOCATION = '/*.parquet'
```


#### OPENROWSET - JSON FILE

- format is csv , jsoncontent column gets all data with each row from each json row
- use json value function to retrieve columns

```
SELECT 
JSON_VALUE(jsonContent, '$.Correlationid') AS Correlationid,
JSON_VALUE(jsonContent, '$.Operationname') AS Operationname,
JSON_VALUE(jsonContent, '$.Level') AS Level,
CAST(JSON_VALUE(jsonContent, '$.Time') AS DATETIMEOFFSET) AS Correlationid,
JSON_VALUE(jsonContent, '$.Subscription') AS Subscription
FROM OPENROWSET(
    BULK 'https://mue10dadls01.blob.core.windows.net/datalake/ShauryaRawat/Azure Course/ActivityLog-01.json',
    FORMAT = 'csv',
    FIELDTERMINATOR = '0x0b',
    FIELDQUOTE = '0x0b',
    ROWTERMINATOR = '0x0a'
)
WITH 
(
  jsonContent varchar(MAX)   
)
AS ROWS
```

## Dedicated SQL Pool
- We can host SQL datawarehouse with the help of dedicated SQL pool
- With Serverless SQL pool, we can define table schema. The data itself resides in external storage
- But if we need to persist the data in actual tables and query them via SQL, we need SQL data warehouse in place
- The data warehouse gets dedicated compute and storage. The data in tables are stored in columner format which reduces data storage costs and improves query performnace.

- We also have external tables in Dedicated SQL pool as it could be maybe used as staging table to load data to final table


## Types of External Tables
- Hadoop external table:
 - Use to read and export data in various data formats such as CSV, Parquet, ORC.
 - Only available in dedicated SQL pool
- Native external tables:
 - Use to read and export data in various data formats sich as CSV and Parquet.
 - Native external table are available in Serverless SQL pool
 - They are in public preview in dedicated SQL pool
  - writing and exporting data using CTEAS and native external tables is only available in serverless sql pool


| External table type | Hadoop | Native |
| ------------- | ---------- | ---------- |
| Serverless SQL pool (https) | Not available | Available |
| Dedicated SQL Pool (abfss) | Available | Only parquet tables are available in public preview |


```
# On dedicated SQL pool connection:
# Notice that link is changed. It is abfss://containername@storageaccount.blob.core.windows.net

CREATE EXTERNAL DATA SOURCE srcActivityLogUsingDedicated
WITH 
(
    LOCATION = 'abfss://datalake@mue10dadls01.blob.core.windows.net',
    TYPE = HADOOP
)


CREATE EXTERNAL FILE FORMAT delimitedTextFileFormat 
WITH 
(
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS(
        FIELD_TERMINATOR = ',',
        FIRST_ROW = 2
    )
)


CREATE EXTERNAL TABLE stg.ActivityLog
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
    LOCATION = '/ShauryaRawat/Azure Course/ActivityLog-01.csv',
    DATA_SOURCE = srcActivityLogUsingDedicated,
    FILE_FORMAT = delimitedTextFileFormat
)


select * from stg.ActivityLog

```


## External Table - Hidden files and folders

- Use dedicated SQL pool for Hadoop processing
- The files will be read from directories automatically since we are using DFS
- The files with '_' prefix will be skipped/hidden
- Path : abfss://container@storageaccount.dfs.core.windows  (This is dfs , not blob)
```
CREATE EXTERNAL DATA SOURCE srcActivityLog
WITH
(
    LOCATION = 'abfss://datalake@mue10dadls01.dfs.core.windows.net/ShauryaRawat/Azure Course',
    TYPE = HADOOP
)


CREATE EXTERNAL TABLE stg.Readfolderdata
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
    LOCATION = '/csv/',
    DATA_SOURCE = srcActivityLog,
    FILE_FORMAT = delimitedTextFileFormat
)


SELECT * from stg.Readfolderdata
```

## Loading data to dedication SQL pool - loading from external table
- We get many additional options/functionality when we use real table
- Query performance becomes extremely fast as data resides in table itself in dedicated sql pool
- If dedicated SQL pool is deleted, tables/data is lost. If paused, it won't be able to query

```
-- Loading data into a SQL pool using polybase
CREATE TABLE STG.PoolActivityLog
WITH 
(
    DISTRIBUTION = ROUND_ROBIN
)
AS
SELECT * FROM STG.ActivityLog


SELECT * FROM STG.PoolActivityLog
```

## Copy Command
```
-- COPY Command
CREATE TABLE stg.PoolActivityLog2
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
    DISTRIBUTION = ROUND_ROBIN
)


COPY INTO STG.PoolActivityLog2
FROM 'https://mue10dadls01.blob.core.windows.net/datalake/ShauryaRawat/Azure Course/ActivityLog-01.csv'
WITH (
    FILE_TYPE = 'CSV',
    FIRSTROW = 2
    -- CREDENTIAL = ''
)


SELECT * FROM STG.PoolActivityLog
```

#### Copy command - Using Parquet
```
COPY INTO STG.PoolActivityLog2
FROM 'https://mue10dadls01.blob.core.windows.net/datalake/ShauryaRawat/Azure Course/ActivityLog-01.parquet'
WITH (
    FILE_TYPE = 'PARQUET'
    -- CREDENTIAL = ''
)
```

## Integrate Section

- Copy data tool
 - From Data Storage like Gen 2 to Dedicated SQL Pool
 - From normal SQL database to dedicated SQL warehouse
- Pipeline
- Link connection



## Synapse Architecture

![Architecture](https://github.com/ShauryaRawat10/Data-Engineering/blob/549f7dce22923b63fd55fdfec674f6a08c6654b6/Azure%20Cloud/Introduction/Storage/SynapseArchitecture.png)

![DWU](https://github.com/ShauryaRawat10/Data-Engineering/blob/549f7dce22923b63fd55fdfec674f6a08c6654b6/Azure%20Cloud/Introduction/Storage/DWU's.png)

## Types of Table - distributing data across compute and distributions

- For Fact tables - best approach is Hash Key
- For dimension tables - best approach is Replicated tables
- For Bulk load to staging - best approach is round robin (default approach)

![Table distributions](https://github.com/ShauryaRawat10/Data-Engineering/blob/664eb63c1dc2e0139452bc97cb9614c73ac0b241/Azure%20Cloud/Introduction/Storage/TanleTypes.png)

Consider using hash distribution table when:
- Table size on disk is more than 2 GB
- Table has frequent insert, update, delete operations
- Can be applied on multiple columns

Data Skew means data is not evenly distributed across the distributions. So, some distibutions take longer than others when running in parallel
- Make sure Hash key column has many unique values
- Make sure hash key column does not have NULLs/ few NULLS
- Make sure hask key column is not date column

Choose a distribution column that minimizes data movement (queries)
- It is used in JOIN, GROUP BY, DISTINCT, OVER, HAVING
- It is not used in WHERE clause

To see how data is distributed:
```
DBCC PDW_SHOWSPACEUSED('table_name')
```

#### Surrogate key for dimension tables
- To uniquely identify each row
- Identity(1,1) is used - It does not mean that it will only start from One and increment by One. It is dependent on distribution

```
CREATE TABLE stg.PoolActivityLogSurrogate
(
    Unique_key int IDENTITY(1,1) NOT NULL,
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
    DISTRIBUTION = REPLICATE
)

# This can generate Unique_Key column in any order, but each will be unique as it is based on distributions
```


#### Slowly changing dimensions
- SCD Type 1: Update the columns directly
- SCD Type 2: Create new row with changes on dates or active/inactive flag indicator
- SCD Type 3: Create new column for reflecting change. Update will modify column value. Row will be one only
















