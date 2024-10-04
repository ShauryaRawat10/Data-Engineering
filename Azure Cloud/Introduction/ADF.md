## ADF
- Cloud based ETL Tool / Data Integration Service
- Data driven workflows that you can orchestrate for data movement
- Linked Service connection to source and destination (Data Lake, Synapse, SQL Server) and pipeline having transformation logic
  - Linkedin service need 'Integration Runtime'

## Integration Service
- Compute infrastructure will spin up when pipeline starts and after completion destroys the infrastructure

## Costing Aspect
- Data Flow takes most amount of cost


## Data Flows
- Advanced transformation
- Visual designer to allow your transformations and going to be part of pipeline
- Run on Apache Spark Cluster
- Once transformation is done, it destroys the apache spark cluster

## Data Flow transformations
- Data Sink Cache: When you want to store immediate value from target table for processing like get(max(customerId)) before incrementing it
- Join
- Select
- Filter
- Derived Column
- Surrogate
- Flatten : expand array elements in JSOn documents (Unroll by, unroll root, )
- Source/Sink

## ADF Pipeline Task
- Delete Task: When you want to get rid of Directories/folder/files after processing

## Integration Runtime
- Files are on azure cloud

## Self Hosted Integration Runtime
- If the files are on your on-premises network or on a virtual machine

```
Handson
1. Create Azure Virtual machine
2. Install web-server - Internet Information Services
3. Install Self-Hosted runtime and register it in ADF
4. Copy Activity - transfer the log data file from web server onto Azure Data Lake
```




