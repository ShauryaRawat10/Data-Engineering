## Microsoft Purview

- Purview brings together solutions across data governance, data security, and complaince so that you can govern and secure your data wherever it likes
- Overall picture of all data assets accross organization
- Data Map
  - You can scan your data stores, extract the metadata and understand data assets
- Data Catalog
  - Get an entire data lineage of your data assets, various stages your data can go through
 

#### Micrososft Purview - ADLS

```
Create Purview resource

Storage -> BLOB data reader -> Assign Access to (Managed Identity) -> Members (Microsft Purview Account)

Purview -> Collections -> Create (Root)
Purview -> Data Sources -> Register (ADLS)

New Scan
```

#### Micrososft Purview - Synapse

```
Create Purview resource

Synapse -> Role (Reader) -> Assign Access to (Managed Identity) -> Members (Microsft Purview Account)

In Synapse Notebook:
CREATE USER [purview3000] FROM EXTERNAL PROVIDER

EXEC sp_addrolemember 'sb_datareader', [purview3000]


Purview -> Data Sources -> Register (Synapse)

New Scan -> Select data pool
```

#### Best practices for data storage
- CSV , JSON, XML
- Avro, Parquet, ORC (Optimized Row Columnar) - These are compressed files, schema is embedded in each file
  - Avro uses row format
  - Parquet, ORC uses column format
- For more Read operations - Parquet, ORC
- Avro - For more write operation, retieval of multiple rows that need to fetch the entire row info

IOT generates lot of data, use directory structure
- {Region}/{SubjectMatter}/In/{yyyy}/{mm}/{dd}/{hh}/
- {Region}/{SubjectMatter}/Out/{yyyy}/{mm}/{dd}/{hh}/
- {Region}/{SubjectMatter}/Bad/{yyyy}/{mm}/{dd}/{hh}/


#### ADLS - Access Tiers
- Premium, Hot, Cool, Cold, Archive
- Hot - Default, frequently accessed
- Cool - infrequently accessed, object must be stored for min 30 days
- Cold - Rarely accessed, needs to be stored for minimum 90 days
- Archieve - Rarely accessed, needs to be changed to other access tier to access data, 180 days minimum storage

Cost
- Storage Cost: Hot> Cool> Cold > Archieve
- Access Cost: Hot< Cool < Cold < Archieve

Container Level : Hot, Cool
File level: Hot, Cool, Cold, Archieve

> For changing Archieve to Hot, we need to select rehydrate also

#### ADLS -> Lifecycle policies
```
Storage Account -> Lifecycle Management -> Add a Rule

Add workflow condition: If Base BLOBs were last modified - 7 days ago
                        then (Move to Cool Storage)
```

#### Monitor
- Central Service to momitor resources

```
Monitor -> Metrics -> Choose resource -> See DWH units and everything

Monitor -> Activity Log -> To see resource sand who created resources

Monitor -> Alert -> Create (Alert Rule)
                 -> Resource - Azure data factory
                 -> Signal name - Failed pipeline run
                 -> Action Group - Send email
                 -> Actions -> Run Notebook or something
                 -> Alert Rule Name
```

#### Azure Data Factory 
- We can view the pipeline runs and see the activities - It is retained for 45 days
- To retain longer - Log Analytics workspaces
  - Direct all the logs here
 
```
Create Log analytics service

Data factory resource -> Diagnostic Settings -> Send (Pipeline runs log, Pipeline activity run log, Trigger runs log)
                                             -> Log Analytics Workspace - Resource specific 
```

#### ADF - Annotations
```
ADF -> Pipeline -> Properties -> Add Annotations (logistics)

Filter using annotation in Monitor ADF
```


#### ADF - Scaling up the runtime
- IR runtime (compute infrastructure)- fully managed, capacity scaling automatic
- Copy activity - we can set integration units
- Self hosted IR runtime - We manage scaling ourself

#### ADF - Troubleshooting
- delimitedTextMoreColumnsThandefined - Source/Target schema has more no of columns,
  - set Binary Copy option

#### Synapse - Monitoring
```
Synapse -> Monitor -> SQL requests 

```



#### Synapse - System Views (Synamic Management Views)
- Manage all logins

```
SELECT * FROM sys.dm_pdw_exec_sessions where status <> 'Closed'
```

- Monitor query execution
```
SELECT * FROM sys.dm_pdw_exec_requests
WHERE status not in ('Compelted', 'Failed', 'cancelled')
 and session_id <> session_id()
```

- Query Plan ( select * from sys.dm_pdw_requests_steps where request_id = 'QID####' )
- data movement on distributed nodes ( Select * FROM sys.dm_pdw_dms_workers where request_id = 'QID####' AND step_index = 2 )
- Monitor query blocking ( Select * from sys.dm_pdw_waits )


#### Workload Management
- Users are loading data in Synapse/ Users are reading data , we can manage workloads by classfying them accrodingly
  - Priority resources, Allocate high resources to some users
 
```
Syanpse resource -> Workload Management -> New Workload group  -> choose (high resources, low resources, medium resources, custom )
```


#### Synapse - Result Set Caching
- When query is executed multiple times, we can enable cache to save compute
- following query don't get cached
  - Built in functions - DateTime.Now(), GetDate()
  - User defined functions
  - Row level security
  - row size larger than 64 KB
  - large dataset - greater than 10 GB

```
In Master:
ALTER TABLE [datapool]
SET QUERY_STORE = ON;

ALTER TABLE [datapool]
SET RESULT_SET_CACHING = ON;

In Datapool (dedicated):
DBSS SHOWRESULTCACHESPACEUSED
```


#### Synapse - Data Skew
- distributions (Hash) may not be distributed equally accross distributions

```
DBCC PDW_SHOWSPACEUSED('dbo.PoolActivityLog')  -- Because this was created using Round robin distribution, data is evenly distributed
```


#### Syanpse - Log Analytics
```
Go to Synapse -> Diagnostic Setting

Select (SQL Request log, Request Steps Log, Waits Log)
Destination -> Log Analytics Workspace 
```

#### Stream Analytics - Optimization
- Streaming Units (SU) represent compute resources allocated to the job
- SU % Utilization if goes beyond 80%, then increase SU
- Backlogged Metrics - If this is non zero value for regular intervals, job can not keep up with streaming events. Scale up Stream Analytics job
- Partition events to achieve multiple throughput (Input partitions = Output partitions)












