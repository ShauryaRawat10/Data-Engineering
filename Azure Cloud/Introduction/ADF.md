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
- Conditional Split : (2 stream , split1 when RG == 'app-grp', split2 - all others are by default here). It is just like router
- Aggregate (Group by: column1 , Aggregates : column2 = count(*))
- Source/Sink

## Schema Drift
- When schema is changing in source, sink auto-map the column to destination

```
Options in Source
- Allow Schema Drift
- Infer drifted column types
- Validate schema
```

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
3. Install Self-Hosted runtime and register it in ADF (It will give 2 key, copy one and paste in web-server )
4. Copy Activity - transfer the log data file from web server onto Azure Data Lake
```

```
Derived Column 1
logdata = split(logdata, " ")  -> Single row will have logdata[1]: value, logdata[2]:value, logdata[3]:value, and so on

Derived Column 2
Logdate = Logdata[1]
IPAddress = Logdata[9]
RequestMethod = Logdata[2]
```



## Activity in ADF

- Get Metadata Activity: Get all BLOBS in comtainer (Field List: Child items)
- For Each Activity: Iterate over the metadata

```
1. Create metadata Activity to get all files on ADLS
2. For-each activity to iterate over the files
   Settings: Items = @activity('GetMetadataBlobs').output.childItems
3. Add Copy data activity to move data from ADLS to Synapse for each file
   Source:
   1. Create Dataset
      Point to the location of files
      Add Parameter: Filename
   2. Source: Filename = @item().Name
      
```

- Stored Procedure Activity
 - It Don't have ability to display output

```
create procedure stg.GetEmployeeName
    @p_employeeDimId int 
AS
    SELECT distinct(Concat(Emp_First_Name, Emp_Last_Name)) AS Emp_Name
    FROM stg.employee_dim
    WHERE Employee_Dim_Id = @p_employeeDimId


EXEC stg.GetEmployeeName @p_employeeDimId = 354402
```

- Lookup Activity
 - Has Stored Procedure which returns value

```
1. Create Lookup Activity
    Stored procedure name : stg.GetEmployeeName
    Parameter: p_employeeDimId = 354402
2. Create Set Variable Activity
   Pipeline Variable
   Variable Name: P_Name
   Variable VAlue: @activity('Lookup1').output.firstRow.Emp_Name
3. Create Set Variable Activity for failure
   Pipeline Return Variable
   Variable Name: P_Name String Default

```

## Triggers
- Storage Event Trigger
 - When BLOB data is uploaded/delete we can enable this trigger to execute the pipeline (Provide Subscription/Storageaccountname/Container)
- Schedule
- Tumbling Window
 - Same as Schedule but it only considers events from last window run to next window run (interval based)
 - We can choose startdate and enddate also

## Git
- Popular version control to manage different versions of code.
- Git based repositories for ADF pipelines

```
Manage -> Git Repository -> Repository Name (Azure DevOps, Github)
```




















