## Spark Pool - Synapse
- Craete Spark Pool
  - It will have minimum 3 nodes
  - Works in distributed computing engine
  - Only charged when Spark pool is running jobs (Spark pool is linked to Synapse)
  - It will install all Java, Spark, etc setup
 
## Develop Notebook in Synapse
- Choose Spark pool
- Write scripts in Spark (Scala), Pyspark(Python), Spark SQL, SparkR (R)

# Spark Pool Notebook

```
Read CSV data

df = spark.read.option('header', True).csv('abfss://data@datalake400700.dfs.core.windows.net/csv/ActivityLog01.csv')
display(df)

```

```
Read Parquet data

df = spark.read.load('abfss://data@datalake400700.dfs.core.windows.net/parquet/ActivityLog01.parquet', format = "parquet")
display(df.select('OperationName', "Status"))

display(df.count())

filteredDF = display(df.where(df['ResourceGroup']=='app-grp'))
filteredDF.count()

from pyspark.sql.function import col
filteredDF = df.filter(col('ResourceGroup').isNotNull())
display(filteredDF)

summarydf = filtereddf.groupBy('ResourceGroup').count()
display(summarydf)
```

## Saving to Delta table

```
tablepath = "/delta/ActivityLog"

df.write.format("delta").save(tablepath)


ActivityLogDF = spark.read.format("delta").load(tablepath)
display(ActivityLogDF)
```


## Writing data using Spark pool to Synapse Dedicated SQL Pool

#### Readingdata in dataframe
```
import org.apache.spark.sql.types

val dataschema = StructType(Array(
  StructField("CorrelationId", StringType, true),
  StructField("Operationname", StringType, true),
  StructField("Status", StringType, true),
  StructField("EventCategory", StringType, true),
  StructField("Time", TimestampType, true),
  StructField("CorrelationId", StringType, true)))

val df = spark.read.format("csv").option("header", "true").schema(dataschema).load('abfss://data@datalake400700.dfs.core.windows.net/csv/ActivityLog01.parquet')

df.printSchema()
```

#### Authenticating 
```
val writeOptionsWithBasicAuth:Map[String, String] = Map(Constants.SERVER -> "synapseworkspace.sql.azuresynapse.net",
                                                        Constants.USER -> "sqladminuser"
                                                        Constants.PASSWORD -> "Microsoft@123"
                                                        Constants.DATA_SOURCE -> "dedicatedpool"
                                                        Constants.TEMP_FOLDER -> "abfss://staging@datalake400700.dfs.core.windows.net",
                                                        Constants.STAGING_STORAGE_ACCOUNT_KEY -> "hcgdskcgjkadwaygskna=12s/"
)

```

#### Writing data
```
import org.apache.spark.sql.SaveMode
import com.microsoft.spark.sqlanalytics.utils.Constants
import org.apache.spark.sql.SqlAnalyticsConnector._

df.write.
    options(writeOptionWithBasicAuth).
    mode(SaveMode.Overwrite).
    synapsesql(tableName = "dedicatedpool.dbo.PoolActivityLog",
               tableType = Constants.INTERNAL,
               location = None
              )
```

## Shared Metadata Tables
- Share database and table btween different spark pools and Serverless SQL pool
  - Serverless SQL pool need file in Parquet/CSV


```
Craete database sparkdb

USE sparkdb

Create table SparkPoolActivityLog
(
  CorrelationId string,
  Operationname string,
  Time string,
  ResourceType string
) USING PARQUET

df = spark.read.load('abfss://data@datalake400700.dfs.core.windows.net/parquet/ActivityLog01.parquet', format = "parquet")
df.write.mode("append").saveAsTable(sparkdb.SparkPoolActivityLog)

The table will be in Lake database in Synapse (Shared db/table)
```

## Synapse database Templates
- Business and technical data definitions pre-designed to meet particular industry needs
- ER diagram

```
Synapse -> Data -> Browse Gallery -> Airlines (ER diagram and schema )
```



## Databricks
- Transactional Support
  - ACID - Atomicity, Consistency, Isolation, Durability
- Open data - Data can be ingested in different formats
- Schema management - able to have structure for your data
- Data Governance - Keep track of your data assets

## Clusters
- General purpose Cluster
  - (Single node, multi-node)
  - Single User (one user), multi-user
  - Permanently deleted after 30 days of termination
- Job based cluster

## Enable DBFS files in catalog
Setting -> Advanced -> DBFS File browser (Enable)

```
# Read data with account key

filepath = "abfss://data@datalake400700.dfs.core.windows.net/parquet/ActivityLog01.parquet"

spark.conf.set(
  "fs.azure.account.key.datalake400700.dfs.core.windows.net"  -- account name
   "bxkjsabchfagdjnakjhgcfahgdhlkasm.dm bsakhgdiakljd+w=="    -- access key
)

df = spark.read.load(filepath, format = "parquet")
display(df)
```

## Which tool
- databricks - spark, ML, AI, DE, lakehouse
- synapse - lakehouse, external tables



```
from pyspark.sql.functions import year, month, dayofyear

display(
  df.select(
    year(col("Time")).alias("Year"),
    month(col("Month")).alias("Month"),
    dayofyear(col("Time")).alias("Day of Year")
  )
)

```


## Reading JSON data - explode
```
{"customerid":1,"customername":"UserA","registered":true,"courses":["AZ-900","AZ-500","AZ-303"],"details" : {"mobile":"111-1112","city":"CityA"}}

filepath = "abfss://data@datalake400700.dfs.core.windows.net/json/CustomerARR.json"

spark.conf.set(
  "fs.azure.account.key.datalake400700.dfs.core.windows.net"  -- account name
   "bxkjsabchfagdjnakjhgcfahgdhlkasm.dm bsakhgdiakljd+w=="    -- access key
)

df = spark.read.format("json").load(filepath)


explodedf = df.select(
                       col("CustomerId"),
                       col("customername"),
                       explode(col("Courses")),
                       col("details.city"),
                       col("details.mobile")
                      )

```

## Saving to a table
```
df.write.mode("overwrite").saveAsTable("ActivityLogData")
```

## Copy Command
```
copy into logdata
from 'abfss://data@datalake400700.dfs.core.windows.net/parquet/ActivityLog01.parquet'
fileformat = PARQUET,
COPY_OPTIONS ('mergeSchema' = 'true');
```


## Streaming data using databricks
- Notebook cell will stream data until its stopped

```
filepath = 'abfss://data@datalake400700.dfs.core.windows.net/parquet/ActivityLog01.parquet'

spark.conf.set(
  "fs.azure.account.key.datalake400700.dfs.core.windows.net"  -- account name
  "bxkjsabchfagdjnakjhgcfahgdhlkasm.dm bsakhgdiakljd+w=="    -- access key
)

checkpoint = "tmp/checkpoint"          -- streaming data needs checkpoint to see the how many events it processed and where it is now
schemalocation = "tmp/schema"          -- streaming data needs schema location while processing

rawdf = spark.readStream.format("cloudFiles")
                         .option("cloudFiles.format", "csv")
                         .option("cloudFiles.schemalocation", schemalocation)
                         .load(filepath)

```

```
display(rawdf)        -- stream data
```

## Performing transformations over streaming data
```
filtereddf = rawdf.filter( col('ResourceGroup').isNotNull() )

filtereddf.writeStream.format("delta").outputMode("append")
                       .option("checkpointLocation", checkpoint)
                       .option("mergeSchema", "true")
                       .table("default.activitylogdata")
```


## Add Schema to readstream

```
rawdf = spark.readStream.format("cloudFiles")
                         .schema(dataSchema)
                         .option("cloudFiles.format", "csv")
                         .option("cloudFiles.schemalocation", schemalocation)
                         .load(filepath)
```


## Write data from databricks to Dedicated Synapse SQL pool

```
# Using JDBC driver connection
filtereddf.writeStream.format("com.databricks.spark.sqldw")
                       .opton("url", "jdbc:sqlserver://dataworkspace40040.sql.azuresynapse.net:1433;database=datapool")
                       .option("user", "sqladminuser")
                       .option("password", "Microsoft@123")
                       .option("tempDir", "abfss://staging@datalake400700.dfs.core.windows.net/databricks")
                       .options("forwardSparkAzureStorageCredentials", "true")
                       .option("dbTable", "dbo.PoolActivityLog")
                       .option("checkpointLocation", checkpoint)
                       .start()
```


## Reading data in Databricks from Synapse
```
logdf = spark.read.format("com.databricks.spark.sqldw")
                       .opton("url", "jdbc:sqlserver://dataworkspace40040.sql.azuresynapse.net:1433;database=datapool")
                       .option("user", "sqladminuser")
                       .option("password", "Microsoft@123")
                       .option("tempDir", "abfss://staging@datalake400700.dfs.core.windows.net/databricks")
                       .options("forwardSparkAzureStorageCredentials", "true")
                       .option("query", "SELECT * FROM dbo.VehicleTollBooth")
                       .load()

display(logdf)
```


## Versioning in Databricks
```
SELECT * FROM appdb.activitylogdata

-- This will give all data and one more column - _rescued_data (atleast in managed table i saw)
```

- Auto-loder - auto detect schema of loaded data, inferschema from source and load into table
- rescue data - Autoloader can rescue data that was unexpected, (for eg: differing data types)


```
DESCRIBE HISTORY appdb.activitylogdata

SELECT * FROM appdb.activitylogdata VERSION AS OF 2
```






