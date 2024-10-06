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















