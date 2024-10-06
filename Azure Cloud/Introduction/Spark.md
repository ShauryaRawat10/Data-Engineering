## Data Frames

```
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

orders = [
  (1, "DP-203 Azure Data Engineer", 10),
  (1, "DP-104 Azure Administrator", 20),
  (1, "DP-900 Azure Data Fundamentals", 30)
]

schema = StructType([
  StructField("id", IntegerType(), True),
  StructField("coursename", StringType(), True),
  StructField("quantity", IntegerType(), True)
]
)

df = spark.creteDataFrame(schema, orders)

df.show()
```

## Read from CSV

```
df = spark.read.csv("D://data//orders.csv", headers = True, inferSchema=True, sep=",")

```

## Scala
- runs on Java
- To install run the following command to use it in notebook

```
built spylon-kernel spylon
python -m spylon_kernel install
```

```
val i = 2

if(i < 10)
{
  println("less tah  10")
}
else
println("Greater than 10")

```

```
import org.apache.spark.sql.types._
import org.apache.spark.sql._

val orders = Seq(
  Row(1, "DP-203 Azure Data Engineer", 10),
  Row(1, "DP-104 Azure Administrator", 20),
  Row(1, "DP-900 Azure Data Fundamentals", 30)
)

val schema = StructType(Array(
  StructField("id", IntegerType(), True),
  StructField("coursename", StringType(), True),
  StructField("quantity", IntegerType(), True)
))

val rdd = spark.sparkContext.parallelize(orders)
val df = spark.createDataFrame(rdd, schema)

df.show()
```


## Read CSV - Scala
```
val df = spark.read.option("header", "true").option("inferSchema", "true").option("delimeter", ",").csv("D://data//orders.csv")
```










