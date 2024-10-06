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
```
