## Stream Processing and Real time
- Latency is less to get results faster
- Processing system needs to be capable of ingesting data at high speed

## Azure Events HUB
- This is a data streaming service that can stream millions of events per second
- This can be from any Source/Destination

#### Event Hub Architecture

![Event Hub Architecture](https://github.com/ShauryaRawat10/Data-Engineering/blob/92af1d993e92899e1af7eacb8d77505459b2e1cd/Azure%20Cloud/Introduction/Storage/EventHubsArchitecture.png)

- Event hub namespace: This is container for multiple event hub
- Event data is stored accross multiple partitions. It helps increase througput of system
  - Each partition generally sustain 1 MB/s throughput
  - We can use partition key to map events to specific partition
- Each event can have body of events (Metadata - Offset of partitions, stream sequence number)
- Event Publisher sends data to Events hub
- Event Consumer consumes events from Event Hub
- Event Consumer is logical grouping of consumers that read data from events hub

Properties
- Event Retention - Standard (7 days), Premium/Dedicated - 90 days max
- Ingress - 1 MB per second or 1000 events per second
- Egress - 2 MB per second or 4096 events per second
- Charging based on throughput unit

## Azure Stream Analytics
- This is fully managed stream processing engine
- Use this to process large amounts of data in real time

Service
- Stream Analytics Job
- Charge based on streaming Units

## Streaming data 
![Stream job](https://github.com/ShauryaRawat10/Data-Engineering/blob/75879668588fa28f09a9e42199f1ad8af2ce5b19/Azure%20Cloud/Introduction/Storage/StreamAnalyticsJob.png)

```
1. Craete Events HUB in EventHubNamespace
2. Generate Stream data using Data Explorer
3. Create Stream Analytics Job
   Input : Events HUB
   Output: Datalake/Synapse
   Query: Make query from JSON data

SELECT
    entrytime,
    carmodel.make,
    carmodel.model,
    state,
    tollamount,
    tag,
    licensePlate
INTO
    [datalake]
FROM
    [eventhub1]
```

## Diagnostic Log data
- Diagonistic logs which tells about Resource interaction data
- It is available on all service

![Dioagnostic Settings](https://github.com/ShauryaRawat10/Data-Engineering/blob/d783f09942a23eca988afb738fb4d0f209caef3d/Azure%20Cloud/Introduction/Storage/DiagonisticSettings_BLOB_1.png)

```
Input data from Diagonistic log (In Storage Account , we enabled all read/write/delete operation logs)
[
  {
    "records": [
      {
        "time": "2024-10-05T14:17:17.7739052Z",
        "resourceId": "/subscriptions/1a041723-c6f3-4a66-9e1e-051ad7056024/resourceGroups/Shaurya-resource-group/providers/Microsoft.Storage/storageAccounts/storageaccount6750/blobServices/default",
        "category": "StorageRead",
        "operationName": "GetBlobServiceProperties",
        "operationVersion": "2020-02-10",
        "schemaVersion": "1.0",
        "statusCode": 200,
        "statusText": "Success",
        "identity": {
          "type": "TrustedAccess",
          "tokenHash": "system-1(F2748467C3C10FA8FEEA1D9168283271C293BD7188AEC3702B826A6D224C43D7)"
        },
        "location": "eastus",
        "properties": {
          "accountName": "storageaccount6750",
          "userAgentHeader": "SRP/1.0",
          "serviceType": "blob",
          "objectKey": "/storageaccount6750",
          "metricResponseType": "Success",
          "tlsVersion": "TLS 1.2",
          "sourceAccessTier": "None"
        },
        "uri": "https://storageaccount6750.blob.core.windows.net:443/?restype=service&comp=properties&sk=system-1",
        "protocol": "HTTPS",
        "resourceType": "Microsoft.Storage/storageAccounts/blobServices"
      }
    ],
    "EventProcessedUtcTime": "2024-10-05T14:24:11.1545107Z",
    "PartitionId": 0,
    "EventEnqueuedUtcTime": "2024-10-05T14:19:10.2850000Z"
  }
]

```
![Read data](https://github.com/ShauryaRawat10/Data-Engineering/blob/d783f09942a23eca988afb738fb4d0f209caef3d/Azure%20Cloud/Introduction/Storage/ReadDiagonisticStreamingdata.png)

```
Select 
    records.ArrayValue.category AS Category,
    records.ArrayValue.operationName AS OperationName,
    records.ArrayValue.properties.objectkey AS ObjectKey,
    records.ArrayValue.[Identity].type AS IdentityType,
    records.ArrayValue.[Identity].tokenHash AS TokenHash
INTO
    [dataLake]
FROM
    [blobhublogs] bh
CROSS APPLY GetArrayElements(bh.records) AS records
```


## Event Hub Capture
- Available on Standard Tier
![PricingTier](https://github.com/ShauryaRawat10/Data-Engineering/blob/d783f09942a23eca988afb738fb4d0f209caef3d/Azure%20Cloud/Introduction/Storage/ChoosePricingTier.png)

![Capture](https://github.com/ShauryaRawat10/Data-Engineering/blob/d3844370f4d0e88f937d0b3618621a9d2ed2d39a/Azure%20Cloud/Introduction/Storage/EventHubCapture.png)

## Debugging Stream Analytics Job
- Monitoring/Job Diagram : Logical/Physical


## Windowing Functions
- Tumbling Windows
  - This is used to segment the data stream into distinct time segments. A function is performed against the data values in the window
  - Data values don't repeat or overlap
 
```
0 sec to 10 sec : Event 1, Event 2
10 sec to 20 sec: Event 3, Event 4, Event 5

Get data from SQL Server -> Auditing

WITH Operations AS (
  Select
    Records.ArrayValue.operationName AS OperationName,
    Records.ArrayValue.properties.action_name AS ActionName,
    Records.ArrayValue.properties.client_ip AS ClientIP
  FROM
    [securityhub] sh
  CROSS APPLY GetArrayElements(sh.records) AS Records
)
SELECT
  COUNT(OperationName) AS OperationCount,
  ActionName,
  ClientIP
INTO
  [OperationTally]
FROM
  Operations
GROUP BY ActionName, ClientIP, TumblingWindow(minute, 2)


We can write multiple select queries for all types of events handling and input/output
```



- Hopping Window
  - Here Events can belong to more than one window

```
-- Every 5 seconds, give me the tweets in last 10 sec

Select Topic, Count(*) AS TotalTweets
FROM TwitterStream TIMESTAMP BY CreatedAt
GROUP BY Topic, HoppingWindow(second, 10,5)
```

- Sliding Window
  - Here Events can belong to more than one window
  - Output events nly for point in time when content of window actually changes

```
Alert me wheneever a topic is mentioned 3 times in under 10 seconds

Select Topic, Count(*) AS TotalTweets
FROM TwitterStream TIMESTAMP BY CreatedAt
GROUP BY Topic, SlidingWindow(second, 10)
HAVING COUNT(*) >= 3
```


## Reference data in Stream Analytics
```
Static data

Reference Input
 - BLOB Storage/ ADLS Gen 2
 - SQL database
```

## Network Security Group (NSG) logs
- Stream these logs about IP traffic flowing through NSG

```
Resource Group - 
1. Virtual Machine - (Ubuntu VM)
   1. Virtual machine
   2. Public IP Address
   3. Network Security Group (has inbound/outbound rules to filter traffic )
      Create NSG Flow logs (Stream only to Storage account)
   4. Network Interface
   5. Network Watcher

```

UDF : 
```
function main(flowlog, index){
   var Items = flowlog.split(',');
   return Items[index];
}

```























