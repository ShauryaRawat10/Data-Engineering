## Streaming data
- Continuously generated and Unbounded data
- Sources: IOT,

#### What is stream processing?
- Traditional batch-oriented data processing is one-off and bounded
- Stream processing is continuous and unbounded

#### Why is stream processing getting popular?
- Data Velocity and Volumes: Rising data velocity and volumes requires continous, incremental processing - cannot process all data in one batch on a schedule
- Real-time analytics: Businesses demand access to freah data for actionable insights and faster, better business decisions
- Operational applications: Critical applications need real-time data for effective, instantaneuos response

- Vast majority of the data in the world is streaming data


#### Stream processing use cases
- Stream processing is a key component of big data applications across all industries
- Examples
  - Notifications
  - Real-time reporting
  - Incremental ETL
  - Update data to serve in real-time
  - Real time decision making
  - Online ML

#### Bounded vs Unbounded dataset
- Bounded Data:
  - Has a finite and unchanging structure at the time of processing
  - The order is static
  - Analogy: Vehicles in parking lot
- Unbounded data:
  - Has an infinite and continuous changing structure at time of processing
  - The order not always sequential
  - Analogy: Vehicles on a highway
 
#### Batch vs Stream processing
- Batch processing
  - Generally refers to processing and analysis of bounded datasets (we can count the number of elements, etc)
  - Typical of appliations where there are loose data latency requirements (i.e day old, week old, month old)
  - This was traditional ETL from transactional systems into analytical systems
- Stream processing
  - Datasets are continuous and unbounded (data is constantly arriving, and must be processed as long as there is new data)
  - Enables low-latency use cases (i.e real time, or near real time)
  - Provides fast, actionable insights (i.e Quality-of-Service, Device monitoring, Recommendations, etc)

#### Advantages of Stream Processing
- A more intuitive way of capturing and processing continuous and unbounded data
- Lower latency for time sensitive applications and use cases
- Better fault-tolerance through checkpointing
- Automatic bookkeeping on new data
- Higher compute utilization and scalability through continuous and incremental processing


#### Challenges of Stream Processing
- Processing out-of-order data based on application timestamps
- Maintaining large amounts of state
- Processing each event exactly once despite machine failures
- Handling load imbalance and stragglers
- Determining how to update output sinks as new events arrive
- Writing data transactionally to output systems

_____________________________________________________________________________________________________________________________________________________



#### What is Structured Streaming
- A scalable, fault tolerant stream processing framework built on Spark SQL engine
- Uses existing structured APIs (DataFrames, SQL Engine) and provides similar API as batch processing API
- Includes stream specific features; end-to-end, exactly-once processing, fault tolerance etc


#### How Structured Streaming Works?
Incremental Updates - Data Stream as an unbounded table
- Streaming data is usually coming in very fast
- The magic behind Spark Sturtured Streaming: Processing infinite data as an incremental table updates

###### Micro-batch processing
- Micro-batch execution - Accumulate small batches of data and process each batch in parallel
- Continous Execution - Continuously listen for new data and process them individually














