##ADF
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