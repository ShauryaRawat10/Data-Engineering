## Data Engineering with Databricks
- Core Components of Lakehouse platform
- Data Science and Engineering workspace UI
- Create and manage clusters using Cluster UI
- Develop and run code in multi-cell databricks notebooks
- Integrate git support using Repos

## Databricks Architecture and Services
![Architecture](https://github.com/ShauryaRawat10/Data-Engineering/blob/8f9b9d44c8e97473f7c9768f76d74442176b8634/Databricks/Learn/Storage/WorkdpaceAndServices.png)

Databricks lakehouse platform is a commercial grade cloud native implementation of the lakehouse model, that is a data storage layer that resides on top of the datalake 
that provides the performance and reliability of the datawarehouse, combined with the flexibility, cost efficiency and scale of the datalake all in one simple open
collaborative platform, providing the best of both worlds enables the variety of data practitioners like SQL analysts, data engineers and ML practitioners to perform their
roles in a unified and scalable way. 

Control Pane:


Data Plane:
Where your data is processed. All compute resource reside in your own cloud account. Data plane host compute resources that recall clusters
They connect to the data store backing databricks file system and optionally provide connection to external data sources, either within the same cloud account or elsewhere
on the internet for eg: JDBC or SQL datasource or datalake stored on S3, Azure blob storage or GCS
1. Workspace Clusters
2. SQL Warehouses

Control Plane:
Consist of backend services that databricks manages in its own cloud account align with the cloud service in use by the customer (Azure, AWS, GCP)
though majority of data elements don't live here. Some elements notebook commands and workspace configuration are stored in control plane and enrypted at REST
Through the control plane and the associated UI and APIs, it provides you the ability to launch clusters, start jobs and get results and interact with table metadata
It has several services:
1. Web APP: Serves databricks UI for you to access. Has 3 services for various personas:
         * Data Science and Engineering Persona 
         * Machine Learning Persona
         * Databricks SQL Analyst Persona
2. Workflow Manager: Provides job workflows and delta live tables (DLT) to orchestrate tasks in n number of ways
3. Cluster Manager: configure and setup spark clusters
4. Jobs: Schedule tasks
5. Unity Catalog: Provides data governance around access control, metadata management, data lineage, and data discovery














