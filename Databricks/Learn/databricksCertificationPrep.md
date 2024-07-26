# Data Engineering with Databricks
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

#### Data Plane:
Where your data is processed. All compute resource reside in your own cloud account. Data plane host compute resources that recall clusters
They connect to the data store backing databricks file system and optionally provide connection to external data sources, either within the same cloud account or elsewhere
on the internet for eg: JDBC or SQL datasource or datalake stored on S3, Azure blob storage or GCS
1. Workspace Clusters
2. SQL Warehouses

#### Control Plane:
Consist of backend services that databricks manages in its own cloud account align with the cloud service in use by the customer (Azure, AWS, GCP)
though majority of data elements don't live here. Some elements notebook commands and workspace configuration are stored in control plane and enrypted at REST
Through the control plane and the associated UI and APIs, it provides you the ability to launch clusters, start jobs and get results and interact with table metadata
It has several services:
* Web APP: Serves databricks UI for you to access.
 Has 3 services for various personas:
  * Data Science and Engineering Persona 
  * Machine Learning Persona
  * Databricks SQL Analyst Persona
* Workflow Manager: Provides job workflows and delta live tables (DLT) to orchestrate tasks in n number of ways
* Cluster Manager: configure and setup spark clusters
* Jobs: Schedule tasks
* Unity Catalog: Provides data governance around access control, metadata management, data lineage, and data discovery

#### Cluster:
Databricks cluster is a set of computational resources and configuration on which you can run data engineering, data science and data analytics workloads.
Run these workloads as set of commands in a notebook or as a job. 
Cluster live in Data Plane within your organization cloud account. Although cluster management is a function of control plane.

## Compute Resources:

#### Clusters:
Collection of VM instances
Distributes workloads across workers

In typical case a Cluster has a driver node alongside one or more Worker nodes. 
Although Databricks provides a single node mode as well. Workloads are distributes across available Worker nodes by the driver. 
Databricks provides 2 main types of Clusters:
1. All-purpose clusters for interactive development
2. Job Clusters for automating workloads

#### Cluster Types:
All-Purpose Cluster | Job Cluster 
------------------- | -------------------
Analyze data collaboratively using interactive notebooks | Run automated Jobs
Create clusters from Workspace or API  |  The databricks job scheduler creates job clusters when running jobs
Manually create or terminate All-purpose cluster | Databricks terminates it when job is complete. We can't restart it
Multiple users can share All=purpose cluster | Job based
Configuration information retained for up to 70 clusters for up to 30 days | Configuration information retained for up to 30 most recently terminated clusters

#### Cluster Configuration:
###### Cluster Mode:
It is operating mode in which there are 2 option
* Single Node Cluster
  * Only one VM instance holding the driver, no worker instances in this configuration
  * Low-cost single-instance cluster catering to single-node machine learning workloads and lightweight exploratory analysis
* MultiNode Cluster (Standard Cluster)
  * General purpose configuration consisting of Vm instances hosting the driver and atleast 1 additional instance for the worker
  * Default mode for workloads developed in any supported language (requires at least 2 VM instances )


#### Databricks Runtime Version:
Runtime is a collection of core software components runnign on the cluster including Apache spark and many other components
* Standard (common): Apache Spark and many other components and updates to provide an optimized big data analytics experiences
* Machine Learning: Adds popular machine learning libraries like Tensorflow, keras, PyTorch, and XGBoost
* Photon: An optional add-on to optimize SQL workloads
For unity catalog connectivity, minimum required version is 10.1 with some featues requiring newer version

#### Access Mode:
Specifies overall security mode of the Cluster. For Standard version, only 4 options available with 2 for Unity catalog

![Access Mode](https://github.com/ShauryaRawat10/Data-Engineering/blob/7da3c80a46bad9f001f08e9a0a64b3a2efc872cf/Databricks/Learn/Storage/Access_Mode_DE1.png)


#### Cluster Policies
Restricted Cluster creation can be achieved by using Policies. Cluster policy respresent a compromise between allowing users to create clusters in unrestricted manner
and completely prohibiting them from creating clusters. Forcing them to request clusters through workspace admin



#### Lab:
1. Navigate to Compute
![Compute_UI](https://github.com/ShauryaRawat10/Data-Engineering/blob/85d2c8c4bef3f55657f18ba9c8af819bb720fdb0/Databricks/Learn/Storage/Compute_UI_DE1.png)

2. Create Cluster
![create cluster](https://github.com/ShauryaRawat10/Data-Engineering/blob/85d2c8c4bef3f55657f18ba9c8af819bb720fdb0/Databricks/Learn/Storage/Cluster_UI_2_DE.png)


#### Databricks Notebooks:
Collaborative, reproducible and enterprise ready
* Supports multi-language: Python, SQL, Scala, R and all in one Notebook
* Real time co-presence, co-editing, and commenting
* Ideal for exploration: Explore, visualize and summarize data with built in charts and data profiles
* Adaptable: Install standard libraries and use local modules
* Reproducible: Automatically track version history and use git version control with Repos
* Get to production faster: Quickly schedule notebooks as jobs or create dashboards from their results, all in Notebook
* Enterprise ready: Enterprise grade access controls, identity management and auditability

###### Notebook magic commands
Use to override default languages, run utilities/auxiliary commands, etc
* %python, %r, %scala, %sql : Switch languages in a command cell
* %sh: Run shell code (only runs on driver node, not worker nodes)
* %fs: Shortcut for dbutils fielsystem commands
* %md: Markdown for styling the display
* %run: Execute a remote notebook from notebook
* %pip: Install new Python libraries

#### dbutils (Databricks Utilities)
Perform various tasks with Databricks using Notebooks
![dbutils](https://github.com/ShauryaRawat10/Data-Engineering/blob/68bed7afe23ceebf50aba8fefb9ea1869974df79/Databricks/Learn/Storage/dbutils_DE1.png)













