#### Basic Services:
###### Resources: 
* Create resource based on different services. Resources are instances of azure service that you create. Virtual machines, virtual networks, and storage accounts are all examples of Azure resources
###### Resource Group: 
* RG is a group of azure resources. Logical container for grouping related resources. 
###### Subscription Service: 
* Used for billing purposes

#### Microsoft Entra Id:
* Previously known as Microsoft active directory
* Identity and access management platform
* User and Role management, License management

***************************************************************************************************************************

## Design and Implement Data Storage Basics:

![Focus](https://github.com/ShauryaRawat10/Data-Engineering/blob/d3cb7162482708609b07b74a28cd3ab86b1b0729/Azure%20Cloud/Introduction/Storage/StorageService_Azure_1.png)

Microsoft Azure offers following options for databases:
1. Oracle
2. MySQL
3. Microsoft SQL Server

We need to install database engine on virtual machine 
This comes with responsibilities for admin:
1. You are responsible for uptime of database server
2. Database backups and restore
3. Patch installation at operating system and database engine level

Azure SQL database service:
If company does not want burden of managing underlying infrastructure, they can opt to use AZure SQL database service
* Underlying server is managed by you. Db software will be place with backup/restore and several other features
* You can simply start hosting your databases. Azure SQL database is cloud version of Microsoft SQL Server

![SQL database](https://github.com/ShauryaRawat10/Data-Engineering/blob/78973f06f70021ce24ed4d8c31c24aa6945eca81/Azure%20Cloud/Introduction/Storage/StorageService_Azure_2.png)

#### Create Azure Storage Account

![Create account](https://github.com/ShauryaRawat10/Data-Engineering/blob/97fda1bc5901e6152bf13e4afce51478f4eb1a0b/Azure%20Cloud/Introduction/Storage/StorageService_Azure_3.png)

Choose redundancy: Locally-redundant storage (LRS)

![Container creation](https://github.com/ShauryaRawat10/Data-Engineering/blob/85165c6659a4ce1594249498c464a0816dedbbc3/Azure%20Cloud/Introduction/Storage/StorageService_Azure_5.png)

![Container](https://github.com/ShauryaRawat10/Data-Engineering/blob/378f98bbb02b0eb2ac8fb79e6f63b86fb2b86634/Azure%20Cloud/Introduction/Storage/StorageService_Azure_6.png)

Copy URL: https://mue10dadls01.blob.core.windows.net/datalake/ShauryaRawat/Azure Course/ActivityLog-01.csv
Change Access level: Enable Anonymous access
                     Blob (anonymous read access for blobs only)


**********************************************************************************************************************************

## Azure Data Lake Gen 2 
> Store data - Unstructured, Semi-structured, Structured

To create Azure Gen 2 storage account while creating:
- Enable hirarchical namespace: It enables files and directory semantics, accelerate big data workloads and enable access control lists (ACLs)

In normal storage there is no option to create directory. While in Gen 2 storage, directory option is there


Parquet: Compressed binary file. 
> JSON Size > CSV Size > Parquet Size


## How to create Azure SQL Database 
Azure SQL database will create 2 resources -> One Azure SQL Database, One Azure SQL Server

![Azure SQL database](https://github.com/ShauryaRawat10/Data-Engineering/blob/7c029e010b0957293bc8983c198e2773717ae201/Azure%20Cloud/Introduction/Storage/AzureSQLDatabase.png)


- Partition by : Group by gives limitation of aggregating of data

> Select productId, OrderQty,
> SUM(OrderQty) OVER (partition by ProductId) as 'total order quantity'
> FROM salesLT.SalesOrderDetail

- Lead and Lag function

> SELECT ProductID, OrderQty, 
> LAG(OrderQty) OVER (ORDER BY ProductId) AS 'Previous Order Quantity'
> FROM SalesLT.SalesOrderDetail

> SELECT ProductID, OrderQty, 
> LEAD(OrderQty) OVER (ORDER BY ProductId) AS 'Next Order Quantity'
> FROM SalesLT.SalesOrderDetail


- WITH Clause: Comman Table Expression (CTE)

> WITH CTE_Products AS
> (
>  SELECT ProductID, OrderQty
>  FROM SalesLT.SalesOrderDetail
> )
> Select * from CTE_Products

- Create table command

> Create Table Student
> (
>  StudentID varchar(100) NOT NULL,
>  StudentName varchar(1000),
>  PRIMARY KEY(StudentID)
> )

> Insert into Student(StudentID, StudentName) Values('S01', 'Shaurya')

> Create Table Orders
> (
>  OrderID varchar(100) NOT NULL,
>  CustomerID varchar(100),
>  DiscountPercent int,
>  PRIMARY KEY(OrderID),
>  FOREIGN KEY(CustomerID) REFERENCES Student(StudentID)
>)































