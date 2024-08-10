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











