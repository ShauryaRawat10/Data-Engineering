## Authorization ADLS Gen 2
- Permission to user
  - Account name and key
- Shared access signature
- Microsoft Entra Id

```
- Auth using Account name and Access keys
  -> Access to all Blob container, queue, table, file shares

- Shared access signature
  -> Generate SAS token, URL by selecting the following (checkmark)
     Allowed service - Blob, File, Queue, Table
     Allowed resource types - Service, Container, Object
     Allowed permissions - Read, Write, Delete, List, Add, Create, Update, Process, Immutable storage, Permanent delete
     Blob versioning permissions - Enable deletion of versions

     Start and expiry date time
     Allowed IP address

- Microsoft Entra ID
  -> Create a user (User service principle, display name)
  -> RBAC : Role based access control to user
            IAM -> Role assignment
                    Owner - full access, assign roles
                    Contributor - full access, not allowed assign roles
                    Reader - View all resource
                    Storage BLOB data contributor - Read, write, delete containers and blobs
                    Storage BLOB data reader - Read blobs and containers
  -> Sign in to Storage-Explorer using new user
  -> Not be able to see Container as it has Data Plane role but not Control Plane role
  -> RBAC - Give Reader role
```

## Using access control lists
- Fine grain access to Container/file/folder

```
Data Lake Storage -> Storage Container -> Folder -> File

- Need to remove Role assignemnt to utilize access control

Read, Write , Execute

Stoarge Explorer -> Connect to Storage as Storage User
Manage access -> Storage User -> Container -> Give read, execute access
Manage access -> Storage User -> folder -> Give read and execute
Manage access -> Storage user -> file -> read
```

## Microsoft Entra ID
- Authentication vs Authorization: Authentication is to verify identity, Authorization is about having right to access resource
- cloud-based identity provider

## Azure Storage Explorer
- Upload, download, and manage Azure Storage Blobs, files, queues, tables and well as ADLS entities and managed disks
- Free open source tool

________________________________________________________________________________________________________________________________________________

## Synapse Security

#### Synapse Column level security
- Control access to table columns 

```
Create SQL based users

CREATE USER Supervisor WITHOUT LOGIN;
CREATE USER UserA WITHOUT LOGIN;

GRANT SELECT ON [dbo].[CourseOrders] TO Supervisor
GRANT SELECT ON [dbo].[CourseOrders](OrderId, Course, Quantity) To UserA;
```

#### Azure Data Studio
- Connect to dedicated SQL pool - Synapse

```
EXECUTE AS USER = 'UserA';

SELECT * from dbo.courseOrders                   -- Give error as other column dont have permission
SELECT OrderID, Quantity from dbo.courseOrders
```


#### Synapse Row level Security
```
Craete schema security;

Create function Security.securitypredicate(@Agent AS nvarchar(50))
  RETURNS TABLE
WITH SCHEMABINDING
AS
  RETURN SELECT 1 AS securitypredicate_result
WHERE @Agent = USER_NAME() OR USER_NAME() = 'Supervisor';   -- @agent is a column which has users name in table


Create security policy filter
add filter predicate Security.securitypredicate(Agent)
ON [dbo].[CourseOrders]
WITH (STATE = ON);
GO

GRANT SELECT ON Security.securitypredicate TO Supervisor;
GRANT SELECT ON Security.securitypredicate TO AgentA;
GRANT SELECT ON Security.securitypredicate TO AgentB;

EXECUTE AS USER = 'UserA';
SELECT * from [dbo].[courseorders];    -- Only getting rows with Agent = AgentA
```

#### Dynamic Data masking in Synapse
- Limits sensitive data explosure by masking it to unathorized user
- For eg: Mask first 16 letters in Credit card, mask username in email, 

```
Resources -> Dedicate SQL Pool -> Dynamic data masking -> Add mask

Choose schema
Choose table
Choose Column
Masking field format -> Email masking function

Create new user

Create login newuser             // In the context of Master
with password = 'user@123'

Create USER newuser              // In the context of dedicated pool
  for login newuser
  with default_schema = dbo

Exec sp_addrolemember 'db_datareader', 'newuser';  // Give raeder access

Login as newuser in data explorer to dedicated sql pool
```

#### Dedicated SQL Pool Encryption
- Transparent data enryption / REST enryption
- Data is stored in underlying disk, we can further encrypt it in disk in data center

```
Dedicated SQL Pool -> Transparent data encryption -> Turn on
```

#### Synapse Workspace encryption
- We can enable encryption of entire SQL workspace
- Key vault : To amnage the encryption key used for encryption purpose

```
IAM  -> Key Valut Crypto Officer

Objects
  - Keys
  - secrets
  - certificates

Generate new encryption key

While Creating new resource for Synapse
 - In security
   Double encryption using a customer managed key = enable
   Key vault and key -> choose key
```

#### Synapse - Access user using Entra ID
- When users are already in Microsoft Entra ID, we can provide access to dedicated SQL Pool directly
- It also enables multi-factor authentication

```
Create new user in Entra ID

Go to Synapse
   -> Settings -> Microsoft Entra ID -> Set admin (new user)

In Synapse studio

CREATE USER [newuser_serviceprinciple]
FROM EXTERNAL PROVIDER
WITH DEFAULT_SCHEMA = dbo;

EXEC sp_addrolemember N'db_datareader', N'newuser_serviceprinciple'

```

#### Synapse - External tables - Microsoft Entra ID
```
for the user in entra id

In the Storage explorer -> give access using access control to file/folder/container
```

#### Network firewall
- Storage account service is a public service
- We can restrict the storage account service to only be accessible from within Virtual network only
  - This is done via Virtual Network Service Endpoints

Virtual Network (    Subnet (     VM, Network interface, public IP addresses, Disks, Network securoty group    )     )

```
Storage account
   -> Networking -> Enabled from selected virtual networks and IP addresses -> Add Virtual network
                                                                                VN name, subnet

Create Virtual machine -> copy the IP address
Go to Virtual network resource -> Service endpoints -> Choose Service (Azure Storage), subnet (default)

```

## Managed Identities
- More secure way to connect to Azure
- We can enable managed Identity
  - ADF
- This creates security principle in Microsoft Entra ID
- The linked service can use manage identity for Authorization purpose


#### Managed Identity in ADF, Synapse
- If you have network firewall and vnet, you should use Managed Identity in ADF, not Account key

```
CREATE DATABASE SCOPED CREDENTIAL storedManagedIdentity
WITH IDENTITY = 'Managed Identity'

CREATE EXTERNAL DATA SOURCE srcActivityLog
WITH (
  LOCATION = 'abfss://data@datalake4000700.dfs.core.windows.net',
  CREDENTIAL = storedManagedIdentity
)

GO to Storage resource -> Add role assignment -> Storage BLOB data Reader Role
                                                 Assign access to -> Managed Identity -> Synapse Workspace
```

#### Data Discovery and Classification
- It provides basic capabilities for discovering, classifying, labeling, and reporting sensitive data in your databases

```
Dedicated SQL Pool resource -> Data Discovery and Classification -> Select tables (mark sensitive)


Synapse resource -> Azure SQL Auditing -> Enable (Log Analytics, Storage, event hub)

Log Analytics resource -> Tables -> Auditingtable -> see data in table and we can create alert rule when sensitive data is queried
```


#### Encryption in ADF
- Use Key Vault to encrypt data in ADF

```
Have ADF resource with Empty data, pool, etc

Create a key in Key Vault
IAM -> Add role assignment -> select role (Key Vault cryto service encryption user) -> Assign access to (Managed Identities - Choose your data factory)

Go to ADF -> Manage -> Customer managed key -> Add key (Azure key vault key url)
``` 

#### databricks - Using secret scope

```
Define application Object in Entra ID
Give access to Application object to ADLS Account
Secret for Application Object can be stored in Key Vault
In Notebook, fetch secret from key vault, access datalake via  use of application object


Go to Entra ID
   -> Manage -> App Registrations -> New Registration -> databricksregis (create)


Storage Account
   -> IAM -> Add Role assignment -> Storage BLOB data reader
                                  -> members (databricksregis) -> assign

Databricks resource -> Certificates and Secrets -> New Client Secret
                                                   (Secret generated)

Key Vault -> Create a Secret -> New -> Get secret value from databricks
```














