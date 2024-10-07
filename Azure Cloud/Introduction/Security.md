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






