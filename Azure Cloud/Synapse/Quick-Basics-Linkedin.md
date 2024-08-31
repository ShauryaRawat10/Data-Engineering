#### Create table script in DEVELOP tab - Distribution = hash
Create table [dbo].[DailySales]
(
    StoreCode VARCHAR(100) NOT NULL,
    Date DATETIME NOT NULL,
    TotalSales BIGINT NOT NULL,
    TotalAmount BIGINT NOT NULL
)
WITH (
    DISTRIBUTION = HASH (StoreCode),
    CLUSTERED COLUMNSTORE INDEX
)

## Copy data tool
