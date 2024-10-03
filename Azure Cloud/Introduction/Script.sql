-- Generate Table and Data - SalesOrderHeader and SalesOrderDetail

CREATE TABLE bkp.SalesOrderHeader (
    SalesOrderId INT PRIMARY KEY,
    RevisionNumber INT,
    OrderDate DATE,
    DueDate DATE,
    ShipDate DATE,
    Status INT,  -- Assuming status is a predefined integer value (e.g., 1: New, 2: In Process, 3: Shipped, 4: Cancelled)
    CustomerID INT,
    SubTotal DECIMAL(10, 2),
    TaxAmt DECIMAL(10, 2),
    Freight DECIMAL(10, 2),
    TotalDue DECIMAL(10, 2)  -- Computed as SubTotal + TaxAmt + Freight
);

INSERT INTO bkp.SalesOrderHeader (SalesOrderId, RevisionNumber, OrderDate, DueDate, ShipDate, Status, CustomerID, SubTotal, TaxAmt, Freight, TotalDue)
VALUES
(1405, 1, '2023-08-31', '2023-09-10', '2023-09-13', 2, 605, 4639.45, 371.16, 55.66, 5066.27),
(1771, 4, '2023-06-17', '2023-06-20', '2023-06-25', 4, 807, 4275.20, 342.02, 60.28, 4677.50),
(1409, 3, '2023-02-04', '2023-02-06', '2023-02-10', 1, 840, 3921.87, 313.75, 57.62, 4293.24),
(1992, 2, '2023-05-20', '2023-05-23', '2023-05-28', 4, 231, 539.66, 43.17, 67.78, 650.61),
(1431, 2, '2023-07-27', '2023-07-30', '2023-07-31', 4, 920, 3768.75, 301.50, 77.89, 4148.14),
(1220, 1, '2023-06-29', '2023-07-08', '2023-07-13', 1, 927, 1348.49, 107.88, 82.67, 1539.04),
(1065, 3, '2023-08-25', '2023-08-28', '2023-09-01', 3, 659, 2076.90, 166.15, 84.25, 2327.30),
(1048, 5, '2023-01-21', '2023-01-23', '2023-01-25', 1, 860, 4595.83, 367.67, 48.40, 5011.90),
(1710, 4, '2023-05-07', '2023-05-13', '2023-05-15', 3, 395, 3928.42, 314.27, 40.69, 4283.38),
(1179, 1, '2023-06-14', '2023-06-19', '2023-06-21', 4, 473, 3626.55, 290.12, 34.36, 3951.03);

Select * from bkp.SalesOrderHeader

CREATE TABLE bkp.SalesOrderDetail (
    SalesOrderId INT NOT NULL, -- Foreign Key to SalesOrderHeader
    SalesOrderDetailId INT NOT NULL, -- Primary Key
    OrderQty INT NOT NULL,
    ProductId INT NOT NULL,
    UnitPrice DECIMAL(10, 2) NOT NULL,
    UnitPriceDiscount DECIMAL(5, 2) NOT NULL DEFAULT 0, -- Discount applied per unit
    LineTotal DECIMAL(10, 2) -- Computed column
);

INSERT INTO bkp.SalesOrderDetail (SalesOrderId, SalesOrderDetailId, OrderQty, ProductId, UnitPrice, UnitPriceDiscount, LineTotal)
VALUES
(1405, 1, 5, 101, 100.00, 0.00, 500.00),
(1405, 2, 3, 102, 150.00, 0.05, 427.50),
(1771, 3, 2, 103, 120.00, 0.10, 216.00),
(1771, 4, 6, 104, 200.00, 0.00, 1200.00),
(1409, 5, 1, 105, 250.00, 0.00, 250.00),
(1409, 6, 7, 106, 80.00, 0.15, 476.00),
(1992, 7, 4, 107, 60.00, 0.00, 240.00),
(1992, 8, 3, 108, 300.00, 0.20, 720.00),
(1431, 9, 2, 109, 500.00, 0.00, 1000.00),
(1431, 10, 5, 110, 75.00, 0.05, 356.25),
(1220, 11, 9, 111, 220.00, 0.10, 1782.00),
(1220, 12, 1, 112, 135.00, 0.00, 135.00),
(1065, 13, 8, 113, 350.00, 0.00, 2800.00),
(1065, 14, 4, 114, 95.00, 0.05, 361.00),
(1048, 15, 6, 115, 180.00, 0.00, 1080.00),
(1048, 16, 3, 116, 110.00, 0.10, 297.00),
(1710, 17, 7, 117, 130.00, 0.00, 910.00),
(1710, 18, 2, 118, 75.00, 0.15, 127.50),
(1179, 19, 5, 119, 275.00, 0.00, 1375.00),
(1179, 20, 4, 120, 90.00, 0.05, 342.00);

Select * from bkp.SalesOrderDetail;





