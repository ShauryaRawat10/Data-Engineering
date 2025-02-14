-- ********** Version History ***************************************************************************
-- US13565_TA15149  - 2024-11-26 - Shaurya Rawat - Initial Build for CustomerInvoiceLineFact Merge Script

-- ******************************************************************************************************


MERGE INTO edw.{p_table_name} AS target
USING (
    Select * 
    FROM stg.{p_table_name} AS stg   
    WHERE stg.ETL_Ins_Upd_Flag IN ('I','U') 
) AS source
ON target.Rec_Src_Application_Id = source.Rec_Src_Application_Id and target.Rec_Src_Key = source.Rec_Src_Key
WHEN MATCHED THEN
    UPDATE SET
        target.Rec_Process_Log_Id = source.Rec_Process_Log_Id,
        target.Rec_Src_Key = source.Rec_Src_Key,
        target.Cus_Trans_Num = source.Cus_Trans_Num,
        target.Cus_Trans_Ln_Num = source.Cus_Trans_Ln_Num,
        target.Rec_Src_Create_Date = source.Rec_Src_Create_Date,
        target.Rec_Src_Create_User = source.Rec_Src_Create_User,
        target.Rec_Src_Update_Date = source.Rec_Src_Update_Date,
        target.Rec_Src_Update_User = source.Rec_Src_Update_User,
        target.Rec_EDW_Hash = source.Rec_EDW_Hash,
        target.Rec_EDW_Update_Date = source.Rec_EDW_Update_Date,
        target.Customer_Account_Dim_Id = source.Customer_Account_Dim_Id,
        target.Product_Dim_Id = source.Product_Dim_Id,
        target.Fiscal_Period_Dim_Id = source.Fiscal_Period_Dim_Id,
        target.Legal_Entity_Dim_Id = source.Legal_Entity_Dim_Id,
        target.Covered_Product_Dim_Id = source.Covered_Product_Dim_Id,
        target.Order_Dim_Id = source.Order_Dim_Id,
        target.Order_Line_Dim_Id = source.Order_Line_Dim_Id,
        target.Quote_Dim_Id = source.Quote_Dim_Id,
        target.Quote_Line_Dim_Id = source.Quote_Line_Dim_Id,
        target.Contract_Line_Dim_Id = source.Contract_Line_Dim_Id,
        target.Transaction_Currency_Code = source.Transaction_Currency_Code,
        target.Accounting_Currency_Code = source.Accounting_Currency_Code,
        target.Reporting_Currency_Code = source.Reporting_Currency_Code,
        target.Unit_of_Measure_Dim_Id = source.Unit_of_Measure_Dim_Id,
        target.Payment_Term_Dim_Id = source.Payment_Term_Dim_Id,
        target.Payment_Specification_Dim_Id = source.Payment_Specification_Dim_Id,
        target.Payment_Method_Dim_Id = source.Payment_Method_Dim_Id,
        target.Invoice_Payment_Status_Dim_Id = source.Invoice_Payment_Status_Dim_Id,
        target.Invoice_Status_Dim_Id = source.Invoice_Status_Dim_Id,
        target.Ledger_Transaction_Type_Dim_Id = source.Ledger_Transaction_Type_Dim_Id,
        target.Invoice_Document_Date_Dim_Id = source.Invoice_Document_Date_Dim_Id,
        target.Delivery_Address_Dim_Id = source.Delivery_Address_Dim_Id,
        target.Transaction_Date_Dim_Id = source.Transaction_Date_Dim_Id,
        target.Invoice_Due_Date_Dim_Id = source.Invoice_Due_Date_Dim_Id,
        target.Document_Image_URL = source.Document_Image_URL,
        target.Voucher_Num = source.Voucher_Num,
        target.Invoice_Num = source.Invoice_Num,
        target.Invoice_Line_Num = source.Invoice_Line_Num,
        target.Invoice_Line_Desc = source.Invoice_Line_Desc,
        target.PO_Num = source.PO_Num,
        target.Src_Invoice_Num = source.Src_Invoice_Num,
        target.Project_Num = source.Project_Num,
        target.Journal_Num = source.Journal_Num,
        target.Invoice_Active_Ind = source.Invoice_Active_Ind,
        target.Invoice_Posted_Ind = source.Invoice_Posted_Ind,
        target.Total_Invoice_Amt_TC = source.Total_Invoice_Amt_TC,
        target.Total_Invoice_Amt_USD = source.Total_Invoice_Amt_USD,
        target.Invoice_Ln_Qty = source.Invoice_Ln_Qty,
        target.Invoice_Ln_Amt_TC = source.Invoice_Ln_Amt_TC,
        target.Invoice_Ln_Tax_Amt_TC = source.Invoice_Ln_Tax_Amt_TC,
        target.Settlement_Amt_TC = source.Settlement_Amt_TC
WHEN NOT MATCHED THEN
    INSERT (
        Rec_Process_Log_Id,
        Rec_Src_Application_Id,
        Rec_Src_Key,
        Cus_Trans_Num,
        Cus_Trans_Ln_Num,
        Rec_Src_Create_Date,
        Rec_Src_Create_User,
        Rec_Src_Update_Date,
        Rec_Src_Update_User,
        Rec_EDW_Hash,
        Rec_EDW_Create_Date,
        Rec_EDW_Update_Date,
        Customer_Account_Dim_Id,
        Product_Dim_Id,
        Fiscal_Period_Dim_Id,
        Legal_Entity_Dim_Id,
        Covered_Product_Dim_Id,
        Order_Dim_Id,
        Order_Line_Dim_Id,
        Quote_Dim_Id,
        Quote_Line_Dim_Id,
        Contract_Line_Dim_Id,
        Transaction_Currency_Code,
        Accounting_Currency_Code,
        Reporting_Currency_Code,
        Unit_of_Measure_Dim_Id,
        Payment_Term_Dim_Id,
        Payment_Specification_Dim_Id,
        Payment_Method_Dim_Id,
        Invoice_Payment_Status_Dim_Id,
        Invoice_Status_Dim_Id,
        Ledger_Transaction_Type_Dim_Id,
        Invoice_Document_Date_Dim_Id,
        Delivery_Address_Dim_Id,
        Transaction_Date_Dim_Id,
        Invoice_Due_Date_Dim_Id,
        Document_Image_URL,
        Voucher_Num,
        Invoice_Num,
        Invoice_Line_Num,
        Invoice_Line_Desc,
        PO_Num,
        Src_Invoice_Num,
        Project_Num,
        Journal_Num,
        Invoice_Active_Ind,
        Invoice_Posted_Ind,
        Total_Invoice_Amt_TC,
        Total_Invoice_Amt_USD,
        Invoice_Ln_Qty,
        Invoice_Ln_Amt_TC,
        Invoice_Ln_Tax_Amt_TC,
        Settlement_Amt_TC
    )
    VALUES (
        source.Rec_Process_Log_Id,
        source.Rec_Src_Application_Id,
        source.Rec_Src_Key,
        source.Cus_Trans_Num,
        source.Cus_Trans_Ln_Num,
        source.Rec_Src_Create_Date,
        source.Rec_Src_Create_User,
        source.Rec_Src_Update_Date,
        source.Rec_Src_Update_User,
        source.Rec_EDW_Hash,
        source.Rec_EDW_Create_Date,
        source.Rec_EDW_Update_Date,
        source.Customer_Account_Dim_Id,
        source.Product_Dim_Id,
        source.Fiscal_Period_Dim_Id,
        source.Legal_Entity_Dim_Id,
        source.Covered_Product_Dim_Id,
        source.Order_Dim_Id,
        source.Order_Line_Dim_Id,
        source.Quote_Dim_Id,
        source.Quote_Line_Dim_Id,
        source.Contract_Line_Dim_Id,
        source.Transaction_Currency_Code,
        source.Accounting_Currency_Code,
        source.Reporting_Currency_Code,
        source.Unit_of_Measure_Dim_Id,
        source.Payment_Term_Dim_Id,
        source.Payment_Specification_Dim_Id,
        source.Payment_Method_Dim_Id,
        source.Invoice_Payment_Status_Dim_Id,
        source.Invoice_Status_Dim_Id,
        source.Ledger_Transaction_Type_Dim_Id,
        source.Invoice_Document_Date_Dim_Id,
        source.Delivery_Address_Dim_Id,
        source.Transaction_Date_Dim_Id,
        source.Invoice_Due_Date_Dim_Id,
        source.Document_Image_URL,
        source.Voucher_Num,
        source.Invoice_Num,
        source.Invoice_Line_Num,
        source.Invoice_Line_Desc,
        source.PO_Num,
        source.Src_Invoice_Num,
        source.Project_Num,
        source.Journal_Num,
        source.Invoice_Active_Ind,
        source.Invoice_Posted_Ind,
        source.Total_Invoice_Amt_TC,
        source.Total_Invoice_Amt_USD,
        source.Invoice_Ln_Qty,
        source.Invoice_Ln_Amt_TC,
        source.Invoice_Ln_Tax_Amt_TC,
        source.Settlement_Amt_TC
    );


-- Update staging table with processed flag
UPDATE stg.{p_table_name}  stg
SET stg.ETL_Ins_Upd_Flag = concat(stg.ETL_Ins_Upd_Flag, ' - Processed');




