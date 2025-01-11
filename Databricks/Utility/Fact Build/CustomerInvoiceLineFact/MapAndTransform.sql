-- ********** Version History  ******************************************************************************************
-- US13565_TA15149  - 2024-11-26 - Shaurya Rawat - Initial Build for CustomerInvoiceLineFact - Sales and Customer Invoice
-- US13565_TA15149  - 2024-11-26 - Shaurya Rawat - Initial Build for CustomerInvoiceLineFact - Project Invoice

-- **********************************************************************************************************************

-- Get Selective Raw data from D365 Source tables 
With CTE_CustTrans as (
    Select
    recid,
    id,
    accountnum,
    transdate,
    transtype,
    dataareaid,
    voucher,
    invoice,
    settleamountcur,
    Case when closed = Date('1900-01-01T00:00:00.000') Then 0
         when closed is not null then 1
         when closed is null then 0
    End AS Custom_Invoice_Status,
    IsDelete,
    amountcur,
    reportingcurrencyamount,
    paymtermid,
    paymspec,
    paymmode,
    duedate,
    documentdate,
    CASE 
      WHEN (amountcur =  settleamountcur) THEN 'FULLY PAID'
      WHEN (amountcur <> settleamountcur AND settleamountcur <> 0) THEN 'PARTIALLY PAID'
      WHEN (amountcur <> settleamountcur AND settleamountcur = 0) THEN 'NOT PAID'  
    END AS Custom_Payment_Status,  
    DL_effectiveDate,
    DL_current_flag
    from raw_d365_export_ukg.custtrans where DL_current_flag = 1
),

CTE_CustInvoiceJournal as (
    Select distinct 
    INVOICEACCOUNT,
    INVOICEDATE,
    DATAAREAID,
    LEDGERVOUCHER,
    INVOICEID, 
    SALESID,
    NUMBERSEQUENCEGROUP,
    POSTEDSTATE,
    deliverypostaladdress,
    sourcedocumentheader,
    DL_effectiveDate,
    DL_current_flag
    from raw_d365_export_ukg.custinvoicejour where DL_current_flag = 1
),

CTE_CustInvoiceTrans as (
    Select
    RECID,
    CREATEDDATETIME,
    CREATEDBY,
    MODIFIEDDATETIME,
    modifiedby,                                  
    CURRENCYCODE,
    LINENUM,
    NAME,
    CUSTINVOICELINEIDREF,
    CUSTOMERLINENUM,
    QTY,
    LINEAMOUNT,
    LINEAMOUNTTAX,
    SALESID,
    SalesUnit,
    INVOICEDATE,
    INVOICEID,
    DATAAREAID,
    NUMBERSEQUENCEGROUP,
    INVENTTRANSID,
    DL_effectiveDate,
    DL_current_flag
    from raw_d365_export_ukg.custinvoicetrans where DL_current_flag = 1
),

CTE_ProjInvoiceJournal as (
    Select distinct 
    invoicedate,
    projinvoiceid,
    dataareaid,
    INVOICEACCOUNT,
    LEDGERVOUCHER,
    sourcedocumentheader,
    deliverypostaladdress,
    dl_effectivedate
    from raw_d365_export_ukg.projinvoicejour where DL_current_flag = 1
),

CTE_ProjInvoiceItem as (
    Select 
    recid,
    createddatetime,
    createdby,
    modifieddatetime,
    modifiedby,
    currencyid,
    itemid,
    txt,
    invoicedate,
    projinvoiceid, 
    projid,
    dataareaid,
    INVENTTRANSID,
    qty,
    lineamount,
    salesid,
    salesunit,
    projtransid,
    taxamount,
    dl_effectivedate
    from raw_d365_export_ukg.projinvoiceitem where DL_current_flag = 1
),

CTE_SalesLine as (
    Select distinct 
    INVENTTRANSID, 
    projtransid,
    DATAAREAID,
    ITEMID,
    case when ukg_sfcoveredproduct is null then '-1'
    else ukg_sfcoveredproduct end as ukg_sfcoveredproduct,
    ukg_sourcesalesorderlineid,
    ukg_sfcontractlineid,
    DL_effectiveDate,
    DL_current_flag
     from raw_d365_export_ukg.salesline where DL_current_flag = 1
),

-- Get current data from existing EDW Dimension tables 
CTE_Product_Dim as (
    Select 
    product_dim_id,
    Prod_Item_Num
    from edw.product_dim where Rec_EDW_Current_Ind = 1
),

-- Get standard Calender Date for Invoices and Transactions
CTE_Date as ( 
    select Date_Dim_Id, Calendar_Date from edw.date_dim 
),

-- Get Dimension Keys from EDW Order_Line_Fact (Order_Dim_Id, Order_Line_Dim_Id, Quote_Dim_Id, Quote_Line_Dim_ID, Product_Dim_Id, Required_By_Product_Dim_Id)
-- Get SalesLine data from D365 Source
CTE_OrderLine_Dim as (
    Select 
    ol_d.rec_src_key,
    sl.INVENTTRANSID,
    sl.DATAAREAID,
    sl.projtransid,
    ol_f.Order_Dim_Id,
    ol_f.Order_Line_Dim_Id,
    ol_f.Quote_Dim_Id,
    ol_f.Quote_Line_Dim_Id,
    ol_f.Product_Dim_Id,
    ol_f.Required_By_Product_Dim_Id
    from edw.Order_Line_Dim ol_d
    inner join edw.Order_Line_Fact ol_f
        on ol_d.Order_Line_Dim_Id = ol_f.Order_Line_Dim_Id 
    inner join CTE_SalesLine sl 
        on ol_d.Rec_Src_Key = sl.ukg_sourcesalesorderlineid
),

-- Prepare Fact table data containing Customer, Sales and Project Invoices
CTE_INVOICE_UNION as (
    
    -- Fact Buiding 01: Customer and Sales Invoices

    Select
    etl.udf_get_application_id('D365_Export','UKG')                        AS Rec_Src_Application_Id,
    '{p_process_log_id}'                                                   AS Rec_Process_Log_Id, 
    concat('C_', cit.RECID)                                                AS Rec_Src_Key,
    cts.RECID                                                              AS CILF_Cus_Trans_Num,
    cit.RECID                                                              AS CILF_Cus_Trans_Ln_Num,
    cit.CREATEDDATETIME                                                    AS Rec_Src_Create_Date,
    cit.CREATEDBY                                                          AS Rec_Src_Create_User,
    cit.MODIFIEDDATETIME                                                   AS Rec_Src_Update_Date,
    cit.modifiedby                                                         AS Rec_Src_Update_User,                 
    ifnull(cad.Customer_Account_Dim_Id, -1)                                AS CILF_Customer_Account_Dim_Id,
    ifnull(ord.Product_Dim_Id, -1)                                         AS CILF_Product_Dim_Id,
    ifnull(fp.Fiscal_Period_Dim_Id, -1)                                    AS CILF_Fiscal_Period_Dim_Id,
    ifnull(le.Legal_Entity_Dim_Id, -1)                                     AS CILF_Legal_Entity_Dim_Id,
    ifnull(ord.Required_By_Product_Dim_Id, -1)                             AS CILF_Covered_Product_Dim_Id,
    ifnull(ord.Order_Dim_Id, -1)                                           AS CILF_Order_Dim_Id,                           
    ifnull(ord.Order_Line_Dim_Id, -1)                                      AS CILF_Order_Line_Dim_Id, 
    ifnull(ord.Quote_Dim_Id, -1)                                           AS CILF_Quote_Dim_Id,
    ifnull(ord.QUOTE_LINE_DIM_ID, -1)                                      AS CILF_Quote_Line_Dim_Id,
    ifnull(c_ln.Contract_Line_Dim_Id, -1)                                  AS CILF_Contract_Line_Dim_Id,
    ifnull(cit.CURRENCYCODE, '-1')                                         AS CILF_Transaction_Currency_Code,
    ifnull(ad.accountingcurrency, '-1')                                    AS CILF_Accounting_Currency_Code,               
    'USD'                                                                  AS CILF_Reporting_Currency_Code,
    ifnull(uom.unit_of_measure_dim_id, -1)                                 AS CILF_Unit_of_Measure_Dim_Id,    
    ifnull(pt.Payment_Term_Dim_Id, -1)                                     AS CILF_Payment_Term_Dim_Id,
    ifnull(ps.Payment_Specification_Dim_Id, -1)                            AS CILF_Payment_Specification_Dim_Id,
    ifnull(pm.Payment_Method_Dim_Id, -1)                                   AS CILF_Payment_Method_Dim_Id,
    ifnull(psd.Invoice_Payment_Status_Dim_Id, -1)                          AS CILF_Invoice_Payment_Status_Dim_Id,                        
    ifnull(isd.Invoice_Status_Dim_Id, -1)                                  AS CILF_Invoice_Status_Dim_Id,                                     
    ifnull(lttd.Ledger_Transaction_Type_Dim_ID, -1)                        AS CILF_Ledger_Transaction_Type_Dim_ID,           
    ifnull(dt_inv_doc.Date_Dim_Id, -1)                                     AS CILF_Invoice_Document_Date_Dim_Id,
    ifnull(pa.Postal_Address_Dim_Id, -1)                                   AS CILF_Delivery_Address_Dim_Id,
    ifnull(dt_trns.Date_Dim_Id, -1)                                        AS CILF_Transaction_Date_Dim_Id,
    ifnull(dt_inv_due.Date_Dim_Id, -1)                                     AS CILF_Invoice_Due_Date_Dim_Id,
    inv_stg.invoicecopyurl                                                 AS CILF_Document_Image_URL,                
    cts.voucher                                                            AS CILF_Voucher_Num,
    cts.invoice                                                            AS CILF_Invoice_Num,
    cit.LINENUM                                                            AS CILF_Invoice_Line_Num,
    cit.NAME                                                               AS CILF_Invoice_Line_Desc,
    concat(cit.CUSTINVOICELINEIDREF, '_' ,cit.CUSTOMERLINENUM)             AS CILF_PO_Num,                               
    cts.id                                                                 AS CILF_Src_Invoice_Num,
    NULL                                                                   AS CILF_Project_Num,
    gje.journalnumber                                                      AS CILF_Journal_Num,
    Case when cts.IsDelete is null then 0
         else 1 end                                                        AS CILF_Invoice_Active_Ind,               
    cij.POSTEDSTATE                                                        AS CILF_Invoice_Posted_Ind,
    cts.amountcur                                                          AS CILF_Total_Invoice_Amt_TC, 
    cts.reportingcurrencyamount                                            AS CILF_Total_Invoice_Amt_USD,
    cit.QTY                                                                AS CILF_Invoice_Ln_Qty,
    cit.LINEAMOUNT                                                         AS CILF_Invoice_Ln_Amt_TC,
    cit.LINEAMOUNTTAX                                                      AS CILF_Invoice_Ln_Tax_Amt_TC,
    cts.settleamountcur                                                    AS CILF_Settlement_Amt_TC
    from CTE_CustTrans cts
    inner join CTE_CustInvoiceJournal cij
        on cts.accountnum = cij.INVOICEACCOUNT
        and cts.transdate = cij.INVOICEDATE
        and cts.dataareaid = cij.DATAAREAID 
        and cts.voucher = cij.LEDGERVOUCHER
        and cts.invoice = cij.INVOICEID
    inner join CTE_CustInvoiceTrans cit
        on cij.SALESID = cit.SALESID
        and cij.INVOICEDATE = cit.INVOICEDATE
        and cij.INVOICEID = cit.INVOICEID
        and cij.DATAAREAID = cit.DATAAREAID
        and cij.NUMBERSEQUENCEGROUP = cit.NUMBERSEQUENCEGROUP
    inner join edw.Ledger_Transaction_Type_Dim lttd
        on lttd.Ledger_Trans_Type_Code = cts.transtype and Ledger_Trans_Type in ('Sales', 'Cust')
    left join edw.customer_account_dim cad 
        on cts.accountnum = cad.Cus_AR_Num and cad.Rec_EDW_Current_Ind = 1
    left join edw.fiscal_period_dim fp
        on cts.transdate between fp.Period_Start_Date and fp.Period_End_Date 
    left join edw.legalentity_dim le 
        on cts.dataareaid = le.Legal_Entity_Num and le.Rec_EDW_Current_Ind = 1
    -- Get Dimension Keys (Order_Dim_Id, Order_Line_Dim_Id, Quote_Dim_Id, Quote_Line_Dim_ID, Product_Dim_Id, Required_By_Product_Dim_Id)
    left join CTE_OrderLine_Dim ord
        on ord.INVENTTRANSID = cit.INVENTTRANSID and ord.DATAAREAID = cit.DATAAREAID
    -- Get Contract_Line_Dim_ID using SalesLine
    left join 
        (
            Select cl.Contract_Line_Dim_Id, sl.INVENTTRANSID, sl.DATAAREAID 
            from edw.contract_line_dim cl
            join CTE_SalesLine sl 
            on cl.Rec_Src_Key = sl.ukg_sfcontractlineid
        ) as c_ln
        on c_ln.INVENTTRANSID = cit.INVENTTRANSID and c_ln.DATAAREAID = cit.DATAAREAID
    -- Get distinct columns required from accounting distribution
    left join 
        (
            Select distinct accountingcurrency, SOURCEDOCUMENTHEADER from raw_d365_export_ukg.accountingdistribution 
        ) ad 
        on ad.SOURCEDOCUMENTHEADER = cij.SOURCEDOCUMENTHEADER
    left join edw.unit_of_measure_dim uom
        on uom.UOM_Code = cit.SalesUnit
    left join edw.payment_term_dim pt
        on pt.Payment_Term_Code = cts.paymtermid
    left join edw.payment_specification_dim ps 
        on ps.Payment_Spec_Code = cts.paymspec and ps.Payment_Method_Code = cts.paymmode
    left join edw.Invoice_Payment_Status_Dim psd
       on psd.Invoice_Payment_Status = cts.Custom_Payment_Status
    left join edw.Invoice_Status_Dim isd
       on isd.Invoice_Status = cts.Custom_Invoice_Status
    left join edw.payment_method_dim pm
        on lower(pm.Payment_Method_Code) = lower(cts.paymmode) 
    left join CTE_Date dt_inv_doc 
        on dt_inv_doc.Calendar_Date = cts.documentdate
    left join CTE_Date dt_trns
        on dt_trns.Calendar_Date = cts.transdate
    left join CTE_Date dt_inv_due
        on dt_inv_due.Calendar_Date = cts.duedate
    left join raw_d365_export_ukg.ukg_invoiceazurestorage inv_stg
        on inv_stg.dataareaid = cts.dataareaid and inv_stg.invoiceid = cts.invoice
    left join edw.postal_address_dim pa 
        on pa.Rec_Src_Key = cij.deliverypostaladdress    
    left join raw_d365_export_ukg.generaljournalentry gje 
        on gje.SUBLEDGERVOUCHERDATAAREAID = cts.dataareaid and gje.SUBLEDGERVOUCHER = cts.voucher and gje.ACCOUNTINGDATE = cts.transdate
    where cts.dl_effectivedate >= '{WaterMark}' OR cij.dl_effectivedate >= '{WaterMark}' OR cit.dl_effectivedate >= '{WaterMark}' 

    
    union all

    
    -- Fact Buiding 02: Project Invoices

    Select
    etl.udf_get_application_id('D365_Export','UKG')                        AS Rec_Src_Application_Id,
    '{p_process_log_id}'                                                   AS Rec_Process_Log_Id, 
    concat('P_', pii.RECID)                                                AS Rec_Src_Key,
    cts.RECID                                                              AS CILF_Cus_Trans_Num,
    pii.RECID                                                              AS CILF_Cus_Trans_Ln_Num,
    pii.CREATEDDATETIME                                                    AS Rec_Src_Create_Date,
    pii.CREATEDBY                                                          AS Rec_Src_Create_User,
    pii.MODIFIEDDATETIME                                                   AS Rec_Src_Update_Date,
    pii.modifiedby                                                         AS Rec_Src_Update_User,               
    ifnull(cad.Customer_Account_Dim_Id, -1)                                AS CILF_Customer_Account_Dim_Id,
    ifnull(ord.product_dim_id, -1)                                         AS CILF_Product_Dim_Id,
    ifnull(fp.Fiscal_Period_Dim_Id, -1)                                    AS CILF_Fiscal_Period_Dim_Id,
    ifnull(le.Legal_Entity_Dim_Id, -1)                                     AS CILF_Legal_Entity_Dim_Id,
    ifnull(ord.Required_By_Product_Dim_Id, -1)                             AS CILF_Covered_Product_Dim_Id,
    ifnull(ord.Order_Dim_Id, -1)                                           AS CILF_Order_Dim_Id,                         
    ifnull(ord.Order_Line_Dim_Id, -1)                                      AS CILF_Order_Line_Dim_Id,
    ifnull(ord.Quote_Dim_Id, -1)                                           AS CILF_Quote_Dim_Id,
    ifnull(ord.QUOTE_LINE_DIM_ID, -1)                                      AS CILF_Quote_Line_Dim_Id,
    ifnull(c_ln.Contract_Line_Dim_Id, -1)                                  AS CILF_Contract_Line_Dim_Id,
    ifnull(pii.currencyid, '-1')                                           AS CILF_Transaction_Currency_Code,
    ifnull(acc.accountingcurrency, '-1')                                   AS CILF_Accounting_Currency_Code,              
    'USD'                                                                  AS CILF_Reporting_Currency_Code,
    ifnull(uom.unit_of_measure_dim_id, -1)                                 AS CILF_Unit_of_Measure_Dim_Id,          
    ifnull(pt.Payment_Term_Dim_Id, -1)                                     AS CILF_Payment_Term_Dim_Id,
    ifnull(ps.Payment_Specification_Dim_Id, -1)                            AS CILF_Payment_Specification_Dim_Id,
    ifnull(pm.Payment_Method_Dim_Id, -1)                                   AS CILF_Payment_Method_Dim_Id,
    ifnull(psd.Invoice_Payment_Status_Dim_Id, -1)                          AS CILF_Invoice_Payment_Status_Dim_Id,                   
    ifnull(isd.Invoice_Status_Dim_Id, -1)                                  AS CILF_Invoice_Status_Dim_Id,                  
    ifnull(lttd.Ledger_Transaction_Type_Dim_ID, -1)                        AS CILF_Ledger_Transaction_Type_Dim_ID,         
    ifnull(dt_inv_doc.Date_Dim_Id, -1)                                     AS CILF_Invoice_Document_Date_Dim_Id,
    ifnull(pa.Postal_Address_Dim_Id, -1)                                   AS CILF_Delivery_Address_Dim_Id,
    ifnull(dt_trns.Date_Dim_Id, -1)                                        AS CILF_Transaction_Date_Dim_Id,
    ifnull(dt_inv_due.Date_Dim_Id, -1)                                     AS CILF_Invoice_Due_Date_Dim_Id,
    inv_stg.invoicecopyurl                                                 AS CILF_Document_Image_URL,                    
    cts.voucher                                                            AS CILF_Voucher_Num,
    cts.invoice                                                            AS CILF_Invoice_Num,
    pii.itemid                                                             AS CILF_Invoice_Line_Num,
    pii.txt                                                                AS CILF_Invoice_Line_Desc,
    NULL                                                                   AS CILF_PO_Num,                                         
    cts.id                                                                 AS CILF_Src_Invoice_Num,
    pii.projid                                                             AS CILF_Project_Num,
    gje.journalnumber                                                      AS CILF_Journal_Num,
    Case when cts.IsDelete is null then 0
         else 1 end                                                        AS CILF_Invoice_Active_Ind,     
    '1'                                                                    AS CILF_Invoice_Posted_Ind,
    cts.amountcur                                                          AS CILF_Total_Invoice_Amt_TC, 
    cts.reportingcurrencyamount                                            AS CILF_Total_Invoice_Amt_USD,
    pii.qty                                                                AS CILF_Invoice_Ln_Qty,
    pii.LINEAMOUNT                                                         AS CILF_Invoice_Ln_Amt_TC,
    pii.taxamount                                                          AS CILF_Invoice_Ln_Tax_Amt_TC,                    
    NULL                                                                   AS CILF_Settlement_Amt_TC
    from CTE_CustTrans cts
    join CTE_ProjInvoiceJournal pij
        on cts.accountnum = pij.INVOICEACCOUNT
        and cts.transdate = pij.INVOICEDATE
        and cts.dataareaid = pij.DATAAREAID 
        and cts.voucher = pij.LEDGERVOUCHER
        and cts.invoice = pij.projinvoiceid                                                                        
    join CTE_ProjInvoiceItem pii
        on pii.INVOICEDATE = pij.INVOICEDATE
        and pii.projinvoiceid = pij.projinvoiceid
        and pii.DATAAREAID = pij.DATAAREAID
    inner join edw.Ledger_Transaction_Type_Dim lttd
        on lttd.Ledger_Trans_Type_Code = cts.transtype and Ledger_Trans_Type = 'Project'
    left join edw.customer_account_dim cad 
        on cts.accountnum = cad.Cus_AR_Num and cad.Rec_EDW_Current_Ind = 1
    left join edw.fiscal_period_dim fp
        on pii.invoicedate between fp.Period_Start_Date and fp.Period_End_Date 
    left join edw.legalentity_dim le 
        on cts.dataareaid = le.Legal_Entity_Num and le.Rec_EDW_Current_Ind = 1
    -- Get Dimension Keys (Order_Dim_Id, Order_Line_Dim_Id, Quote_Dim_Id, Quote_Line_Dim_ID, Product_Dim_Id, Required_By_Product_Dim_Id)
    left join CTE_OrderLine_Dim ord
        on ord.INVENTTRANSID = pii.INVENTTRANSID and ord.DATAAREAID = pii.DATAAREAID and ord.projtransid = pii.projtransid and ord.projtransid is not null
    -- Get Contract_Line_Dim_ID using SalesLine
    left join 
        (
            Select cl.Contract_Line_Dim_Id, sl.INVENTTRANSID, sl.DATAAREAID 
            from edw.contract_line_dim cl
            join CTE_SalesLine sl 
            on cl.Rec_Src_Key = sl.ukg_sfcontractlineid
        ) as c_ln
        on c_ln.INVENTTRANSID = pii.INVENTTRANSID and c_ln.DATAAREAID = pii.DATAAREAID
    left join 
        (
            Select distinct gje.SUBLEDGERVOUCHER, gje.SUBLEDGERVOUCHERDATAAREAID, le.accountingcurrency
            from raw_d365_export_ukg.generaljournalentry gje
            left join raw_d365_export_ukg.ledger le
            on gje.ledger = le.RECID
        ) acc
        on acc.SUBLEDGERVOUCHER = cts.voucher and acc.SUBLEDGERVOUCHERDATAAREAID = cts.dataareaid
    left join edw.unit_of_measure_dim uom
        on uom.UOM_Code = pii.SalesUnit
    left join edw.payment_term_dim pt
        on pt.Payment_Term_Code = cts.paymtermid
    left join edw.payment_specification_dim ps 
        on ps.Payment_Spec_Code = cts.paymspec and ps.Payment_Method_Code = cts.paymmode
    left join edw.Invoice_Payment_Status_Dim psd
       on psd.Invoice_Payment_Status = cts.Custom_Payment_Status
    left join edw.Invoice_Status_Dim isd
       on isd.Invoice_Status = cts.Custom_Invoice_Status
    left join edw.payment_method_dim pm
        on lower(pm.Payment_Method_Code) = lower(cts.paymmode) 
    left join CTE_Date dt_inv_doc 
        on dt_inv_doc.Calendar_Date = cts.documentdate
    left join CTE_Date dt_trns
        on dt_trns.Calendar_Date = cts.transdate
    left join CTE_Date dt_inv_due
        on dt_inv_due.Calendar_Date = cts.duedate
    left join raw_d365_export_ukg.ukg_invoiceazurestorage inv_stg
        on inv_stg.dataareaid = cts.dataareaid and inv_stg.invoiceid = cts.invoice
    left join edw.postal_address_dim pa 
        on pa.Rec_Src_Key = pij.deliverypostaladdress    
    left join raw_d365_export_ukg.generaljournalentry gje 
        on gje.SUBLEDGERVOUCHERDATAAREAID = cts.dataareaid and gje.SUBLEDGERVOUCHER = cts.voucher and gje.ACCOUNTINGDATE = cts.transdate 
    where cts.dl_effectivedate >= '{WaterMark}' OR pij.dl_effectivedate >= '{WaterMark}' OR pii.dl_effectivedate >= '{WaterMark}' 
)



-- Bulding Staging Fact table for Customer, Sales and Project Invoices 

INSERT OVERWRITE stg.{p_table_name} BY NAME 

    SELECT 
    Rec_Src_Application_Id                      AS Rec_Src_Application_Id,
    Rec_Process_Log_Id                          AS Rec_Process_Log_Id,
    Rec_Src_Key                                 AS Rec_Src_Key,
    CILF_Cus_Trans_Num                          AS Cus_Trans_Num,
    CILF_Cus_Trans_Ln_Num                       AS Cus_Trans_Ln_Num,
    Rec_Src_Create_Date                         AS Rec_Src_Create_Date,
    Rec_Src_Create_User                         AS Rec_Src_Create_User,
    Rec_Src_Update_Date                         AS Rec_Src_Update_Date,
    Rec_Src_Update_User                         AS Rec_Src_Update_User,
    '{current_timestamp}'                       AS Rec_EDW_Update_Date,
    '{current_timestamp}'                       AS Rec_EDW_Create_Date,
    CILF_Customer_Account_Dim_Id                AS Customer_Account_Dim_Id,
    CILF_Product_Dim_Id                         AS Product_Dim_Id,
    CILF_Fiscal_Period_Dim_Id                   AS Fiscal_Period_Dim_Id,
    CILF_Legal_Entity_Dim_Id                    AS Legal_Entity_Dim_Id,
    CILF_Covered_Product_Dim_Id                 AS Covered_Product_Dim_Id,
    CILF_Order_Dim_Id                           AS Order_Dim_Id,
    CILF_Order_Line_Dim_Id                      AS Order_Line_Dim_Id,
    CILF_Quote_Dim_Id                           AS Quote_Dim_Id,
    CILF_Quote_Line_Dim_Id                      AS Quote_Line_Dim_Id,
    CILF_Contract_Line_Dim_Id                   AS Contract_Line_Dim_Id,
    CILF_Transaction_Currency_Code              AS Transaction_Currency_Code,
    CILF_Accounting_Currency_Code               AS Accounting_Currency_Code,
    CILF_Reporting_Currency_Code                AS Reporting_Currency_Code,
    CILF_Unit_of_Measure_Dim_Id                 AS Unit_of_Measure_Dim_Id,
    CILF_Payment_Term_Dim_Id                    AS Payment_Term_Dim_Id,
    CILF_Payment_Specification_Dim_Id           AS Payment_Specification_Dim_Id,
    CILF_Payment_Method_Dim_Id                  AS Payment_Method_Dim_Id,
    CILF_Invoice_Payment_Status_Dim_Id          AS Invoice_Payment_Status_Dim_Id,
    CILF_Invoice_Status_Dim_Id                  AS Invoice_Status_Dim_Id,
    CILF_Ledger_Transaction_Type_Dim_ID         AS Ledger_Transaction_Type_Dim_ID,
    CILF_Invoice_Document_Date_Dim_Id           AS Invoice_Document_Date_Dim_Id,
    CILF_Delivery_Address_Dim_Id                AS Delivery_Address_Dim_Id,
    CILF_Transaction_Date_Dim_Id                AS Transaction_Date_Dim_Id,
    CILF_Invoice_Due_Date_Dim_Id                AS Invoice_Due_Date_Dim_Id,
    CILF_Document_Image_URL                     AS Document_Image_URL,
    CILF_Voucher_Num                            AS Voucher_Num,
    CILF_Invoice_Num                            AS Invoice_Num,
    CILF_Invoice_Line_Num                       AS Invoice_Line_Num,
    CILF_Invoice_Line_Desc                      AS Invoice_Line_Desc,
    CILF_PO_Num                                 AS PO_Num,
    CILF_Src_Invoice_Num                        AS Src_Invoice_Num,
    CILF_Project_Num                            AS Project_Num,
    CILF_Journal_Num                            AS Journal_Num,
    CILF_Invoice_Active_Ind                     AS Invoice_Active_Ind,
    CILF_Invoice_Posted_Ind                     AS Invoice_Posted_Ind,
    CILF_Total_Invoice_Amt_TC                   AS Total_Invoice_Amt_TC,
    CILF_Total_Invoice_Amt_USD                  AS Total_Invoice_Amt_USD,
    CILF_Invoice_Ln_Qty                         AS Invoice_Ln_Qty,
    CILF_Invoice_Ln_Amt_TC                      AS Invoice_Ln_Amt_TC,
    CILF_Invoice_Ln_Tax_Amt_TC                  AS Invoice_Ln_Tax_Amt_TC,
    CILF_Settlement_Amt_TC                      AS Settlement_Amt_TC,
    HASH(   Cus_Trans_Num,      
            Cus_Trans_Ln_Num,       
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
        )                                                                  AS Rec_EDW_Hash,
    'I'                                                                    AS ETL_Ins_Upd_Flag    
    FROM CTE_INVOICE_UNION;



-- Updating the Insert_Update_Flag in Staging Zone to identify the records to be inserted or updated into EDW Zone

-- Classifying records in Staging layer
-- ETL_Ins_Upd_Flag = 'I' : Insert (default)
-- ETL_Ins_Upd_Flag = 'U' : Update 
-- ETL_Ins_Upd_Flag = 'Ignore' : No Operation

MERGE INTO stg.{p_table_name} stg 
USING edw.{p_table_name} fact ON stg.Rec_Src_Key = fact.Rec_Src_Key
  AND stg.Rec_Src_Application_Id = fact.Rec_Src_Application_Id
  WHEN MATCHED THEN
UPDATE
  SET  stg.ETL_Ins_Upd_Flag = CASE WHEN stg.Rec_EDW_Hash <> fact.Rec_EDW_Hash THEN 'U' ELSE 'Ignore' END;








