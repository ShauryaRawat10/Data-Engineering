-- ********** Version History  *******************************************************
-- US9858  - 2024-06-18 - Shaurya Rawat / Kerri - Change Dim to source from Salesforce and OEBS directly
-- US10250 - 2024-06-22 - KerriB - Added Prod_Line_Item_Cat
-- US11086 - 2024-07-23 - Shaurya Rawat - Unmastering Seasonal, Rental and SaaS Products
-- US11907 - 2024-09-24 - Shaurya Rawat - Unmastering Hosting and Professional Services, Added Prod_Legacy_Name

-- ***********************************************************************************
MERGE INTO edw.{p_table_name} target
USING 
    ( 
	SELECT    
        stg.Rec_Src_Key AS MergeKey,
        stg.Rec_Process_Log_Id,
        stg.Rec_Src_Application_Id
        ,stg.Rec_Src_Key
        ,stg.Rec_Src_Create_Date
        ,stg.Rec_Src_Create_User
        ,stg.Rec_Src_Update_Date
        ,stg.Rec_Src_Update_User
        ,stg.Rec_EDW_Hash
        ,stg.Rec_EDW_Create_Date
        ,stg.Rec_EDW_Update_Date
        ,stg.Rec_EDW_Effective_Start_Date
        ,stg.Rec_EDW_Effective_End_Date
        ,stg.Rec_EDW_Current_Ind
        ,stg.Prod_Name    
        ,stg.Prod_Legacy_Item_Num
        ,stg.Prod_Legacy_Inventory_Item_Num
        ,stg.Prod_Legacy_Org_Num
        ,stg.Prod_Item_Num
        ,stg.Prod_Ext_ID
        ,stg.Prod_Cat
        ,stg.Prod_Family
        ,stg.Prod_Hosting_Component_Ind
        ,stg.Prod_PSA_Contract_Type
        ,stg.Prod_Cus_Asset_Ind
        ,stg.Prod_Billing_Freq
        ,stg.Prod_UOM
        ,stg.Prod_Active_Ind
        ,stg.Prod_Subinventory
        ,stg.Prod_Solution
        ,stg.Prod_Solution_Cat_1
        ,stg.Prod_Solution_Cat_2
        ,stg.Prod_Type
        ,stg.Prod_Strategic_Offering
        ,stg.Prod_Core_Class
        ,stg.Prod_Sales_Rpt_Bus_Grp
        ,stg.Prod_Sales_Rpt_Bus_Cat
        ,stg.Prod_Sales_Rpt_Prod_Cat
        ,stg.Prod_Sales_Rpt_Cat
        ,stg.Prod_Line_Cat
        ,stg.Prod_Line_Bus_Cat
        ,stg.Prod_LOB
        ,stg.Prod_Line_Bus_Line
        ,stg.Prod_Line_Features
        ,stg.Prod_VSOE_Cat
        ,stg.Prod_Version_Cat 
        ,stg.Prod_Tax_Cat
        ,stg.Prod_License_Type
        ,stg.Prod_Price_Type
        ,stg.Prod_Pricing_Method
        ,stg.Prod_Subscription_Pricing_Type
        ,stg.Prod_Subscription_Type
        ,stg.Prod_Cloud_Context
        ,stg.Prod_Percent_of_Total_Include_Ind 
        ,stg.Prod_Charge_Type
        ,stg.Prod_Billing_Type
        ,stg.Prod_SMB_Billing_Item_Num
        ,stg.Prod_Enterprise_Billing_Item_Num
        ,stg.Prod_Usage_Billing_Item_Num
        ,stg.Prod_Billing_Role
        ,stg.Prod_Bundle_Type
        ,stg.Prod_Separate_Orders_Ind
        ,stg.Prod_HLE_Type
        ,stg.Prod_HLE_Desc
        ,stg.Prod_HLE_Value
        ,stg.Prod_Svc_Product_Family
        ,stg.Prod_Svc_Product_Line
        ,stg.Prod_Svc_Product_Domain
        ,stg.Prod_Svc_Product_Type
        ,stg.Prod_License_Method_Value
        ,stg.Prod_Status
        ,stg.Prod_Item_Type
        ,stg.Prod_Legacy_UOM
        ,stg.Prod_Inv_Org
        ,stg.Prod_Subinventory_Code
        ,stg.Prod_Form_Product_Ind
        ,stg.Prod_Solution_Detail
        ,stg.Prod_Line_Item_Cat
        ,stg.Prod_Revenue_Product_Desc
        ,stg.ETL_Ins_Upd_Flag
      FROM stg.{p_table_name} AS stg   
        WHERE ETL_Ins_Upd_Flag = 'U'
UNION ALL
    SELECT 
        NULL AS MergeKey
        ,stg.Rec_Process_Log_Id
        ,stg.Rec_Src_Application_Id
        ,stg.Rec_Src_Key
        ,stg.Rec_Src_Create_Date
        ,stg.Rec_Src_Create_User
        ,stg.Rec_Src_Update_Date
        ,stg.Rec_Src_Update_User
        ,stg.Rec_EDW_Hash
        ,stg.Rec_EDW_Create_Date
        ,stg.Rec_EDW_Update_Date
        ,stg.Rec_EDW_Effective_Start_Date
        ,stg.Rec_EDW_Effective_End_Date
        ,stg.Rec_EDW_Current_Ind
        ,stg.Prod_Name
        ,stg.Prod_Legacy_Item_Num
        ,stg.Prod_Legacy_Inventory_Item_Num
        ,stg.Prod_Legacy_Org_Num
        ,stg.Prod_Item_Num
        ,stg.Prod_Ext_ID
        ,stg.Prod_Cat
        ,stg.Prod_Family
        ,stg.Prod_Hosting_Component_Ind
        ,stg.Prod_PSA_Contract_Type
        ,stg.Prod_Cus_Asset_Ind
        ,stg.Prod_Billing_Freq
        ,stg.Prod_UOM
        ,stg.Prod_Active_Ind
        ,stg.Prod_Subinventory
        ,stg.Prod_Solution
        ,stg.Prod_Solution_Cat_1
        ,stg.Prod_Solution_Cat_2
        ,stg.Prod_Type
        ,stg.Prod_Strategic_Offering
        ,stg.Prod_Core_Class
        ,stg.Prod_Sales_Rpt_Bus_Grp
        ,stg.Prod_Sales_Rpt_Bus_Cat
        ,stg.Prod_Sales_Rpt_Prod_Cat
        ,stg.Prod_Sales_Rpt_Cat
        ,stg.Prod_Line_Cat
        ,stg.Prod_Line_Bus_Cat
        ,stg.Prod_LOB
        ,stg.Prod_Line_Bus_Line
        ,stg.Prod_Line_Features
        ,stg.Prod_VSOE_Cat
        ,stg.Prod_Version_Cat
        ,stg.Prod_Tax_Cat
        ,stg.Prod_License_Type
        ,stg.Prod_Price_Type
        ,stg.Prod_Pricing_Method
        ,stg.Prod_Subscription_Pricing_Type
        ,stg.Prod_Subscription_Type
        ,stg.Prod_Cloud_Context
        ,stg.Prod_Percent_of_Total_Include_Ind
        ,stg.Prod_Charge_Type
        ,stg.Prod_Billing_Type
        ,stg.Prod_SMB_Billing_Item_Num
        ,stg.Prod_Enterprise_Billing_Item_Num
        ,stg.Prod_Usage_Billing_Item_Num
        ,stg.Prod_Billing_Role
        ,stg.Prod_Bundle_Type
        ,stg.Prod_Separate_Orders_Ind
        ,stg.Prod_HLE_Type
        ,stg.Prod_HLE_Desc
        ,stg.Prod_HLE_Value
        ,stg.Prod_Svc_Product_Family
        ,stg.Prod_Svc_Product_Line
        ,stg.Prod_Svc_Product_Domain
        ,stg.Prod_Svc_Product_Type
         ,stg.Prod_License_Method_Value
        ,stg.Prod_Status
        ,stg.Prod_Item_Type
        ,stg.Prod_Legacy_UOM
        ,stg.Prod_Inv_Org
        ,stg.Prod_Subinventory_Code
        ,stg.Prod_Form_Product_Ind
        ,stg.Prod_Solution_Detail
        ,stg.Prod_Line_Item_Cat
        ,stg.Prod_Revenue_Product_Desc      
        ,stg.ETL_Ins_Upd_Flag
    FROM stg.{p_table_name} AS stg 
    WHERE ETL_Ins_Upd_Flag IN ('I', 'U')           
) AS source
ON target.Rec_Src_Key = source.MergeKey
AND target.Rec_Src_Application_Id = source.Rec_Src_Application_Id
AND target.Rec_EDW_Current_Ind = 1

WHEN MATCHED THEN 
UPDATE SET 
    target.Rec_EDW_Update_Date = source.Rec_EDW_Update_Date,
    target.Rec_EDW_Effective_End_Date = source.Rec_EDW_Effective_Start_Date,
    target.Rec_EDW_Current_Ind = 0

WHEN NOT MATCHED THEN 
INSERT
	(    
     target.Rec_Process_Log_Id
    ,target.Rec_Src_Application_Id
    ,target.Rec_Src_Key
    ,target.Rec_Src_Create_Date
    ,target.Rec_Src_Create_User
    ,target.Rec_Src_Update_Date
    ,target.Rec_Src_Update_User
    ,target.Rec_EDW_Hash
    ,target.Rec_EDW_Create_Date
    ,target.Rec_EDW_Update_Date
    ,target.Rec_EDW_Effective_Start_Date
    ,target.Rec_EDW_Effective_End_Date
    ,target.Rec_EDW_Current_Ind
    ,target.Prod_Name
    ,target.Prod_Legacy_Item_Num
    ,target.Prod_Legacy_Inventory_Item_Num
    ,target.Prod_Legacy_Org_Num
    ,target.Prod_Item_Num
    ,target.Prod_Ext_ID
    ,target.Prod_Cat
    ,target.Prod_Family
    ,target.Prod_Hosting_Component_Ind
    ,target.Prod_PSA_Contract_Type
    ,target.Prod_Cus_Asset_Ind
    ,target.Prod_Billing_Freq
    ,target.Prod_UOM
    ,target.Prod_Active_Ind
    ,target.Prod_Subinventory
    ,target.Prod_Solution
    ,target.Prod_Solution_Cat_1
    ,target.Prod_Solution_Cat_2
    ,target.Prod_Type
    ,target.Prod_Strategic_Offering
    ,target.Prod_Core_Class    
    ,target.Prod_Sales_Rpt_Bus_Grp
    ,target.Prod_Sales_Rpt_Bus_Cat
    ,target.Prod_Sales_Rpt_Prod_Cat
    ,target.Prod_Sales_Rpt_Cat
    ,target.Prod_Line_Cat
    ,target.Prod_Line_Bus_Cat
    ,target.Prod_LOB
    ,target.Prod_Line_Bus_Line
    ,target.Prod_Line_Features
    ,target.Prod_VSOE_Cat
    ,target.Prod_Version_Cat
    ,target.Prod_Tax_Cat
    ,target.Prod_License_Type
    ,target.Prod_Price_Type
    ,target.Prod_Pricing_Method
    ,target.Prod_Subscription_Pricing_Type
    ,target.Prod_Subscription_Type
    ,target.Prod_Cloud_Context
    ,target.Prod_Percent_of_Total_Include_Ind
    ,target.Prod_Charge_Type
    ,target.Prod_Billing_Type
    ,target.Prod_SMB_Billing_Item_Num
    ,target.Prod_Enterprise_Billing_Item_Num
    ,target.Prod_Usage_Billing_Item_Num
    ,target.Prod_Billing_Role
    ,target.Prod_Bundle_Type
    ,target.Prod_Separate_Orders_Ind
    ,target.Prod_HLE_Type
    ,target.Prod_HLE_Desc
    ,target.Prod_HLE_Value
    ,target.Prod_Svc_Product_Family
    ,target.Prod_Svc_Product_Line
    ,target.Prod_Svc_Product_Domain
    ,target.Prod_Svc_Product_Type
    ,target.Prod_License_Method_Value
    ,target.Prod_Status
    ,target.Prod_Item_Type
    ,target.Prod_Legacy_UOM
    ,target.Prod_Inv_Org
    ,target.Prod_Subinventory_Code
    ,target.Prod_Form_Product_Ind
    ,target.Prod_Solution_Detail
    ,target.Prod_Line_Item_Cat
    ,target.Prod_Revenue_Product_Desc          
    )
VALUES
    (source.Rec_Process_Log_Id
    ,source.Rec_Src_Application_Id
    ,source.Rec_Src_Key
    ,source.Rec_Src_Create_Date
    ,source.Rec_Src_Create_User
    ,source.Rec_Src_Update_Date
    ,source.Rec_Src_Update_User
    ,source.Rec_EDW_Hash
    ,source.Rec_EDW_Create_Date
    ,source.Rec_EDW_Update_Date
    ,source.Rec_EDW_Effective_Start_Date
    ,source.Rec_EDW_Effective_End_Date
    ,source.Rec_EDW_Current_Ind
    ,source.Prod_Name
    ,source.Prod_Legacy_Item_Num
    ,source.Prod_Legacy_Inventory_Item_Num
    ,source.Prod_Legacy_Org_Num
    ,source.Prod_Item_Num
    ,source.Prod_Ext_ID
    ,source.Prod_Cat
    ,source.Prod_Family
    ,source.Prod_Hosting_Component_Ind
    ,source.Prod_PSA_Contract_Type
    ,source.Prod_Cus_Asset_Ind
    ,source.Prod_Billing_Freq
    ,source.Prod_UOM
    ,source.Prod_Active_Ind
    ,source.Prod_Subinventory
    ,source.Prod_Solution
    ,source.Prod_Solution_Cat_1
    ,source.Prod_Solution_Cat_2
    ,source.Prod_Type
    ,source.Prod_Strategic_Offering
    ,source.Prod_Core_Class
    ,source.Prod_Sales_Rpt_Bus_Grp
    ,source.Prod_Sales_Rpt_Bus_Cat
    ,source.Prod_Sales_Rpt_Prod_Cat
    ,source.Prod_Sales_Rpt_Cat
    ,source.Prod_Line_Cat
    ,source.Prod_Line_Bus_Cat
    ,source.Prod_LOB
    ,source.Prod_Line_Bus_Line
    ,source.Prod_Line_Features
    ,source.Prod_VSOE_Cat
    ,source.Prod_Version_Cat
    ,source.Prod_Tax_Cat
    ,source.Prod_License_Type
    ,source.Prod_Price_Type
    ,source.Prod_Pricing_Method
    ,source.Prod_Subscription_Pricing_Type
    ,source.Prod_Subscription_Type
    ,source.Prod_Cloud_Context
    ,source.Prod_Percent_of_Total_Include_Ind
    ,source.Prod_Charge_Type
    ,source.Prod_Billing_Type
    ,source.Prod_SMB_Billing_Item_Num
    ,source.Prod_Enterprise_Billing_Item_Num
    ,source.Prod_Usage_Billing_Item_Num
    ,source.Prod_Billing_Role
    ,source.Prod_Bundle_Type
    ,source.Prod_Separate_Orders_Ind
    ,source.Prod_HLE_Type
    ,source.Prod_HLE_Desc
    ,source.Prod_HLE_Value
    ,source.Prod_Svc_Product_Family
    ,source.Prod_Svc_Product_Line
    ,source.Prod_Svc_Product_Domain
    ,source.Prod_Svc_Product_Type
    ,source.Prod_License_Method_Value
    ,source.Prod_Status
    ,source.Prod_Item_Type
    ,source.Prod_Legacy_UOM
    ,source.Prod_Inv_Org
    ,source.Prod_Subinventory_Code
    ,source.Prod_Form_Product_Ind
    ,source.Prod_Solution_Detail
    ,source.Prod_Line_Item_Cat
    ,source.Prod_Revenue_Product_Desc         
);

-- Special Handling Code - -- Update end date to max start date for the legacy inventory ID & inactivate		
-- Inactivate existing oracle product for a late arriving sf product to ensure no dupicate products due to different originating sources
-- Exclude SF-OEBS unmastering for Hosting and Professional Services (using inventory_id)
MERGE INTO edw.{p_table_name}  AS dim
  USING (
          select Prod_Item_Num,  MAX(Rec_Src_Application_Id) as maxApplId, MIN(Rec_Src_Application_Id) as minApplId, 
          COUNT(CASE WHEN Prod_Legacy_Inventory_Item_Num != -1 THEN Prod_Legacy_Inventory_Item_Num END) AS distinctInventoryItemCount
          from edw.{p_table_name}
          where Rec_EDW_Current_Ind = 1
            and Prod_Item_Num is not null
          group by Prod_Item_Num
          having count(Prod_Item_Num) > 1 and COUNT(CASE WHEN Prod_Legacy_Inventory_Item_Num != -1 THEN Prod_Legacy_Inventory_Item_Num END) > 1          -- more than 1 product item num 
            and maxApplId = 5 and minApplId = 1   -- ensures we get products that have both an ORA-only(105) & SF rows (SF can resuse the same product item number)  
  ) AS src
    ON (dim.Prod_Item_Num=src.Prod_Item_Num
    AND dim.Rec_Src_Key LIKE '%_105' -- ORA only  - make sure it's an ORA only row
    AND dim.Rec_Src_Application_Id = 1
    AND dim.Rec_EDW_Current_Ind = 1)

  WHEN MATCHED THEN 
  UPDATE SET 
    dim.Rec_EDW_Current_Ind = 0,
    dim.Rec_EDW_Effective_End_Date =   -- update the end date with the current max update date (now)
        (Select max(x.Rec_EDW_Update_Date) 
           from edw.{p_table_name} x
           where Rec_EDW_Current_Ind = 1) ; 



UPDATE stg.{p_table_name}  stg
SET stg.ETL_Ins_Upd_Flag = concat(stg.ETL_Ins_Upd_Flag, ' - Processed');