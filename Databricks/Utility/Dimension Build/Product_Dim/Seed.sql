MERGE INTO edw.Product_Dim AS tgt 
USING (
      SELECT 
          -1 AS Product_Dim_Id,
          -1 AS Rec_Process_Log_Id,
          etl.udf_get_application_id('Salesforce','UKG') AS Rec_Src_Application_Id,  
          '-1' as Rec_Src_Key,
          '1900-01-01' AS Rec_EDW_Create_Date,
          '1900-01-01' AS Rec_EDW_Update_Date,
          '1900-01-01' AS Rec_EDW_Effective_Start_Date,
          '1900-01-01' AS Rec_EDW_Effective_End_Date,
          0 AS Rec_EDW_Current_Ind,
          'N/A' AS Prod_Legacy_Org_Num,
          0 AS Prod_Hosting_Component_Ind,
          0 AS Prod_Cus_Asset_Ind,
          0 AS Prod_Active_Ind,
          0 AS Prod_Separate_Orders_Ind,
          0 AS Prod_Form_Product_Ind,
          0 AS Prod_Percent_of_Total_Include_Ind
      UNION ALL 
          SELECT 
          -2 AS Product_Dim_Id,
          -2 AS Rec_Process_Log_Id,
          etl.udf_get_application_id('Salesforce','UKG') AS Rec_Src_Application_Id,  
          '-2' as Rec_Src_Key,
          '1900-01-01' AS Rec_EDW_Create_Date,
          '1900-01-01' AS Rec_EDW_Update_Date,
          '1900-01-01' AS Rec_EDW_Effective_Start_Date,
          '1900-01-01' AS Rec_EDW_Effective_End_Date,
          0 AS Rec_EDW_Current_Ind,
          'N/A' AS Prod_Legacy_Org_Num,
          0 AS Prod_Hosting_Component_Ind,
          0 AS Prod_Cus_Asset_Ind,
          0 AS Prod_Active_Ind,
          0 AS Prod_Separate_Orders_Ind,
          0 AS Prod_Form_Product_Ind,
          0 AS Prod_Percent_of_Total_Include_Ind
    ) AS src 
    ON (tgt.Product_Dim_Id = src.Product_Dim_Id 
    AND tgt.Rec_Src_Key = src.Rec_Src_Key)
WHEN NOT MATCHED 
THEN 
  INSERT (Product_Dim_Id,
          Rec_Process_Log_Id,
          Rec_Src_Application_Id,
          Rec_Src_Key,
          Rec_EDW_Create_Date,
          Rec_EDW_Update_Date,
          Rec_EDW_Effective_Start_Date,
          Rec_EDW_Effective_End_Date,
          Rec_EDW_Current_Ind,
          Prod_Legacy_Org_Num,
          Prod_Hosting_Component_Ind,
          Prod_Cus_Asset_Ind,
          Prod_Active_Ind,
          Prod_Separate_Orders_Ind,
          Prod_Form_Product_Ind,
          Prod_Percent_of_Total_Include_Ind
         ) 
  VALUES (src.Product_Dim_Id,
          src.Rec_Process_Log_Id,
          src.Rec_Src_Application_Id,
          src.Rec_Src_Key,
          src.Rec_EDW_Create_Date,
          src.Rec_EDW_Update_Date,
          src.Rec_EDW_Effective_Start_Date,
          src.Rec_EDW_Effective_End_Date,
          src.Rec_EDW_Current_Ind,
          src.Prod_Legacy_Org_Num,
          src.Prod_Hosting_Component_Ind,
          src.Prod_Cus_Asset_Ind,
          src.Prod_Active_Ind,
          src.Prod_Separate_Orders_Ind,
          src.Prod_Form_Product_Ind,
          src.Prod_Percent_of_Total_Include_Ind
          );