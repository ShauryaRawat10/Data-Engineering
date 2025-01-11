-- ********** Version History  *******************************************************
-- US9858  - 2024-06-18 - Shaurya Rawat / Kerri - Change Dim to source from Salesforce and OEBS directly
-- US10250 - 2024-06-22 - KerriB - Added Prod_Line_Item_Cat
-- US11086 - 2024-07-23 - Shaurya Rawat - Unmastering Seasonal, Rental and SaaS Products
-- US11907 - 2024-09-24 - Shaurya Rawat - Unmastering Hosting and Professional Services, Added Prod_Legacy_Name

-- ***********************************************************************************
-- OEBS Item Category Set IDs and their meaning:
-- 1100000001 (Product Line)
-- 1100000002 (Sales Reporting)
-- 1100000003 (Tax)
-- 1100000004 (Version)
-- 1100000145 (Strategic Offering)
-- 1100000146 (Product Type)
-- 1100000147 (Functional Category 1)
-- 1100000148 (Functional Category 2)

INSERT OVERWRITE stg.{p_table_name} BY NAME
SELECT * 
FROM
(
  -- Build Item Category CTE to easily obtain cateory values for OBES items
  WITH OEBS_ITEM_CATEGORY_CTE AS (
    SELECT
    mic.INVENTORY_ITEM_ID,
    mic.ORGANIZATION_ID,
    mic.CATEGORY_SET_ID,
    mic.CATEGORY_ID,
    mcs.CATEGORY_SET_NAME,
    CONCAT_WS(' ', mc.segment1, mc.segment2, mc.segment3, mc.segment4, mc.segment5, mc.segment6, mc.segment7) AS CATEGORY_VALUE,
    mc.SEGMENT1,
    mc.SEGMENT2,
    mc.SEGMENT3,
    mc.SEGMENT4,
    mc.SEGMENT5,
    mc.SEGMENT6,
    mc.SEGMENT7,
    mc.SEGMENT8,
    GREATEST(mic.DL_effectiveDate,mcs.DL_effectiveDate,mc.DL_effectiveDate) AS ItemCatg_DL_effectiveDate
    FROM raw_oebs_kronos.mtl_item_categories mic
    LEFT OUTER JOIN raw_oebs_kronos.mtl_category_sets mcs  
        ON mcs.category_set_id=mic.category_set_id
      AND mcs.DL_current_flag = 1
    LEFT OUTER JOIN raw_oebs_kronos.mtl_categories_b mc 
        ON mc.category_id = mic.category_id
      AND mc.DL_current_flag = 1 
    WHERE mic.DL_current_flag = 1
      AND mic.LAST_UPDATE_DATE = (   
          SELECT MAX(x.LAST_UPDATE_DATE)
          FROM  raw_oebs_kronos.mtl_item_categories x
          WHERE x.DL_current_flag = 1
            AND x.INVENTORY_ITEM_ID=mic.INVENTORY_ITEM_ID 
            AND x.ORGANIZATION_ID=mic.ORGANIZATION_ID 
            AND x.CATEGORY_SET_ID=mic.CATEGORY_SET_ID 
          )
  )

  -- build SF source data 
  , SF_Product_CTE AS (
        SELECT
        SF.Id,
        SF.CreatedDate,
        SF.CreatedById,
        SF.LastModifiedDate,
        SF.LastModifiedById,
        SF.Name,
        SF.Oracle_Billing_Item_Number__c,
        SF.ProductCode,
        SF.ExternalId__c,
        SF.Product_Category__c,
        case when lower(SF.Family) like 'professional services%' then 'Professional Services' 
        when lower(SF.Family) like 'hosting%' then 'Hosting' 
        else REPLACE(trim(SF.Family), CHAR(160), '') end AS Family,
        SF.Hosting_Component__c,
        SF.PSA_Contract_Type__c,
        SF.CA__c,
        SF.SBQQ__BillingFrequency__c,
        SF.Unit_of_Measure__c,
        SF.IsActive,
        SF.Subinventory__c,
        SF.Platform__c,
        SF.Solution_category__c,
        SF.functional_category_2__c,
        SF.Product_Type__c,
        SF.strategic_Offering__c,
        SF.Core_Indicator__c,
        SF.Sales_Reporting_Cat_Segment_3__c,
        SF.Sales_Reporting_Cat_Segment_2__c,
        SF.Sales_Reporting_Category__c,
        SF.Product_Line_Category__c,
        SF.Product_Line_Category_Segment3__c,
        SF.Product_Line_Category_Segment4__c,
        SF.Product_Line_Category_Segment5__c,
        SF.Product_Line_Category_Segment1__c,
        SF.VSOE_Category__c,
        SF.Version_Category__c,
        SF.Tax_Category__c,
        SF.License_Type__c,
        SF.Price_Type__c,
        SF.SBQQ__PricingMethod__c,
        SF.SBQQ__SubscriptionPricing__c,
        SF.SBQQ__SubscriptionType__c,
        SF.Cloud_Context__c,
        SF.SBQQ__IncludeInMaintenance__c,
        SF.SBQQ__ChargeType__c,
        SF.SBQQ__BillingType__c,
        SF.Oracle_Billing_Item_Number__c,
        SF.Oracle_Billing_Item_Number_Enterprise__c,
        SF.Oracle_Billing_Item_Number_Usage__c,
        SF.Billing_Role__c,
        SF.Bundle_Type__c,
        SF.Separate_Orders__c,
        SF.HLE_Type__c,
        SF.HLE_Entitlement_Description__c,
        SF.HLE_Value__c,
        SF.Service_Product_Line__c,
        SF.Service_Product_Domain__c,
        SF.Service_Product_Type__c,
        SF.License_Method_Value__c,
        SF.Status__c,
        SF.Item_Type__c,
        SF.inventory_Org__c,
        SF.Subinventory_code__c,
        SF.Product_Form_Product__c,
        SF.Solution_Detail__c,
        SF.DL_effectiveDate,
        SF.DL_current_flag
    FROM raw_salesforce_ukg.product2 SF
    WHERE SF.DL_current_flag = 1
  )

  -- build the sf matching to use in OEBS source data CTE
  , SF_matching_CTE AS (
      SELECT ProductCode,
      max(DL_effectiveDate) as DL_effectiveDate
      FROM SF_Product_CTE
      WHERE DL_current_flag = 1
      GROUP BY ProductCode 
    )

  -- build OEBS source data with SF match flag
  , OEBS_Products_CTE AS (
      SELECT
      MSI.CREATION_DATE, 
      MSI.LAST_UPDATE_DATE,
      MSI.CREATED_BY,
      MSI.LAST_UPDATED_BY,
      trim(replace(replace(MSI.DESCRIPTION, char(13), ''), char(10), '')) AS DESCRIPTION,
      CAST(MSI.INVENTORY_ITEM_ID AS INT) AS INVENTORY_ITEM_ID,
      CAST(MSI.ORGANIZATION_ID AS INT) AS ORGANIZATION_ID,
      MSI.INVENTORY_ITEM_STATUS_CODE,
      MSI.ITEM_TYPE,
      MSI.PRIMARY_UOM_CODE,  
      MSI.SEGMENT1             AS OEBS_Item_Num,
      PROD_LINE.SEGMENT1       AS Prod_Line_Seg1,  
      PROD_LINE.SEGMENT3       AS Prod_Line_Seg3,
      PROD_LINE.SEGMENT4       AS Prod_Line_Seg4,
      PROD_LINE.SEGMENT6       AS Prod_Line_Seg6,
      PROD_LINE.CATEGORY_VALUE AS Prod_Line_CatgVal, 
      SALES_RPT.SEGMENT2       AS Sales_Rpt_Seg2,
      SALES_RPT.SEGMENT3       AS Sales_Rpt_Seg3, 
      SALES_RPT.CATEGORY_VALUE AS Sales_Rpt_CatgVal, 
      PROD_TYPE.SEGMENT1       AS Prod_Type_Seg1, 
      FUNC_CAT1.SEGMENT1       AS Func_Cat1_Seg1,
      FUNC_CAT2.SEGMENT1       AS Func_Cat2_Seg1,
      STRAT_OFFER.SEGMENT1     AS Strat_Offer_Seg1,  
      VERS_CAT.SEGMENT1        AS Vers_Cat_Seg1,
      TAX.SEGMENT1             AS Tax_Seg1,
      GL.SEGMENT5              AS GL_Seg5,

      CASE WHEN SF.ProductCode=MSI.SEGMENT1 AND SF.ProductCode is not null THEN 1
        ELSE 0
        END                    AS SF_Matching_Ind,   -- identifies if the OEBS row can be matched to Salesforce

      MSI.DL_effectiveDate                    AS OEBS_DL_effectiveDate,
      PROD_LINE.ItemCatg_DL_effectiveDate     AS Prod_Line_DL_effectiveDate,
      SALES_RPT.ItemCatg_DL_effectiveDate     AS Sales_Rpt_DL_effectiveDate,
      PROD_TYPE.ItemCatg_DL_effectiveDate     AS Prod_Type_DL_effectiveDate,
      FUNC_CAT1.ItemCatg_DL_effectiveDate     AS Func_Cat1_DL_effectiveDate,
      FUNC_CAT2.ItemCatg_DL_effectiveDate     AS Func_Cat2_DL_effectiveDate,
      STRAT_OFFER.ItemCatg_DL_effectiveDate   AS Strat_Offer_DL_effectiveDate,
      VERS_CAT.ItemCatg_DL_effectiveDate      AS Vers_Cat_DL_effectiveDate,
      TAX.ItemCatg_DL_effectiveDate           AS Tax_DL_effectiveDate,
      GL.DL_effectiveDate                     AS OEBS_gl_DL_effectiveDate, 
      SF.DL_effectiveDate                     AS OEBS_SF_match_DL_effectiveDate

      FROM raw_oebs_kronos.mtl_system_items_b MSI
      LEFT OUTER JOIN 
        ( SELECT DISTINCT SEGMENT1, SEGMENT2, SEGMENT3, SEGMENT4, SEGMENT5, SEGMENT6, CATEGORY_VALUE, INVENTORY_ITEM_ID, ORGANIZATION_ID, ItemCatg_DL_effectiveDate 
          FROM OEBS_ITEM_CATEGORY_CTE WHERE CATEGORY_SET_ID = '1100000001'
        ) AS PROD_LINE
        ON MSI.INVENTORY_ITEM_ID = PROD_LINE.INVENTORY_ITEM_ID 
        AND MSI.ORGANIZATION_ID = PROD_LINE.ORGANIZATION_ID    
      LEFT OUTER JOIN 
        ( SELECT DISTINCT SEGMENT1, SEGMENT2, SEGMENT3, SEGMENT4, SEGMENT5, SEGMENT6, CATEGORY_VALUE, INVENTORY_ITEM_ID, ORGANIZATION_ID, ItemCatg_DL_effectiveDate 
          FROM OEBS_ITEM_CATEGORY_CTE WHERE CATEGORY_SET_ID = '1100000002'
        ) AS SALES_RPT
        ON MSI.INVENTORY_ITEM_ID = SALES_RPT.INVENTORY_ITEM_ID 
        AND MSI.ORGANIZATION_ID = SALES_RPT.ORGANIZATION_ID
      LEFT OUTER JOIN 
        ( SELECT DISTINCT SEGMENT1, INVENTORY_ITEM_ID, ORGANIZATION_ID, CATEGORY_SET_ID, ItemCatg_DL_effectiveDate 
          FROM OEBS_ITEM_CATEGORY_CTE WHERE CATEGORY_SET_ID = '1100000003'
        ) AS TAX
        ON MSI.INVENTORY_ITEM_ID = TAX.INVENTORY_ITEM_ID 
        AND MSI.ORGANIZATION_ID = TAX.ORGANIZATION_ID
      LEFT OUTER JOIN 
        ( SELECT DISTINCT SEGMENT1, INVENTORY_ITEM_ID, ORGANIZATION_ID, ItemCatg_DL_effectiveDate 
          FROM OEBS_ITEM_CATEGORY_CTE WHERE CATEGORY_SET_ID = '1100000004'
        ) AS VERS_CAT
        ON MSI.INVENTORY_ITEM_ID = VERS_CAT.INVENTORY_ITEM_ID 
        AND MSI.ORGANIZATION_ID = VERS_CAT.ORGANIZATION_ID
      LEFT OUTER JOIN 
        ( SELECT DISTINCT SEGMENT1, INVENTORY_ITEM_ID, ORGANIZATION_ID, ItemCatg_DL_effectiveDate 
          FROM OEBS_ITEM_CATEGORY_CTE WHERE CATEGORY_SET_ID = '1100000145'
        ) AS STRAT_OFFER
        ON MSI.INVENTORY_ITEM_ID = STRAT_OFFER.INVENTORY_ITEM_ID 
        AND MSI.ORGANIZATION_ID = STRAT_OFFER.ORGANIZATION_ID 
      LEFT OUTER JOIN 
        ( SELECT DISTINCT SEGMENT1, INVENTORY_ITEM_ID, ORGANIZATION_ID, ItemCatg_DL_effectiveDate 
          FROM OEBS_ITEM_CATEGORY_CTE WHERE CATEGORY_SET_ID = '1100000146'
        ) AS PROD_TYPE
        ON MSI.INVENTORY_ITEM_ID = PROD_TYPE.INVENTORY_ITEM_ID 
        AND MSI.ORGANIZATION_ID = PROD_TYPE.ORGANIZATION_ID    
      LEFT OUTER JOIN 
        ( SELECT DISTINCT SEGMENT1, INVENTORY_ITEM_ID, ORGANIZATION_ID, ItemCatg_DL_effectiveDate 
          FROM OEBS_ITEM_CATEGORY_CTE WHERE CATEGORY_SET_ID = '1100000147'
        ) FUNC_CAT1
        ON MSI.INVENTORY_ITEM_ID = FUNC_CAT1.INVENTORY_ITEM_ID 
        AND MSI.ORGANIZATION_ID = FUNC_CAT1.ORGANIZATION_ID
      LEFT OUTER JOIN 
        ( SELECT DISTINCT SEGMENT1, INVENTORY_ITEM_ID, ORGANIZATION_ID, ItemCatg_DL_effectiveDate 
          FROM OEBS_ITEM_CATEGORY_CTE WHERE CATEGORY_SET_ID = '1100000148'
        ) FUNC_CAT2
        ON MSI.INVENTORY_ITEM_ID = FUNC_CAT2.INVENTORY_ITEM_ID 
        AND MSI.ORGANIZATION_ID = FUNC_CAT2.ORGANIZATION_ID
      LEFT OUTER JOIN raw_oebs_kronos.gl_code_combinations_kfv GL
        ON GL.CODE_COMBINATION_ID = MSI.COST_OF_SALES_ACCOUNT
        AND GL.DL_current_flag = 1
      LEFT OUTER JOIN SF_matching_CTE SF 
        ON SF.ProductCode = MSI.SEGMENT1
      WHERE MSI.ORGANIZATION_ID = '105'   -- org id 105 is the default OEBS org that contains the base data we need for Oracle products
        AND MSI.DL_current_flag = 1  
    )
  
  -- Restrict salesforce to OEBS match for specific combination sets 
  -- ***************************************************************
  -- Restricted Oracle records 
  -- Restrict_Match_Ind = 1 (Seasonal, Rental, Saas) - Specific combination
  -- Restrict_Match_Ind = 2 (Hosting, Professional Services) + Description mismatch
  
   , OEBS_With_Restrict_Match AS (
      SELECT DISTINCT
        m1.Id AS Salesforce_ID,
        m1.family AS Salesforce_family,
        ORA.*,
        CASE
            WHEN ORA.OEBS_Item_Num is null then -1
            WHEN EXISTS (
                SELECT 1
                FROM SF_Product_CTE m2
                WHERE ORA.OEBS_Item_Num = m2.ProductCode 
                  AND m1.Id != m2.Id 
                  AND (
                        ( m1.Family = 'SaaS Application' AND m2.Family = 'Perpetual Software') OR
                        ( m1.Family = 'Hardware - Rental' AND m2.Family = 'Hardware - Purchase') OR
                        ( m1.Family = 'SaaS Application - Seasonal' AND m2.Family = 'SaaS Application')
                      )
            ) THEN 1
            WHEN m1.family = 'Hosting' and lower(m1.name) != lower(ORA.DESCRIPTION) THEN 2
            WHEN m1.family = 'Professional Services' and lower(m1.name) != lower(ORA.DESCRIPTION) THEN 2
            ELSE 0
        END AS Restrict_Match_Ind
      FROM 
        SF_Product_CTE m1
      FULL JOIN 
        OEBS_Products_CTE ORA 
      ON ORA.OEBS_Item_Num = m1.ProductCode
    )

  -- build CTE of the SF & SF/ORA matching UNION to ORA only rows
  ,CTE_Union AS (

  -- SF Only & SF/ORA matches  
  SELECT
    etl.udf_get_application_id('Salesforce','UKG')    AS Rec_Src_Application_Id, 
    SF.Id                                             AS Rec_Src_Key,
    SF.CreatedDate                                    AS Rec_Src_Create_Date,
    SF.CreatedById                                    AS Rec_Src_Create_User,
    SF.LastModifiedDate                               AS Rec_Src_Update_Date,
    SF.LastModifiedById                               AS Rec_Src_Update_User,

    IFNULL(SF.Name,ORA.DESCRIPTION)                   AS P_Name,
    IFNULL(SF.Oracle_Billing_Item_Number__c,ORA.OEBS_Item_Num) AS P_Legacy_Item_Num,
    ORA.INVENTORY_ITEM_ID                             AS P_Legacy_Inventory_Item_Num,
    ORA.ORGANIZATION_ID                               AS P_Legacy_Org_Num,
    IFNULL(SF.ProductCode,ORA.OEBS_Item_Num)          AS P_Item_Num,
    cast(SF.ExternalId__c as integer)                 AS P_Ext_ID,
    IFNULL(SF.Product_Category__c,ORA.Sales_Rpt_Seg3) AS P_Cat,
    SF.Family                                         AS P_Family,
    cast(SF.Hosting_Component__c as integer)          AS P_Hosting_Component_Ind,
    SF.PSA_Contract_Type__c                           AS P_PSA_Contract_Type,
    cast(SF.CA__c as integer)                         AS P_Cus_Asset_Ind,
    SF.SBQQ__BillingFrequency__c                      AS P_Billing_Freq,
    SF.Unit_of_Measure__c                             AS P_UOM,
    cast(SF.IsActive as integer)                      AS P_Active_Ind,  
    SF.Subinventory__c                                AS P_Subinventory,
    SF.Platform__c                                    AS P_Solution,
    IFNULL(SF.Solution_category__c,ORA.Func_Cat1_Seg1)              AS P_Solution_Cat_1,
    IFNULL(SF.functional_category_2__c,ORA.Func_Cat2_Seg1)          AS P_Solution_Cat_2,
    IFNULL(SF.Product_Type__c,ORA.Prod_Type_Seg1)                   AS P_Type,
    IFNULL(SF.strategic_Offering__c ,ORA.Strat_Offer_Seg1)          AS P_Strategic_Offering,
    SF.Core_Indicator__c                                            AS P_Core_Class,
    IFNULL(SF.Sales_Reporting_Cat_Segment_3__c,ORA.Sales_Rpt_Seg3)  AS P_Sales_Rpt_Bus_Grp,
    IFNULL(SF.Sales_Reporting_Cat_Segment_2__c,ORA.Sales_Rpt_Seg2)  AS P_Sales_Rpt_Bus_Cat,
    IFNULL(SF.Sales_Reporting_Cat_Segment_3__c,ORA.Sales_Rpt_Seg3)  AS P_Sales_Rpt_Prod_Cat,
    IFNULL(SF.Sales_Reporting_Category__c,ORA.Sales_Rpt_CatgVal)    AS P_Sales_Rpt_Cat,
    IFNULL(SF.Product_Line_Category__c,ORA.Prod_Line_CatgVal)       AS P_Line_Cat,
    IFNULL(SF.Product_Line_Category_Segment3__c,ORA.Prod_Line_Seg3) AS P_Line_Bus_Cat,
    IFNULL(SF.Product_Line_Category_Segment4__c,ORA.GL_Seg5)        AS P_LOB,
    IFNULL(SF.Product_Line_Category_Segment4__c,ORA.Prod_Line_Seg4) AS P_Line_Bus_Line,
    IFNULL(SF.Product_Line_Category_Segment5__c,ORA.Prod_Line_Seg6) AS P_Line_Features,
    IFNULL(SF.Product_Line_Category_Segment1__c, ORA.Prod_Line_Seg1) AS P_Line_Item_Cat,
    SF.VSOE_Category__c                               AS P_VSOE_Cat,
    IFNULL(SF.Version_Category__c,ORA.Vers_Cat_Seg1)  AS P_Version_Cat,
    IFNULL(SF.Tax_Category__c,ORA.Tax_Seg1)           AS P_Tax_Cat,
    SF.License_Type__c                                AS P_License_Type,
    SF.Price_Type__c                                  AS P_Price_Type,
    SF.SBQQ__PricingMethod__c                         AS P_Pricing_Method,
    SF.SBQQ__SubscriptionPricing__c                   AS P_Subscription_Pricing_Type,
    SF.SBQQ__SubscriptionType__c                      AS P_Subscription_Type,
    SF.Cloud_Context__c                               AS P_Cloud_Context,
    CAST(SF.SBQQ__IncludeInMaintenance__c as integer) AS P_Percent_of_Total_Include_Ind,
    SF.SBQQ__ChargeType__c                            AS P_Charge_Type,
    SF.SBQQ__BillingType__c                           AS P_Billing_Type,
    IFNULL(SF.Oracle_Billing_Item_Number__c,ORA.OEBS_Item_Num)            AS P_SMB_Billing_Item_Num,
    IFNULL(SF.Oracle_Billing_Item_Number_Enterprise__c,ORA.OEBS_Item_Num) AS P_Enterprise_Billing_Item_Num,
    IFNULL(SF.Oracle_Billing_Item_Number_Usage__c,ORA.OEBS_Item_Num)      AS P_Usage_Billing_Item_Num,
    SF.Billing_Role__c                                AS P_Billing_Role,
    SF.Bundle_Type__c                                 AS P_Bundle_Type,
    cast(SF.Separate_Orders__c as integer)            AS P_Separate_Orders_Ind,
    SF.HLE_Type__c                                    AS P_HLE_Type,
    SF.HLE_Entitlement_Description__c                 AS P_HLE_Desc,
    SF.HLE_Value__c                                   AS P_HLE_Value,
    SF.Family                                         AS P_Svc_Product_Family,
    SF.Service_Product_Line__c                        AS P_Svc_Product_Line,
    SF.Service_Product_Domain__c                            AS P_Svc_Product_Domain,
    SF.Service_Product_Type__c                              AS P_Svc_Product_Type,
    SF.License_Method_Value__c                              AS P_License_Method_Value,
    IFNULL(SF.Status__c,ORA.INVENTORY_ITEM_STATUS_CODE)     AS P_Status,
    IFNULL(SF.Item_Type__c,ORA.ITEM_TYPE)                   AS P_Item_Type,
    ORA.PRIMARY_UOM_CODE                                    AS P_Legacy_UOM,
    SF.inventory_Org__c                                     AS P_Inv_Org,
    SF.Subinventory_code__c                                 AS P_Subinventory_Code,
    cast(SF.Product_Form_Product__c as integer)             AS P_Form_Product_Ind,
    SF.Solution_Detail__c                                   AS P_Solution_Detail,
    ORA.DESCRIPTION                                         AS P_Revenue_Product_Desc,

    SF.DL_effectiveDate  AS SF_effectiveDate,
    ORA.OEBS_DL_effectiveDate,
    ORA.Prod_Line_DL_effectiveDate,
    ORA.Sales_Rpt_DL_effectiveDate,
    ORA.Prod_Type_DL_effectiveDate,
    ORA.Func_Cat1_DL_effectiveDate,
    ORA.Func_Cat2_DL_effectiveDate,
    ORA.Strat_Offer_DL_effectiveDate,
    ORA.Vers_Cat_DL_effectiveDate,
    ORA.Tax_DL_effectiveDate,
    ORA.OEBS_gl_DL_effectiveDate, 
    ORA.OEBS_SF_match_DL_effectiveDate

    FROM SF_Product_CTE SF
    LEFT JOIN 
    (
      SELECT * FROM OEBS_With_Restrict_Match where Restrict_Match_Ind = 0
    ) ORA
    ON ORA.OEBS_Item_Num=SF.ProductCode and ORA.Salesforce_ID = SF.Id
    WHERE SF.DL_current_flag = 1

  UNION

  -- ORA Only Rows
  SELECT
    etl.udf_get_application_id('OEBS','Kronos')       AS Rec_Src_Application_Id, 
    concat(ORA.OEBS_Item_Num,'_',ORA.ORGANIZATION_ID) AS Rec_Src_Key,
    ORA.CREATION_DATE       AS Rec_Src_Create_Date,
    Replace(ORA.CREATED_BY, '.0', '' ) AS Rec_Src_Create_User,
    ORA.LAST_UPDATE_DATE    AS Rec_Src_Update_Date,
    'N/A'                   AS Rec_Src_Update_User,
    ORA.DESCRIPTION         AS P_Name,
    ORA.OEBS_Item_Num       AS P_Legacy_Item_Num,
    ORA.INVENTORY_ITEM_ID   AS P_Legacy_Inventory_Item_Num,
    ORA.ORGANIZATION_ID     AS P_Legacy_Org_Num,
    ORA.OEBS_Item_Num       AS P_Item_Num,
    NULL                    AS P_Ext_ID,
    ORA.Sales_Rpt_Seg3      AS P_Cat,
    NULL                    AS P_Family,
    NULL                    AS P_Hosting_Component_Ind,
    NULL                    AS P_PSA_Contract_Type,
    NULL                    AS P_Cus_Asset_Ind,
    NULL                    AS P_Billing_Freq,
    NULL                    AS P_UOM,
    NULL                    AS P_Active_Ind,  
    NULL                    AS P_Subinventory,
    NULL                    AS P_Solution,
    ORA.Func_Cat1_Seg1      AS P_Solution_Cat_1,
    ORA.Func_Cat2_Seg1      AS P_Solution_Cat_2,
    ORA.Prod_Type_Seg1      AS P_Type,
    ORA.Strat_Offer_Seg1    AS P_Strategic_Offering,
    NULL                    AS P_Core_Class,
    ORA.Sales_Rpt_Seg3      AS P_Sales_Rpt_Bus_Grp,   -- verify
    ORA.Sales_Rpt_Seg2      AS P_Sales_Rpt_Bus_Cat,
    ORA.Sales_Rpt_Seg3      AS P_Sales_Rpt_Prod_Cat,  -- verify
    ORA.Sales_Rpt_CatgVal   AS P_Sales_Rpt_Cat,
    ORA.Prod_Line_CatgVal   AS P_Line_Cat,
    ORA.Prod_Line_Seg3      AS P_Line_Bus_Cat,
    ORA.GL_Seg5             AS P_LOB,
    ORA.Prod_Line_Seg4      AS P_Line_Bus_Line,
    ORA.Prod_Line_Seg6      AS P_Line_Features,
    ORA.Prod_Line_Seg1      AS P_Line_Item_Cat,
    NULL                    AS P_VSOE_Cat,
    ORA.Vers_Cat_Seg1       AS P_Version_Cat,
    ORA.Tax_Seg1            AS P_Tax_Cat,
    NULL                    AS P_License_Type,
    NULL                    AS P_Price_Type,
    NULL                    AS P_Pricing_Method,
    NULL                    AS P_Subscription_Pricing_Type,
    NULL                    AS P_Subscription_Type,
    NULL                    AS P_Cloud_Context,
    NULL                    AS P_Percent_of_Total_Include_Ind,
    NULL                    AS P_Charge_Type,
    NULL                    AS P_Billing_Type,
    ORA.OEBS_Item_Num       AS P_SMB_Billing_Item_Num,
    ORA.OEBS_Item_Num       AS P_Enterprise_Billing_Item_Num,
    ORA.OEBS_Item_Num       AS P_Usage_Billing_Item_Num,
    NULL                    AS P_Billing_Role,
    NULL                    AS P_Bundle_Type,
    NULL                    AS P_Separate_Orders_Ind,
    NULL                    AS P_HLE_Type,
    NULL                    AS P_HLE_Desc,
    NULL                    AS P_HLE_Value,
    NULL                    AS P_Svc_Product_Family,
    NULL                    AS P_Svc_Product_Line,
    NULL                    AS P_Svc_Product_Domain,
    NULL                    AS P_Svc_Product_Type,
    NULL                    AS P_License_Method_Value,
    ORA.INVENTORY_ITEM_STATUS_CODE  AS P_Status,
    ORA.ITEM_TYPE           AS P_Item_Type,
    ORA.PRIMARY_UOM_CODE    AS P_Legacy_UOM,
    NULL                    AS P_Inv_Org,
    NULL                    AS P_Subinventory_Code,
    NULL                    AS P_Form_Product_Ind,
    NULL                    AS P_Solution_Detail,
    ORA.DESCRIPTION         AS P_Revenue_Product_Desc,

    NULL AS SF_effectiveDate,
    ORA.OEBS_DL_effectiveDate,
    ORA.Prod_Line_DL_effectiveDate,
    ORA.Sales_Rpt_DL_effectiveDate,
    ORA.Prod_Type_DL_effectiveDate,
    ORA.Func_Cat1_DL_effectiveDate,
    ORA.Func_Cat2_DL_effectiveDate,
    ORA.Strat_Offer_DL_effectiveDate,
    ORA.Vers_Cat_DL_effectiveDate,
    ORA.Tax_DL_effectiveDate,
    ORA.OEBS_gl_DL_effectiveDate, 
    ORA.OEBS_SF_match_DL_effectiveDate

    FROM OEBS_With_Restrict_Match ORA
    WHERE (
        -- If salesforce is not matching
        ORA.SF_Matching_Ind = 0 OR 
        -- If salesforce is matching, but we are restricting matches for 'Professional Services' and 'Hosting'    
        (   ORA.Restrict_Match_Ind = 2
            AND OEBS_Item_Num not in  
            (
                SELECT OEBS_Item_Num FROM OEBS_With_Restrict_Match where Restrict_Match_Ind = 0
            )
        
        )
    )
    AND ORA.OEBS_Item_Num IS NOT NULL
  )

  -- build dim from union CTE
  SELECT DISTINCT
    '{p_process_log_id}' AS Rec_Process_Log_Id,
    Rec_Src_Application_Id, 
    Rec_Src_Key,
    Rec_Src_Create_Date,
    Rec_Src_Create_User,
    Rec_Src_Update_Date,
    Rec_Src_Update_User,
    '{current_timestamp}'   AS   Rec_EDW_Create_Date,
    '{current_timestamp}'		AS   Rec_EDW_Update_Date,
    to_date('1900-01-01', 'yyyy-MM-dd') AS Rec_EDW_Effective_Start_Date,
    to_date('9999-12-31', 'yyyy-MM-dd') AS Rec_EDW_Effective_End_Date,
    1 											AS Rec_EDW_Current_Ind,

    P_Name                      AS Prod_Name,
    IFNULL(P_Legacy_Item_Num,-1) AS Prod_Legacy_Item_Num,
    IFNULL(P_Legacy_Inventory_Item_Num, -1) AS Prod_Legacy_Inventory_Item_Num,
    IFNULL(P_Legacy_Org_Num,-1) AS Prod_Legacy_Org_Num,
    P_Item_Num                  AS Prod_Item_Num,
    P_Ext_ID                    AS Prod_Ext_ID,
    P_Cat                       AS Prod_Cat,
    P_Family                    AS Prod_Family,
    IFNULL(P_Hosting_Component_Ind,0) AS Prod_Hosting_Component_Ind,
    P_PSA_Contract_Type         AS Prod_PSA_Contract_Type,
    IFNULL(P_Cus_Asset_Ind,0)   AS Prod_Cus_Asset_Ind,
    P_Billing_Freq              AS Prod_Billing_Freq,
    P_UOM                       AS Prod_UOM,
    IFNULL(P_Active_Ind,0)      AS Prod_Active_Ind,  
    P_Subinventory              AS Prod_Subinventory,
    P_Solution                  AS Prod_Solution,
    P_Solution_Cat_1            AS Prod_Solution_Cat_1,
    P_Solution_Cat_2            AS Prod_Solution_Cat_2,
    P_Type                      AS Prod_Type,
    P_Strategic_Offering        AS Prod_Strategic_Offering,
    P_Core_Class                AS Prod_Core_Class,
    P_Sales_Rpt_Bus_Grp         AS Prod_Sales_Rpt_Bus_Grp,  
    P_Sales_Rpt_Bus_Cat         AS Prod_Sales_Rpt_Bus_Cat,
    P_Sales_Rpt_Prod_Cat        AS Prod_Sales_Rpt_Prod_Cat,  
    P_Sales_Rpt_Cat             AS Prod_Sales_Rpt_Cat,
    P_Line_Cat                  AS Prod_Line_Cat,
    P_Line_Bus_Cat              AS Prod_Line_Bus_Cat,
    P_LOB                       AS Prod_LOB,
    P_Line_Bus_Line             AS Prod_Line_Bus_Line,
    P_Line_Features             AS Prod_Line_Features,
    P_VSOE_Cat                  AS Prod_VSOE_Cat,
    P_Version_Cat               AS Prod_Version_Cat,
    P_Tax_Cat                   AS Prod_Tax_Cat,
    P_License_Type              AS Prod_License_Type,
    P_Price_Type                AS Prod_Price_Type,
    P_Pricing_Method            AS Prod_Pricing_Method,
    P_Subscription_Pricing_Type AS Prod_Subscription_Pricing_Type,
    P_Subscription_Type         AS Prod_Subscription_Type,
    P_Cloud_Context             AS Prod_Cloud_Context,
    IFNULL(P_Percent_of_Total_Include_Ind,0)  AS Prod_Percent_of_Total_Include_Ind,
    P_Charge_Type                   AS Prod_Charge_Type,
    P_Billing_Type                  AS Prod_Billing_Type,
    P_SMB_Billing_Item_Num          AS Prod_SMB_Billing_Item_Num,
    P_Enterprise_Billing_Item_Num   AS Prod_Enterprise_Billing_Item_Num,
    P_Usage_Billing_Item_Num        AS Prod_Usage_Billing_Item_Num,
    P_Billing_Role                  AS Prod_Billing_Role,
    P_Bundle_Type                   AS Prod_Bundle_Type,
    IFNULL(P_Separate_Orders_Ind,0) AS Prod_Separate_Orders_Ind,
    P_HLE_Type                      AS Prod_HLE_Type,
    P_HLE_Desc                      AS Prod_HLE_Desc,
    P_HLE_Value                     AS Prod_HLE_Value,
    P_Svc_Product_Family            AS Prod_Svc_Product_Family,
    P_Svc_Product_Line              AS Prod_Svc_Product_Line,
    P_Svc_Product_Domain            AS Prod_Svc_Product_Domain,
    P_Svc_Product_Type              AS Prod_Svc_Product_Type,
    P_License_Method_Value          AS Prod_License_Method_Value,
    P_Status                        AS Prod_Status,
    P_Item_Type                     AS Prod_Item_Type,
    P_Legacy_UOM                    AS Prod_Legacy_UOM,
    P_Inv_Org                       AS Prod_Inv_Org,
    P_Subinventory_Code             AS Prod_Subinventory_Code,
    IFNULL(P_Form_Product_Ind,0)    AS Prod_Form_Product_Ind,
    P_Solution_Detail               AS Prod_Solution_Detail,
    P_Line_Item_Cat                 AS Prod_Line_Item_Cat,
    P_Revenue_Product_Desc          AS Prod_Revenue_Product_Desc,

    'I'                             AS ETL_Ins_Upd_Flag,
    HASH(Prod_Name,
          Prod_Legacy_Item_Num,
          Prod_Legacy_Inventory_Item_Num,
          Prod_Legacy_Org_Num,
          Prod_Item_Num, 
          Prod_Ext_ID, 
          Prod_Cat,
          Prod_Family,
          Prod_Hosting_Component_Ind,
          Prod_PSA_Contract_Type,
          Prod_Cus_Asset_Ind, 
          Prod_Billing_Freq,
          Prod_UOM, 
          Prod_Active_Ind, 
          Prod_Subinventory,
          Prod_Solution,
          Prod_Solution_Cat_1,
          Prod_Solution_Cat_2,
          Prod_Type,
          Prod_Strategic_Offering,
          Prod_Core_Class,
          Prod_Sales_Rpt_Bus_Grp,
          Prod_Sales_Rpt_Bus_Cat,
          Prod_Sales_Rpt_Prod_Cat,
          Prod_Sales_Rpt_Cat,
          Prod_Line_Cat,
          Prod_Line_Bus_Cat,
          Prod_LOB,
          Prod_Line_Bus_Line,
          Prod_Line_Features,
          Prod_VSOE_Cat,
          Prod_Version_Cat,
          Prod_Tax_Cat,
          Prod_License_Type,
          Prod_Price_Type,
          Prod_Pricing_Method ,
          Prod_Subscription_Pricing_Type,
          Prod_Subscription_Type,
          Prod_Cloud_Context,
          Prod_Percent_of_Total_Include_Ind, 
          Prod_Charge_Type,
          Prod_Billing_Type,
          Prod_SMB_Billing_Item_Num,
          Prod_Enterprise_Billing_Item_Num,
          Prod_Usage_Billing_Item_Num,
          Prod_Billing_Role,
          Prod_Bundle_Type,
          Prod_Separate_Orders_Ind, 
          Prod_HLE_Type ,
          Prod_HLE_Desc ,
          Prod_HLE_Value,
          Prod_Svc_Product_Family, 
          Prod_Svc_Product_Line, 
          Prod_Svc_Product_Domain,
          Prod_Svc_Product_Type, 
          Prod_License_Method_Value, 
          Prod_Status, 
          Prod_Item_Type, 
          Prod_Legacy_UOM, 
          Prod_Inv_Org, 
          Prod_Subinventory_Code, 
          Prod_Form_Product_Ind, 
          Prod_Solution_Detail,
          Prod_Line_Item_Cat,
          Prod_Revenue_Product_Desc
           ) AS Rec_EDW_Hash 

    FROM CTE_Union
    WHERE (SF_effectiveDate >= '{WaterMark}' 
          OR OEBS_DL_effectiveDate >= '{WaterMark}'
          OR Prod_Line_DL_effectiveDate >= '{WaterMark}'
          OR Sales_Rpt_DL_effectiveDate >= '{WaterMark}'
          OR Prod_Type_DL_effectiveDate >= '{WaterMark}'
          OR Func_Cat1_DL_effectiveDate >= '{WaterMark}'
          OR Func_Cat2_DL_effectiveDate >= '{WaterMark}'
          OR Strat_Offer_DL_effectiveDate >= '{WaterMark}'
          OR Vers_Cat_DL_effectiveDate >= '{WaterMark}'
          OR Tax_DL_effectiveDate >= '{WaterMark}'
          OR OEBS_gl_DL_effectiveDate >= '{WaterMark}'
          OR OEBS_SF_match_DL_effectiveDate  >= '{WaterMark}')
);        

--SCD Type 2
  MERGE INTO stg.{p_table_name} stg
  USING edw.{p_table_name} dim
  ON (stg.Rec_Src_Key = dim.Rec_Src_Key AND stg.Rec_Src_Application_Id = dim.Rec_Src_Application_Id AND dim.Rec_EDW_Current_Ind = 1)
  WHEN MATCHED THEN
  UPDATE SET stg.ETL_Ins_Upd_Flag = CASE WHEN stg.Rec_EDW_Hash <> dim.Rec_EDW_Hash THEN 'U' ELSE 'Ignore' END
          ,stg.Rec_EDW_Effective_Start_Date = CASE WHEN stg.Rec_EDW_Hash <> dim.Rec_EDW_Hash THEN '{current_timestamp}' ELSE stg.Rec_EDW_Effective_Start_Date END;

/* =============================================== SPECIAL SCENARIO HANDLING  ==================================================

  Scenario 1: SF & ORA come in at the same time and are immediately matched (happy path)
      -- no special handling

  Scenario 2: ORA exists first, late arriving SF is joined after (common scenario)
      -- Update end date to max start date for the legacy inventory ID & inactivate	---> handled in merge.sql update			

  Scenario 3:  SF exists first, late arriving ORA is joined after (uncommon scenario)  
        -- Insert ORA only row 1900 to current time, inactivate	 **** WILL NOT HANDLE THIS RIGHT NOW, THIS "should never" OCCUR ****			

  Scenario 4:  SF, ORA exist separately, SF is change and then matched to ORA(uncommon scenario)
      -- Update end date to max start date for the legacy inventory ID & inactivate	---> handled in merge.sql update	

  Scenario 5:  SF, ORA matched together.  Updates made Then ORA and SF split
      -- Allow standard insert and then update ORA only row start date to current time before merge.sql (below)

  Scenario 6: SF, ORA matched together, then SF is matched to a different ORA    
      -- Allow standard insert and then update ORA only row start date to current time before merge.sql  (below)

  Scenario 7:  SF , ORA exist separately.  Then ORA/SF are joined. Then ORA and SF split (uncommon)
      -- Allow standard insert and then update ORA only row start date to current time before merge.sql  (below)  

  Scenario 8:  SF , ORA exist separately.  Then ORA/SF are joined. Then SF is joined to different ORA (uncommon)    
      -- Allow standard insert and then update ORA only row start date to current time before merge.sql  (below)
      -- Insert ORA only row 1900 to current time, inactivate  **** WILL NOT HANDLE THIS RIGHT NOW, THIS "should never" OCCUR ****
  */

-- --> Special Handling for Scenarios 5,6,7,8.1 
-- update the effective start date for ORA only rows from 1900-01-01 to current timestamp if the product code already exists in EDW history.
UPDATE stg.{p_table_name} stg
SET stg.Rec_EDW_Effective_Start_Date = '{current_timestamp}'
WHERE stg.Rec_EDW_Current_Ind = 1
  AND stg.Rec_Src_Application_Id = 1
  AND stg.Rec_EDW_Effective_Start_Date = to_date('1900-01-01', 'yyyy-MM-dd')
  AND stg.Prod_Item_Num in (Select DISTINCT x.Prod_Item_Num from edw.{p_table_name} x where x.Prod_Item_Num=stg.Prod_Item_Num);
