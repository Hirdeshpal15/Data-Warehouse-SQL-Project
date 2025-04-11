/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/



-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS 
SELECT
    ROW_NUMBER() OVER( ORDER BY cst_key ) AS Customer_Key -- Surrogate key
    ,ci.cust_id AS Customer_id
    ,ci.cst_key AS Customer_number
    ,ci.cst_firstname AS First_Name
    ,ci.cst_lastname AS Late_Name
    ,la.CNTRY AS Country
    ,ci.cst_martial_status AS Martial_Status
    CASE 
        WHEN ci.cst_gndr ! = 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
        ELSE COALESCE(ca.gen,'n/a')  			   -- Fallback to ERP data
    END AS Gender
       ,ca.bdate AS Birth_date
       ,ci.cst_create_date AS Create_Date
FROM silver.crm_cust_info ci     
LEFT JOIN silver.erp_CUST_AZ12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_LOC_A101 la
    ON ci.cst_key = la.cid;
GO




  
-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================


IF OBJECT_ID('gold.dim_product', 'V') IS NOT NULL
    DROP VIEW gold.dim_product;
GO

CREATE VIEW gold.dim_product AS 
SELECT 
    ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt,pn.prd_key) AS Product_Key,, -- Surrogate key
    pn.prd_id AS Product_Id,
    pn.prd_key AS Product_Number,
    pn.prd_nm AS Product_Name,
    pn.cat_id AS Category_id,
    pc.cat AS Category,
    pc.subcat AS Sub_Category,
    pc.maintenance AS Maintenance,
    pn.prd_cost AS Product_Cost,
    pn.prd_line AS Product_Line,
    pn.prd_start_dt AS Product_Start_Date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_PX_CAT_G1V2 pc
    ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL;   -- Filter out all historical data
GO




  

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
  
IF OBJECT_ID('gold.facts_sales', 'V') IS NOT NULL
    DROP VIEW gold.facts_sales;
GO

CREATE VIEW gold.facts_sales AS
SELECT 
    sd.sls_ord_num AS Order_Number,
    pr.Product_Key,
    cu.Customer_Id,
    sd.sls_order_dt AS Order_Date,
    sd.sls_ship_dt AS Shipping_Date,
    sd.sls_due_dt AS Due_Date,
    sd.sls_quantity AS Quantity,
    sd.sls_sales AS Sales_Amount,
    sd.sls_price AS Price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_product pr
    ON sd.sls_prd_key = pr.Product_Number
LEFT JOIN gold.dim_customerS cu
     ON sd.sls_cust_id = cu.Customer_id;
GO
