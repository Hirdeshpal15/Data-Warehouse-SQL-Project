/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/






USE DataWarehouse;


1. CREATING THE DATA TABLES FOR BROZE SCHEMA

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
    cust_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_martial_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE
);


IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
);


IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);


IF OBJECT_ID('bronze.erp_CUST_AZ12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_CUST_AZ12;
CREATE TABLE bronze.erp_CUST_AZ12(
    CID NVARCHAR(50),
    BDATE DATE,
    GEN NVARCHAR(50)
);


IF OBJECT_ID('bronze.erp_LOC_A101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_LOC_A101;
CREATE TABLE bronze.erp_LOC_A101(
    CID NVARCHAR(50),
    CNTRY NVARCHAR(50)
);


IF OBJECT_ID('bronze.erp_PX_CAT_G1V2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_PX_CAT_G1V2;
CREATE TABLE bronze.erp_PX_CAT_G1V2(
    ID NVARCHAR(50),
    CAT NVARCHAR(50),
    SUBCAT NVARCHAR(50),
    MAINTENANCE NVARCHAR(50)
);








2. This does not work directly in the ADS/ SO MANUALLY DONE IT BY EXTENSION
2.1 Also creating the stored Procedure in order all the tables can be excuted at once.

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME;
    BEGIN TRY
        PRINT '==============================';
        PRINT 'LOADING BRONZE LAYER';
        PRINT '==============================';

        PRINT '----------------------------';
        PRINT 'LOADING CRM TABLES';
        PRINT '----------------------------';
        

        SET @start_time = GETDATE();
        PRINT ' >> TRUNCATING THE TABLES: bronze.crm_cust_info';
        TRUNCATING TABLE bronze.crm_cust_info;

        PRINT '>> INSERT DATA INTO  THE TABLES: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM '/Users/hirdeshpal/Downloads/Coding/Data Warehouse/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
        WITH(
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE(); 
        PRINT '>> LOAD DURATION' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds';
        PRINT '-------------------------------'




        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING THE TABLES: bronze.crm_prd_info'
        TRUNCATING TABLE bronze.crm_prd_info;

        PRINT ' >> INSERT DATA INTO  THE TABLES: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM '/Users/hirdeshpal/Downloads/Coding/Data Warehouse/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
        WITH(
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE(); 
        PRINT '>> LOAD DURATION' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds';
        PRINT '-------------------------------'
        


        PRINT '----------------------------------';
        PRINT 'LOADING ERP TABLES';

         .......... for all other 6 the same;

    END TRY
    BEGIN CATCH
        PRINT '============================================';
        PRINT 'ERROR MESSAGE:' + ERROR_MESSAGE();
        PRINT 'ERROR MESSAGE:' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'ERROR MESSAGE:' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '============================================';
    END CATCH
END


4. IN NEXT QUERY YOU CAN RUN THIS SO THAT ALL THE DATA CAN BE EXCUTE AT ONCE

EXEC bronze.load_bronze







5. TO DO THE TABLES

DROP TABLE [bronze].[crm_cust_info];




USE DataWarehouse;
SELECT * FROM [bronze].[crm_cust_info];

