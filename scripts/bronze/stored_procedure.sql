/*
--------------------------------------------------------------------------------
-- File Name: load_bronze.sql
-- Description: This script defines the stored procedure `bronze.load_bronze`
--              which loads raw data from CSV files into the `bronze` layer.
--
-- Purpose:
--  - Extracts raw data from CSV files stored locally.
--  - Loads the extracted data into staging tables in the `bronze` layer.
--  - Ensures each table is truncated before loading new data.
--  - Logs execution times for performance monitoring.
--  - Captures errors in case of failure.
--
-- Tables Affected:
--  1. bronze.crm_cust_info      (Customer Information)
--  2. bronze.crm_prd_info       (Product Information)
--  3. bronze.crm_sales_details  (Sales Details)
--  4. bronze.erp_cust_az12      (ERP Customer Data)
--  5. bronze.erp_loc_a101       (ERP Location Data)
--  6. bronze.erp_px_cat_g1v2    (Product Category Information)
--
-- Execution:
--  - Run `EXEC bronze.load_bronze;` to load data into the `bronze` layer.
--  - Ensure the CSV file paths are correctly set before execution.
--
-- Author: Lokender Singh
-- Date: 23-02-2025
--------------------------------------------------------------------------------
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
     
	 DECLARE @start_time DATETIME, @end_time DATETIME;
     BEGIN TRY
   
                
			PRINT '==========================================';
			PRINT 'LOADING BRONZE LAYER';
			PRINT '==========================================';


			PRINT '===============---------==================';
			PRINT 'lOADING CRM TABLES';

			SET @start_time = GETDATE();

			PRINT 'CUSTOMER INFORMATION TABLE';

			TRUNCATE TABLE bronze.crm_cust_info;
			BULK INSERT bronze.crm_cust_info
			FROM 'C:\Users\Dell\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH (
				 FIRSTROW = 2,
				 FIELDTERMINATOR = ',',
				 TABLOCK
			);

			SET @end_time = GETDATE();
			PRINT '>> LOAD DURATION:' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

	        PRINT '>> ======================= <<';


			SET @start_time = GETDATE();
			PRINT 'PRODUCT INFORMATION TABLE';

			TRUNCATE TABLE bronze.crm_prd_info;
			BULK INSERT bronze.crm_prd_info
			FROM 'C:\Users\Dell\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH (
				 FIRSTROW = 2,
				 FIELDTERMINATOR = ',',
				 TABLOCK
			);

			SET @end_time = GETDATE();
			PRINT '>> LOAD DURATION:' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

	        PRINT '>> ======================= <<';


			SET @start_time = GETDATE();
			PRINT 'SALES DETAILS';

			TRUNCATE TABLE bronze.crm_sales_details;
			BULK INSERT bronze.crm_sales_details
			FROM 'C:\Users\Dell\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH (
				 FIRSTROW = 2,
				 FIELDTERMINATOR = ',',
				 TABLOCK
			);

			SET @end_time = GETDATE();
			PRINT '>> LOAD DURATION:' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

	        PRINT '>> ======================= <<';

	
	        SET @start_time = GETDATE();

			PRINT '>> ============ERP_CUST_AZ12================ <<';

			TRUNCATE TABLE bronze.erp_cust_az12;
			BULK INSERT bronze.erp_cust_az12
			FROM 'C:\Users\Dell\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			WITH (
				 FIRSTROW = 2,
				 FIELDTERMINATOR = ',',
				 TABLOCK
			);

			SET @end_time = GETDATE();
			PRINT '>> LOAD DURATION:' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

	        PRINT '>> ======================= <<';


			
			SET @start_time = GETDATE();
			PRINT '>> ============ERP_LOC_A101================ <<';

			TRUNCATE TABLE bronze.erp_loc_a101;
			BULK INSERT bronze.erp_loc_a101
			FROM 'C:\Users\Dell\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
			WITH (
				 FIRSTROW = 2,
				 FIELDTERMINATOR = ',',
				 TABLOCK
			);

			SET @end_time = GETDATE();
			PRINT '>> LOAD DURATION:' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

	        PRINT '>> ======================= <<';

	
	        SET @start_time = GETDATE();
			PRINT '>> ============ ERP_px_cat_g1v2 ================ <<';

			TRUNCATE TABLE bronze.erp_px_cat_g1v2;
			BULK INSERT bronze.erp_px_cat_g1v2
			FROM 'C:\Users\Dell\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			WITH (
				 FIRSTROW = 2,
				 FIELDTERMINATOR = ',',
				 TABLOCK
			);

			SET @end_time = GETDATE();
			PRINT '>> LOAD DURATION:' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

	        PRINT '>> ======================= <<';

     END TRY

	 BEGIN CATCH 
	         
			 PRINT '=====================================';

			 PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
			 PRINT 'ERROR MESSAGE'+ ERROR_MESSAGE();
			 PRINT 'ERROR MESSAGE'+ CAST (ERROR_NUMBER() AS NVARCHAR);

			 PRINT '=====================================';

	 END CATCH

	

END


EXEC bronze.load_bronze
