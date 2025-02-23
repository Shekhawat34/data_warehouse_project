/*
--------------------------------------------------------------------------------
-- File Name: load_silver.sql
-- Description: This script defines a stored procedure `silver.load_silver`
--              that loads and transforms data from the `bronze` layer into
--              the `silver` layer of the data warehouse.
--
-- Purpose:
--  - Extracts data from the `bronze` layer tables.
--  - Cleans, formats, and standardizes the extracted data.
--  - Loads the transformed data into the corresponding `silver` tables.
--  - Ensures data consistency and quality by handling NULL values, incorrect
--    formats, and standardizing categorical values.
--
-- Tables Affected:
--  1. silver.crm_cust_info      (Customer Information)
--  2. silver.crm_prd_info       (Product Information)
--  3. silver.crm_sales_details  (Sales Details)
--  4. silver.erp_cust_az12      (ERP Customer Data)
--  5. silver.erp_loc_a101       (ERP Location Data)
--  6. silver.erp_px_cat_g1v2    (Product Category Information)
--
-- Key Transformations:
--  - Trims whitespace from text fields.
--  - Standardizes categorical fields (e.g., gender, marital status, country).
--  - Converts date fields to valid formats.
--  - Ensures `NULL` values are handled appropriately.
--  - Splits and restructures composite keys (e.g., `prd_key`).
--  - Uses window functions to ensure only the latest records are retained.
--
-- Execution:
--  - The procedure `silver.load_silver` should be executed periodically
--    to refresh the `silver` layer with the latest cleaned and transformed data.
--
-- Author: Lokender Singh
-- Date: 23-02-2024
--------------------------------------------------------------------------------
*/





CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

	PRINT '>> Truncating data: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>> Inserting data into: silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info(
	   cst_id,
	   cst_key,
	   cst_firstname,
	   cst_lastname,
	   cst_material_status,
	   cst_gndr,
	   cst_create_date
	)
	SELECT 
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,

	CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
		 WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
		 ELSE 'n/a'

	END cst_material_status,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		 ELSE 'n/a'

	END cst_gndr,
     
	cst_create_date
	FROM (
	SELECT * ,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
	)t WHERE flag_last =1 ;


	PRINT '>> Truncating data: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT '>> Inserting data into: silver.crm_prd_info';

	INSERT INTO silver.crm_prd_info(
	   prd_id,
	   cat_id,
	   prd_key,
	   prd_nm,
	   prd_cost,
	   prd_line,
	   prd_start_dt,
	   prd_end_dt
	)
	SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_' )AS cat_id, 
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost,0) AS prd_cost,

	CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		 ELSE 'n/a'
     
	END AS prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
	FROM bronze.crm_prd_info;




	PRINT '>> Truncating data: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT '>> Inserting data into: silver.crm_sales_details';

	INSERT INTO silver.crm_sales_details(
		 sls_ord_num,
		 sls_prd_key,
		 sls_cust_id,
		 sls_order_dt,
		 sls_ship_dt,
		 sls_due_dt,
		 sls_sales,
		 sls_quantity,
		 sls_price
	)
	SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt = 0 or LEN(sls_order_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)

	END sls_order_dt,

	CASE WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)

	END sls_ship_dt,


	CASE WHEN sls_due_dt = 0 or LEN(sls_due_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)

	END sls_due_dt,
	CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity*ABS(sls_price)
			  THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales,
	sls_quantity,

	CASE WHEN sls_price IS NULL OR sls_price <=0
			  THEN sls_price / NULLIF(sls_quantity,0)
		ELSE sls_price
	END AS sls_price

	FROM bronze.crm_sales_details;




	PRINT '>> Truncating data: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT '>> Inserting data into: silver.erp_cust_az12';

	INSERT INTO silver.erp_cust_az12(
	 cid,
	 bdate,
	 gen
	 )
	SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END AS cid,

	CASE WHEN bdate > GETDATE() THEN NULL
		 ELSE bdate
	END AS bdate,

	CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
		 ELSE 'n/a'
	END AS gen
	FROM bronze.erp_cust_az12;





	PRINT '>> Truncating data: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT '>> Inserting data into: silver.erp_loc_a101';

	INSERT INTO silver.erp_loc_a101(
	  cid,
	  cntry
	)
	SELECT 
	REPLACE(cid,'-','') cid,
	CASE WHEN TRIM(cntry) ='DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
		 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		 ELSE TRIM(cntry)
	END AS cntry
	FROM bronze.erp_loc_a101;




	PRINT '>> Truncating data: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT '>> Inserting data into: silver.erp_px_cat_g1v2';

	INSERT INTO silver.erp_px_cat_g1v2(
	  id,
	  cat,
	  subcat,
	  maintenance
	)
	SELECT 
	id,
	cat,
	subcat,
	maintenance
	FROM bronze.erp_px_cat_g1v2;

END
