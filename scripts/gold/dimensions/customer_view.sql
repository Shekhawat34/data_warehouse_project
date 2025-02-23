
/*
--------------------------------------------------------------------------------
-- File Name: dim_customers.sql
-- Description: This script creates a `gold` layer view named `gold.dim_customers`.
--
-- Purpose:
--  - This view provides a cleaned and standardized customer dimension table 
--    for analytics and reporting.
--  - It integrates customer data from multiple `silver` tables, ensuring data 
--    consistency and completeness.
--  - It assigns a surrogate key (`customer_key`) for better performance in 
--    dimensional modeling.
--
-- Tables Used:
--  1. silver.crm_cust_info    (Core customer information)
--  2. silver.erp_cust_az12    (Additional customer demographic data)
--  3. silver.erp_loc_a101     (Customer location data)
--
-- Key Transformations:
--  - Generates a unique `customer_key` using `ROW_NUMBER()`.
--  - Standardizes gender information by merging multiple data sources.
--  - Ensures `NULL` values and missing data are properly handled.
--  - Joins customer demographic and location details for a complete view.
--
-- Execution:
--  - This view should be used in the `gold` layer for analytical queries.
--  - It enables faster retrieval and reporting of customer-related insights.
--
-- Author: Lokender Singh
-- Date: 23-02-2025
--------------------------------------------------------------------------------
*/


CREATE VIEW gold.dim_customers AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_material_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
	     ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
	
	
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON  ci.cst_key= ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON  ci.cst_key= la.cid
