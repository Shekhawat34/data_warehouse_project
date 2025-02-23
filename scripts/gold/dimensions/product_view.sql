/*
--------------------------------------------------------------------------------
-- File Name: dim_products.sql
-- Description: This script creates a `gold` layer view named `gold.dim_products`.
--
-- Purpose:
--  - This view provides a cleaned and structured product dimension table 
--    for analytics and reporting.
--  - It integrates product details from multiple `silver` tables.
--  - It assigns a surrogate key (`product_key`) for optimized dimensional modeling.
--
-- Tables Used:
--  1. silver.crm_prd_info      (Core product information)
--  2. silver.erp_px_cat_g1v2   (Product category mapping)
--
-- Key Transformations:
--  - Generates a unique `product_key` using `ROW_NUMBER()`.
--  - Joins category and subcategory details for a complete product view.
--  - Filters only active products by excluding records where `prd_end_dt IS NULL`.
--  - Standardizes product costing and classification.
--
-- Execution:
--  - This view should be used in the `gold` layer for analytical queries.
--  - It enables faster retrieval and reporting of product-related insights.
--
-- Author: Lokender Singh
-- Date: 23-02-2025
--------------------------------------------------------------------------------
*/


CREATE VIEW gold.dim_products AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance AS maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_cost,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL 
