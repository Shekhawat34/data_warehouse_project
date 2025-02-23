
/*
--------------------------------------------------------------------------------
-- File Name: fact_sales.sql
-- Description: This script creates a `gold` layer fact table named `gold.fact_sales`.
--
-- Purpose:
--  - This view represents the sales fact table for analytical reporting.
--  - It integrates sales transactions with product and customer dimensions.
--  - It enables easy querying of sales performance across different metrics.
--
-- Tables Used:
--  1. silver.crm_sales_details (Sales transaction details)
--  2. gold.dim_products        (Product dimension for reference)
--  3. gold.dim_customers       (Customer dimension for reference)
--
-- Key Transformations:
--  - Maps `sls_prd_key` to `product_key` from `gold.dim_products`.
--  - Maps `sls_cust_id` to `customer_key` from `gold.dim_customers`.
--  - Provides sales metrics including sales amount, quantity, and price.
--  - Standardizes sales-related dates (order date, shipping date, due date).
--
-- Execution:
--  - This view is designed for use in the `gold` layer for analytical queries.
--  - It supports BI tools, dashboards, and reporting on sales performance.
--
-- Author: Lokender Singh
-- Date: 23-02-2025
--------------------------------------------------------------------------------
*/

CREATE VIEW gold.fact_sales AS
SELECT 
	sd.sls_ord_num AS order_number,
	pr.product_key ,
	cu.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id

