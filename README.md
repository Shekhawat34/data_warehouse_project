# Data Warehouse Project – Bronze, Silver & Gold Layers

# 📌 Overview
This project focuses on building a modern Data Warehouse (DWH) using a multi-layered architecture to enable efficient data processing, storage, and analytics. The data is sourced from multiple systems such as CRM, ERP, and Sales platforms, and structured using ETL pipelines to ensure data integrity and usability.

📁 Data Layers Architecture
🔹 Bronze Layer (Raw Data)
Stores raw, unprocessed data from various sources.
Maintains historical records for auditing and traceability.
Example Tables:
crm_cust_info (Customer data from CRM)
erp_cust_az12 (Customer details from ERP)
crm_prd_info (Product information)
crm_sales_details (Raw sales transactions)
🔹 Silver Layer (Transformed Data)
Cleans, standardizes, and applies business rules to the raw data.
Combines multiple sources to create a unified dataset.
Example Tables:
erp_loc_a101 (Customer location details)
erp_px_cat_g1v2 (Product category mapping)
🔹 Gold Layer (Analytical Data)
Stores optimized data for analytics and reporting.
Contains fact and dimension tables to support BI dashboards and insights.
Example Tables:
dim_customers (Customer dimension)
dim_products (Product dimension)
fact_sales (Sales fact table)
🚀 Key Features
✅ ETL Process – Extract, Transform, and Load data from multiple sources.
✅ Data Cleansing & Normalization – Ensuring high-quality, structured data.
✅ Optimized Data Models – Using Fact-Dimension modeling for efficient querying.
✅ Business Intelligence Ready – Supports dashboards, KPIs, and reporting tools.


