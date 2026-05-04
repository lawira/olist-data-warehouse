/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Galaxy Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_dates
-- =============================================================================

IF OBJECT_ID('gold.dim_dates', 'V') IS NOT NULL
	DROP VIEW gold.dim_dates;
GO
CREATE VIEW gold.dim_dates AS
SELECT
	date_key,
	full_date,
	day,
	day_name,
	day_of_week,
	is_weekend,
	week_of_year,
	month,
	month_name,
	quarter,
	quarter_name,
	year
FROM silver.dates;
GO

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers;
GO
CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER(ORDER BY customer_id) customer_key,
	customer_id,
	customer_unique_id unique_id,
	customer_zip_code_prefix zip_code_prefix,
	customer_city city,
	customer_state state
FROM silver.app_customers;
GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
	DROP VIEW gold.dim_products;
GO
CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER(ORDER BY product_id) product_key,
	p.product_id,
	COALESCE(pc.product_category_name_english, p.product_category_name) category_name,
	p.product_name_lenght name_length,
	p.product_description_lenght description_length,
	p.product_photos_qty photos_quantity,
	p.product_weight_g weight_g,
	p.product_length_cm length_cm,
	p.product_height_cm height_cm,
	p.product_width_cm width_cm
FROM silver.app_products p
LEFT JOIN silver.app_product_category_name_translation pc ON p.product_category_name = pc.product_category_name;
GO

-- =============================================================================
-- Create Dimension: gold.dim_sellers
-- =============================================================================

IF OBJECT_ID('gold.dim_sellers', 'V') IS NOT NULL
	DROP VIEW gold.dim_sellers;
GO
CREATE VIEW gold.dim_sellers AS
SELECT
	ROW_NUMBER() OVER(ORDER BY seller_id) seller_key,
	seller_id,
	seller_zip_code_prefix zip_code_prefix,
	seller_city city,
	seller_state state
FROM silver.app_sellers
GO

-- =============================================================================
-- Create Dimension: gold.fact_orders
-- =============================================================================

IF OBJECT_ID('gold.fact_orders', 'V') IS NOT NULL
	DROP VIEW gold.fact_orders;
GO
CREATE VIEW gold.fact_orders AS
SELECT
	ROW_NUMBER() OVER(ORDER BY o.order_id) order_key,
	o.order_id,
	c.customer_key,
	o.order_status,
	CAST(FORMAT(CAST(o.order_purchase_timestamp AS DATE), 'yyyyMMdd') AS INT) purchase_date_key,
	CAST(FORMAT(CAST(o.order_approved_at AS DATE), 'yyyyMMdd') AS INT) approved_date_key,
	CAST(FORMAT(CAST(o.order_delivered_carrier_date AS DATE), 'yyyyMMdd') AS INT) carrier_date_key,
	CAST(FORMAT(CAST(o.order_delivered_customer_date AS DATE), 'yyyyMMdd') AS INT) delivered_date_key,
	CAST(FORMAT(CAST(o.order_estimated_delivery_date AS DATE), 'yyyyMMdd') AS INT) estimated_date_key,
	o.datetime_flag,
	o.status_flag
FROM silver.app_orders o
LEFT JOIN gold.dim_customers c ON o.customer_id = c.customer_id;
GO

-- =============================================================================
-- Create Dimension: gold.fact_order_items
-- =============================================================================

IF OBJECT_ID('gold.fact_order_items', 'V') IS NOT NULL
	DROP VIEW gold.fact_order_items;
GO
CREATE VIEW gold.fact_order_items AS
SELECT
	o.order_key,
	oi.order_item_id,
	p.product_key,
	s.seller_key,
	CAST(FORMAT(CAST(oi.shipping_limit_date AS DATE), 'yyyyMMdd') AS INT) limit_date_key,
	oi.price,
	oi.freight_value
FROM silver.app_order_items oi
LEFT JOIN gold.dim_products p ON oi.product_id = p.product_id
LEFT JOIN gold.dim_sellers s ON oi.seller_id = s.seller_id
LEFT JOIN gold.fact_orders o ON oi.order_id = o.order_id;
GO

-- =============================================================================
-- Create Dimension: gold.fact_payments
-- =============================================================================

IF OBJECT_ID('gold.fact_payments', 'V') IS NOT NULL
	DROP VIEW gold.fact_payments;
GO
CREATE VIEW gold.fact_payments AS
SELECT
	o.order_key,
	op.payment_sequential,
	op.payment_type,
	op.payment_installments,
	op.payment_value
FROM silver.app_order_payments op
LEFT JOIN gold.fact_orders o ON op.order_id = o.order_id;
GO

-- =============================================================================
-- Create Dimension: gold.fact_order_reviews
-- =============================================================================

IF OBJECT_ID('gold.fact_order_reviews', 'V') IS NOT NULL
	DROP VIEW gold.fact_order_reviews;
GO
CREATE VIEW gold.fact_order_reviews AS
SELECT
	ri.review_id,
	o.order_key,
	ri.review_score score,
	ri.review_comment_title comment_title,
	ri.review_comment_message comment_message,
	CAST(FORMAT(CAST(ri.review_creation_date AS DATE), 'yyyyMMdd') AS INT) creation_date_key,
	CAST(FORMAT(CAST(ri.review_answer_timestamp AS DATE), 'yyyyMMdd') AS INT) answer_timestamp_key
FROM silver.app_order_reviews ri
LEFT JOIN gold.fact_orders o ON ri.order_id = o.order_id
WHERE ri.review_id_count = 1;
GO
