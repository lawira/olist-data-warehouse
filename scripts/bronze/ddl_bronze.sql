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

IF OBJECT_ID('bronze.app_customers', 'U') IS NOT NULL
	DROP TABLE bronze.app_customers;
GO
CREATE TABLE bronze.app_customers (
	customer_id NVARCHAR(50),
	customer_unique_id NVARCHAR(50),
	customer_zip_code_prefix NVARCHAR(50),
	customer_city NVARCHAR(50),
	customer_state NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.app_geolocation', 'U') IS NOT NULL
	DROP TABLE bronze.app_geolocation;
GO
CREATE TABLE bronze.app_geolocation (
	geolocation_zip_code_prefix NVARCHAR(50),
	geolocation_lat DECIMAL(19, 16),
	geolocation_lng DECIMAL(19, 16),
	geolocation_city NVARCHAR(50),
	geolocation_state NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.app_order_items', 'U') IS NOT NULL
	DROP TABLE bronze.app_order_items;
GO
CREATE TABLE bronze.app_order_items (
	order_id NVARCHAR(50),
	order_item_id int,
	product_id NVARCHAR(50),
	seller_id NVARCHAR(50),
	shipping_limit_date DATETIME2,
	price DECIMAL(19, 4),
	freight_value DECIMAL(19, 4)
);
GO

IF OBJECT_ID('bronze.app_order_payments', 'U') IS NOT NULL
	DROP TABLE bronze.app_order_payments;
GO
CREATE TABLE bronze.app_order_payments (
	order_id NVARCHAR(50),
	payment_sequential INT,
	payment_type NVARCHAR(50),
	payment_installments INT,
	payment_value DECIMAL(19, 4)
);
GO

IF OBJECT_ID('bronze.app_order_reviews', 'U') IS NOT NULL
	DROP TABLE bronze.app_order_reviews;
GO
CREATE TABLE bronze.app_order_reviews (
	review_id NVARCHAR(50),
	order_id NVARCHAR(50),
	review_score INT,
	review_comment_title NVARCHAR(50),
	review_comment_message NVARCHAR(MAX),
	review_creation_date DATETIME2,
	review_answer_timestamp DATETIME2
);
GO

IF OBJECT_ID('bronze.app_orders', 'U') IS NOT NULL
	DROP TABLE bronze.app_orders;
GO
CREATE TABLE bronze.app_orders (
	order_id NVARCHAR(50),
	customer_id NVARCHAR(50),
	order_status NVARCHAR(50),
	order_purchase_timestamp DATETIME2,
	order_approved_at DATETIME2,
	order_delivered_carrier_date DATETIME2,
	order_delivered_customer_date DATETIME2,
	order_estimated_delivery_date DATETIME2
);
GO

IF OBJECT_ID('bronze.app_product_category_name_translation', 'U') IS NOT NULL
	DROP TABLE bronze.app_product_category_name_translation;
GO
CREATE TABLE bronze.app_product_category_name_translation (
	product_category_name NVARCHAR(50),
	product_category_name_english NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.app_products', 'U') IS NOT NULL
	DROP TABLE bronze.app_products;
GO
CREATE TABLE bronze.app_products (
	product_id NVARCHAR(50),
	product_category_name NVARCHAR(50),
	product_name_lenght INT,
	product_description_lenght INT,
	product_photos_qty INT,
	product_weight_g INT,
	product_length_cm INT,
	product_height_cm INT,
	product_width_cm INT
);
GO

IF OBJECT_ID('bronze.app_sellers', 'U') IS NOT NULL
	DROP TABLE bronze.app_sellers;
GO
CREATE TABLE bronze.app_sellers (
	seller_id NVARCHAR(50),
	seller_zip_code_prefix NVARCHAR(50),
	seller_city NVARCHAR(50),
	seller_state NVARCHAR(50)
);
GO
