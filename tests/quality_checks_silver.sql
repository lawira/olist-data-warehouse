/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.app_customers'
-- ====================================================================

-- Check for NULLS and Empties
-- Expectation: No Results
SELECT
	COUNT(*)
FROM silver.app_customers
WHERE
	customer_id IS NULL OR
	customer_unique_id IS NULL OR
	customer_zip_code_prefix IS NULL OR
	customer_city IS NULL OR
	customer_state IS NULL;

SELECT
	COUNT(*)
FROM silver.app_customers
WHERE
	customer_id = '' OR
	customer_unique_id = '' OR
	customer_zip_code_prefix = '' OR
	customer_city = '' OR
	customer_state = '';

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT
	COUNT(*)
FROM silver.app_customers
WHERE
	customer_id != TRIM(customer_id) OR
	customer_unique_id != TRIM(customer_unique_id) OR
	customer_zip_code_prefix != TRIM(customer_zip_code_prefix) OR
	customer_city != TRIM(customer_city) OR
	customer_state != TRIM(customer_state);

-- Check Structural Errors / Data Types
SELECT
	COUNT(*)
FROM silver.app_customers
WHERE customer_id LIKE '"%' OR customer_id LIKE '%"'; 

SELECT
	COUNT(*)
FROM silver.app_customers
WHERE customer_unique_id LIKE '"%' OR customer_unique_id LIKE '%"';

SELECT
	COUNT(*)
FROM silver.app_customers
WHERE customer_zip_code_prefix LIKE '"%' OR customer_zip_code_prefix LIKE '%"';

-- Check for Duplicates in Primary Key
-- Expectation: No Results
SELECT
	customer_id,
	COUNT(*)
FROM silver.app_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Standardization & Consistency
SELECT DISTINCT
	customer_city
FROM silver.app_customers;

-- ====================================================================
-- Checking 'silver.app_geolocation'
-- ====================================================================

-- Check for NULLS, Empties or Zeros
-- Expectation: No Results
SELECT
	COUNT(*)
FROM silver.app_geolocation
WHERE
	geolocation_zip_code_prefix IS NULL OR
	geolocation_lat IS NULL OR
	geolocation_lng IS NULL OR
	geolocation_city IS NULL OR
	geolocation_state IS NULL;

SELECT
	COUNT(*)
FROM silver.app_geolocation
WHERE
	geolocation_zip_code_prefix = '' OR
	geolocation_lat = 0 OR
	geolocation_lng = 0 OR
	geolocation_city = '' OR
	geolocation_state = '';

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT
	*
FROM silver.app_geolocation
WHERE
	geolocation_zip_code_prefix != TRIM(geolocation_zip_code_prefix) OR
	geolocation_city != TRIM(geolocation_city) OR
	geolocation_state != TRIM(geolocation_state);

-- Check Structural Errors / Data Types
SELECT
	geolocation_zip_code_prefix,
	COUNT(*)
FROM silver.app_geolocation
GROUP BY geolocation_zip_code_prefix
HAVING geolocation_zip_code_prefix LIKE '"%' OR geolocation_zip_code_prefix LIKE '%"';

SELECT
	geolocation_city
FROM silver.app_geolocation
WHERE REGEXP_LIKE(geolocation_city, '[^a-zA-Z ]');

SELECT
	geolocation_city
FROM (
	SELECT
		geolocation_city
	FROM silver.app_geolocation
	WHERE REGEXP_LIKE(SUBSTRING(geolocation_city, 1, 1), '[^a-zA-Z ]')
)t;

-- Standardization & Consistency
SELECT DISTINCT
	geolocation_state
FROM silver.app_geolocation;

-- ====================================================================
-- Checking 'silver.app_order_items'
-- ====================================================================

-- Check for NULLS, Empties, Zeros and Negative Numbers
-- Expectation: No Results
SELECT
	COUNT(*)
FROM silver.app_order_items
WHERE
	order_id IS NULL OR
	order_item_id IS NULL OR
	product_id IS NULL OR
	seller_id IS NULL OR
	shipping_limit_date IS NULL OR
	price IS NULL OR
	freight_value IS NULL;

SELECT
	COUNT(*)
FROM silver.app_order_items
WHERE
	order_id = '' OR
	order_item_id < 0 OR
	product_id = '' OR
	seller_id = '' OR
	shipping_limit_date = '' OR
	price < 0 OR
	freight_value < 0;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT
	COUNT(*)
FROM silver.app_order_items
WHERE
	order_id != TRIM(order_id) OR
	product_id != TRIM(product_id) OR
	seller_id != TRIM(seller_id);

-- Check for Invalid Dates
-- Expectation: No Results
SELECT
	*
FROM silver.app_order_items
WHERE shipping_limit_date > GETDATE();

-- Check Structural Errors / Data Types
-- Expectation: No Results
SELECT
	COUNT(*)
FROM silver.app_order_items
WHERE
	order_id LIKE '"%' OR order_id LIKE '%"' OR
	order_item_id LIKE '"%' OR order_item_id LIKE '%"' OR
	product_id LIKE '"%' OR product_id LIKE '%"' OR
	seller_id LIKE '"%' OR seller_id LIKE '%"' OR
	shipping_limit_date LIKE '"%' OR shipping_limit_date LIKE '%"' OR
	price LIKE '"%' OR price LIKE '%"'OR
	freight_value LIKE '"%' OR freight_value LIKE '%"';

-- ====================================================================
-- Checking 'silver.app_order_payments'
-- ====================================================================

-- Check for NULLS, Empties, Zeros or Negative Numbers
-- Expectation: No Results
SELECT
	COUNT(*)
FROM silver.app_order_payments
WHERE
	order_id IS NULL OR
	payment_sequential IS NULL OR
	payment_type IS NULL OR
	payment_installments IS NULL OR
	payment_value IS NULL

SELECT
	*
FROM silver.app_order_payments
WHERE
	order_id = '' OR
	payment_sequential <= 0 OR
	payment_type = '' OR
	payment_installments <= 0 OR
	payment_value < 0;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT
	COUNT(*)
FROM silver.app_order_payments
WHERE
	order_id != TRIM(order_id) OR
	payment_type != TRIM(payment_type);

-- Check Structural Errors / Data Types
-- Expectation: No Results
SELECT
	COUNT(*)
FROM silver.app_order_payments
WHERE order_id LIKE '"%' OR order_id LIKE '%"'

-- Check for Duplicates
-- Expectation: No Results
SELECT
	*
FROM (
	SELECT
		order_id,
		payment_sequential,
		payment_type,
		payment_installments,
		payment_value,
		ROW_NUMBER() OVER(PARTITION BY order_id, payment_sequential ORDER BY order_id) flag_duplicate
	FROM silver.app_order_payments
)t
WHERE flag_duplicate > 1;

-- Standardization & Consistency
SELECT DISTINCT
	payment_type
FROM silver.app_order_payments;

-- ====================================================================
-- Checking 'silver.app_order_payments'
-- ====================================================================

-- Check for NULLS, Empties, Zeros or Negative Numbers
-- Expectation: No Results
SELECT
	COUNT(*)
FROM silver.app_order_reviews
WHERE
	review_id IS NULL OR
	order_id IS NULL OR
	review_score IS NULL OR
	review_comment_title IS NULL OR
	review_comment_message IS NULL OR
	review_creation_date IS NULL OR
	review_answer_timestamp IS NULL;

SELECT
	*
FROM silver.app_order_reviews
WHERE
	review_id = '' OR
	order_id = '' OR
	review_score <= 0 OR
	review_comment_title = '' OR
	review_comment_message = '';

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT
	COUNT(*)
FROM silver.app_order_reviews
WHERE
	review_id != TRIM(review_id) OR
	order_id != TRIM(order_id) OR
	review_comment_title != TRIM(review_comment_title) OR
	review_comment_message != TRIM(review_comment_message);

-- Check Structural Errors / Data Types
SELECT
	*
FROM silver.app_order_reviews
WHERE
	review_id LIKE '"%' OR review_id LIKE '%"' OR
	order_id LIKE '"%' OR order_id LIKE '%"';

SELECT
	*
FROM silver.app_order_reviews
WHERE
	REGEXP_LIKE(review_comment_title, '[^a-zA-Z ]') OR
	REGEXP_LIKE(review_comment_message, '[^a-zA-Z ]');

-- Check for Invalid Dates
-- Expectation: No Results
SELECT
	*
FROM silver.app_order_reviews
WHERE review_creation_date > review_answer_timestamp;

-- Check for Duplicates
-- Expectation: Flagged duplicate review_ids & order_ids
SELECT
	*
FROM (
	SELECT
		review_id,
		order_id,
		COUNT(review_id) OVER(PARTITION BY review_id) review_id_count,
		COUNT(order_id) OVER(PARTITION BY order_id) order_id_count
	FROM silver.app_order_reviews
)t
WHERE review_id_count > 1;

-- Standardization & Consistency
SELECT DISTINCT
	review_score
FROM silver.app_order_reviews;

-- ====================================================================
-- Checking 'silver.app_order_payments'
-- ====================================================================

-- Check for NULLS & Empties
-- Expectation: No Results
SELECT
	COUNT(*)
FROM silver.app_orders
WHERE
	order_id IS NULL OR
	customer_id IS NULL OR
	order_status IS NULL

SELECT
	*
FROM silver.app_orders
WHERE
	order_id = '' OR
	customer_id = '' OR
	order_status = '';

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT
	*
FROM silver.app_orders
WHERE
	order_id != TRIM(order_id) OR
	customer_id != TRIM(customer_id) OR
	order_status != TRIM(order_status);

-- Check Structural Errors / Data Types
-- Expectation: No Results
SELECT
	COUNT(*)
FROM silver.app_orders
WHERE
	order_id LIKE '"%' OR order_id LIKE '%"' OR
	customer_id LIKE '"%' OR customer_id LIKE '%"';

-- Check for Invalid Dates
-- Expectation: Invalid Dates & Status flagged
SELECT
	COUNT(*)
FROM silver.app_orders
WHERE
	order_purchase_timestamp IS NULL OR
	order_approved_at IS NULL OR
	order_delivered_carrier_date IS NULL OR
	order_delivered_customer_date IS NULL OR
	order_estimated_delivery_date IS NULL;

SELECT
	*
FROM silver.app_orders
WHERE 
	order_delivered_carrier_date < order_purchase_timestamp OR order_delivered_customer_date < order_purchase_timestamp OR
	order_delivered_carrier_date < order_approved_at OR order_delivered_customer_date < order_approved_at;

SELECT
	*
FROM silver.app_orders
WHERE order_status = 'delivered' AND order_delivered_carrier_date IS NULL AND order_delivered_customer_date IS NULL;

SELECT
	*
FROM silver.app_orders
WHERE order_status != 'delivered' AND order_delivered_customer_date IS NOT NULL AND datetime_flag = 'invalid';

-- Check for Duplicates on Primary Key
-- Expectation: No Results
SELECT
	order_id,
	COUNT(*)
FROM silver.app_orders
GROUP BY order_id
HAVING COUNT(*) > 1;

-- Standardization & Consistency
SELECT DISTINCT
	order_status
FROM silver.app_orders;

-- ====================================================================
-- Checking 'silver.app_products'
-- ====================================================================

-- Check for NULLS, Empties and Negative Numbers
-- Expectation: No Results
SELECT
	*
FROM silver.app_products
WHERE
	product_id IS NULL OR
	product_category_name IS NULL OR
	product_name_lenght IS NULL OR
	product_description_lenght IS NULL OR
	product_photos_qty IS NULL OR
	product_weight_g IS NULL OR
	product_length_cm IS NULL OR
	product_height_cm IS NULL OR
	product_width_cm IS  NULL;

SELECT
	*
FROM silver.app_products
WHERE
	product_id = '' OR
	product_category_name = '' OR
	product_name_lenght < 0 OR
	product_description_lenght < 0 OR
	product_photos_qty < 0 OR
	product_weight_g < 0 OR
	product_length_cm < 0 OR
	product_height_cm < 0 OR
	product_width_cm < 0;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT
	*
FROM silver.app_products
WHERE
	product_id != TRIM(product_id) OR
	product_category_name != TRIM(product_id)

-- Check Structural Errors / Data Types
-- Expectation: No Results
SELECT
	COUNT(*)
FROM silver.app_products
WHERE
	product_id LIKE '"%' OR product_id LIKE '%"' OR
	product_category_name LIKE '"%' OR product_category_name LIKE '%"';

-- Check for Duplicates on Primary Key
-- Expectation: No Results
SELECT
	product_id,
	COUNT(*)
FROM silver.app_products
GROUP BY product_id
HAVING COUNT(*) > 1;

-- Standardization & Consistency
SELECT DISTINCT
	product_category_name
FROM silver.app_products;

-- ====================================================================
-- Checking 'silver.app_product_category_name_translation'
-- ====================================================================

-- Check for NULLS and Empties
-- Expectation: No Results
SELECT
	*
FROM silver.app_product_category_name_translation
WHERE
	product_category_name IS NULL OR
	product_category_name_english IS NULL;

SELECT
	*
FROM silver.app_product_category_name_translation
WHERE
	product_category_name = '' OR
	product_category_name_english = ''

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT
	*
FROM silver.app_product_category_name_translation
WHERE
	product_category_name != TRIM(product_category_name) OR
	product_category_name_english != TRIM(product_category_name_english);

-- Check for Duplicates
-- Expectation: No Results
SELECT
	product_category_name,
	COUNT(*)
FROM silver.app_product_category_name_translation
GROUP BY product_category_name
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'silver.app_sellers'
-- ====================================================================

-- Check for NULLS and Empties
-- Expectation: No Results
SELECT
	*
FROM silver.app_sellers
WHERE
	seller_id IS NULL OR
	seller_zip_code_prefix IS NULL OR
	seller_city IS NULL OR
	seller_state IS NULL;

SELECT
	*
FROM silver.app_sellers
WHERE
	seller_id = '' OR
	seller_zip_code_prefix = '' OR
	seller_city = '' OR
	seller_state = '';

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT
	*
FROM silver.app_sellers
WHERE
	seller_id != TRIM(seller_id) OR
	seller_zip_code_prefix != TRIM(seller_zip_code_prefix) OR
	seller_city != TRIM(seller_city) OR
	seller_state != TRIM(seller_state);

-- Check Structural Errors / Data Types
SELECT
	*
FROM silver.app_sellers
WHERE
	REGEXP_LIKE(seller_id, '[^a-zA-Z0-9]') OR
	REGEXP_LIKE(seller_zip_code_prefix, '[^0-9]') OR
	REGEXP_LIKE(seller_city, '["]') OR
	REGEXP_LIKE(seller_state, '[^A-Z]');

-- Check for Duplicates on Primary Key
-- Expectation: No Results
SELECT
	seller_id,
	COUNT(*)
FROM silver.app_sellers
GROUP BY seller_id
HAVING COUNT(*) > 1;

-- Standardization & Consistency
SELECT DISTINCT
	seller_state
FROM silver.app_sellers
ORDER BY seller_state;
