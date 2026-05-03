/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading APP Tables';
		PRINT '------------------------------------------------';

		-- Loading silver.app_customers
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.app_customers';
		TRUNCATE TABLE silver.app_customers;
		PRINT '>> Inserting Data Into: silver.app_customers';
		INSERT INTO silver.app_customers (
			customer_id,
			customer_unique_id,
			customer_zip_code_prefix,
			customer_city,
			customer_state
		)
		SELECT
			REPLACE(customer_id, '"', '') customer_id,
			REPLACE(customer_unique_id, '"', '') customer_unique_id,
			REPLACE(customer_zip_code_prefix, '"', '') customer_zip_code_prefix,
			UPPER(customer_city) customer_city,
			customer_state
		FROM bronze.app_customers;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.app_geolocation
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.app_geolocation';
		TRUNCATE TABLE silver.app_geolocation;
		PRINT '>> Inserting Data Into: silver.app_geolocation';
		INSERT INTO silver.app_geolocation (
			geolocation_zip_code_prefix,
			geolocation_lat,
			geolocation_lng,
			geolocation_city,
			geolocation_state
		)
		SELECT
			REPLACE(geolocation_zip_code_prefix, '"', '') geolocation_zip_code_prefix,
			geolocation_lat,
			geolocation_lng,
			REGEXP_REPLACE(geolocation_city, '["`´.*4º ]', '') geolocation_city,
			REGEXP_REPLACE(geolocation_state, '[^A-Z]', '') geolocation_state
		FROM bronze.app_geolocation;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-------------------------------------------------

		-- Loading silver.app_order_items
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.app_order_items';
		TRUNCATE TABLE silver.app_order_items;
		PRINT '>> Inserting Data Into: silver.app_order_items';
		INSERT INTO silver.app_order_items (
			order_id,
			order_item_id,
			product_id,
			seller_id,
			shipping_limit_date,
			price,
			freight_value
		)
		SELECT
			REPLACE(order_id, '"', '') order_id,
			order_item_id,
			REPLACE(product_id, '"', '')  product_id,
			REPLACE(seller_id, '"', '') seller_id,
			shipping_limit_date,
			price,
			freight_value
		FROM bronze.app_order_items;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.app_order_payments
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.app_order_payments';
		TRUNCATE TABLE silver.app_order_payments;
		PRINT '>> Inserting Data Into: silver.app_order_payments';
		INSERT INTO silver.app_order_payments (
			order_id,
			payment_sequential,
			payment_type,
			payment_installments,
			payment_value
		)
		SELECT
			REPLACE(order_id, '"', '') order_id,
			payment_sequential,
			payment_type,
			REPLACE(payment_installments, 0, 1) payment_installments,
			payment_value
		FROM bronze.app_order_payments;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.app_order_reviews
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.app_order_reviews';
		TRUNCATE TABLE silver.app_order_reviews;
		PRINT '>> Inserting Data Into: silver.app_order_reviews';
		INSERT INTO silver.app_order_reviews (
			review_id,
			order_id,
			review_score,
			review_comment_title,
			review_comment_message,
			review_creation_date,
			review_answer_timestamp,
			review_id_count,
			order_id_count
		)
		SELECT
			review_id,
			order_id,
			review_score,
			CASE
				WHEN review_comment_title IS NULL THEN 'n/a'
				WHEN review_comment_title = '' THEN 'n/a'
				ELSE TRIM(review_comment_title)
			END review_comment_title,
			CASE
				WHEN review_comment_message IS NULL THEN 'n/a'
				WHEN review_comment_message = '' THEN 'n/a'
				ELSE TRIM(review_comment_message)
			END review_comment_message,
			review_creation_date,
			review_answer_timestamp,
			COUNT(review_id) OVER(PARTITION BY review_id ORDER BY review_id) review_id_count,
			COUNT(order_id) OVER(PARTITION BY order_id ORDER BY order_id) order_id_count
		FROM bronze.app_order_reviews;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.app_order_reviews
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.app_order_reviews';
		TRUNCATE TABLE silver.app_order_reviews;
		PRINT '>> Inserting Data Into: silver.app_order_reviews';
		WITH HandleNullCTE AS (
			SELECT
				REPLACE(order_id, '"', '') order_id,
				REPLACE(customer_id, '"', '') customer_id,
				order_status,
				order_purchase_timestamp,
				CASE
					WHEN order_approved_at IS NULL AND order_purchase_timestamp IS NOT NULL AND order_status != 'canceled' AND order_status != 'unavailable' THEN order_purchase_timestamp
					ELSE order_approved_at
				END order_approved_at,
				CASE
					WHEN order_delivered_carrier_date IS NULL AND order_delivered_customer_date IS NOT NULL THEN order_delivered_customer_date
					ELSE order_delivered_carrier_date
				END order_delivered_carrier_date,
				CASE
					WHEN order_delivered_carrier_date IS NULL AND order_delivered_carrier_date IS NOT NULL THEN order_delivered_carrier_date
					ELSE order_delivered_carrier_date
				END order_delivered_customer_date,
				order_estimated_delivery_date
			FROM bronze.app_orders
		),
		HandleDatetimeCTE AS (
			SELECT
				order_id,
				customer_id,
				order_status,
				order_purchase_timestamp,
				order_approved_at,
				CASE
					WHEN order_delivered_carrier_date < order_purchase_timestamp THEN order_delivered_customer_date
					ELSE order_delivered_carrier_date
				END order_delivered_carrier_date,
				CASE
					WHEN order_delivered_customer_date < order_purchase_timestamp THEN order_delivered_carrier_date
					ELSE order_delivered_customer_date
				END order_delivered_customer_date,
				order_estimated_delivery_date
			FROM HandleNullCTE
		),
		DatetimeFlagCTE AS (
		SELECT
			order_id,
			customer_id,
			order_status,
			order_purchase_timestamp,
			order_approved_at,
			order_delivered_carrier_date,
			order_delivered_customer_date,
			order_estimated_delivery_date,
			CASE
				WHEN order_delivered_carrier_date < order_purchase_timestamp OR order_delivered_customer_date < order_purchase_timestamp THEN 'invalid'
				WHEN order_delivered_carrier_date < order_approved_at OR order_delivered_customer_date < order_approved_at THEN 'invalid'
				ELSE 'valid'
			END datetime_flag
		FROM HandleDatetimeCTE
		),
		StatusFlagCTE AS (
			SELECT
				order_id,
				customer_id,
				order_status,
				order_purchase_timestamp,
				order_approved_at,
				order_delivered_carrier_date,
				order_delivered_customer_date,
				order_estimated_delivery_date,
				datetime_flag,
				CASE
					WHEN order_status = 'delivered' AND order_delivered_carrier_date IS NULL AND order_delivered_customer_date IS NULL THEN 'invalid'
					WHEN order_status != 'delivered' AND order_delivered_customer_date IS NOT NULL AND datetime_flag = 'invalid' THEN 'invalid'
					ELSE 'valid'
				END status_flag
			FROM DatetimeFlagCTE
		)
		INSERT INTO silver.app_orders (
			order_id,
			customer_id,
			order_status,
			order_purchase_timestamp,
			order_approved_at,
			order_delivered_carrier_date,
			order_delivered_customer_date,
			order_estimated_delivery_date,
			datetime_flag,
			status_flag
		)
		SELECT
			*
		FROM StatusFlagCTE;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.app_products
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.app_products';
		TRUNCATE TABLE silver.app_products;
		PRINT '>> Inserting Data Into: silver.app_products';
		INSERT INTO silver.app_products (
			product_id,
			product_category_name,
			product_name_lenght,
			product_description_lenght,
			product_photos_qty,
			product_weight_g,
			product_length_cm,
			product_height_cm,
			product_width_cm
		)
		SELECT
			REPLACE(product_id, '"', '') product_id,
			TRIM(COALESCE(product_category_name, 'n/a')) product_category_name,
			COALESCE(product_name_lenght, 0) product_name_lenght,
			COALESCE(product_description_lenght, 0) product_description_lenght,
			COALESCE(product_photos_qty, 0) product_photos_qty,
			COALESCE(product_weight_g, 0) product_weight_g,
			COALESCE(product_length_cm, 0 ) product_length_cm,
			COALESCE(product_height_cm, 0) product_height_cm,
			COALESCE(product_width_cm, 0) product_width_cm
		FROM bronze.app_products;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.app_product_category_name_translation
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.app_product_category_name_translation';
		TRUNCATE TABLE silver.app_product_category_name_translation;
		PRINT '>> Inserting Data Into: silver.app_product_category_name_translation';
		INSERT INTO silver.app_product_category_name_translation (
			product_category_name,
			product_category_name_english
		)
		SELECT
			product_category_name,
			product_category_name_english
		FROM bronze.app_product_category_name_translation;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.app_sellers
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.app_sellers';
		TRUNCATE TABLE silver.app_sellers;
		PRINT '>> Inserting Data Into: silver.app_sellers';
		INSERT INTO silver.app_sellers (
			seller_id,
			seller_zip_code_prefix,
			seller_city,
			seller_state
		)
		SELECT
			REGEXP_REPLACE(seller_id, '[^a-zA-Z0-9]', '') seller_id,
			REGEXP_REPLACE(seller_zip_code_prefix, '[^0-9]', '') seller_zip_code_prefix,
			UPPER(REPLACE(seller_city, '"', '')) seller_city,
			REGEXP_REPLACE(TRIM(seller_state), '[^A-Z]', '')  seller_state
		FROM bronze.app_sellers;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.dates
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.dates';
		TRUNCATE TABLE silver.dates;
		PRINT '>> Inserting Data Into: silver.dates';
		WITH datesCTE AS (
		    SELECT
				CAST('2016-01-01' AS DATE) full_date
		    UNION ALL
		    SELECT DATEADD(DAY, 1, full_date)
		    FROM datesCTE
		    WHERE full_date < '2018-12-31'
		)
		INSERT INTO silver.dates (
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
		)
		SELECT
		    CAST(FORMAT(full_date, 'yyyyMMdd') AS INT) date_key,
		    full_date,
		    DAY(full_date) day,
		    DATENAME(WEEKDAY, full_date) day_name,
		    DATEPART(WEEKDAY, full_date) day_of_week,
		    CASE 
		        WHEN DATEPART(WEEKDAY, full_date) IN (1, 7) 
		        THEN 1 ELSE 0 
		    END is_weekend,
		    DATEPART(WEEK, full_date) week_of_year,
		    MONTH(full_date) month,
		    DATENAME(MONTH, full_date) month_name,
		    DATEPART(QUARTER, full_date) quarter,
		    CONCAT('Q', DATEPART(QUARTER, full_date)) quarter_name,
		    YEAR(full_date) year
		FROM datesCTE
		OPTION (MAXRECURSION 0);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '==========================================';
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';

	END TRY
	BEGIN CATCH
		PRINT '==========================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '==========================================';
	END CATCH
END
