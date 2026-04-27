/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME2, @end_time DATETIME2, @batch_start_time DATETIME2, @batch_end_time DATETIME2;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '======================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '======================================================';

		PRINT '------------------------------------------------------';
		PRINT 'Loading APP Tables';
		PRINT '------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.app_customers'
		TRUNCATE TABLE bronze.app_customers;
		PRINT '>> Inserting Data Into: bronze.app_customers'
		BULK INSERT bronze.app_customers
		FROM 'C:\Users\ariwp\Documents\DataAnalysis\SQL\olist_data_warehouse\datasets\source_app\customers.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.app_geolocation'
		TRUNCATE TABLE bronze.app_geolocation;
		PRINT '>> Inserting Data Into: bronze.app_geolocation'
		BULK INSERT bronze.app_geolocation
		FROM 'C:\Users\ariwp\Documents\DataAnalysis\SQL\olist_data_warehouse\datasets\source_app\geolocation.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.app_order_items'
		TRUNCATE TABLE bronze.app_order_items;
		PRINT '>> Inserting Data Into: bronze.app_order_items'
		BULK INSERT bronze.app_order_items
		FROM 'C:\Users\ariwp\Documents\DataAnalysis\SQL\olist_data_warehouse\datasets\source_app\order_items.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.app_order_payments'
		TRUNCATE TABLE bronze.app_order_payments;
		PRINT '>> Inserting Data Into: bronze.app_order_payments'
		BULK INSERT bronze.app_order_payments
		FROM 'C:\Users\ariwp\Documents\DataAnalysis\SQL\olist_data_warehouse\datasets\source_app\order_payments.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.app_order_reviews'
		TRUNCATE TABLE bronze.app_order_reviews;
		PRINT '>> Inserting Data Into: bronze.app_order_reviews'
		BULK INSERT bronze.app_order_reviews
		FROM 'C:\Users\ariwp\Documents\DataAnalysis\SQL\olist_data_warehouse\datasets\source_app\order_reviews.csv'
		WITH (
			FORMAT = 'CSV',
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		TRUNCATE TABLE bronze.app_orders;
		BULK INSERT bronze.app_orders
		FROM 'C:\Users\ariwp\Documents\DataAnalysis\SQL\olist_data_warehouse\datasets\source_app\orders.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.app_product_category_name_translation'
		TRUNCATE TABLE bronze.app_product_category_name_translation;
		PRINT '>> Inserting Data Into: bronze.app_product_category_name_translation'
		BULK INSERT bronze.app_product_category_name_translation
		FROM 'C:\Users\ariwp\Documents\DataAnalysis\SQL\olist_data_warehouse\datasets\source_app\product_category_name_translation.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.app_products'
		TRUNCATE TABLE bronze.app_products;
		PRINT '>> Inserting Data Into: bronze.app_products'
		BULK INSERT bronze.app_products
		FROM 'C:\Users\ariwp\Documents\DataAnalysis\SQL\olist_data_warehouse\datasets\source_app\products.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.app_sellers'
		TRUNCATE TABLE bronze.app_sellers;
		PRINT '>> Inserting Data Into: bronze.app_sellers'
		BULK INSERT bronze.app_sellers
		FROM 'C:\Users\ariwp\Documents\DataAnalysis\SQL\olist_data_warehouse\datasets\source_app\sellers.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '==========================================';
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '==========================================';
	END TRY
	BEGIN CATCH
		PRINT '======================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '======================================================';
	END CATCH
END
