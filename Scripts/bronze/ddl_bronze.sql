/*
================================================================================
DDL Script: Create Bronze Tables
================================================================================

Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables
    if they already exist.

    Run this script to re-define the DDL structure of 'bronze' tables.

================================================================================
*/
Use DataWareHouse;
GO

Create or alter procedure bronze.load_bronze as 
BEGIN  
	BEGIN TRY
		DECLARE @batch_start_time datetime;
		DECLARE @batch_end_time datetime;
		DECLARE @start_time datetime;
		DECLARE @end_time datetime;
		SET @batch_start_time = getdate();
		SET @start_time = getdate(); 
		PRINT '=====================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=====================================';
		Print 'Truncating bronze.crm ';
		Truncate table bronze.crm_cust_info  

		bulk insert bronze.crm_cust_info
		from 'C:\Users\kaifu\OneDrive\Documents\BA\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);  


		-- select count (*) from bronze.crm_prd_info

		Truncate table bronze.crm_prd_info

		bulk insert bronze.crm_prd_info
		from 'C:\Users\kaifu\OneDrive\Documents\BA\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		-- select count (*) from bronze.crm_sales_details

		Truncate table bronze.crm_sales_details

		bulk insert bronze.crm_sales_details
		from 'C:\Users\kaifu\OneDrive\Documents\BA\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		SET @end_time = getdate();
		Print 'time it took was ' + cast(datediff ( second , @start_time , @end_time )as nvarchar) + ' seconds';

		-- select count (*) from bronze.crm_sales_details
		Print 'Truncating bronze.erp ';
		Truncate table bronze.erp_cust_az12

		bulk insert bronze.erp_cust_az12
		from 'C:\Users\kaifu\OneDrive\Documents\BA\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		-- select count (*) from bronze.erp_cust_az12

		Truncate table bronze.erp_loc_a101

		bulk insert bronze.erp_loc_a101
		from 'C:\Users\kaifu\OneDrive\Documents\BA\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
			with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		-- select count (*) from bronze.erp_loc_a101

		Truncate table bronze.erp_px_cat_g1v2

		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\kaifu\OneDrive\Documents\BA\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		-- select count (*) from bronze.erp_px_cat_g1v2

		SET @batch_end_time = getdate();
		Print  '----------------------------------------------------------------';
		print 'it took '+ cast (datediff(second , @batch_start_time ,@batch_end_time) as nvarchar) +' seconds for the bronze layer to load';
		Print  '----------------------------------------------------------------';
	END TRY
	BEGIN CATCH 
		Print  '--------------------------------';
		Print  'ERROR'+ ERROR_MESSAGE() + CAST( ERROR_NUMBER() AS Nvarchar);
		Print  '--------------------------------';
	END CATCH 
END  
