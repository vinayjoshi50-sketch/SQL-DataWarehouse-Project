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

-- Stored Procedure
create or alter procedure bronze.load_bronze as 
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
		set @batch_start_time = getdate() ;
		print'========================================';
		print'Loading Bronze Layer';
		print'========================================';


		print'----------------------------------------';
		print'Loading CRM Tables';
		print'----------------------------------------';

		set @start_time = getdate();
		print'>> Truncating Table: bronze.crm_cust_info '
		truncate table bronze.crm_cust_info;

		print'>> Inserting data into : bronze.crm_cust_info '
		bulk insert bronze.crm_cust_info
		from 'C:\Users\vjosh\Downloads\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print'>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
		print'----------------------------------------';

		set @start_time = getdate();
		print'>> Truncating Table: bronze.crm_prd_info ' 
		truncate table bronze.crm_prd_info;

		print'>> Inserting data into : bronze.crm_prd_info '
		bulk insert bronze.crm_prd_info
		from 'C:\Users\vjosh\Downloads\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print'>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
		print'----------------------------------------';


		set @start_time = getdate();
		print'>> Truncating Table: bronze.crm_sales_details ' 
		truncate table bronze.crm_sales_details;

		print'>> Inserting data into : bronze.crm_sales_details '
		bulk insert bronze.crm_sales_details
		from 'C:\Users\vjosh\Downloads\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print'>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
		print'----------------------------------------';

		print'----------------------------------------';
		print'Loading ERP Tables';
		print'----------------------------------------';
	

		set @start_time = getdate();
		print'>> Truncating Table: bronze.erp_cust_az12 '
		truncate table bronze.erp_cust_az12;

		print'>> Inserting data into : bronze.erp_cust_az12 '
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\vjosh\Downloads\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print'>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
		print'----------------------------------------';


		set @start_time = getdate();
		print'>> Truncating Table: bronze.erp_loc_a101 '
		truncate table bronze.erp_loc_a101;

		print'>> Inserting data into : bronze.erp_loc_a101 '
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\vjosh\Downloads\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print'>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
		print'----------------------------------------';


		set @start_time = getdate();
		print'>> Truncating Table: bronze.erp_px_cat_g1v2 '
		truncate table bronze.erp_px_cat_g1v2;

		print'>> Inserting data into : bronze.erp_px_cat_g1v2 '
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\vjosh\Downloads\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print'>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
		print'----------------------------------------';

		set @batch_end_time = getdate();
		print '=================================================='
		print 'Loading Bronze Layer is Completed'
		print '  -Total Load Duration: ' + cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar) + ' seconds';
		print '=================================================='
	end try
	begin catch
		print '=================================================='
		print 'Error Ocurred During Loading Bronze Layer '
		print 'Error Message' + Error_Message();
		print 'Error Message' + cast(Error_Number () as nvarchar);
		print 'Error Message' + cast(Error_State() as nvarchar);
		print '=================================================='
	end catch
end 
