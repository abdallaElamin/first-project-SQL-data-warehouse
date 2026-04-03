/*
====================================================================================================
Stored Procedure: Load Bronze Layer(Source --> Bronze)
====================================================================================================
Script Purpose:
  This stored procedure loads data into the 'bronze' schema from external CSV files.
  It performs the following actions:
    -Truncates the bronze tables before loading data.
    -Use the 'Bulk Insert' command to load data from CSV files to bronze tables.

Parameters:
  None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
	EXEC bronze.load_bronze;
====================================================================================================
*/

create or alter procedure bronze.load_bronze as
begin
declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime
	begin try
		print '===================================================';
		print 'Loading Bronze Layer';
		print '===================================================';


		print '---------------------------------------------------';
		print 'Loading CRM Tables';
		print '---------------------------------------------------';

		set @batch_start_time = getdate()
		print'** Table Name: bronze.crm_cust_info **'
		set @start_time = getdate();
		print '>> Truncating Table: bronze.crm_cust_info'
		truncate table bronze.crm_cust_info;

		print '>> inserting Data into: bronze.crm_cust_info'
		bulk insert Bronze.crm_cust_info
		from 'D:\First_DW_Project\Dataset\source_crm\cust_info.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time = getdate();
		print '>> Loading duration is ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Second';


		print'>> --------------------------------------------------'
		print'** Table Name: bronze.crm_prd_info **'

		set @start_time = getdate();
		print '>> Truncating Table: bronze.crm_prd_info'
		truncate table bronze.crm_prd_info;

		print '>> inserting Data into: bronze.crm_prd_info'
		bulk insert Bronze.crm_prd_info
		from 'D:\First_DW_Project\Dataset\source_crm\prd_info.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time = getdate();
		print '>> Loading duration is ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' Second';


		print'>> --------------------------------------------------'
		print'** Table Name: bronze.crm_sales_details **'

		set @start_time = getdate();
		print '>> Truncating Table: bronze.crm_sales_details'
		truncate table bronze.crm_sales_details;

		print '>> inserting Data into: bronze.crm_sales_details'
		bulk insert Bronze.crm_sales_details
		from 'D:\First_DW_Project\Dataset\source_crm\sales_details.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time = getdate();
		print '>> Loading duration is ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' Second';



		print '---------------------------------------------------';
		print 'Loading ERP Tables';
		print '---------------------------------------------------';
		
		print'** Table Name: bronze.erp_cust_az12 **'
		set @start_time = getdate();
			print '>> Truncating Table: bronze.erp_cust_az12'
			truncate table bronze.erp_cust_az12;

			print '>> inserting Data into: Bronze.erp_cust_az12'
			bulk insert Bronze.erp_cust_az12
			from 'D:\First_DW_Project\Dataset\source_erp\CUST_AZ12.csv'
			with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
			);
		set @end_time = getdate();
		print '>> Loading duration is ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' Second';


		print'>> --------------------------------------------------'
		print'** Table Name: bronze.erp_loc_a101 **'

		set @start_time = getdate();
			print '>> Truncating Table: bronze.erp_loc_a101'
			truncate table bronze.erp_loc_a101;

			print '>> inserting Data into: Bronze.erp_loc_a101'
			bulk insert Bronze.erp_loc_a101
			from 'D:\First_DW_Project\Dataset\source_erp\LOC_A101.csv'
			with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
			);
		set @end_time = getdate();
		print '>> Loading duration is ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' Second';


		print'>> --------------------------------------------------'
		print'** Table Name: bronze.erp_px_cat_g1v2 **'

		set @start_time = getdate();
			print '>> Truncating Table: bronze.erp_px_cat_g1v2'
			truncate table bronze.erp_px_cat_g1v2;

			print '>> inserting Data into: Bronze.erp_px_cat_g1v2'
			bulk insert Bronze.erp_px_cat_g1v2
			from 'D:\First_DW_Project\Dataset\source_erp\PX_CAT_G1V2.csv'
			with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
			);
		set @end_time = getdate();
		print '>> Loading duration is ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Second';

		set @batch_end_time = getdate()
		print'>> --------------------------------------------------'
		print '** Full Time for Loading **'
		print 'Time of loading bronze layer is '+ cast (datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' Second'
	
	end try
	begin catch
		print '======================================================';
		print 'Error Message Occured During Bronze Layer';
		print 'Error Message' + error_message();
		print 'Error Message' + cast (error_number() as nvarchar);
		print 'Error Message' + cast (error_state() as nvarchar);
		print '======================================================';
	end catch;
end;
