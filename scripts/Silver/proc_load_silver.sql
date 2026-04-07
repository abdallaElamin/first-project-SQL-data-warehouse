/*
=========================================================================================
Store Procedure: Load Silver Layer (Bronze --> Silver)
=========================================================================================
Script Pupose:
  This stored procedure performe the ETL (Extract, Transform, Load) process to
  populate the 'silver' schema tables from the 'bronze' schema

Actions performed:
  - Truncates silver tables.
  - Inserts Transformed amd cleansed data from Bronze into Silver tables

Parameters:
  None.
  This stored procedure does not accept any parameters or return any values.

Usage Examples
  Exec silver.load_silver
========================================================================================
*/
Create or Alter Procedure  silver.load_silver as
begin
declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime
    begin try
        print '===================================================';
		print 'Loading Silver Layer';
		print '===================================================';


		print '---------------------------------------------------';
		print 'Loading CRM Tables';
		print '---------------------------------------------------';

        set @batch_start_time = getdate()
        print'** Table Name: silver.crm_cust_info **'
        set @start_time = getdate();
        Print '>> Truncating Table: silver.crm_cust_info'
        truncate table silver.crm_cust_info

        Print '>> Inserting Data Into: silver.crm_cust_info'
        Insert into silver.crm_cust_info(
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_material_status,
        cst_gndr,
        cst_create_date)
        select 
        cst_id,
        cst_key,
        trim(cst_firstname) as cst_firstname, -- To remove spaces
        trim(cst_lastname) as cst_lastname, -- To remove spaces
        case when trim(Upper(cst_material_status)) = ('S') then 'Single' -- to make data consistency
             when trim(Upper(cst_material_status)) = ('M') then 'Married' -- to make data consistency
             else 'N/A'
        end cst_material_status,
        case when trim(Upper(cst_gndr)) = ('F') then 'Female' -- to make data consistency
             when trim(Upper(cst_gndr)) = ('M') then 'Male' -- to make data consistency
             else 'N/A'
        end cst_gndr,
        cst_create_date
        from(
        select 
        *,
        row_number() over (partition by cst_id order by cst_create_date desc) as flag_last -- to remove duplicates and null
        from bronze.crm_cust_info) t 
        where flag_last = 1
        set @end_time = getdate();
		print '>> Loading duration is ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Second';

        
        print'>> --------------------------------------------------'
		print'** Table Name: silver.crm_prd_info **'

		set @start_time = getdate();
        Print '>> Truncating Table: silver.crm_prd_info'
        Truncate table silver.crm_prd_info

        Print '>> Inserting Data Into: silver.crm_prd_info'
        Insert into silver.crm_prd_info(
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt)
        SELECT prd_id,
               replace (substring(prd_key, 1, 5), '-', '_') as Cat_id,
               substring (prd_key, 7, len(prd_key)) as prd_key,
               prd_nm,
               isnull (prd_cost, 0) as prd_cost,
               case upper(trim(prd_line)) 
                    when 'M' then 'Mountain'
                    when 'R' then 'Road'
                    when 'S' then 'Other Sales'
                    when 'T' then 'Touring'
                    else 'N/A'
                end as prd_line,
               cast (prd_start_dt as date) as prd_start_dt,
               cast (lead (prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt
          FROM [Datawarehouse].[bronze].[crm_prd_info]
        set @end_time = getdate();
		print '>> Loading duration is ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' Second'

          
        print'>> --------------------------------------------------'
		print'** Table Name: silver.crm_sales_details **'

		set @start_time = getdate();
          Print '>> Truncating Table: silver.crm_sales_details'
          Truncate table silver.crm_sales_details

          Print '>> Inserting Data Into: silver.crm_sales_details'
          Insert into silver.crm_sales_details(
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price)
        SELECT sls_ord_num,
               sls_prd_key,
               sls_cust_id,
               case when sls_order_dt = 0 or len(sls_order_dt) != 8 then Null
                    else cast (cast(sls_order_dt as nvarchar)as date) 
               end as sls_order_dt,
               case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then Null
                    else cast (cast(sls_ship_dt as nvarchar)as date) 
               end as sls_ship_dt,
               case when sls_due_dt = 0 or len(sls_due_dt) != 8 then Null
                    else cast (cast(sls_due_dt as nvarchar)as date) 
               end as sls_due_dt,
               case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)
                   then sls_quantity * ABS(sls_price)
                   else sls_sales
               end as sls_sales,
               sls_quantity, 
               case when sls_price is null or sls_price <= 0
                   then sls_sales / nullif(sls_quantity,0)
                   else sls_price
               end as sls_price
          FROM [Datawarehouse].[bronze].[crm_sales_details]
        set @end_time = getdate();
		print '>> Loading duration is ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' Second'


        print '---------------------------------------------------';
		print 'Loading ERP Tables';
		print '---------------------------------------------------';
       
		print'** Table Name: silver.erp_cust_az12 **'

		set @start_time = getdate();
        Print '>> Truncating Table: silver.erp_cust_az12'
        Truncate table silver.erp_cust_az12

        Print '>> Inserting Data Into: silver.erp_cust_az12'
        Insert into silver.erp_cust_az12(
            cid,
            bdate,
            gen)
        SELECT 
               case when cid like 'NAS%' then substring (cid, 4, len(cid))
                else cid
               end as cid,
               case when bdate > getdate() then Null
                else bdate
               end as bdate,
                case when upper(trim(gen)) in ('Female', 'F') then 'Female'
               when upper(trim(gen)) in ('Male', 'M') then 'Male'
                else 'N/A'
               end as gen
          FROM [Datawarehouse].[bronze].[erp_cust_az12]
        set @end_time = getdate();
		print '>> Loading duration is ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' Second'

  
        print'>> --------------------------------------------------'
		print'** Table Name: silver.erp_loc_a101 **'

		set @start_time = getdate();
        Print '>> Truncating Table: silver.erp_loc_a101'
        Truncate table silver.erp_loc_a101

        Print '>> Inserting Data Into: silver.erp_loc_a101'
        Insert into silver.erp_loc_a101(
            cid,
            cntry)
        SELECT 
            replace(cid, '-', '') as cid,
            case when upper(trim(cntry)) in ('DE', 'GERMANY') then 'Germany'
                 when upper(trim(cntry)) in ('US', 'USA', 'UNITED STATES') then 'United States'
                 when upper(trim(cntry)) = 'AUSTRALIA' then 'Australia'
                 when upper(trim(cntry)) = 'UNITED KINGDOM' then 'United Kingdom'
                 when upper(trim(cntry)) = 'CANADA' then 'Canada'
                 when upper(trim(cntry)) = 'FRANCE' then 'France'
                 else 'N/A'
            end as cntry
        FROM [Datawarehouse].[bronze].[erp_loc_a101]
        set @end_time = getdate();
		print '>> Loading duration is ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Second'


       
        print'>> --------------------------------------------------'
		print'** Table Name: silver.erp_px_cat_g1v2 **'

		set @start_time = getdate();
        Print '>> Truncating Table: silver.erp_px_cat_g1v2'
        Truncate table silver.erp_px_cat_g1v2

        Print '>> Inserting Data Into: silver.erp_px_cat_g1v2'
        Insert into silver.erp_px_cat_g1v2(
            id,
            cat,
            subcat,
            maintenace)
        SELECT id,
               cat,
               subcat,
               maintenace
          FROM [Datawarehouse].[bronze].[erp_px_cat_g1v2] 
        set @end_time = getdate();
		print '>> Loading duration is ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Second'

        set @batch_end_time = getdate()
		print'>> --------------------------------------------------'
		print '** Full Time for Loading **'
		print 'Time of loading Silver layer is '+ cast (datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' Second'
    end try
    begin catch
    	print '======================================================';
		print 'Error Message Occured During Bronze Layer';
		print 'Error Message' + error_message();
		print 'Error Message' + cast (error_number() as nvarchar);
		print 'Error Message' + cast (error_state() as nvarchar);
		print '======================================================';
    end catch
end;
