/*
========================================================================================
Data Transformation 
========================================================================================
Srcipt Purpose:
  This script perform various data transformation like consistency, accuracy, and standardization
  across the 'silver' schema.

It includes Transformation:
  - Null or duplicate primary keys.
  - Unwanted spaces in string fields.
  - Data standardization and consistency.
  - Invaild date ranges and orders.
  - Data consistency between related fields.

Usage Notes:
  - Run these transformation before data loading silver layer.
========================================================================================
*/
-- ** crm_cust_info **
-- Check for Null or Duplicates in Primary Key
-- Expectation: No Results

select 
cst_id, count(*)
from bronze.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null

-- Check for unwanted spaces
-- Expectation: No Results
-- First Name
select *
--cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname)

-- Last Name
select 
cst_lastname 
from bronze.crm_cust_info
where cst_lastname != trim(cst_lastname)

-- Gender
select 
cst_gndr 
from bronze.crm_cust_info
where cst_gndr != trim(cst_gndr)

-- Data Standardization & consistency
select 
distinct cst_gndr 
from bronze.crm_cust_info

select 
distinct cst_material_status 
from bronze.crm_cust_info

/*
=====================================================================================
*/
-- ** crm_prd_info ** 
-- Check for Null or Duplicates in Primary Key
-- Expectation: No Results
  select prd_id, count(*)
  from bronze.crm_prd_info
  group by prd_id
  having count(*) > 1 or prd_id is null

-- Check for unwanted spaces
-- Expectation: No Results
select prd_nm
from bronze.crm_prd_info
where prd_nm != trim(prd_nm)

-- check for Null or Nagative numbers
-- Exepectation: No Result
select prd_cost,* 
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null

-- Data Standardization & consistency
select distinct prd_line 
from bronze.crm_prd_info

-- Check for Invalid Date Orders
select *,
lead (prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as prd_end_dt_test
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

/*
=====================================================================================
*/
-- ** crm_Sales_details **
-- Check for unwanted spaces
-- Expectation: No Results
select * 
from bronze.crm_sales_details
where sls_ord_num != trim(sls_ord_num)

-- Check all products are part of silver.crm_prd_info
select * 
from bronze.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info)

-- Check all customers are part of silver.crm_prd_info
select * 
from bronze.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info)

-- Check for Invalid Dates

         -- ** sls_ship_dt **
select 
    nullif (sls_order_dt,0) as sls_order_dt -- convert it 0 to null 
    from bronze.crm_sales_details
    where sls_order_dt <= 0 or
          len (sls_order_dt) != 8 or        -- check the length of field must be 8
          sls_order_dt > 20500101 or
          sls_order_dt < 19000101           -- check for outliers by validing the bounderies of the date range 

          -- ** sls_ship_dt **
select sls_ship_dt
    from bronze.crm_sales_details
    where sls_ship_dt <= 0 or 
          len (sls_ship_dt) != 8 or        -- check the length of field must be 8
          sls_ship_dt > 20500101 or
          sls_ship_dt < 19000101           -- check for outliers by validing the bounderies of the date range 

          -- ** sls_due_dt **
select sls_due_dt 
    from bronze.crm_sales_details
    where sls_due_dt <= 0 or 
          len (sls_due_dt) != 8 or        -- check the length of field must be 8
          sls_due_dt > 20500101 or
          sls_due_dt < 19000101           -- check for outliers by validing the bounderies of the date range 

select *
    from bronze.crm_sales_details
    where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

-- Check consistency between: Sales, Quantity, and Price
-->> Sales = Quantity * Price
-->> Value must not be Null, Zero, or Nagative.
select distinct
sls_sales as old_sales,
sls_quantity,
sls_price as old_price,
case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)
     then sls_quantity * ABS(sls_price)
     else sls_sales
end as sls_sales,
case when sls_price is null or sls_price <= 0
     then sls_sales / nullif(sls_quantity,0)
     else sls_price
end as sls_price

from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0

/*
=====================================================================================
*/
-- ** erp_cust_az12 **
-- cid column in bronze.erp_cust_az12 table must be match cst_key in silver.crm_cust_info table
select 
case when cid like 'NAS%' then substring (cid, 4, len(cid))
     else cid
end as cid,
bdate,
gen 
from bronze.erp_cust_az12
where case when cid like 'NAS%' then substring (cid, 4, len(cid))
     else cid
end not in (select cst_key from silver.crm_cust_info)

-- Identify Out_of Range Date
select 
  case when bdate > getdate() then Null
       else bdate
  end as bdate
from bronze.erp_cust_az12
where bdate > GETDATE()

-- Data Standardization & consistency
select distinct
  gen as old_gen,
  case when upper(trim(gen)) in ('Female', 'F') then 'Female'
       when upper(trim(gen)) in ('Male', 'M') then 'Male'
       else 'N/A'
  end as gen
from bronze.erp_cust_az12

/*
=====================================================================================
*/
-- ** erp_loc_a101 **
-- cid column in bronze.erp_loc_a101 table must be match cst_key in silver.crm_cust_info table
select 
cid as old_cid,
replace(cid, '-', '') as cid
from bronze.erp_loc_a101 
where replace(cid, '-', '') 
not in (select cst_key from silver.crm_cust_info)

-- Data Standardization & consistency
-- Normalize and Handle all Missing or Blank Country Codes
select distinct
    cntry as old_cntry,
    case when upper(trim(cntry)) in ('DE', 'GERMANY') then 'Germany'
         when upper(trim(cntry)) in ('US', 'USA', 'UNITED STATES') then 'United States'
         when upper(trim(cntry)) = 'AUSTRALIA' then 'Australia'
         when upper(trim(cntry)) = 'UNITED KINGDOM' then 'United Kingdom'
         when upper(trim(cntry)) = 'CANADA' then 'Canada'
         when upper(trim(cntry)) = 'FRANCE' then 'France'
         else 'N/A'
    end as cntry
from bronze.erp_loc_a101

/*
=====================================================================================
*/
-- ** erp_px_cat_g1v2 **
-- cid column in bronze.erp_px_cat_g1v2 table must be match cst_key in silver.crm_prd_info table
select id 
from bronze.erp_px_cat_g1v2
where id not in (select cat_id from silver.crm_prd_info)

-- Check Unwanted Spaces
Select * 
from bronze.erp_px_cat_g1v2
where cat != trim(cat) 
or subcat != trim(subcat)
or maintenace != trim(maintenace)

-- Data Standardization & consistency
select distinct 
cat
from bronze.erp_px_cat_g1v2

select distinct 
subcat
from bronze.erp_px_cat_g1v2

select distinct 
maintenace
from bronze.erp_px_cat_g1v2
