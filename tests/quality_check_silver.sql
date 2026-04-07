/*
========================================================================================
Quality Checks
========================================================================================
Srcipt Purpose:
This script perform various quality checks for data consistency, accurancy, and standardization
across the 'silver' schema.
It includes checks for:
- Null or duplicate primary keys.
- Unwanted spaces in string fields.
- Data standardization and consistency.
- Invaild date ranges and orders.
- Data consistency between related fields.

Usage Notes:
- Run these checks after data loading silver layer.
- Investigate and resolve any discrepancies found during the checks.
========================================================================================
*/
-- ** crm_cust_info ** 

-- Check for Null or Duplicates in Primary Key
-- Expectation: No Results
select 
cst_id, count(*)
from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null

-- Check for unwanted spaces
-- Expectation: No Results
-- First Name
select 
cst_firstname
from silver.crm_cust_info
where cst_firstname != trim(cst_firstname)

-- Last Name
select 
cst_lastname 
from silver.crm_cust_info
where cst_lastname != trim(cst_lastname)

-- Gender
select 
cst_gndr 
from silver.crm_cust_info
where cst_gndr != trim(cst_gndr)

-- Data Standardization & consistency
select 
distinct cst_gndr 
from silver.crm_cust_info

select 
distinct cst_material_status 
from silver.crm_cust_info

-- Check data on silver.crm_cust_info at all
select * from silver.crm_cust_info

/*
===========================================================================================
*/
-- ** crm_prd_info ** 
-- Check for Null or Duplicates in Primary Key
-- Expectation: No Results
  select prd_id, count(*)
  from silver.crm_prd_info
  group by prd_id
  having count(*) > 1 or prd_id is null

-- Check for unwanted spaces
-- Expectation: No Results
select prd_nm
from silver.crm_prd_info
where prd_nm != trim(prd_nm)

-- check for Null or Nagative numbers
-- Exepectation: No Result
select prd_cost,* 
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null

-- Data Standardization & consistency
select distinct prd_line 
from silver.crm_prd_info

-- Check for Invalid Date Orders
select *
from silver.crm_prd_info
where prd_end_dt < prd_start_dt

/*
=====================================================================================
*/
-- ** crm_Sales_details **
-- Check for unwanted spaces
-- Expectation: No Results
select * 
from silver.crm_sales_details
where sls_ord_num != trim(sls_ord_num)

-- Check all products are part of silver.crm_prd_info
select * 
from silver.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info)

-- Check all customers are part of silver.crm_prd_info
select * 
from silver.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info)

select *
    from silver.crm_sales_details
    where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

-- Check consistency between: Sales, Quantity, and Price
-->> Sales = Quantity * Price
-->> Value must not be Null, Zero, or Nagative.
select distinct
sls_sales,
sls_quantity,
sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0

select * from silver.crm_sales_details


/*
=====================================================================================
*/
-- ** erp_cust_az12 **
-- cid column in silver.erp_cust_az12 table must be match cst_key in silver.crm_cust_info table
select 
    cid,
    bdate,
    gen 
from silver.erp_cust_az12
where cid not in (select cst_key from silver.crm_cust_info)

-- Identify Out_of Range Date
select 
    bdate
from silver.erp_cust_az12
where bdate > GETDATE()

-- Data Standardization & consistency
select distinct
    gen
from silver.erp_cust_az12

select * from silver.erp_cust_az12

/*
=====================================================================================
*/
-- ** erp_loc_a101 **
-- cid column in silver.erp_loc_a101 table must be match cst_key in silver.crm_cust_info table
select 
cid,
cntry
from silver.erp_loc_a101 
where cid 
not in (select cst_key from silver.crm_cust_info)

-- Data Standardization & consistency
select distinct
     cntry
from silver.erp_loc_a101

select * from silver.erp_loc_a101

/*
=====================================================================================
*/
-- ** erp_px_cat_g1v2 **
-- cid column in silver.erp_px_cat_g1v2 table must be match cst_key in silver.crm_prd_info table
select id 
from silver.erp_px_cat_g1v2
where id not in (select cat_id from silver.crm_prd_info)

-- Check Unwanted Spaces
Select * 
from silver.erp_px_cat_g1v2
where cat != trim(cat) 
or subcat != trim(subcat)
or maintenace != trim(maintenace)

-- Data Standardization & consistency
select distinct 
cat
from silver.erp_px_cat_g1v2

select distinct 
subcat
from silver.erp_px_cat_g1v2

select distinct 
maintenace
from silver.erp_px_cat_g1v2

select * from silver.erp_px_cat_g1v2
