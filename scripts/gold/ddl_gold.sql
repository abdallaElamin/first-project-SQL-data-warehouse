/*
====================================================================================
DDL Script: Create Gold Tables
====================================================================================
Script Purpose:
  This script create tables for the gold layer in the data warehouse.
  The gold layer represents the final dimension and fact tables (Star Schema)
  
  Each table performs transformations and combines data from the silver layer
  to produce a clean, enriched, and business-ready dataset.

Usage:
  - These tables can be queried directly for analytics and reporting
====================================================================================
*/

if object_id ('gold.dim_customers_t', 'U') is not null
	drop table gold.dim_customers_t;
create table gold.dim_customers_t(
	customer_key int,
	customer_id int,
	customer_number nvarchar(50),
	first_name nvarchar(50),
	last_name nvarchar(50),
	country nvarchar(50),
	marital_status nvarchar(50),
	gender nvarchar(50),
	birthdate date,
	create_date date
);


if object_id ('gold.dim_products_t', 'U') is not null
	drop table gold.dim_products_t;
create table gold.dim_products_t(
	product_key int,
	product_id int,
	product_number nvarchar(50),
	product_name nvarchar(50),
	category_id nvarchar(50),
	category nvarchar(50),
	subcategory nvarchar(50),
	maintenace nvarchar(50),
	cost int,
	product_line nvarchar(50),
	start_date date
);

if object_id ('gold.fact_sales_t', 'U') is not null
	drop table gold.fact_sales_t;
create table gold.fact_sales_t(
	order_number nvarchar(50),
	product_key int,
	customer_key int,
	order_date date,
	shipping_date date,
	due_date date,
	sales_amount int,
	quantity int,
	price int
);
