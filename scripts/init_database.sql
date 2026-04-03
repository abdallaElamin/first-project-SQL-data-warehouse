/*
===================================================================
Create Database and Schemas
===================================================================
Script Purpose
	This script creates a new database named 'Datawarehouse' after checking if it already exists.
	if the database exists, it is dropped and recreated Additionally, the script sets up three schemas
	within the database: 'Bronze', 'Silver', and 'Gold'

Warning:
	Running this script will drop the entire 'Datawarehose' database if it exists.
	All data in the database will be permanently deleted.
	Proceed with caution and ensure you have proper backups before running this script.
*/


use master
Go

-- Drop and recreate the 'Datawarehouse' database
if exists (select * from sys.databases where name= 'datawarehouse')
begin
  alter database datawarehouse set single_user with rollback immediate;
  drop database Datawarehouse;
end;
Go

-- Create database
create database Datawarehouse;
Go 

use datawarehouse;
Go

-- Create Schemas
create schema Bronze;
Go

create schema Silver;
Go

create schema Gold;
