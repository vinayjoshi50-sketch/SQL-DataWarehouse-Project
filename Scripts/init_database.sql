/*=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	*/

use master ;
go

-- Drop and recreate the 'DataWarehouse' database
if exists (select 1 from sys.databases where name ='Datawarehouse')
begin
    alter database DataWarehouse set single_user with rollback immediate ;
    drop database DataWarehouse;

end;
go

-- Create the database 'DataWarehouse'
create database DataWarehouse;
go

use DataWarehouse
go

-- Create Schemas
create schema bronze;
go

create schema silver;
go

create schema gold;
go
