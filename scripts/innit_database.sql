/*
---------------------------------------------------
---------------------------------------------------

Script Purpose:
    This script creates a new databse DataWarehouse after checking if it exists. If it exists it is dropped
    and recreated. Within DataWarehouse 3 schemas are created bronze, silver and gold.

Warning:
    Running this script will drop DataWarehouse database if it already exists. All data from the database will 
    get permanently deleted. Proceed causously and ensure you have proper backups before run ning script.
*/


USE master;

if exists(select 1 from sys.databases where name='DataWarehouse')
begin
    alter database DataWarehouse set SINGLE_USER  with rollback immediate;
    drop database  DataWarehouse;
end;
go


create database  DataWarehouse;
go

use DataWarehouse;
go

create  schema bronze;
go

create  schema silver;
go

create  schema gold;
go
