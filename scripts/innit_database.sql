/* 
Script creates a DataWarehouse database with bronze, silver and gold schemas. After that it checks if schemas are created. 
Warning: By running this script DataWarehouse dataset will be recreated and you'll lose all the data from it unless you've created a backup. 
*/
USE master; 


IF EXISTS(SELECT 1 from sys.databases where name='DataWarehouse')
    BEGIN
        ALTER DATABASE DataWarehouse 
        SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
        DROP DATABASE DataWarehouse;
    END;

CREATE DATABASE DataWarehouse;
go
USE DataWarehouse
go

CREATE SCHEMA bronze; 
go 

CREATE SCHEMA silver; 
go 

CREATE SCHEMA gold;
go

SELECT * FROM 
sys.schemas 
WHERE name in ('bronze', 'silver', 'gold');
