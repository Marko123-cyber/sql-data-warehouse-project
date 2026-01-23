/* 
Script to show all tables at once
*/


USE DataWarehouse;
GO
CREATE OR ALTER PROCEDURE silver.show_tables as
BEGIN

select * 
from silver.crm_cust_info; 


select * 
from silver.crm_prd_info; 

select * 
from silver.crm_sales_details; 

select * 
from silver.erp_cust_az12; 

select * 
from silver.erp_loc_a101; 

select * 
from silver.erp_px_cat_g1v2; 

END;
go


EXEC silver.show_tables;
