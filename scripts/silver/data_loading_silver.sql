CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN

-- ==================
-- CRM customer info
-- ==================

-- Clean Table
TRUNCATE TABLE silver.crm_cust_info;

INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT 
cst_id, 
cst_key, 
TRIM(cst_firstname) AS cst_firstname, 
TRIM(cst_lastname) as cst_lastname,
CASE WHEN UPPER(cst_marital_status)='M' THEN 'Married'
     WHEN UPPER(cst_marital_status)='S' THEN 'Single'
     WHEN cst_marital_status IS NULL THEN 'n/a'
END AS cst_marital_status,
CASE WHEN UPPER(cst_gndr)='M' THEN 'Male'
     WHEN UPPER(cst_gndr)='F' THEN 'Female'
     WHEN cst_gndr IS NULL THEN 'n/a'
END AS cst_gndr,
cst_create_date 
FROM (
SELECT 
*, 
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) as rn
FROM  bronze.crm_cust_info
) t
where cst_id IS NOT NULL AND rn=1;



-- ======================
-- CRM product info
-- ======================

-- Cleaned table 
TRUNCATE TABLE silver.crm_prd_info;
INSERT INTO silver.crm_prd_info
(
    prd_id, 
    sls_prd_key, 
    id,
    prd_nm, 
    prd_cost, 
    prd_line, 
    prd_start_dt, 
    prd_end_dt
)
SELECT 
prd_id,
SUBSTRING(prd_key, 7, len(prd_key)) as sls_prd_key,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as id,
prd_nm, 
case when prd_cost is null then 0 
     else prd_cost 
end as prd_cost,
prd_line,
prd_start_dt,
case when prd_start_dt>prd_end_dt then DATEADD(DAY, -1, prd_end_dt_tset)
     else prd_end_dt 
end as prd_end_dt
FROM (
SELECT 
prd_id,
prd_key, 
prd_nm, 
case when prd_cost is null then 0 
     else prd_cost 
end as prd_cost,
case when prd_line='R' then 'Railing'
     when prd_line='S' then 'Stationary'
     when prd_line='M' then 'Mounted'
     when prd_line='T' then 'Tailored'
     when prd_line is null then 'n/a'
end as prd_line,
prd_start_dt, 
prd_end_dt, 
LEAD(prd_start_dt) over(PARTITION BY prd_key order by prd_start_dt) as prd_end_dt_tset
FROM bronze.crm_prd_info
) t;

/* 
=====================
CRM Sales Details
=====================
*/

-- Clean Table
TRUNCATE TABLE silver.crm_sales_details;
INSERT INTO silver.crm_sales_details
(
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt<=0 or len(sls_order_dt)!=8 then NULL 
     else cast(cast(sls_order_dt as nvarchar) as date)
end as sls_order_dt,
case when sls_ship_dt<=0 or len(sls_ship_dt)!=8 then NULL 
     else cast(cast(sls_ship_dt as nvarchar) as date)
end as sls_ship_dt,
case when sls_due_dt<=0 or len(sls_due_dt)!=8 then NULL 
     else cast(cast(sls_due_dt as nvarchar) as date)
end as sls_due_dt,
case when sls_sales is null or sls_sales<=0 or sls_sales!=sls_quantity*sls_price then sls_quantity*abs(sls_price)
     else sls_sales
end as sls_sales,
sls_quantity,
case when sls_price is null or sls_price=0 then sls_sales/nullif(sls_quantity, 0)
     when sls_price<0 then abs(sls_price)
     else sls_price
end as sls_price 
from bronze.crm_sales_details;


/* 
ERP CUST AZ12
*/

--Clean table
TRUNCATE TABLE silver.erp_cust_az12;
INSERT INTO silver.erp_cust_az12
(
    cid, 
    bdate, 
    gen
)
SELECT 
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))  
        ELSE cid
    END AS cid,
    CASE 
        WHEN bdate>GETDATE() THEN NULL 
        ELSE bdate
    END as bdate,
    CASE 
        WHEN UPPER(SUBSTRING(gen, 1, 1)) in ('F') then 'Female'
        WHEN UPPER(SUBSTRING(gen, 1, 1)) in ('M') then 'Male'
        ELSE 'n/a'
    END AS gen
FROM bronze.erp_cust_az12;


/* 
=====================
ERP Cust Loc
=====================
*/


--Clean table
TRUNCATE TABLE silver.erp_loc_a101;
INSERT INTO silver.erp_loc_a101
(
    cid, 
    cntry
)
SELECT 
     REPLACE(cid, '-', '') AS cid,
     CASE 
          WHEN cntry LIKE '%USA%' THEN 'United States'
          WHEN cntry LIKE '%US%' THEN 'United States'
          WHEN cntry LIKE '%DE%' THEN 'Germany'
          WHEN TRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(9), ''), CHAR(10), ''), CHAR(13), '')) = '' THEN 'n/a'
          ELSE TRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(9), ''), CHAR(10), ''), CHAR(13), ''))
     END AS cntry
FROM bronze.erp_loc_a101;



/* 
=======================
ERP PX CAT G1V2
=======================
*/

-- Clean table
TRUNCATE TABLE silver.erp_px_cat_g1v2;
INSERT INTO silver.erp_px_cat_g1v2
(
    id, 
    cat, 
    subcat, 
    maintanance
)
select 
id,
cat,
subcat,
case when maintanance like '%Yes%' then 'Yes'
     else 'No'
end as maintanance
from bronze.erp_px_cat_g1v2;

END;

GO

USE DataWarehouse;

GO

EXEC silver.load_silver;
