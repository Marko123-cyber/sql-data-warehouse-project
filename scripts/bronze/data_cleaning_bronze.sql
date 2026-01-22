
-- ==================
-- CRM customer info
-- ==================
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
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_id) as rn
FROM  bronze.crm_cust_info
) t
where cst_id IS NOT NULL AND rn=1;



-- Check to see if cst_id has unique indecies
select cst_id
from bronze.crm_cust_info
group by cst_id
having count(*)>1;

-- Check for trailing spaces 

select cst_firstname 
from bronze.crm_cust_info
where trim(cst_firstname)!=cst_firstname;

select cst_lastname 
from bronze.crm_cust_info
where trim(cst_lastname)!=cst_lastname;

-- Check for distinct values of categorical columns 
SELECT DISTINCT cst_marital_status
from bronze.crm_cust_info;

SELECT DISTINCT cst_gndr
from bronze.crm_cust_info;

-- ======================
-- CRM product info
-- ======================

select *
from bronze.crm_prd_info;

-- Cleaned table 
SELECT 
prd_id,
prd_key,
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



-- Check if its joinable with sales_details
select * 
from
(SELECT 
prd_id,
prd_key,
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
) t) c 
join bronze.crm_sales_details s
on s.sls_prd_key=c.sls_prd_key;

-- Check if it is joinable with erp_px_cat_g1v2 

select * 
from
(SELECT 
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
) t) c 
join bronze.erp_px_cat_g1v2 e
on e.id=c.id;



-- Check to see if cst_id has unique indecies
select prd_id
from bronze.crm_prd_info
group by prd_id
having count(*)>1;


-- Check for trailing spaces 

select prd_key 
from bronze.crm_prd_info
where trim(prd_key)!=prd_key;

select prd_nm 
from bronze.crm_prd_info
where trim(prd_nm)!=prd_nm;

select prd_line 
from bronze.crm_prd_info
where trim(prd_line)!=prd_line;

--check of prd_cost has no unusual values 

select prd_cost 
from bronze.crm_prd_info
where prd_cost<0 or prd_cost is NULL;

-- Check for distinct values of categorical columns 

SELECT DISTINCT prd_line 
from bronze.crm_prd_info;

-- Check if there are cases when prd_start_dt>prd_end_dt

SELECT 
* 
FROM bronze.crm_prd_info
WHERE prd_start_dt>prd_end_dt;



/* 
=====================
CRM Sales Details
=====================
*/


SELECT * 
FROM bronze.crm_sales_details;

-- Clean Table

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
case when sls_price is null or sls_price=0 then sls_sales/coalesce(sls_quantity, 0)
     when sls_price<0 then abs(sls_price)
     else sls_price
end as sls_price 
from bronze.crm_sales_details;



-- Check for unconvertable intiger forms of date
SELECT sls_order_dt
from bronze.crm_sales_details
where sls_order_dt<=0 or len(sls_order_dt)!=8;

SELECT sls_ship_dt
from bronze.crm_sales_details
where sls_ship_dt<=0 or len(sls_ship_dt)!=8;

SELECT sls_due_dt
from bronze.crm_sales_details
where sls_due_dt<=0 or len(sls_due_dt)!=8;


-- Check for sales, quantities and price abnormalities

select 
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_price!=sls_quantity*sls_price or sls_price is null or sls_quantity is null or sls_sales is null or sls_price<=0 or sls_quantity<=0 or sls_price<=0;


-- Check if sls_order_dt>sls_ship date
SELECT *
FROM bronze.crm_sales_details
where 
case when sls_order_dt<=0 or len(sls_order_dt)!=8 then NULL 
     else cast(cast(sls_order_dt as nvarchar) as date)
end 
> 
case when sls_ship_dt<=0 or len(sls_ship_dt)!=8 then NULL 
     else cast(cast(sls_ship_dt as nvarchar) as date)
end;


-- Check for trailing spaces 

select sls_ord_num 
from bronze.crm_sales_details
where trim(sls_ord_num)!=sls_ord_num;

select sls_prd_key 
from bronze.crm_sales_details
where trim(sls_prd_key)!=sls_prd_key;

select sls_price 
from bronze.crm_sales_details
where trim(sls_price)!=sls_price;



/* 
ERP CUST AZ12
*/
select *
from bronze.erp_cust_az12;


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



-- Check if it is now joinable with crm cust info
SELECT 
*
FROM(
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
FROM bronze.erp_cust_az12
) e
JOIN bronze.crm_cust_info c 
on e.cid=c.cst_key;



-- Check if id is duplicated or null

SELECT 
cid
FROM bronze.erp_cust_az12
group by cid
having count(*)>1 or cid is null;

-- Check for trailing spaces

select cid 
from bronze.erp_cust_az12
where trim(cid)!=cid;

select gen 
from bronze.erp_cust_az12
where trim(gen)!=gen;

-- Check for distinct gen values

select distinct gen
from bronze.erp_cust_az12;

-- Check for bdate

select bdate
from bronze.erp_cust_az12
where bdate<'1925-01-01' or bdate>GETDATE();









