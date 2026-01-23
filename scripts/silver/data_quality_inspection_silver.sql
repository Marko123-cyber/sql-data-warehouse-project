/* 
Data quality inspection for silver tables. Most of code is just collection of bronze_data inspection just a bit modified in some cases or removed for our use case.
*/
USE DataWarehouse;
GO


CREATE OR ALTER PROCEDURE silver.data_quality_check
AS 
BEGIN

    -- ==================
    -- CRM customer info
    -- ==================

    -- Check to see if cst_id has unique indecies
    SELECT cst_id
    FROM silver.crm_cust_info
    GROUP BY cst_id
    HAVING COUNT(*) > 1 OR cst_id IS NULL;

    -- Check for trailing spaces 
    SELECT cst_firstname 
    FROM silver.crm_cust_info
    WHERE TRIM(cst_firstname) != cst_firstname;

    SELECT cst_lastname 
    FROM silver.crm_cust_info
    WHERE TRIM(cst_lastname) != cst_lastname;

    -- Check for distinct values of categorical columns 
    SELECT DISTINCT cst_marital_status
    FROM silver.crm_cust_info;

    SELECT DISTINCT cst_gndr
    FROM silver.crm_cust_info;



    -- ======================
    -- CRM product info
    -- ======================

    -- Check if joinable with sales_details
    SELECT *
    FROM silver.crm_prd_info c
    JOIN silver.crm_sales_details s
        ON s.sls_prd_key = c.sls_prd_key;


    -- Check if joinable with erp_px_cat_g1v2
    SELECT *
    FROM silver.crm_prd_info c
    JOIN silver.erp_px_cat_g1v2 e
        ON e.id = c.id;


    -- Check to see if prd_id has duplicate or null indecies 
    SELECT prd_id
    FROM silver.crm_prd_info
    GROUP BY prd_id
    HAVING COUNT(*) > 1 OR prd_id IS NULL;

    -- Check for trailing spaces 
    SELECT prd_nm 
    FROM silver.crm_prd_info
    WHERE TRIM(prd_nm) != prd_nm;

    SELECT prd_line 
    FROM silver.crm_prd_info
    WHERE TRIM(prd_line) != prd_line;

    -- Check of prd_cost has no unusual values 
    SELECT prd_cost 
    FROM silver.crm_prd_info
    WHERE prd_cost < 0 OR prd_cost IS NULL;

    -- Check for distinct values of categorical columns 
    SELECT DISTINCT prd_line 
    FROM silver.crm_prd_info;

    -- Check if there are cases when prd_start_dt > prd_end_dt
    SELECT *
    FROM silver.crm_prd_info
    WHERE prd_start_dt > prd_end_dt;


    /* 
    =====================
    CRM Sales Details
    =====================
    */

    -- Check for sales, quantities and price abnormalities
    SELECT 
        sls_sales,
        sls_quantity,
        sls_price
    FROM silver.crm_sales_details
    WHERE sls_price != sls_quantity * sls_price 
        OR sls_price IS NULL 
        OR sls_quantity IS NULL 
        OR sls_sales IS NULL 
        OR sls_price <= 0 
        OR sls_quantity <= 0;

    -- Check if sls_order_dt > sls_ship_dt
    SELECT *
    FROM silver.crm_sales_details
    WHERE sls_order_dt > sls_ship_dt;

    -- Check for trailing spaces 
    SELECT sls_ord_num 
    FROM silver.crm_sales_details
    WHERE TRIM(sls_ord_num) != sls_ord_num;

    SELECT sls_prd_key 
    FROM silver.crm_sales_details
    WHERE TRIM(sls_prd_key) != sls_prd_key;


    /* 
    =====================
    ERP CUST AZ12
    =====================
    */

    -- Check if it is joinable with crm cust info
    SELECT *
    FROM (
        SELECT 
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END AS cid,
            CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END AS bdate,
            CASE 
                WHEN UPPER(SUBSTRING(gen, 1, 1)) = 'F' THEN 'Female'
                WHEN UPPER(SUBSTRING(gen, 1, 1)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS gen
        FROM silver.erp_cust_az12
    ) e
    JOIN silver.crm_cust_info c
        ON e.cid = c.cst_key;

    -- Check if id is duplicated or null
    SELECT cid
    FROM silver.erp_cust_az12
    GROUP BY cid
    HAVING COUNT(*) > 1 OR cid IS NULL;

    -- Check for trailing spaces
    SELECT cid 
    FROM silver.erp_cust_az12
    WHERE TRIM(cid) != cid;

    SELECT gen 
    FROM silver.erp_cust_az12
    WHERE TRIM(gen) != gen;

    -- Check for distinct gen values
    SELECT DISTINCT gen
    FROM silver.erp_cust_az12;

    -- Check for bdate abnormalities
    SELECT bdate
    FROM silver.erp_cust_az12
    WHERE bdate > GETDATE();


    /* 
    =====================
    ERP Cust Loc
    =====================
    */

    -- Check if joinable with crm cust info
    SELECT *
    FROM (
        SELECT 
            REPLACE(cid, '-', '') AS cid,
            CASE 
                WHEN cntry LIKE N'USA%' THEN 'United States'
                WHEN cntry LIKE N'US%' THEN 'United States'
                WHEN cntry LIKE N'DE%' THEN 'Germany'
                WHEN cntry LIKE N'Australia%' THEN 'Australia'
                WHEN TRIM(cntry) = N'' THEN 'n/a'
                ELSE cntry
            END AS cntry
        FROM silver.erp_loc_a101
    ) e
    JOIN silver.crm_cust_info c
        ON e.cid = c.cst_key;

    -- Check for duplicate id or null id
    SELECT cid
    FROM silver.erp_loc_a101
    GROUP BY cid
    HAVING COUNT(*) > 1 OR cid IS NULL;

    -- Check for trailing spaces 
    SELECT cid 
    FROM silver.erp_loc_a101
    WHERE TRIM(cid) != cid;

    SELECT cntry 
    FROM silver.erp_loc_a101
    WHERE TRIM(cntry) != cntry;

    -- Check for distinct categorical values
    SELECT DISTINCT cntry
    FROM silver.erp_loc_a101;


    /* 
    =======================
    ERP PX CAT G1V2
    =======================
    */

    -- Check if joinable
    SELECT *
    FROM (
        SELECT 
            REPLACE(id, '_', '-') AS id,
            cat,
            subcat,
            CASE WHEN maintanance LIKE '%Yes%' THEN 'Yes' ELSE 'No' END AS maintanance
        FROM silver.erp_px_cat_g1v2
    ) e
    JOIN silver.crm_prd_info c
        ON e.id = c.id;

    -- check for trailing spaces 
    SELECT id 
    FROM silver.erp_px_cat_g1v2
    WHERE TRIM(id) != id;

    SELECT cat 
    FROM silver.erp_px_cat_g1v2
    WHERE TRIM(cat) != cat;

    SELECT subcat 
    FROM silver.erp_px_cat_g1v2
    WHERE TRIM(subcat) != subcat;

    SELECT maintanance 
    FROM silver.erp_px_cat_g1v2
    WHERE TRIM(maintanance) != maintanance;

    -- check for duplicate or null ids
    SELECT id
    FROM silver.erp_px_cat_g1v2
    GROUP BY id
    HAVING COUNT(*) > 1 OR id IS NULL;

    -- check categorical cols unique values
    SELECT DISTINCT cat
    FROM silver.erp_px_cat_g1v2;

    SELECT DISTINCT subcat
    FROM silver.erp_px_cat_g1v2;

    SELECT DISTINCT maintanance
    FROM silver.erp_px_cat_g1v2;

END;
GO
EXEC silver.data_quality_check;
