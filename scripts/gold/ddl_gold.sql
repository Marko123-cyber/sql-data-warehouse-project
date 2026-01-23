/* 
Script creates view for final gold layer. Gold layer represents the final version of the star schema.
Views can be used in buisness logic.
*/



USE DataWarehouse;
GO

/* 
====================
Customer Dimension
====================
*/
CREATE OR ALTER VIEW gold.dim_customers AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY ci.cst_create_date, ci.cst_id) AS customer_key_surrogate,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_business_key,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    cl.cntry AS country,
    CASE 
        WHEN ci.cst_gndr <> 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,
    ci.cst_marital_status AS marital_status,
    ca.bdate AS birth_day,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS cl
    ON cl.cid = ci.cst_key;
GO


/* 
====================
Product Dimension
====================
*/
CREATE OR ALTER VIEW gold.dim_products AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY pi.prd_id) AS product_key_surrogate,
    pi.prd_id AS product_id,
    pi.sls_prd_key AS sales_product_key,
    pi.prd_nm AS product_name,
    pi.prd_line AS product_line,
    pi.prd_cost AS product_cost,
    pc.maintanance AS maintanance,
    pi.id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pi.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pi
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
    ON pi.id = pc.id
WHERE pi.prd_end_dt IS NULL;
GO


/* 
====================
Sales Fact 
====================
*/

CREATE OR ALTER VIEW gold.fact_sales AS
SELECT 
    s.sls_ord_num,
    p.product_key_surrogate AS product_key,
    c.customer_key_surrogate AS customer_key,
    s.sls_order_dt as order_date,
    s.sls_ship_dt as shipping_date,
    s.sls_due_dt as due_date, 
    s.sls_sales as sales_amount, 
    s.sls_quantity as quantity,
    s.sls_price as price
FROM silver.crm_sales_details AS s
LEFT JOIN gold.dim_products AS p
    ON s.sls_prd_key = p.sales_product_key
LEFT JOIN gold.dim_customers AS c
    ON s.sls_cust_id = c.customer_id;
