USE DataWarehouse;
GO

CREATE OR ALTER VIEW gold.dim_customers AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY ci.cst_create_date, ci.cst_id) AS customer_key,

    -- hidden business key (for joins only)
    ci.cst_id AS customer_business_id,

    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    cl.cntry AS country,
    CASE 
        WHEN ci.cst_gndr <> 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,
    ci.cst_marital_status AS marital_status,
    ca.bdate AS birth_date,
    ci.cst_create_date AS customer_created_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid          -- business key used ONLY internally
LEFT JOIN silver.erp_loc_a101 AS cl
    ON cl.cid = ci.cst_key;
GO
    
CREATE OR ALTER VIEW gold.dim_products AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY pi.prd_id) AS product_key,

    -- hidden business key (for joins only)
    pi.sls_prd_key AS product_business_key,

    pi.prd_nm AS product_name,
    pi.prd_line AS product_line,
    pi.prd_cost AS product_cost,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintanance AS maintenance_type,
    pi.prd_start_dt AS product_start_date
FROM silver.crm_prd_info AS pi
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
    ON pi.id = pc.id
WHERE pi.prd_end_dt IS NULL;
GO
    
CREATE OR ALTER VIEW gold.fact_sales AS
SELECT 
    s.sls_ord_num AS order_key,

    p.product_key,
    c.customer_key,

    s.sls_order_dt AS order_date,
    s.sls_ship_dt AS shipping_date,
    s.sls_due_dt AS due_date,
    s.sls_sales AS sales_amount,
    s.sls_quantity AS quantity,
    s.sls_price AS price
FROM silver.crm_sales_details AS s
LEFT JOIN gold.dim_products AS p
    ON s.sls_prd_key = p.product_business_key   
LEFT JOIN gold.dim_customers AS c
    ON s.sls_cust_id = c.customer_business_id; 




