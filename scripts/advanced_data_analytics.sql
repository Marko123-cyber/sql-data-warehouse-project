USE DataWarehouse;
GO

-- Change over time 
SELECT
DATETRUNC(month, order_date) as order_date ,
sum(sales_amount) as total_Sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
FROM gold.fact_sales
where order_date is not null
group by DATETRUNC(month, order_date)
order by DATETRUNC(month, order_date);
GO

-- Cumulative analysis

-- Calculate total sales per month 
-- and running total of sales over time


select 
order_date,
total_sales, 
sum(total_sales) over(partition by year(order_date) order by order_date) as yearly_running_total_sales,
avg(total_sales) over(partition by year(order_date) order by order_date) as yearly_running_avg_sales
from
(
SELECT 
DATETRUNC(month, order_date) as order_date,
sum(sales_amount) as total_sales,
avg(sales_amount) as avg_sales
from gold.fact_sales 
where order_date is not null
group by DATETRUNC(month, order_date)
) t;
GO
-- Performance Analysis

with yearly_product_sales as (
select 
year(s.order_date) as order_year,
p.product_name,
sum(s.sales_amount) as current_sales
from gold.fact_sales s
left join gold.dim_products p
on s.product_key=p.product_key
where s.order_date is not null
group by year(s.order_date), p.product_name
)
SELECT
order_year,
product_name,
current_sales,
avg(current_sales) over(PARTITION BY product_name) as avg_sales,
current_sales-avg(current_sales) over(PARTITION BY product_name) as diff_avg,
case when current_sales-avg(current_sales) over(PARTITION BY product_name)>0 then 'Below Avg'
     when current_sales-avg(current_sales) over(PARTITION BY product_name)<0 then 'Above Avg'
     else 'Avg'
end as avg_change,
lag(current_sales) over (PARTITION BY product_name order by order_year) as py_sales,
current_sales-lag(current_sales) over (PARTITION BY product_name order by order_year) as diff_py,
case when current_sales-lag(current_sales) over (PARTITION BY product_name order by order_year) >0 then 'Increasing'
     when current_sales-lag(current_sales) over (PARTITION BY product_name order by order_year) <0 then 'Decreasing'
     else 'No Change'
end as avg_change
FROM yearly_product_sales
order by product_name, order_year;
GO

-- Part to whole analyis

with category_sales as 
(
select 
category,
sum(sales_amount) as total_sales
from gold.fact_sales s
LEFT JOIN gold.dim_products p
on s.product_key=p.product_key
group by category
) 
SELECT 
category,
total_sales,
sum(total_sales) over() as overall_sales,
concat(round(cast(total_sales as float)/sum(total_sales) over()*100, 2), '%') as percentage_total
FROM category_sales
order by percentage_total desc;
GO
-- Data segmentation


with cost_ranges as
( 
select 
p.product_key,
p.product_name,
p.product_cost, 
case when p.product_cost<100 then 'Below 100'
     when p.product_cost BETWEEN 100 and 500 then '100-500'
     when p.product_cost BETWEEN 500 and 1000 then '500-1000'
     else 'Above 1000'
end as cost_range
from gold.dim_products p
)
select 
cost_range,
count(product_key) as total_products
from cost_ranges
group by cost_range;
GO


with customer_spending as 
(
select 
c.customer_key,
sum(s.sales_amount) as total_spending,
min(order_date) as first_order,
max(order_date) as last_order,
DATEDIFF(month, min(order_date), max(order_date)) as lifespan
from gold.fact_sales s
left join gold.dim_customers c
on s.customer_key=c.customer_key
group by c.customer_key
)
select 
customer_segment, 
count(customer_key) as total_customers
from(
select 
customer_key,
case when lifespan>=12 and total_spending>5000 then 'VIP'
     when lifespan>=12 and total_spending<=5000 then 'Regular'
     else 'New'
end as customer_segment
from customer_spending
) t
group by customer_segment;
GO



-- Base querry: retreiving core columns from tabkle


CREATE or ALTER VIEW gold.report_customers as 
with base_querry as 
(
SELECT 
s.order_key,
s.product_key,
c.customer_key,
s.order_date,
s.sales_amount,
s.quantity,
CONCAT(c.first_name, ' ', c.last_name) as customer_name,
DATEDIFF(year,c.birth_date, GETDATE()) as age
FROM gold.fact_sales s
left join gold.dim_customers c
on s.customer_key=c.customer_key
where order_date is not null
),
customer_aggregation as 
(
SELECT 
customer_key,
customer_name,
age,
count(distinct order_key) as total_orders,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity,
count(distinct product_key) as total_products,
max(order_date) as last_order_date,
DATEDIFF(month, min(order_date), max(order_date)) as lifespan
from base_querry
group by 
customer_key,
customer_name,
age
)
SELECT
customer_key,
customer_name,
age,
case when age<20 then 'Uner 20'
     when age BETWEEN 20 and 29 then '20-29'
     when age BETWEEN 30 and 39 then '30-39'
     when age BETWEEN 40 and 49 then '40-49'
     else '50 and above'
end as age_group,
case when lifespan>=12 and total_sales>5000 then 'VIP'
     when lifespan>=12 and total_sales<=5000 then 'Regular'
     else 'New'
end as customer_segment,
total_orders,
total_sales,
total_quantity,
lifespan,
total_products,
last_order_date,
DATEDIFF(month, last_order_date, GETDATE()) as recency,
case when total_orders=0 then 0
     else total_sales/total_orders 
end as avg_order_value,
case when lifespan= 0 then 0
     else total_sales/lifespan
end as avg_monthly_spend
from customer_aggregation;
GO

select *
from gold.report_customers;
GO 

CREATE OR ALTER VIEW gold.report_products AS
WITH base_query AS
(
    SELECT 
        s.order_key,
        s.product_key,
        p.product_name,
        p.category,
        p.subcategory,
        p.product_cost,
        s.order_date,
        s.sales_amount,
        s.quantity
    FROM gold.fact_sales s
    LEFT JOIN gold.dim_products p
        ON s.product_key = p.product_key
    WHERE s.order_date IS NOT NULL
),
product_aggregation AS
(
    SELECT 
        product_key,
        product_name,
        category,
        subcategory,
        product_cost,
        COUNT(DISTINCT order_key) AS total_orders,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity,
        MAX(order_date) AS last_order_date,
        DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
    FROM base_query
    GROUP BY 
        product_key,
        product_name,
        category,
        subcategory,
        product_cost
)
SELECT
    product_key,
    product_name,
    category,
    subcategory,

    /* Product segmentation */
    CASE 
        WHEN product_cost < 100 THEN 'Low cost'
        WHEN product_cost BETWEEN 100 AND 500 THEN 'Mid cost'
        WHEN product_cost BETWEEN 500 AND 1000 THEN 'High cost'
        ELSE 'Premium'
    END AS cost_segment,

    CASE 
        WHEN lifespan >= 12 AND total_sales > 5000 THEN 'Top seller'
        WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Steady'
        ELSE 'New'
    END AS product_segment,

    total_orders,
    total_sales,
    total_quantity,
    lifespan,
    last_order_date,
    DATEDIFF(month, last_order_date, GETDATE()) AS recency,

    /* KPIs */
    CASE 
        WHEN total_orders = 0 THEN 0
        ELSE total_sales / total_orders
    END AS avg_order_value,

    CASE 
        WHEN lifespan = 0 THEN 0
        ELSE total_sales / lifespan
    END AS avg_monthly_sales

FROM product_aggregation;
GO

select *
from gold.report_products;
