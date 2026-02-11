SELECT
year(order_date) as order_year,
month(order_date) as order_year,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
FROM gold.fact_sales
where order_date is not null
group by year(order_date), month(order_date)
order by year(order_date), month(order_date)

-- 2nd way

SELECT
format(order_date, 'yyy-MMM') as order_date,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
FROM gold.fact_sales
where order_date is not null
group by format(order_date, 'yyy-MMM')
order by format(order_date, 'yyy-MMM')


-- Calculate the total sales for each month

select 
order_date,
total_sales,
sum(total_sales) over (order by order_date) as running_total_sales,
avg(avg_price) over (order by order_date) as moving_average_price
from
(
SELECT
DATETRUNC(month, order_date) as order_date,
sum(sales_amount) as total_sales,
avg(price) as avg_price
FROM gold.fact_sales
where order_date is not null
group by DATETRUNC(month, order_date)
) t


-- Performance analysis
WITH yearly_product_sales AS (
    SELECT
        YEAR(f.order_date) AS order_year,
        p.product_name,
        SUM(f.sales_amount) AS current_sales
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL
    GROUP BY 
        YEAR(f.order_date),
        p.product_name
)
SELECT
    order_year,
    product_name,
    current_sales,
    AVG(current_sales) OVER (PARTITION BY product_name) AS avg_sales,
    current_sales - AVG(current_sales) OVER (PARTITION BY product_name) AS diff_avg,
    CASE 
        WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above Avg'
        WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Avg'
        ELSE 'Avg'
    END AS avg_change,

    -- Year-over-Year Analysis
    LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS py_sales,
    current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS diff_py,
    CASE 
        WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
        WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
        ELSE 'No Change'
    END AS py_change
FROM yearly_product_sales
ORDER BY product_name, order_year;


-- Part to whole analysis, to find the proportion of one part compared with whole, to find the percentage
with category_sales as (
select 
category,
sum(sales_amount) total_sales
from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key
group by category)

select
category,
total_sales,
sum(total_sales) over () overall_sales,
concat(round((cast(total_sales as float)/sum(total_sales) over ())*100,2),'%') as percentage_of_total
from category_sales
order by total_sales desc

-- Data segmentation, group the data base on specific ranges

with product_segments as (
select
product_key,
product_name,
cost,
case when cost < 100 then 'Below 100'
     when cost between 100 and 500 then '100-500'
     when cost between 500 and 1000 then '500-1000'
     else 'above 1000'
end cost_range
from gold.dim_products)

select
cost_range,
count(product_key) as total_products
from product_segments
group by cost_range
order by total_products desc


-- aditional segmentation 
with customer_spending as (
select
c.customer_key,
sum(sales_amount) as total_spending,
min(order_date) as first_order,
max(order_date) as last_order,
datediff (month, min(order_date), max(order_date)) as lifespan
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key
group by c.customer_key
)

select
customer_segment,
count(customer_key) as total_customers
from(
    select
    customer_key,
    case when lifespan >= 12 and total_spending > 5000 then 'vip'
         when lifespan >+ 12 and total_spending <= 5000 then 'regular'
         else 'new'
    end customer_segment
    from customer_spending) t
group by customer_segment
order by total_customers desc






















