
-- Analyze Sales Performance over Time 
select year(order_date) as order_year,
month(order_date) as order_month,
sum(sales_amount)as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from [gold.fact_sales]
where order_date is not null
Group by year(order_date) ,month(order_date)
Order by year(order_date),month(order_date)


select datetrunc(year,order_date) as order_date,
sum(sales_amount)as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from [gold.fact_sales]
where order_date is not null
Group by datetrunc(year ,order_date) 
Order by datetrunc(year,order_date)


select datetrunc(month,order_date) as order_date,
sum(sales_amount)as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from [gold.fact_sales]
where order_date is not null
Group by datetrunc(month ,order_date) 
Order by datetrunc(month,order_date)




select format(order_date ,'yyyy-mmm') as order_date,
sum(sales_amount)as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from [gold.fact_sales]
where order_date is not null
Group by format(order_date ,'yyyy-mmm') 
Order by format(order_date ,'yyyy-mmm')

--How many new customer were  added each year
SELECT datetrunc(year ,create_date) as create_year,
count(customer_key)as total_customer
from [gold.dim_customers]
group by datetrunc(year ,create_date)
order by datetrunc(year, create_date)


--CUMULATIVE ANALYSIS
--Aggregate the data progressively over time .Helps to understand whether
--our business is growing or declining.

--1.Calcualate the total sales per month 
-- and the running total of sales over time

select order_date,
total_sales,
sum(total_sales) over(partition by order_date order by order_date) as running_total_sales ,
avg(avg_price) over(partition by order_date order by order_date) as moving_average_price
from
(
select datetrunc(year,order_date) as order_date,
sum(sales_amount)as total_sales,
avg(price) as avg_price
from [gold.fact_sales]
where order_date is not null
group by datetrunc(year,order_date)
)T

---Performance Analysis
--Comaparing the current value to target value.
--Helps measure success and compare performance.

--- Analyze the yearly performance of products by comparing each product's
--sales to both its average sales performance and the privious year's sales.

WITH yearly_product_sales AS (
    SELECT 
        YEAR(f.order_date) AS order_year,
        p.product_name,
        SUM(f.sales_amount) AS current_sales
    FROM [gold.fact_sales] f
    LEFT JOIN [gold.report_products] p
        ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL
    GROUP BY YEAR(f.order_date), p.product_name
)
SELECT 
    order_year, 
    product_name, 
    current_sales,
    AVG(current_sales) OVER (PARTITION BY product_name) AS avg_sales,
    current_sales - AVG(current_sales) OVER (PARTITION BY product_name) AS diff_avg,  -- Added missing comma
    CASE 
        WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) > 0 THEN 'above avg'
        WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'below avg'
        ELSE 'avg'
    END AS avg_change
FROM yearly_product_sales
ORDER BY product_name, order_year;

---Part to whole 
-- Analyze  the an individual part is performing compared  to the overall,
--allowing us too understanting which category has the gretest impact on the business.

--which categories contribute the most to overall sales?
with category_sales as(
select category,sum(sales_amount) as total_sales
from [gold.fact_sales] f
 LEFT join [gold.report_products] p
on p.product_key =f.product_key
group by category
)
SELECT 
	category,
    total_sales,
    sum(total_sales) over () as overall_sales,
    round((cast(total_sales as float) / sum(total_sales) over ()) * 100, 2)
	as percentage_of_total
FROM category_sales;

---Data Segmentation
--group the data based on a specific range. Helps understand the correlation two measure.

--Segment products into cost ranges and count how 
--many products fall into each segment.
with product_segment as (
select product_key ,
product_name,
cost as cost_renge,
case when cost <100 then 'below 100'
when cost between 100 and 500 then '100'
when cost between 500 and 1000 then '500-100'
else 'above 1000'
end 'abov 1000'
from [gold.report_products]
)
select cost_renge,
	count(product_key) as total_product
	from product_segment
	group by cost_renge
	order by total_product desc

/*Group customers into three segement bassed on their spending behaivior :
-VIP: at least 12 month of history and spending more than ₹5000. 
-Regular: at least 12 month of history and spending more than ₹5000 or less.
-New :lifespan less than 12 month.
-And find the total number of customers by each  group.*/
with customer_spending as(
SELECT 
f.customer_key,
sum(f.sales_amount) as total_spending,
min(order_date)as first_order,
max(order_date)as last_order,
datediff(month, min(order_date), max(order_date))as lifespan
from [gold.fact_sales] f
left join [gold.dim_customers] c
on f.customer_key = c.customer_key
group by f.customer_key
)
select 
customer_segment,
count(customer_key)as total_customers
from 
(
select 
customer_key,
total_spending,
lifespan,
case 
	WHEN lifespan >= 12 and total_spending > 5000 then 'vip'
	WHEN lifespan >= 12 and total_spending <= 5000 THEN 'regular'
	ELSE 'new'
	end as customer_segment
from customer_spending )t 
group by customer_segment
order by total_customers desc



/* Reporting ,
Customer Reporting
-- this report consolidates key customer metrics and behaviors
Highlights:
	1. Gathers essential fields such as names, ages and transaction details.
	2. Segments customers into categries (VIP, Regular ,new) and age groups.
	3. Aggregates customer -level metrics:
	-total_orders
	-tota_sales
	-total_quantity purchased
	-total product
	-lifespan (in month)
	4. calculates valuavble KPIs:
	-recency (month since last order)
	-average order value
	-average monthly spend
	*/

CREATE VIEW gold.report_products as
WITH base_query AS (
/*-1.Basic query Retrieves core columns from tables
*/
    SELECT 
        f.product_key, 
        f.order_date,
        f.sales_amount,
        f.quantity,  -- Added quantity to be used later
        c.customer_key,
        c.customer_number,
        c.first_name, 
        c.last_name,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,  -- Fixed CONCAT syntax
        DATEDIFF(YEAR, c.birthdate, GETDATE()) AS age
    FROM [gold.fact_sales] f
    LEFT JOIN [gold.dim_customers] c
        ON c.customer_key = f.customer_key
    WHERE f.order_date IS NOT NULL
), 
customer_aggregation AS (
/*----------------------------------------------------------------------
2. product aggregations: Summarizes key metrics at the customer level
*/
    SELECT 
        customer_key,
        customer_number,
        customer_name,
        age,
        COUNT(sales_amount) AS total_orders,-- Changed column name to total_orders
		SUM(quantity) AS total_quantity,
        COUNT(DISTINCT customer_key) AS total_products,
        MAX(order_date) AS last_order_date,
        SUM(sales_amount) AS total_sales,  -- Added total_sales since it's used later
        DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan
    FROM base_query
    GROUP BY 
        customer_key,
        customer_number,
        customer_name,
        age
)
SELECT 
    customer_key,
    customer_number,
    customer_name,
    age,
    CASE
        when age < 20 then 'Under 20'
        when age BETWEEN 20 AND 29 then '20-29'
        when age BETWEEN 30 AND 39 then '30-39'
        when age BETWEEN 40 AND 49 then '40-49'
        else '50 and above'
    end as age_group,
    case 
        WHEN total_sales > 5000 THEN 'VIP'
        WHEN total_sales <= 5000 THEN 'Regular'
        ELSE 'New'
    END AS customer_segment,
    DATEDIFF(MONTH, last_order_date, GETDATE()) AS recency, -- Fixed missing comma
    total_orders,
    total_sales,
    total_quantity,
    total_products,
    lifespan,
    -- Compute Average Order Value (AOV)
    CASE 
        WHEN total_orders = 0 THEN 0
        ELSE total_sales / total_orders 
    END AS avg_order_value,
    -- Compute Average Monthly Spend
    CASE 
        WHEN lifespan = 0 THEN total_sales
        ELSE total_sales / lifespan 
    END AS avg_monthly_spend
FROM customer_aggregation;


