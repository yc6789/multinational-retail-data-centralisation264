-- how many stores does the busines have and in which countries
SELECT country_code AS country_code,
        COUNT(DISTINCT store_code) AS total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores DESC;

-- Which locations currently have the most stores?

SELECT  locality,
        COUNT(store_code) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 7;

-- WHICH months produced the larget amount of sales
SELECT  ROUND(CAST(SUM(p.product_price * o.product_quantity) AS NUMERIC), 2) AS total_sales,
        d.month AS month
FROM orders_table AS o
JOIN dim_products AS p ON p.product_code = o.product_code
JOIN dim_date_times AS d ON d.date_uuid = o.date_uuid
GROUP BY d.month
ORDER BY total_sales DESC
LIMIT 6;

-- How many sales are coming from online vs offline
SELECT COUNT(*) AS numbers_of_sales,
       SUM(o.product_quantity) AS product_quantity_count,
       CASE
           WHEN s.store_type = 'Web Portal' THEN 'Web'
           ELSE 'Offline'
       END AS location
FROM orders_table AS o   
JOIN dim_store_details AS s ON s.store_code = o.store_code
GROUP BY location
ORDER BY numbers_of_sales ASC;

-- What percentage of sales come through each type of store
WITH sales_by_store AS (
    SELECT s.store_type,
            ROUND(CAST(SUM(p.product_price * o.product_quantity) AS NUMERIC), 2) AS total_sales
    FROM orders_table AS o
    JOIN dim_store_details AS s ON s.store_code = o.store_code
    JOIN dim_products AS p ON p.product_code = o.product_code
    GROUP BY s.store_type
),
total_sales AS (
    SELECT SUM(total_sales) AS total_overall_sales
    FROM sales_by_store
)
SELECT s.store_type,
        total_sales,
       ROUND(s.total_sales / t.total_overall_sales * 100, 2) AS percentage_sales
FROM sales_by_store s
CROSS JOIN total_sales t
ORDER BY percentage_sales DESC;

-- Which month in each year produced the highest cost of sales
WITH monthly_sales AS (
    SELECT ROUND(CAST(SUM(p.product_price * o.product_quantity) AS NUMERIC), 2) AS total_sales,
           d.year,
           d.month
    FROM orders_table AS o
    JOIN dim_date_times AS d ON d.date_uuid = o.date_uuid
    JOIN dim_products AS p ON p.product_code = o.product_code
    GROUP BY d.year, d.month
)
SELECT total_sales, year, month
FROM (
    SELECT year, 
           month, 
           total_sales,
           ROW_NUMBER() OVER (PARTITION BY year ORDER BY total_sales DESC) AS sales_rank
    FROM monthly_sales
) ranked_sales
WHERE sales_rank = 1
ORDER BY total_sales DESC
LIMIT 10;


-- What is our staff headcount
SELECT SUM(staff_numbers) AS total_staff_numbers,
        country_code
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;


-- Which German store type is selling the most
SELECT ROUND(CAST(SUM(p.product_price * o.product_quantity) AS NUMERIC), 2) AS total_sales,
       s.store_type
FROM orders_table AS o
JOIN dim_products AS p ON p.product_code = o.product_code
JOIN dim_store_details AS s ON s.store_code = o.store_code
WHERE s.country_code = 'DE'
GROUP BY s.store_type
ORDER BY total_sales ASC;

-- How quickly is the company making sales
WITH sales_with_lag AS (
    SELECT
        d.year,
        d.timestamp,
        LEAD(d.timestamp) OVER (PARTITION BY d.year ORDER BY d.timestamp) AS next_sale_timestamp
    FROM
        orders_table AS o
    JOIN
        dim_date_times AS d ON o.date_uuid = d.date_uuid
)
, time_differences AS (
    SELECT
        year,
        (next_sale_timestamp - timestamp) AS time_diff
    FROM
        sales_with_lag
    WHERE
        next_sale_timestamp IS NOT NULL
)
, avg_time_differences AS (
    SELECT
        year,
        AVG(EXTRACT(EPOCH FROM time_diff)) AS avg_time_seconds,  -- Avg time difference in seconds
        AVG(time_diff) AS avg_time_interval -- Original interval for formatting
    FROM
        time_differences
    GROUP BY
        year
)
SELECT
    year,
    json_build_object(
        'hours', EXTRACT(HOUR FROM avg_time_interval),
        'minutes', EXTRACT(MINUTE FROM avg_time_interval),
        'seconds', EXTRACT(SECOND FROM avg_time_interval),
        'milliseconds', EXTRACT(MILLISECOND FROM avg_time_interval)
    ) AS avg_time_taken
FROM
    avg_time_differences
ORDER BY
    avg_time_seconds DESC;







