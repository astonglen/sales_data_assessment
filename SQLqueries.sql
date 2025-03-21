-- a) Count the total number of records
SELECT COUNT(*) AS total_records
FROM sales_data;

-- b) Find the total sales amount by region
SELECT region, SUM(net_sale) AS total_sales
FROM sales_data
GROUP BY region;

-- c) Find the average sales amount per transaction
SELECT AVG(net_sale) AS avg_sales_per_transaction
FROM sales_data;

-- d) Check for duplicate OrderId values within the same region
SELECT
    OrderId,
    region,
    COUNT(*) AS occurrences
FROM sales_data
GROUP BY OrderId, region
HAVING occurrences > 1;
