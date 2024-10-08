drop table if exists sales;
CREATE TABLE sales (
year INT,
country VARCHAR(20),
product VARCHAR(32),
profit INT
);

INSERT INTO
sales (
year,
country,
product,
profit
)
VALUES (2024, 'USA', 'Laptop', 1200),
(
2024,
'USA',
'Smartphone',
2000
),
(
2024,
'USA',
'Smartphone',
NULL
),
(
2024,
'USA',
'Smartphone',
NULL
),
(2024, 'USA', 'Printer', 1001),
(2024, 'USA', 'Laptop', 2010),
(
2024,
'Canada',
'Smartphone',
800
),
(
2024,
'Canada',
'Smartphone',
900
),
(
2024,
'Canada',
'Smartphone',
500
),
(2023, 'China', 'Printer', 400),
(2024, 'China', 'Printer', 500),
(2024, 'China', 'Printer', 600),
(2023, NULL, 'Printer', 700),
(NULL, 'India', 'Mouse', 500),
(NULL, 'China', 'Mouse', 300),
(NULL, NULL, 'Mouse', 150),
(NULL, NULL, 'Printer', 200),
(NULL, NULL, NULL, NULL);

SELECT
year,
country,
product,
SUM(profit) AS profit,
grouping(year),
grouping(country),
grouping(product)
FROM sales
GROUP BY
grouping sets (
(),
(year, country),
(country, product),
(),
(year, country)
)
ORDER BY profit;


SELECT *
FROM (
SELECT year, SUM(profit) AS profit
FROM sales
GROUP BY
rollup(year)
) AS dt
ORDER BY year DESC;

SELECT year, country, product, SUM(profit) AS profit, grouping(year, country, product) as my_grouping
FROM sales
GROUP BY
cube (year, country, product)
ORDER BY profit,  my_grouping ;

SELECT year, country, product, COUNT(profit) AS my_count, grouping(year, country, product) as my_grouping
FROM sales
GROUP BY
rollup (year, country, product)
ORDER BY my_count DESC, my_grouping ;

SELECT year, country, product, SUM(profit) AS profit, grouping(year, country, product) as my_grouping
FROM sales
GROUP BY
cube (year, country, product)
having grouping(year, country, product) > 4
ORDER BY profit DESC, my_grouping;