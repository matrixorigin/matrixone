-- @bvt:issue#24020
-- Test: CTE with WHERE filter pushdown
-- Verifies that filtering conditions within/after CTE are correctly pushed down
-- and produce correct results with ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)

drop table if exists ds_t_int_hsicl_dtl;
create table ds_t_int_hsicl_dtl (
    id int auto_increment primary key,
    stock_code varchar(20),
    industry_name varchar(100),
    modified_date date
);

insert into ds_t_int_hsicl_dtl (stock_code, industry_name, modified_date) values
('S001', 'Consumer Discretionary', '2025-10-01'),
('S001', 'Technology', '2025-10-15'),
('S001', 'Consumer Discretionary', '2025-10-30'),
('S002', 'Consumer Discretionary', '2025-09-01'),
('S002', 'Healthcare', '2025-10-20'),
('S003', 'Technology', '2025-10-25'),
('S003', 'Technology', '2025-10-28'),
('S004', 'Consumer Discretionary', '2025-11-05'),
('S004', 'Consumer Discretionary', '2025-10-10'),
('S005', 'Healthcare', '2025-10-12'),
('S005', 'Consumer Discretionary', '2025-10-29');

-- Case 1: Original reproduction query from issue
-- ROW_NUMBER + PARTITION BY + outer WHERE filter
SELECT COUNT(*) AS direct_rows FROM (
    SELECT stock_code, industry_name FROM (
        SELECT stock_code, industry_name,
            ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY modified_date DESC) AS rn
        FROM ds_t_int_hsicl_dtl WHERE modified_date <= '2025-10-31'
    ) t WHERE rn = 1
) d WHERE industry_name = 'Consumer Discretionary';

-- Case 2: Same logic using WITH (CTE)
WITH ranked AS (
    SELECT stock_code, industry_name,
        ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY modified_date DESC) AS rn
    FROM ds_t_int_hsicl_dtl
    WHERE modified_date <= '2025-10-31'
),
latest AS (
    SELECT stock_code, industry_name FROM ranked WHERE rn = 1
)
SELECT COUNT(*) AS cte_rows FROM latest WHERE industry_name = 'Consumer Discretionary';

-- Case 3: Verify both approaches return the same count
WITH ranked AS (
    SELECT stock_code, industry_name,
        ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY modified_date DESC) AS rn
    FROM ds_t_int_hsicl_dtl
    WHERE modified_date <= '2025-10-31'
),
latest AS (
    SELECT stock_code, industry_name FROM ranked WHERE rn = 1
)
SELECT
    (SELECT COUNT(*) FROM latest WHERE industry_name = 'Consumer Discretionary') AS cte_count,
    (SELECT COUNT(*) FROM (
        SELECT stock_code, industry_name FROM (
            SELECT stock_code, industry_name,
                ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY modified_date DESC) AS rn
            FROM ds_t_int_hsicl_dtl WHERE modified_date <= '2025-10-31'
        ) t WHERE rn = 1
    ) d WHERE industry_name = 'Consumer Discretionary') AS subquery_count;

-- Case 4: CTE with multiple filter conditions pushed down
WITH ranked AS (
    SELECT stock_code, industry_name,
        ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY modified_date DESC) AS rn
    FROM ds_t_int_hsicl_dtl
    WHERE modified_date <= '2025-10-31'
)
SELECT stock_code, industry_name FROM ranked
WHERE rn = 1 AND industry_name IN ('Consumer Discretionary', 'Technology')
ORDER BY stock_code;

-- Case 5: Nested CTE with filter at each level
WITH base AS (
    SELECT * FROM ds_t_int_hsicl_dtl WHERE modified_date <= '2025-10-31'
),
ranked AS (
    SELECT stock_code, industry_name,
        ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY modified_date DESC) AS rn
    FROM base
)
SELECT COUNT(*) FROM ranked WHERE rn = 1 AND industry_name = 'Consumer Discretionary';

-- Case 6: CTE referenced multiple times with different filters
WITH ranked AS (
    SELECT stock_code, industry_name,
        ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY modified_date DESC) AS rn
    FROM ds_t_int_hsicl_dtl
    WHERE modified_date <= '2025-10-31'
),
latest AS (
    SELECT stock_code, industry_name FROM ranked WHERE rn = 1
)
SELECT
    (SELECT COUNT(*) FROM latest WHERE industry_name = 'Consumer Discretionary') AS cd_count,
    (SELECT COUNT(*) FROM latest WHERE industry_name = 'Technology') AS tech_count,
    (SELECT COUNT(*) FROM latest WHERE industry_name = 'Healthcare') AS hc_count;

drop table if exists ds_t_int_hsicl_dtl;
-- @bvt:issue
