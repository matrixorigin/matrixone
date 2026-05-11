-- !!!result below are same as postgresql and mysql
-- !!!
-- input: 5
-- null, null
SELECT stddev_samp(x), var_samp(x) FROM (SELECT 5 AS x) t;
-- input: 5,6
-- not,not
SELECT stddev_samp(x), var_samp(x) FROM (SELECT 5 AS x union select 6 as x) t;
-- input: 5
-- null, null
SELECT stddev_samp(x), var_samp(x) FROM (SELECT 5 AS x union select 5 as x) t;
-- input: 5,6
-- not,not
SELECT stddev_samp(x), var_samp(x) FROM (SELECT 5 AS x union all select 6 as x) t;
-- input: 5,5
-- not, not
SELECT stddev_samp(x), var_samp(x) FROM (SELECT 5 AS x union all select 5 as x) t;
----------------------
-- Mysql does not support the syntax below
-- input: 5,6
-- not,not
SELECT stddev_samp(DISTINCT x), var_samp(DISTINCT x) FROM (SELECT 5 AS x union select 6 as x) t;
-- input: 5
-- null, null
SELECT stddev_samp(DISTINCT x), var_samp(DISTINCT x) FROM (SELECT 5 AS x union select 5 as x) t;
-- input: 5,6
-- not,not
SELECT stddev_samp(DISTINCT x), var_samp(DISTINCT x) FROM (SELECT 5 AS x union all select 6 as x) t;
-- input: 5
-- not
SELECT stddev_samp(DISTINCT x), var_samp(DISTINCT x) FROM (SELECT 5 AS x union all select 5 as x) t;

