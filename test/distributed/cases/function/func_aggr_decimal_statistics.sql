-- DECIMAL statistical result domains and tiny nonzero values (#30179).
DROP DATABASE IF EXISTS decimal_statistics;
CREATE DATABASE decimal_statistics;
USE decimal_statistics;
CREATE TABLE tiny (g INT, d DECIMAL(38,18));
INSERT INTO tiny VALUES (1,0.000000000000000001),(1,0.000000000000000002),
(1,0.000000000000000003),(1,NULL),(2,0.000000000000000002),(3,NULL);

-- Raw client metadata must be DOUBLE; scaled bounds also reject a rounded zero.
-- @metacmp(true)
SELECT VAR_POP(d), VAR_SAMP(d), STDDEV_POP(d), STDDEV_SAMP(d) FROM tiny WHERE g=1;
SELECT VAR_POP(d)*1e36 BETWEEN 0.66666666666666 AND 0.66666666666668 AS vp,
       VAR_SAMP(d)*1e36 BETWEEN 0.99999999999999 AND 1.00000000000001 AS vs,
       STDDEV_POP(d)*1e18 BETWEEN 0.81649658092772 AND 0.81649658092774 AS sp,
       STDDEV_SAMP(d)*1e18 BETWEEN 0.99999999999999 AND 1.00000000000001 AS ss
FROM tiny WHERE g=1;
-- Force the existing multi-CN aggregation path for the grouped result.
SET @decimal_statistics_hints=@@optimizer_hints;
SET @@optimizer_hints='execType=2';
SELECT g, VAR_POP(d)*1e36 BETWEEN 0.66666666666666 AND 0.66666666666668 AS vp,
          VAR_SAMP(d)*1e36 BETWEEN 0.99999999999999 AND 1.00000000000001 AS vs,
          STDDEV_POP(d)*1e18 BETWEEN 0.81649658092772 AND 0.81649658092774 AS sp,
          STDDEV_SAMP(d)*1e18 BETWEEN 0.99999999999999 AND 1.00000000000001 AS ss
FROM tiny WHERE g=1 GROUP BY g;
SET @@optimizer_hints=@decimal_statistics_hints;
SELECT g, VAR_POP(d) OVER (PARTITION BY g)*1e36 BETWEEN 0.66666666666666 AND 0.66666666666668 AS vp,
          VAR_SAMP(d) OVER (PARTITION BY g)*1e36 BETWEEN 0.99999999999999 AND 1.00000000000001 AS vs,
          STDDEV_POP(d) OVER (PARTITION BY g)*1e18 BETWEEN 0.81649658092772 AND 0.81649658092774 AS sp,
          STDDEV_SAMP(d) OVER (PARTITION BY g)*1e18 BETWEEN 0.99999999999999 AND 1.00000000000001 AS ss
FROM tiny WHERE g=1 ORDER BY d;

-- SQL PREPARE reuses its plan for a nonempty and singleton population.
PREPARE decimal_statistics_stmt FROM 'SELECT VAR_POP(d), VAR_SAMP(d), STDDEV_POP(d), STDDEV_SAMP(d) FROM tiny WHERE g=?';
SET @decimal_statistics_group=1;
-- @metacmp(true)
EXECUTE decimal_statistics_stmt USING @decimal_statistics_group;
SET @decimal_statistics_group=2;
EXECUTE decimal_statistics_stmt USING @decimal_statistics_group;
DEALLOCATE PREPARE decimal_statistics_stmt;
SELECT VAR_POP(d), VAR_SAMP(d), STDDEV_POP(d), STDDEV_SAMP(d) FROM tiny WHERE g=3;
SELECT VAR_POP(d), VAR_SAMP(d), STDDEV_POP(d), STDDEV_SAMP(d) FROM tiny WHERE g=0;

-- Aliases materialize DOUBLE, while exact SUM/AVG preserve their own domains.
CREATE TABLE domains AS SELECT VAR_POP(d) AS vp, VAR_SAMP(d) AS vs,
 STDDEV_POP(d) AS sp, STDDEV_SAMP(d) AS ss, VARIANCE(d) AS variance_alias,
 STD(d) AS std_alias, STDDEV(d) AS stddev_alias, SUM(d) AS decimal_sum, AVG(d) AS decimal_avg FROM tiny;
SELECT column_name,data_type FROM information_schema.columns
WHERE table_schema='decimal_statistics' AND table_name='domains' ORDER BY ordinal_position;

-- DECIMAL64 scale18 also requires squared units below the input scale.
CREATE TABLE small (d DECIMAL(18,18));
INSERT INTO small VALUES (0.000000000000000001),(0.000000000000000002),(0.000000000000000003),(0.000000000000000001),(NULL);
SELECT VAR_POP(DISTINCT d)*1e36 BETWEEN 0.66666666666666 AND 0.66666666666668 AS vp,
 VAR_SAMP(DISTINCT d)*1e36 BETWEEN 0.99999999999999 AND 1.00000000000001 AS vs,
 STDDEV_POP(DISTINCT d)*1e18 BETWEEN 0.81649658092772 AND 0.81649658092774 AS sp,
 STDDEV_SAMP(DISTINCT d)*1e18 BETWEEN 0.99999999999999 AND 1.00000000000001 AS ss FROM small;
DROP DATABASE decimal_statistics;
