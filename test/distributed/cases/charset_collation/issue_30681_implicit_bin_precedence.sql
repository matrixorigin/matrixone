-- @suite
-- @case
-- @desc: Implicit utf8mb4 _bin wins over _ci in JOIN and UNION (#30681)
-- @label:bvt

DROP DATABASE IF EXISTS issue_30681;
CREATE DATABASE issue_30681;
USE issue_30681;

CREATE TABLE ci_source (id INT, s VARCHAR(10) COLLATE utf8mb4_unicode_ci);
CREATE TABLE bin_source (id INT, s VARCHAR(10) COLLATE utf8mb4_bin);
INSERT INTO ci_source VALUES (1, 'Apple');
INSERT INTO bin_source VALUES (1, 'apple'), (2, 'Apple');

SELECT ci_source.s AS ci_value, bin_source.s AS bin_value
FROM ci_source JOIN bin_source ON ci_source.s = bin_source.s ORDER BY bin_source.id;
SELECT ci_source.s AS ci_value, bin_source.s AS bin_value
FROM bin_source JOIN ci_source ON bin_source.s = ci_source.s ORDER BY bin_source.id;
SELECT id, s FROM bin_source
WHERE s = (SELECT s FROM ci_source LIMIT 1) ORDER BY id;
SELECT id, s FROM bin_source
WHERE EXISTS (SELECT 1 FROM ci_source WHERE ci_source.s = bin_source.s) ORDER BY id;
SELECT ci_source.s AS ci_value, bin_source.s AS bin_value
FROM ci_source LEFT JOIN bin_source ON ci_source.s = bin_source.s ORDER BY bin_source.id;

SELECT s FROM ci_source UNION SELECT s FROM bin_source ORDER BY s;
SELECT s FROM bin_source UNION SELECT s FROM ci_source ORDER BY s;
SELECT COUNT(*) AS distinct_rows
FROM (SELECT s FROM ci_source UNION SELECT s FROM bin_source) AS u;
SELECT COUNT(*) AS all_rows
FROM (SELECT s FROM ci_source UNION ALL SELECT s FROM bin_source) AS u;

-- Equal explicit coercibility remains an illegal mix.
SELECT 'Apple' COLLATE utf8mb4_unicode_ci = 'apple' COLLATE utf8mb4_bin;
CREATE TABLE other_ci (s VARCHAR(10) COLLATE utf8mb4_general_ci);
INSERT INTO other_ci VALUES ('apple');
SELECT ci_source.s FROM ci_source JOIN other_ci ON ci_source.s = other_ci.s;

DROP DATABASE issue_30681;
