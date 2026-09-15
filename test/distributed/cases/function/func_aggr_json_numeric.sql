-- JSON operands for the six numeric aggregate families use the existing
-- JSON-to-DOUBLE conversion boundary before aggregate execution.
DROP TABLE IF EXISTS json_numeric_agg;
CREATE TABLE json_numeric_agg (id INT, grp INT, j JSON);
INSERT INTO json_numeric_agg VALUES (1, 1, '1'), (2, 1, '2.5'), (3, 1, '3'), (4, 1, 'null'), (5, 1, NULL), (6, 2, '1'), (7, 2, '1.0'), (8, 2, '"1"'), (9, 2, '4'), (10, 2, 'null'), (11, 2, NULL), (12, 3, '"2.5"'), (13, 4, '9007199254740992'), (14, 4, '9007199254740993'), (15, 4, '-9007199254740993');

SELECT SUM(j), AVG(j), VAR_POP(j), VAR_SAMP(j), STDDEV_POP(j), STDDEV_SAMP(j) FROM json_numeric_agg WHERE grp = 1;
SELECT grp, SUM(j) FROM json_numeric_agg GROUP BY grp ORDER BY grp;
SELECT SUM(DISTINCT j), AVG(DISTINCT j), VAR_POP(DISTINCT j), VAR_SAMP(DISTINCT j), STDDEV_POP(DISTINCT j), STDDEV_SAMP(DISTINCT j) FROM json_numeric_agg WHERE grp = 2;
SELECT id, SUM(j) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM json_numeric_agg WHERE grp = 1 ORDER BY id;
SELECT SUM(j) FROM json_numeric_agg WHERE grp = 3;
SELECT SUM(j), SUM(DISTINCT j) FROM json_numeric_agg WHERE grp = 4;

DROP TABLE json_numeric_agg;
