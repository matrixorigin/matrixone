-- DISTINCT aggregate IDs retain the same empty-input contract as their base aggregate.
DROP DATABASE IF EXISTS correlated_distinct_aggregate_28300;
CREATE DATABASE correlated_distinct_aggregate_28300;
USE correlated_distinct_aggregate_28300;

CREATE TABLE parent_t(id INT PRIMARY KEY);
CREATE TABLE child_t(parent_id INT, v INT);
INSERT INTO parent_t VALUES (1), (2), (3);
INSERT INTO child_t VALUES (1, 10), (1, 10), (1, 20), (1, NULL), (2, NULL);

SELECT p.id,
       (SELECT COUNT(DISTINCT c.v) FROM child_t c WHERE c.parent_id = p.id) <=> CASE WHEN p.id = 1 THEN 2 ELSE 0 END AS count_ok,
       (SELECT SUM(DISTINCT c.v) FROM child_t c WHERE c.parent_id = p.id) <=> CASE WHEN p.id = 1 THEN 30 ELSE NULL END AS sum_ok,
       (SELECT AVG(DISTINCT c.v) FROM child_t c WHERE c.parent_id = p.id) <=> CASE WHEN p.id = 1 THEN 15.0 ELSE NULL END AS avg_ok,
       (SELECT GROUP_CONCAT(DISTINCT c.v ORDER BY c.v) FROM child_t c WHERE c.parent_id = p.id) <=> CASE WHEN p.id = 1 THEN '10,20' ELSE NULL END AS concat_ok
FROM parent_t p
ORDER BY p.id;

DROP DATABASE correlated_distinct_aggregate_28300;
