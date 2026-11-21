DROP TABLE IF EXISTS cte_32799_join_build;

-- Issue #32799: a recursive member may become productive after an empty round.
WITH RECURSIVE r(s, v) AS (
  SELECT 0, 1
  UNION ALL
  SELECT 1, v * 2 FROM r WHERE s = 0
  UNION ALL
  SELECT 2, v + 10 FROM r WHERE s = 1
  UNION ALL
  SELECT 3, v - 5 FROM r WHERE s IN (0, 1)
)
SELECT s, v FROM r ORDER BY s, v;

-- Related later-member regression: the second member only matches the next frontier.
WITH RECURSIVE r(s, v) AS (
  SELECT 0, 10
  UNION ALL
  SELECT 1, v + 1 FROM r WHERE s = 0
  UNION ALL
  SELECT 2, v + 3 FROM r WHERE s = 1
)
SELECT s, v FROM r ORDER BY s, v;

-- Duplicate elimination still converges after all members finish the round.
WITH RECURSIVE r(s, v) AS (
  SELECT 0, 1
  UNION DISTINCT
  SELECT 1, v * 2 FROM r WHERE s = 0
  UNION DISTINCT
  SELECT 2, v + 10 FROM r WHERE s = 1
  UNION DISTINCT
  SELECT 3, v - 5 FROM r WHERE s IN (0, 1)
)
SELECT s, v FROM r ORDER BY s, v;


-- Every member completes before DISTINCT decides the global frontier is empty.
WITH RECURSIVE r(n, member_id) AS (
  SELECT 0, 0
  UNION DISTINCT
  SELECT 0, 0 FROM r
  UNION DISTINCT
  SELECT 1, 1 FROM r
)
SELECT n, member_id FROM r ORDER BY n, member_id;


DROP TABLE IF EXISTS cte_32799_join_build;
CREATE TABLE cte_32799_join_build (n INT PRIMARY KEY, delta INT);
INSERT INTO cte_32799_join_build VALUES (1, 10), (2, 20), (3, 30);

-- A recursive member joins an independent build input; a later member only
-- becomes productive on the next frontier.
WITH RECURSIVE r(n, v) AS (
  SELECT 0, 0
  UNION ALL
  SELECT b.n, r.v + b.delta
  FROM r JOIN cte_32799_join_build AS b ON b.n = r.n + 1
  WHERE r.n < 3
  UNION ALL
  SELECT 100, v + 100 FROM r WHERE n = 1
)
SELECT n, v FROM r ORDER BY n, v;

DROP TABLE cte_32799_join_build;


-- A downstream LIMIT returns a bounded result from a multi-member recursive CTE.
WITH RECURSIVE r(n, member_id) AS (
  SELECT 0, 0
  UNION ALL
  SELECT n + 1, 0 FROM r WHERE n < 3
  UNION ALL
  SELECT 2, 1 FROM r WHERE n = 1
)
SELECT n, member_id FROM r LIMIT 1;
