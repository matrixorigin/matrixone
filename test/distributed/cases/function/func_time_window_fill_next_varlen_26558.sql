-- Regression for #26558: FILL(next) must not crash CN when materializing wide VARCHAR gaps
DROP DATABASE IF EXISTS fill_next_crash_26558;
CREATE DATABASE fill_next_crash_26558;
USE fill_next_crash_26558;

CREATE TABLE t (
  tenant INT NOT NULL,
  ts DATETIME NOT NULL,
  v VARCHAR(256),
  PRIMARY KEY (tenant, ts)
);

INSERT INTO t VALUES
  (1, '2025-01-01 00:00:00', REPEAT('a', 256)),
  (1, '2025-01-01 16:40:00', REPEAT('b', 256));

-- This query used to terminate the CN with SIGSEGV in BuildVarlenaNoInline.
-- Expected: 1001 rows with materialized VARCHAR values for each filled window.
SELECT COUNT(*) AS n, SUM(LENGTH(v)) AS bytes
FROM (
  SELECT tenant, _wstart, MAX(v) AS v
  FROM t
  GROUP BY tenant
  INTERVAL(ts, 1, minute) GAPFILL(PARTITION) FILL(next)
) AS x;

-- Expected output: (1001, 256256)

-- Cleanup
DROP DATABASE fill_next_crash_26558;
