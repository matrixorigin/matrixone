-- Deterministic Iceberg Tier A seed for Local Nessie + MinIO.
-- The driver script replaces __CATALOG__ with the Spark catalog name.

CREATE NAMESPACE IF NOT EXISTS __CATALOG__.tpch_sf01;
CREATE NAMESPACE IF NOT EXISTS __CATALOG__.nyc_taxi;
CREATE NAMESPACE IF NOT EXISTS __CATALOG__.evolution;
CREATE NAMESPACE IF NOT EXISTS __CATALOG__.delete_files;
CREATE NAMESPACE IF NOT EXISTS __CATALOG__.dml;
CREATE NAMESPACE IF NOT EXISTS __CATALOG__.refs;
CREATE NAMESPACE IF NOT EXISTS __CATALOG__.writer;
CREATE NAMESPACE IF NOT EXISTS __CATALOG__.maintenance;

DROP TABLE IF EXISTS __CATALOG__.tpch_sf01.orders;
DROP TABLE IF EXISTS __CATALOG__.nyc_taxi.trips_small;
DROP TABLE IF EXISTS __CATALOG__.evolution.users;
DROP TABLE IF EXISTS __CATALOG__.delete_files.orders_mor;
DROP TABLE IF EXISTS __CATALOG__.delete_files.orders_mor_append_only;
DROP TABLE IF EXISTS __CATALOG__.dml.accounts;
DROP TABLE IF EXISTS __CATALOG__.dml.accounts_updates;
DROP TABLE IF EXISTS __CATALOG__.dml.accounts_by_region;
DROP TABLE IF EXISTS __CATALOG__.dml.accounts_by_region_stage;
DROP TABLE IF EXISTS __CATALOG__.refs.orders_branch;
DROP TABLE IF EXISTS __CATALOG__.writer.gold_kpi;
DROP TABLE IF EXISTS __CATALOG__.maintenance.orders_small;

CREATE TABLE __CATALOG__.tpch_sf01.orders (
  order_id BIGINT,
  cust_id BIGINT,
  order_status STRING,
  order_date DATE,
  total_price DECIMAL(12, 2),
  bucket INT
) USING iceberg
PARTITIONED BY (bucket)
TBLPROPERTIES ('format-version'='2');

INSERT INTO __CATALOG__.tpch_sf01.orders VALUES
  (1, 10, 'O', DATE '2026-01-01', CAST(100.25 AS DECIMAL(12, 2)), 1),
  (2, 20, 'F', DATE '2026-01-02', CAST(200.50 AS DECIMAL(12, 2)), 1),
  (3, 30, 'O', DATE '2026-02-01', CAST(300.75 AS DECIMAL(12, 2)), 2);

INSERT INTO __CATALOG__.tpch_sf01.orders VALUES
  (4, 40, 'P', DATE '2026-02-02', CAST(400.00 AS DECIMAL(12, 2)), 2),
  (5, 50, 'O', DATE '2026-03-01', CAST(500.10 AS DECIMAL(12, 2)), 3);

CREATE TABLE __CATALOG__.nyc_taxi.trips_small (
  trip_id BIGINT,
  pickup_ts TIMESTAMP,
  pickup_date DATE,
  passenger_count INT,
  fare DOUBLE,
  zone STRING
) USING iceberg
PARTITIONED BY (pickup_date)
TBLPROPERTIES ('format-version'='2');

INSERT INTO __CATALOG__.nyc_taxi.trips_small VALUES
  (1001, TIMESTAMP '2026-01-01 07:30:00', DATE '2026-01-01', 1, 12.50D, 'midtown'),
  (1002, TIMESTAMP '2026-01-01 08:00:00', DATE '2026-01-01', 2, 18.75D, 'downtown'),
  (1003, TIMESTAMP '2026-01-02 09:15:00', DATE '2026-01-02', 1, 23.00D, 'airport');

CREATE TABLE __CATALOG__.evolution.users (
  id INT,
  user_name STRING,
  age INT,
  score FLOAT,
  credit DECIMAL(9, 2)
) USING iceberg
TBLPROPERTIES ('format-version'='2');

INSERT INTO __CATALOG__.evolution.users VALUES
  (1, 'alice', 31, CAST(10.5 AS FLOAT), CAST(100.25 AS DECIMAL(9, 2))),
  (2, 'bob', 42, CAST(20.25 AS FLOAT), CAST(200.50 AS DECIMAL(9, 2)));

ALTER TABLE __CATALOG__.evolution.users RENAME COLUMN user_name TO full_name;
ALTER TABLE __CATALOG__.evolution.users ADD COLUMN region STRING;
ALTER TABLE __CATALOG__.evolution.users ALTER COLUMN age TYPE BIGINT;
ALTER TABLE __CATALOG__.evolution.users ALTER COLUMN score TYPE DOUBLE;
ALTER TABLE __CATALOG__.evolution.users ALTER COLUMN credit TYPE DECIMAL(12, 2);
ALTER TABLE __CATALOG__.evolution.users ALTER COLUMN region AFTER full_name;

INSERT INTO __CATALOG__.evolution.users (id, full_name, region, age, score, credit) VALUES
  (3, 'carol', 'ksa', 29, 30.75D, CAST(300.75 AS DECIMAL(12, 2)));

CREATE TABLE __CATALOG__.delete_files.orders_mor (
  order_id BIGINT,
  hidden_key BIGINT,
  bucket INT,
  amount BIGINT
) USING iceberg
PARTITIONED BY (bucket)
TBLPROPERTIES (
  'format-version'='2',
  'write.delete.mode'='merge-on-read',
  'write.update.mode'='merge-on-read',
  'write.merge.mode'='merge-on-read'
);

INSERT INTO __CATALOG__.delete_files.orders_mor VALUES
  (1, 101, 1, 1000),
  (2, 102, 1, 2000),
  (3, 103, 2, 3000),
  (4, 104, 2, 4000),
  (5, 105, 3, 5000);

DELETE FROM __CATALOG__.delete_files.orders_mor WHERE order_id IN (2, 5);

CREATE TABLE __CATALOG__.delete_files.orders_mor_append_only (
  order_id BIGINT,
  hidden_key BIGINT,
  bucket INT,
  amount BIGINT
) USING iceberg
PARTITIONED BY (bucket)
TBLPROPERTIES (
  'format-version'='2',
  'write.delete.mode'='merge-on-read',
  'write.update.mode'='merge-on-read',
  'write.merge.mode'='merge-on-read'
);

INSERT INTO __CATALOG__.delete_files.orders_mor_append_only VALUES
  (11, 111, 1, 11000),
  (12, 112, 1, 12000),
  (13, 113, 2, 13000);

DELETE FROM __CATALOG__.delete_files.orders_mor_append_only WHERE order_id = 12;

CREATE TABLE __CATALOG__.dml.accounts (
  account_id BIGINT,
  balance BIGINT,
  region STRING
) USING iceberg
PARTITIONED BY (region)
TBLPROPERTIES (
  'format-version'='2',
  'write.delete.mode'='merge-on-read',
  'write.update.mode'='merge-on-read',
  'write.merge.mode'='merge-on-read'
);

INSERT INTO __CATALOG__.dml.accounts VALUES
  (-9001, 100, 'ksa'),
  (-9002, 200, 'ksa'),
  (-9003, 300, 'uae'),
  (-9004, 400, 'uae');

CREATE TABLE __CATALOG__.dml.accounts_updates (
  account_id BIGINT,
  balance BIGINT,
  region STRING
) USING iceberg
TBLPROPERTIES ('format-version'='2');

INSERT INTO __CATALOG__.dml.accounts_updates VALUES
  (-9002, 220, 'ksa'),
  (-9005, 500, 'ksa');

CREATE TABLE __CATALOG__.dml.accounts_by_region (
  account_id BIGINT,
  balance BIGINT,
  region STRING
) USING iceberg
PARTITIONED BY (region)
TBLPROPERTIES (
  'format-version'='2',
  'write.delete.mode'='merge-on-read',
  'write.update.mode'='merge-on-read',
  'write.merge.mode'='merge-on-read'
);

INSERT INTO __CATALOG__.dml.accounts_by_region VALUES
  (-9101, 100, 'ksa'),
  (-9102, 200, 'ksa'),
  (-9201, 300, 'uae'),
  (-9202, 400, 'uae');

CREATE TABLE __CATALOG__.dml.accounts_by_region_stage (
  account_id BIGINT,
  balance BIGINT,
  region STRING
) USING iceberg
TBLPROPERTIES ('format-version'='2');

INSERT INTO __CATALOG__.dml.accounts_by_region_stage VALUES
  (-9301, 900, 'ksa'),
  (-9302, 901, 'ksa');

CREATE TABLE __CATALOG__.refs.orders_branch (
  order_id BIGINT,
  bucket INT,
  amount BIGINT
) USING iceberg
PARTITIONED BY (bucket)
TBLPROPERTIES ('format-version'='2');

INSERT INTO __CATALOG__.refs.orders_branch VALUES
  (1, 1, 100),
  (2, 2, 200);

ALTER TABLE __CATALOG__.refs.orders_branch CREATE BRANCH tier_a_branch;
ALTER TABLE __CATALOG__.refs.orders_branch CREATE TAG tier_a_tag;

CREATE TABLE __CATALOG__.writer.gold_kpi (
  id BIGINT,
  region STRING,
  metric_date DATE,
  kpi_value BIGINT,
  source STRING
) USING iceberg
PARTITIONED BY (region)
TBLPROPERTIES ('format-version'='2');

INSERT INTO __CATALOG__.writer.gold_kpi VALUES
  (1, 'ksa', DATE '2026-06-01', 100, 'spark_base'),
  (2, 'uae', DATE '2026-06-01', 200, 'spark_base');

INSERT INTO __CATALOG__.writer.gold_kpi VALUES
  (3, 'ksa', DATE '2026-06-02', 300, 'spark_base');

CREATE TABLE __CATALOG__.maintenance.orders_small (
  order_id BIGINT,
  bucket INT,
  amount BIGINT
) USING iceberg
PARTITIONED BY (bucket)
TBLPROPERTIES ('format-version'='2');

INSERT INTO __CATALOG__.maintenance.orders_small VALUES
  (1, 1, 100);

INSERT INTO __CATALOG__.maintenance.orders_small VALUES
  (2, 1, 200);

INSERT INTO __CATALOG__.maintenance.orders_small VALUES
  (3, 2, 300);

INSERT INTO __CATALOG__.maintenance.orders_small VALUES
  (4, 2, 400);
