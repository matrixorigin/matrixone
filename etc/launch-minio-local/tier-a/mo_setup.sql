-- MatrixOne mapping setup for Iceberg Tier A.
-- The driver script replaces __MO_DB__, __MO_CATALOG__, __CATALOG_URI__, and __WAREHOUSE__.

CREATE DATABASE IF NOT EXISTS __MO_DB__;

DROP TABLE IF EXISTS __MO_DB__.tpch_sf01_orders;
DROP TABLE IF EXISTS __MO_DB__.tpch_sf01_orders_imported_native;
DROP TABLE IF EXISTS __MO_DB__.nyc_taxi_trips_small;
DROP TABLE IF EXISTS __MO_DB__.evolution_users;
DROP TABLE IF EXISTS __MO_DB__.delete_files_orders_append_only;
DROP TABLE IF EXISTS __MO_DB__.delete_files_orders_mor;
DROP TABLE IF EXISTS __MO_DB__.dml_accounts;
DROP TABLE IF EXISTS __MO_DB__.dml_accounts_updates;
DROP TABLE IF EXISTS __MO_DB__.dml_accounts_by_region;
DROP TABLE IF EXISTS __MO_DB__.dml_accounts_by_region_stage;
DROP TABLE IF EXISTS __MO_DB__.refs_orders_branch;
DROP TABLE IF EXISTS __MO_DB__.writer_gold_kpi;
DROP TABLE IF EXISTS __MO_DB__.maintenance_orders_small;
DROP ICEBERG CATALOG IF EXISTS __MO_CATALOG__;

CREATE ICEBERG CATALOG __MO_CATALOG__
WITH (
  'type'='rest',
  'uri'='__CATALOG_URI__',
  'warehouse'='__WAREHOUSE__',
  'auth_mode'='none'
);

CALL iceberg_register_access(
  '__MO_CATALOG__',
  'scope=cluster,account_id=0,external_principal=local-tier-a,endpoint=localhost,region=us-east-1,bucket=mo-iceberg'
);

CREATE EXTERNAL TABLE __MO_DB__.tpch_sf01_orders (
  order_id BIGINT,
  cust_id BIGINT,
  order_status TEXT,
  order_date DATE,
  total_price DECIMAL(12, 2),
  bucket INT
) ENGINE = ICEBERG
WITH (
  'catalog'='__MO_CATALOG__',
  'namespace'='tpch_sf01',
  'table'='orders',
  'ref'='main',
  'read_mode'='append_only',
  'write_mode'='append_only'
);

CREATE EXTERNAL TABLE __MO_DB__.nyc_taxi_trips_small (
  trip_id BIGINT,
  pickup_ts DATETIME(6),
  pickup_date DATE,
  passenger_count INT,
  fare DOUBLE,
  zone TEXT
) ENGINE = ICEBERG
WITH (
  'catalog'='__MO_CATALOG__',
  'namespace'='nyc_taxi',
  'table'='trips_small',
  'ref'='main',
  'read_mode'='append_only',
  'write_mode'='read_only'
);

CREATE EXTERNAL TABLE __MO_DB__.evolution_users (
  id INT,
  full_name TEXT,
  region TEXT,
  age BIGINT,
  score DOUBLE,
  credit DECIMAL(12, 2)
) ENGINE = ICEBERG
WITH (
  'catalog'='__MO_CATALOG__',
  'namespace'='evolution',
  'table'='users',
  'ref'='main',
  'read_mode'='append_only',
  'write_mode'='read_only'
);

CREATE EXTERNAL TABLE __MO_DB__.delete_files_orders_append_only (
  order_id BIGINT,
  hidden_key BIGINT,
  bucket INT,
  amount BIGINT
) ENGINE = ICEBERG
WITH (
  'catalog'='__MO_CATALOG__',
  'namespace'='delete_files',
  'table'='orders_mor_append_only',
  'ref'='main',
  'read_mode'='append_only',
  'write_mode'='read_only'
);

CREATE EXTERNAL TABLE __MO_DB__.delete_files_orders_mor (
  order_id BIGINT,
  hidden_key BIGINT,
  bucket INT,
  amount BIGINT
) ENGINE = ICEBERG
WITH (
  'catalog'='__MO_CATALOG__',
  'namespace'='delete_files',
  'table'='orders_mor',
  'ref'='main',
  'read_mode'='merge_on_read',
  'write_mode'='merge_on_read'
);

CREATE EXTERNAL TABLE __MO_DB__.dml_accounts (
  account_id BIGINT,
  balance BIGINT,
  region TEXT
) ENGINE = ICEBERG
WITH (
  'catalog'='__MO_CATALOG__',
  'namespace'='dml',
  'table'='accounts',
  'ref'='main',
  'read_mode'='merge_on_read',
  'write_mode'='merge_on_read'
);

CREATE EXTERNAL TABLE __MO_DB__.dml_accounts_updates (
  account_id BIGINT,
  balance BIGINT,
  region TEXT
) ENGINE = ICEBERG
WITH (
  'catalog'='__MO_CATALOG__',
  'namespace'='dml',
  'table'='accounts_updates',
  'ref'='main',
  'read_mode'='append_only',
  'write_mode'='read_only'
);

CREATE EXTERNAL TABLE __MO_DB__.dml_accounts_by_region (
  account_id BIGINT,
  balance BIGINT,
  region TEXT
) ENGINE = ICEBERG
WITH (
  'catalog'='__MO_CATALOG__',
  'namespace'='dml',
  'table'='accounts_by_region',
  'ref'='main',
  'read_mode'='merge_on_read',
  'write_mode'='merge_on_read'
);

CREATE EXTERNAL TABLE __MO_DB__.dml_accounts_by_region_stage (
  account_id BIGINT,
  balance BIGINT,
  region TEXT
) ENGINE = ICEBERG
WITH (
  'catalog'='__MO_CATALOG__',
  'namespace'='dml',
  'table'='accounts_by_region_stage',
  'ref'='main',
  'read_mode'='append_only',
  'write_mode'='read_only'
);

CREATE EXTERNAL TABLE __MO_DB__.refs_orders_branch (
  order_id BIGINT,
  bucket INT,
  amount BIGINT
) ENGINE = ICEBERG
WITH (
  'catalog'='__MO_CATALOG__',
  'namespace'='refs',
  'table'='orders_branch',
  'ref'='main',
  'read_mode'='append_only',
  'write_mode'='read_only'
);

CREATE EXTERNAL TABLE __MO_DB__.writer_gold_kpi (
  id BIGINT,
  region TEXT,
  metric_date DATE,
  kpi_value BIGINT,
  source TEXT
) ENGINE = ICEBERG
WITH (
  'catalog'='__MO_CATALOG__',
  'namespace'='writer',
  'table'='gold_kpi',
  'ref'='main',
  'read_mode'='append_only',
  'write_mode'='append_only'
);

CREATE EXTERNAL TABLE __MO_DB__.maintenance_orders_small (
  order_id BIGINT,
  bucket INT,
  amount BIGINT
) ENGINE = ICEBERG
WITH (
  'catalog'='__MO_CATALOG__',
  'namespace'='maintenance',
  'table'='orders_small',
  'ref'='main',
  'read_mode'='merge_on_read',
  'write_mode'='merge_on_read'
);
