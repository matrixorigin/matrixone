-- MatrixOne mapping setup for the NYC TLC public dataset.
-- The driver script replaces placeholders before executing this file.

CREATE DATABASE IF NOT EXISTS __MO_DB__;

DROP TABLE IF EXISTS __MO_DB__.yellow_tripdata;
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
  'scope=cluster,account_id=0,external_principal=local-nyc-tlc,endpoint=localhost,region=us-east-1,bucket=mo-iceberg'
);

CREATE EXTERNAL TABLE __MO_DB__.yellow_tripdata (
  vendor_id INT,
  tpep_pickup_datetime TIMESTAMP(6),
  tpep_dropoff_datetime TIMESTAMP(6),
  passenger_count INT,
  trip_distance DOUBLE,
  rate_code_id INT,
  store_and_fwd_flag TEXT,
  pu_location_id INT,
  do_location_id INT,
  payment_type INT,
  fare_amount DECIMAL(12, 2),
  extra DECIMAL(12, 2),
  mta_tax DECIMAL(12, 2),
  tip_amount DECIMAL(12, 2),
  tolls_amount DECIMAL(12, 2),
  improvement_surcharge DECIMAL(12, 2),
  total_amount DECIMAL(12, 2),
  congestion_surcharge DECIMAL(12, 2),
  airport_fee DECIMAL(12, 2)
) ENGINE = ICEBERG
WITH (
  'catalog'='__MO_CATALOG__',
  'namespace'='public_nyc_tlc',
  'table'='yellow_tripdata',
  'ref'='main',
  'read_mode'='merge_on_read',
  'write_mode'='read_only'
);
