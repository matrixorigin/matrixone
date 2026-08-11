-- NYC TLC public dataset seed for Local Nessie + MinIO.
-- The driver script replaces placeholders before executing this file.

CREATE NAMESPACE IF NOT EXISTS __CATALOG__.public_nyc_tlc;

DROP TABLE IF EXISTS __CATALOG__.public_nyc_tlc.yellow_tripdata;

CREATE TABLE __CATALOG__.public_nyc_tlc.yellow_tripdata
USING iceberg
PARTITIONED BY (months(tpep_pickup_datetime))
TBLPROPERTIES (
  'format-version'='2',
  'write.delete.mode'='merge-on-read',
  'write.update.mode'='merge-on-read',
  'write.merge.mode'='merge-on-read'
)
AS
SELECT *
FROM (
  SELECT
    CAST(`VendorID` AS INT) AS vendor_id,
    CAST(tpep_pickup_datetime AS TIMESTAMP) AS tpep_pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS tpep_dropoff_datetime,
    CAST(passenger_count AS INT) AS passenger_count,
    CAST(trip_distance AS DOUBLE) AS trip_distance,
    CAST(`RatecodeID` AS INT) AS rate_code_id,
    CAST(store_and_fwd_flag AS STRING) AS store_and_fwd_flag,
    CAST(`PULocationID` AS INT) AS pu_location_id,
    CAST(`DOLocationID` AS INT) AS do_location_id,
    CAST(payment_type AS INT) AS payment_type,
    CAST(fare_amount AS DECIMAL(12, 2)) AS fare_amount,
    CAST(extra AS DECIMAL(12, 2)) AS extra,
    CAST(mta_tax AS DECIMAL(12, 2)) AS mta_tax,
    CAST(tip_amount AS DECIMAL(12, 2)) AS tip_amount,
    CAST(tolls_amount AS DECIMAL(12, 2)) AS tolls_amount,
    CAST(improvement_surcharge AS DECIMAL(12, 2)) AS improvement_surcharge,
    CAST(total_amount AS DECIMAL(12, 2)) AS total_amount,
    CAST(congestion_surcharge AS DECIMAL(12, 2)) AS congestion_surcharge,
    CAST(`Airport_fee` AS DECIMAL(12, 2)) AS airport_fee
  FROM parquet.`__SOURCE_PARQUET__`
  WHERE tpep_pickup_datetime >= TIMESTAMP '__START_TS__'
    AND tpep_pickup_datetime < TIMESTAMP '__END_TS__'
  __LIMIT_CLAUSE__
) src;
