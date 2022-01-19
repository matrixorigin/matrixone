# **Complete a NYC Test with MatrixOne**

**New York City (NTC) Taxi** data set captures detailed information on billions of individual taxi trips in New York City, including pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts(Most of the raw data comes from the NYC Taxi & Limousine Commission).  

By going through this tutorial, you’ll learn how to complete some queries on NYC Taxi data with MatrixOne.

For detail description and instructions for downloading about **NYC Taxi Data**, you can see:  

* [https://github.com/toddwschneider/nyc-taxi-data](https://github.com/toddwschneider/nyc-taxi-data)  

* [http://tech.marksblogg.com/billion-nyc-taxi-rides-redshift.html](http://tech.marksblogg.com/billion-nyc-taxi-rides-redshift.html). 

## **Before you begin** 

Make sure you have already [installed MatrixOne](../install-standalone-matrixone.md) and [connected to MatrixOne Server](../connect-to-matrixone-server.md).
  

## **1. Download and Import data**
This section of the tutorial references [here](https://github.com/toddwschneider/nyc-taxi-data), and you can get original information about how to download and import raw data there.

The data set has 1.7 billion rows data and takes up 450 GB of space, so make sure there are enough space to hold the data.  

* <font size=4>**Install PostgreSQL and PostGIS**</font>  

Both are available via [Homebrew](https://brew.sh) on Mac.  

* <font size=4>**Download raw data**</font>

```
./download_raw_data.sh && ./remove_bad_rows.sh
```
The remove_bad_rows.sh script fixes two particular files that have a few rows with too many columns.  
For more detailed information about this, you can see the original references.

* <font size=4>**Initialize database and set up schema**</font>

```
./initialize_database.sh
```

* <font size=4>**Import taxi and FHV data**</font>

```
./import_trip_data.sh 
./import_fhv_trip_data.sh
```

* <font size=4>**Optional: download and import 2014 Uber data**</font>

The FiveThirtyEight Uber dataset contains Uber trip records from Apr–Sep 2014. Uber and other FHV (Lyft, Juno, Via, etc.) data is available since Jan 2015 in the TLC's data.

```
./download_raw_2014_uber_data.sh 
./import_2014_uber_trip_data.sh
```


## **2. Exporting the data from PostgreSQL**

```
COPY
(
    SELECT trips.id,
           trips.vendor_id,
           trips.pickup_datetime,
           trips.dropoff_datetime,
           trips.store_and_fwd_flag,
           trips.rate_code_id,
           trips.pickup_longitude,
           trips.pickup_latitude,
           trips.dropoff_longitude,
           trips.dropoff_latitude,
           trips.passenger_count,
           trips.trip_distance,
           trips.fare_amount,
           trips.extra,
           trips.mta_tax,
           trips.tip_amount,
           trips.tolls_amount,
           trips.ehail_fee,
           trips.improvement_surcharge,
           trips.total_amount,
           trips.payment_type,
           trips.trip_type,
           trips.pickup_location_id,
           trips.dropoff_location_id,

           cab_types.type cab_type,

           weather.precipitation rain,
           weather.snow_depth,
           weather.snowfall,
           weather.max_temperature max_temp,
           weather.min_temperature min_temp,
           weather.average_wind_speed wind,

           pick_up.gid pickup_nyct2010_gid,
           pick_up.ctlabel pickup_ctlabel,
           pick_up.borocode pickup_borocode,
           pick_up.boroname pickup_boroname,
           pick_up.ct2010 pickup_ct2010,
           pick_up.boroct2010 pickup_boroct2010,
           pick_up.cdeligibil pickup_cdeligibil,
           pick_up.ntacode pickup_ntacode,
           pick_up.ntaname pickup_ntaname,
           pick_up.puma pickup_puma,
           
           drop_off.gid dropoff_nyct2010_gid,
           drop_off.ctlabel dropoff_ctlabel,
           drop_off.borocode dropoff_borocode,
           drop_off.boroname dropoff_boroname,
           drop_off.ct2010 dropoff_ct2010,
           drop_off.boroct2010 dropoff_boroct2010,
           drop_off.cdeligibil dropoff_cdeligibil,
           drop_off.ntacode dropoff_ntacode,
           drop_off.ntaname dropoff_ntaname,
           drop_off.puma dropoff_puma
           
           FROM trips
           LEFT JOIN cab_types
               ON trips.cab_type_id = cab_types.id
           LEFT JOIN central_park_weather_observations weather
               ON weather.date = trips.pickup_datetime::date
           LEFT JOIN nyct2010 pick_up
               ON pick_up.gid = trips.pickup_nyct2010_gid
           LEFT JOIN nyct2010 drop_off
               ON drop_off.gid = trips.dropoff_nyct2010_gid
) TO '/matrixone/export_data/trips.tsv';
```


## **3. Create tables in MatrixOne**
```
CREATE TABLE trips
(
    trip_id                 int unsigned,
    vendor_id               varchar(64),
    pickup_datetime         bigint unsigned,
    dropoff_datetime        bigint unsigned,
    store_and_fwd_flag      char(1),
    rate_code_id            smallint unsigned,
    pickup_longitude        double,
    pickup_latitude         double,
    dropoff_longitude       double,
    dropoff_latitude        double,
    passenger_count         smallint unsigned,
    trip_distance           double,
    distance                bigint,
    fare_amount             float,
    extra                   float,
    mta_tax                 float,
    tip_amount              float,
    tolls_amount            float,
    ehail_fee               float,
    improvement_surcharge   float,
    total_amount            float,
    payment_type            varchar(64),
    trip_type               smallint unsigned,
    pickup                  varchar(64),
    dropoff                 varchar(64),
    cab_type                varchar(64),
    precipitation           float,
    snow_depth              float,
    snowfall                float,
    max_temperature         smallint,
    min_temperature         smallint,
    average_wind_speed      float,
    pickup_nyct2010_gid     smallint unsigned,
    pickup_ctlabel          varchar(64),
    pickup_borocode         smallint unsigned,
    pickup_boroname         varchar(64),
    pickup_ct2010           varchar(64),
    pickup_boroct2010       varchar(64),
    pickup_cdeligibil       char(1),
    pickup_ntacode          varchar(64),
    pickup_ntaname          varchar(64),
    pickup_puma             varchar(64),
    dropoff_nyct2010_gid    smallint unsigned,
    dropoff_ctlabel         varchar(64),
    dropoff_borocode        smallint unsigned,
    dropoff_boroname        varchar(64),
    dropoff_ct2010          varchar(64),
    dropoff_boroct2010      varchar(64),
    dropoff_cdeligibil      varchar(64),
    dropoff_ntacode         varchar(64),
    dropoff_ntaname         varchar(64),
    dropoff_puma            varchar(64)
) ;
```

## **4. Insert data into the created tables**

```
load data infile '/matrixone/export_data/trips.tsv ' into table trips FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';
```


Then you can query data in MatrixOne with the created table. 

## **5. Run Queries**

```
# Q1
SELECT cab_type, count(*) FROM trips GROUP BY cab_type;

# Q2
SELECT passenger_count, avg(total_amount) FROM trips GROUP BY passenger_count;

# Q3
SELECT passenger_count, year(pickup_datetime) as year, count(*) FROM trips GROUP BY passenger_count, year;

# Q4
SELECT passenger_count, year(pickup_datetime) as year, round(trip_distance) AS  distance, count(*) as count 
FROM trips 
GROUP BY passenger_count, year, distance 
ORDER BY year,count DESC;
```