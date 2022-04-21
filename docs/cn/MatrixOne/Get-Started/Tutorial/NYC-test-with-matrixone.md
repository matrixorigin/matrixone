# **完成NYC测试**

**NYC Taxi(纽约出租车)**  数据集收集了纽约市数十亿出租车行程的详细信息，包括接送日期/时间、接送地点、行程距离、详细票价、费率、支付类型和乘客数量。  
通过本教程，您将了解如何使用MatrixOne来完成对NYC Taxi数据集的查询。

该数据集的详细信息以及下载教程可参见：   
* [https://github.com/toddwschneider/nyc-taxi-data](https://github.com/toddwschneider/nyc-taxi-data)  

* [http://tech.marksblogg.com/billion-nyc-taxi-rides-redshift.html](http://tech.marksblogg.com/billion-nyc-taxi-rides-redshift.html). 

## **准备工作** 

确保你已经安装了[单机版MatrixOne](../install-standalone-matrixone.md)并[连接到MatrixOne服务](../connect-to-matrixone-server.md).
  

## **1. 下载并导入数据**
本节教程参考[https://github.com/toddwschneider/nyc-taxi-data](https://github.com/toddwschneider/nyc-taxi-data)，您可以从此处了解关于数据集下载和导入的详细信息。  
该数据集共有17亿行数据，占用450 GB的空间，请确保有足够的空间来存放数据。

* <font size=4>**安装PostgreSQL与PostGIS**</font>  

在Mac上，两者均可通过[Homebrew](https://brew.sh)安装。

* <font size=4>**下载原始数据**</font>

```
./download_raw_data.sh && ./remove_bad_rows.sh
```
`remove_bad_rows.sh`脚本修复了两个特定的文件，其中数据极少但是列字段很多。

* <font size=4>**初始化数据库**</font>

```
./initialize_database.sh
```

* <font size=4>**导入数据**</font>

```
./import_trip_data.sh 
./import_fhv_trip_data.sh
```

* <font size=4>**可选操作：下载并导入2014年Uber数据**</font>

FiveThirtyEight Uber数据集包含了2014年4月至9月的Uber出行记录。Uber和其他FHV (Lyft、Juno、Via等)的数据自2015年1月以来就被存储在TLC中。

```
./download_raw_2014_uber_data.sh 
./import_2014_uber_trip_data.sh
```


## **2. 从PostgreSQL导出数据**

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


## **3. 在MatrixOne中建表**
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

## **4. 向表中插入数据**

```
load data infile '/matrixone/export_data/trips.tsv ' into table trips 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n';
```

然后便可以使用MatrixOne中的查询语句来查询表中数据。

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
