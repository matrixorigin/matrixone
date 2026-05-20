drop database if exists parquet_json_db;
create database parquet_json_db;
use parquet_json_db;

drop table if exists ext_parquet_json;
create external table ext_parquet_json (
    id int,
    col_json json,
    part_id int
) infile{'filepath'='$resources/hive_partition/json_string/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partition_columns'='part_id'};

select
    id,
    col_json,
    json_unquote(json_extract(col_json, '$.k')) as k,
    json_extract(col_json, '$.n') as n,
    part_id
from ext_parquet_json
where part_id = 1
order by id;

select count(*) as null_jsons from ext_parquet_json where col_json is null;

drop table if exists ext_parquet_json;
drop database if exists parquet_json_db;
