drop database if exists show_create_hive_ext_db;
create database show_create_hive_ext_db;
use show_create_hive_ext_db;

create external table local_hive_show (
    id int,
    part_id int
) infile{'filepath'='/data/test/', 'format'='parquet', 'hive_partitioning'='true', 'hive_partition_columns'='part_id'}
  fields terminated by ',' enclosed by '"' lines terminated by '\n';
show create table local_hive_show;

create external table s3_hive_show (
    id int,
    part_id int
) url s3option {
    'endpoint'='http://minio.local',
    'access_key_id'='AK',
    'secret_access_key'='SK',
    'bucket'='my-bucket',
    'filepath'='data/test/',
    'region'='us-east-1',
    'provider'='minio',
    'format'='parquet',
    'hive_partitioning'='true',
    'hive_partition_columns'='part_id'
};
show create table s3_hive_show;

drop database if exists show_create_hive_ext_db;
