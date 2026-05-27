drop pitr if exists pitr_24559;
drop database if exists restore_hive_pitr_24559;

create database restore_hive_pitr_24559;
create pitr pitr_24559 for database restore_hive_pitr_24559 range 1 'h';
use restore_hive_pitr_24559;

create table base_t (id int primary key);
insert into base_t values (1), (2);

create external table hive_ext (
    id int,
    amount double,
    year int
) infile{
    'filepath'='$resources/hive_partition/single_level/',
    'format'='parquet',
    'hive_partitioning'='true',
    'hive_partition_columns'='year'
};

select count(*) from base_t;
select count(*) from hive_ext;
show full tables from restore_hive_pitr_24559;

create table hive_ext_clone clone hive_ext;

drop pitr if exists pitr_24559;
drop database if exists restore_hive_pitr_24559;
