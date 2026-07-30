drop snapshot if exists sp_24559;
drop database if exists restore_hive_24559;

create database restore_hive_24559;
use restore_hive_24559;

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
show full tables from restore_hive_24559;

create snapshot sp_24559 for account;
insert into base_t values (3);

restore table restore_hive_24559.hive_ext{snapshot="sp_24559"};
restore database restore_hive_24559{snapshot="sp_24559"};

select count(*) from restore_hive_24559.base_t;
show full tables from restore_hive_24559;

drop snapshot if exists sp_24559;
drop database if exists restore_hive_24559;
