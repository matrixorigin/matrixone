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
select count(*) from mo_catalog.mo_tables where reldatabase = 'restore_hive_24559' and relname = 'base_t' and relkind = 'r';
select count(*) from mo_catalog.mo_tables where reldatabase = 'restore_hive_24559' and relname = 'hive_ext' and relkind = 'e';

create snapshot sp_24559 for account;
insert into base_t values (3);

restore account sys database restore_hive_24559 table hive_ext from snapshot sp_24559;
restore account sys database restore_hive_24559 from snapshot sp_24559;

select count(*) from restore_hive_24559.base_t;
select count(*) from mo_catalog.mo_tables where reldatabase = 'restore_hive_24559' and relname = 'base_t' and relkind = 'r';
select count(*) from mo_catalog.mo_tables where reldatabase = 'restore_hive_24559' and relname = 'hive_ext';

drop snapshot if exists sp_24559;
drop database if exists restore_hive_24559;
