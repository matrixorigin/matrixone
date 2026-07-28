-- @suit
-- @case
-- @desc: issue #12062 - parquet external table can be queried and joined with internal tables
-- @label:bvt

drop database if exists parquet_external_table_bvt;
create database parquet_external_table_bvt;
use parquet_external_table_bvt;

drop table if exists ext_parquet_basic;
create external table ext_parquet_basic (
    id int,
    amount double
) infile{'filepath'='$resources/hive_partition/non_hive/simple.parquet', 'format'='parquet'};

select count(*) as cnt from ext_parquet_basic;
select * from ext_parquet_basic order by id;

drop table if exists dim_amount;
create table dim_amount (
    id int,
    label varchar(20)
);
insert into dim_amount values
    (1, 'first'),
    (3, 'third');

select d.label, e.amount
from ext_parquet_basic e
join dim_amount d
    on e.id = d.id
order by e.id;

drop database if exists parquet_external_table_bvt;
