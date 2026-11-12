-- @suit
-- @case
-- @desc: issue #31920 - read-only file external tables reject TRUNCATE and ALTER with typed diagnostics
-- @label:bvt

drop database if exists external_table_truncate_alter;
create database external_table_truncate_alter;
use external_table_truncate_alter;

create external table ext_ro_csv (id int, s varchar(16)) infile{'filepath'='$resources/external_table_truncate_alter/ro.csv','format'='csv'};
select id, s from ext_ro_csv order by id;
truncate table ext_ro_csv;
select id, s from ext_ro_csv order by id;
alter table ext_ro_csv add column extra int;
alter table ext_ro_csv modify column s varchar(32);
alter table ext_ro_csv change column s s2 varchar(16);
select id, s from ext_ro_csv order by id;
insert into ext_ro_csv values (9, 'x');

create external table ext_ro_jsonline (id int, s varchar(16)) infile{'filepath'='$resources/external_table_truncate_alter/ro.jsonline','format'='jsonline','jsondata'='object'};
select id, s from ext_ro_jsonline order by id;
truncate table ext_ro_jsonline;
select id, s from ext_ro_jsonline order by id;
alter table ext_ro_jsonline add column extra int;
select id, s from ext_ro_jsonline order by id;

create external table ext_ro_parquet (id int, s varchar(16)) infile{'filepath'='$resources/external_table_truncate_alter/ro.parquet','format'='parquet'};
select id, s from ext_ro_parquet order by id;
truncate table ext_ro_parquet;
select id, s from ext_ro_parquet order by id;
alter table ext_ro_parquet add column extra int;
select id, s from ext_ro_parquet order by id;

select column_name, data_type from information_schema.columns where table_schema='external_table_truncate_alter' and table_name='ext_ro_csv' order by ordinal_position;
select column_name, data_type from information_schema.columns where table_schema='external_table_truncate_alter' and table_name='ext_ro_parquet' order by ordinal_position;

create table t_native (id int);
insert into t_native values (1), (2);
select count(*) as cnt from t_native;
truncate table t_native;
select count(*) as cnt from t_native;

drop database if exists external_table_truncate_alter;
