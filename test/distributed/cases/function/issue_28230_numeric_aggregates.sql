-- #28230: MEDIAN needs a 39-digit result; SUM(INT) needs DECIMAL(32,0) metadata.
drop database if exists issue_28230_numeric_aggregates;
create database issue_28230_numeric_aggregates;
use issue_28230_numeric_aggregates;

create table d(v decimal(38,0));
insert into d values
    (99999999999999999999999999999999999998),
    (99999999999999999999999999999999999999);
select median(v) from d;
create table dm as select median(v) x from d;
select data_type,numeric_precision,numeric_scale from information_schema.columns
where table_schema='issue_28230_numeric_aggregates' and table_name='dm' and column_name='x';

create table i(v int);
insert into i values (1),(2);
select sum(v) from i;
create table sm as select sum(v) x from i;
select data_type,numeric_precision,numeric_scale from information_schema.columns
where table_schema='issue_28230_numeric_aggregates' and table_name='sm' and column_name='x';

drop database issue_28230_numeric_aggregates;
