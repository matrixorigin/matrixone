select * from metadata_scan('table_func_metadata_scan.no_exist_table', '*') g;
drop table if exists t;
create table t(a int, b varchar);
insert into t values(1, null);
insert into t values(2, "abc");
insert into t select * from t;
insert into t select * from t;
insert into t select * from t;
insert into t select * from t;
insert into t select * from t;
insert into t select * from t;
insert into t select * from t;
insert into t select * from t;
insert into t select * from t;
insert into t select * from t;
insert into t select * from t;
insert into t select * from t;
select count(*) from t;
-- @separator:table
select mo_ctl('dn', 'flush', 'table_func_metadata_scan.t');
select count(*) from metadata_scan('table_func_metadata_scan.t', '*') g;
select count(*) from metadata_scan('table_func_metadata_scan.t', 'a') g;
select count(*) from metadata_scan('table_func_metadata_scan.t', 'c') g;
select col_name, rows_cnt, null_cnt, origin_size from metadata_scan('table_func_metadata_scan.t', 'a') g;
select col_name, rows_cnt, null_cnt, origin_size from metadata_scan('table_func_metadata_scan.t', '*') g;
select sum(origin_size) from metadata_scan('table_func_metadata_scan.t', '*') g;
select min(bit_cast(`min` as int)), max(bit_cast(`max` as int)), sum(bit_cast(`sum` as bigint)) from metadata_scan('table_func_metadata_scan.t', 'a') g;

select approx_count(*) from t;
insert into t select * from t;
insert into t select * from t;
-- @separator:table
select mo_ctl('dn', 'flush', 'table_func_metadata_scan.t');
select approx_count(*) from t;

drop table if exists t;

-- int max value + 1, not overflow
-- bigint max value + 1, overflow
create table t(a int, b bigint);
insert into t values(2147483647, 9223372036854775807);
insert into t values(1, 1);
select count(*) from t;
-- @separator:table
select mo_ctl('dn', 'flush', 'table_func_metadata_scan.t');
select bit_cast(`sum` as bigint) from metadata_scan('table_func_metadata_scan.t', 'a') g;
select sum(a) from t;
select bit_cast(`sum` as bigint) from metadata_scan('table_func_metadata_scan.t', 'b') g;
select sum(b) from t;
-- @separator:table
drop table if exists t;
create table t(a float, b decimal(10, 8));
insert into t values(1.1, 1);
insert into t values(2.0, 2);
insert into t select * from t;
insert into t select * from t;
insert into t select * from t;
insert into t select * from t;
insert into t select * from t;
insert into t select * from t;
insert into t select * from t;
insert into t select * from t;
insert into t select * from t;
insert into t select * from t;
insert into t select * from t;
insert into t select * from t;
select count(*) from t;
-- @separator:table
select mo_ctl('dn', 'flush', 'table_func_metadata_scan.t');
select sum(bit_cast(`sum` as double)) from metadata_scan('table_func_metadata_scan.t', 'a') g;
select sum(a) from t;
select sum(bit_cast(`sum` as decimal(10, 8))) from metadata_scan('table_func_metadata_scan.t', 'b') g;
select sum(b) from t;
-- @separator:table
drop table if exists t;