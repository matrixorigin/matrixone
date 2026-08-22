-- datastream external table: larger-volume loads, NULL handling, and
-- AND/OR condition pushdown.  Requires jstfu on 127.0.0.1:4444:
--   make jstfu && optools/jstfu_bvt.sh <resources_dir> <mo_host:mo_port>

drop database if exists datastream_bvt2;
create database datastream_bvt2;
use datastream_bvt2;

-- ============================================================
-- 1) Load a 10000-row CSV through the jstfu file datasource and
--    verify against a direct LOAD DATA of the same file.
-- ============================================================
create external table ext_parallel_file (col1 int, col2 varchar(200), col3 varchar(200)) engine = datastream with ('server' = '127.0.0.1', 'port' = '4444', 'table' = 'file_parallel');
create table t_parallel (col1 int, col2 varchar(200), col3 varchar(200));
insert into t_parallel select * from ext_parallel_file;
select count(*) from t_parallel;
create table t_parallel_ref (col1 int, col2 varchar(200), col3 varchar(200));
load data infile '$resources/load_data/test_parallel.csv' into table t_parallel_ref fields terminated by ',';
select count(*) from t_parallel_ref;
-- datastream load and LOAD DATA agree byte for byte
select count(*) from (select * from t_parallel except select * from t_parallel_ref) diff1;
select count(*) from (select * from t_parallel_ref except select * from t_parallel) diff2;

-- ============================================================
-- 2) Load a wide CSV with many NULL fields (rawlog, 2426 rows,
--    20 columns) and verify NULL handling against LOAD DATA.
-- ============================================================
create external table ext_rawlog_file (
  raw_item varchar(1024), node_uuid varchar(36), node_type varchar(64),
  span_id varchar(16), statement_id varchar(36), logger_name varchar(1024),
  `timestamp` datetime, `level` varchar(1024), caller varchar(1024),
  message text, extra text, err_code varchar(1024), error text,
  stack varchar(4096), span_name varchar(1024), parent_span_id varchar(16),
  start_time datetime, end_time datetime, duration bigint unsigned, resource text
) engine = datastream with ('server' = '127.0.0.1', 'port' = '4444', 'table' = 'file_rawlog');
create table t_rawlog (
  raw_item varchar(1024), node_uuid varchar(36), node_type varchar(64),
  span_id varchar(16), statement_id varchar(36), logger_name varchar(1024),
  `timestamp` datetime, `level` varchar(1024), caller varchar(1024),
  message text, extra text, err_code varchar(1024), error text,
  stack varchar(4096), span_name varchar(1024), parent_span_id varchar(16),
  start_time datetime, end_time datetime, duration bigint unsigned, resource text
);
insert into t_rawlog select * from ext_rawlog_file;
select count(*) from t_rawlog;
create table t_rawlog_ref like t_rawlog;
load data infile '$resources/external_table_file/rawlog_withnull.csv' into table t_rawlog_ref fields terminated by ',' enclosed by '"' lines terminated by '\n';
-- same record count: the file has quoted fields with embedded newlines, so
-- this checks jstfu's record-aligned chunking against LOAD DATA's parsing
select count(*) from t_rawlog_ref;
-- NULL columns survive identically through both load paths
select count(*) from t_rawlog where `timestamp` is null;
select count(*) from t_rawlog_ref where `timestamp` is null;
select count(*) from t_rawlog where start_time is null and end_time is null;
select count(*) from t_rawlog_ref where start_time is null and end_time is null;
select count(*) from t_rawlog where span_name is null or duration is null;
select count(*) from t_rawlog_ref where span_name is null or duration is null;
select count(*) from (select * from t_rawlog except select * from t_rawlog_ref) diff3;
select count(*) from (select * from t_rawlog_ref except select * from t_rawlog) diff4;

-- ============================================================
-- 3) Pushdown hint semantics, observable through the file source:
--    the file source ignores the filter, so recheck=false returns the
--    whole file while recheck=true (default) repairs the result locally.
-- ============================================================
create external table ext_parallel_norecheck (col1 int, col2 varchar(200), col3 varchar(200)) engine = datastream with ('server' = '127.0.0.1', 'port' = '4444', 'table' = 'file_parallel', 'recheck' = 'false');
select count(*) from ext_parallel_file where col1 < 100;
select count(*) from ext_parallel_norecheck where col1 < 100;

-- ============================================================
-- 4) Conditions through the jdbc datasource: ${FILTER} is applied
--    server-side, so recheck=true and recheck=false must return the
--    same data for every pushable condition, including AND/OR.
-- ============================================================
create external table ext_jdbc_par (col1 int, col2 varchar(200), col3 varchar(200)) engine = datastream with ('server' = '127.0.0.1', 'port' = '4444', 'table' = 'jdbc_parallel', 'recheck' = 'true');
create external table ext_jdbc_par_nr (col1 int, col2 varchar(200), col3 varchar(200)) engine = datastream with ('server' = '127.0.0.1', 'port' = '4444', 'table' = 'jdbc_parallel', 'recheck' = 'false');

-- simple comparison
select count(*) from ext_jdbc_par where col1 < 100;
select count(*) from ext_jdbc_par_nr where col1 < 100;
-- AND
select count(*) from ext_jdbc_par where col1 >= 100 and col1 < 300;
select count(*) from ext_jdbc_par_nr where col1 >= 100 and col1 < 300;
select min(col1), max(col1) from ext_jdbc_par where col1 >= 100 and col1 < 300;
select min(col1), max(col1) from ext_jdbc_par_nr where col1 >= 100 and col1 < 300;
-- OR
select count(*) from ext_jdbc_par where col1 < 50 or col1 >= 9950;
select count(*) from ext_jdbc_par_nr where col1 < 50 or col1 >= 9950;
-- nested AND/OR
select count(*) from ext_jdbc_par where (col1 < 100 or col1 >= 9900) and col1 <> 0;
select count(*) from ext_jdbc_par_nr where (col1 < 100 or col1 >= 9900) and col1 <> 0;
select sum(col1) from ext_jdbc_par where (col1 < 10 and col1 > 5) or (col1 > 9990 and col1 < 9995);
select sum(col1) from ext_jdbc_par_nr where (col1 < 10 and col1 > 5) or (col1 > 9990 and col1 < 9995);
-- IN / BETWEEN / NOT
select count(*) from ext_jdbc_par where col1 in (1, 500, 9999, 20000);
select count(*) from ext_jdbc_par_nr where col1 in (1, 500, 9999, 20000);
select count(*) from ext_jdbc_par where col1 between 4000 and 4200;
select count(*) from ext_jdbc_par_nr where col1 between 4000 and 4200;
select count(*) from ext_jdbc_par where not (col1 < 9990);
select count(*) from ext_jdbc_par_nr where not (col1 < 9990);
-- mixed pushable + non-pushable conjunct: the modulo term cannot be
-- deparsed, so it always stays local even with recheck=false
select count(*) from ext_jdbc_par where col1 < 100 and col1 % 2 = 0;
select count(*) from ext_jdbc_par_nr where col1 < 100 and col1 % 2 = 0;

-- jdbc conditions over the rawlog table: datetime, strings, NULL tests
create external table ext_jdbc_raw (
  raw_item varchar(1024), node_uuid varchar(36), node_type varchar(64),
  span_id varchar(16), statement_id varchar(36), logger_name varchar(1024),
  `timestamp` datetime, `level` varchar(1024), caller varchar(1024),
  message text, extra text, err_code varchar(1024), error text,
  stack varchar(4096), span_name varchar(1024), parent_span_id varchar(16),
  start_time datetime, end_time datetime, duration bigint unsigned, resource text
) engine = datastream with ('server' = '127.0.0.1', 'port' = '4444', 'table' = 'jdbc_rawlog', 'recheck' = 'true');
create external table ext_jdbc_raw_nr (
  raw_item varchar(1024), node_uuid varchar(36), node_type varchar(64),
  span_id varchar(16), statement_id varchar(36), logger_name varchar(1024),
  `timestamp` datetime, `level` varchar(1024), caller varchar(1024),
  message text, extra text, err_code varchar(1024), error text,
  stack varchar(4096), span_name varchar(1024), parent_span_id varchar(16),
  start_time datetime, end_time datetime, duration bigint unsigned, resource text
) engine = datastream with ('server' = '127.0.0.1', 'port' = '4444', 'table' = 'jdbc_rawlog', 'recheck' = 'false');

select count(*) from ext_jdbc_raw where `level` = 'info' and `timestamp` > '2022-11-01 11:11:24';
select count(*) from ext_jdbc_raw_nr where `level` = 'info' and `timestamp` > '2022-11-01 11:11:24';
select count(*) from ext_jdbc_raw where `level` = 'error' or `level` = 'warn';
select count(*) from ext_jdbc_raw_nr where `level` = 'error' or `level` = 'warn';
select count(*) from ext_jdbc_raw where span_name is not null and duration > 0;
select count(*) from ext_jdbc_raw_nr where span_name is not null and duration > 0;
select count(*) from ext_jdbc_raw where `timestamp` is null or (`level` = 'debug' and caller like 'export%');
select count(*) from ext_jdbc_raw_nr where `timestamp` is null or (`level` = 'debug' and caller like 'export%');

-- ============================================================
-- 5) ETL from jdbc source with AND/OR filters into another table
-- ============================================================
create table dest_par (col1 int, col2 varchar(200), col3 varchar(200));
insert into dest_par select * from ext_jdbc_par where col1 < 2500 or col1 >= 7500;
insert into dest_par select * from ext_jdbc_par_nr where col1 >= 2500 and col1 < 7500;
select count(*) from dest_par;
select count(*) from (select * from dest_par except select * from t_parallel) diff5;

drop database datastream_bvt2;
