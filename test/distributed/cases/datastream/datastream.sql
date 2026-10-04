-- datastream external table (ENGINE = DATASTREAM) BVT.
-- The happy-path cases need the jstfu server running on 127.0.0.1:4444:
--   make jstfu && optools/jstfu_bvt.sh <resources_dir> <mo_host:mo_port>
-- DDL-validation cases below need no server.

-- DDL validation errors
create external table ds_err1 (a int) engine = datastream;
create external table ds_err2 (a int) engine = datastream with ('server' = '127.0.0.1');
create external table ds_err3 (a int) engine = datastream with ('server' = 'h', 'port' = 'abc', 'table' = 't');
create external table ds_err4 (a int) engine = datastream with ('server' = 'h', 'port' = '4444', 'table' = 't', 'bogus' = '1');
create external table ds_err5 (a int) engine = datastream with ('server' = 'h', 'port' = '4444', 'table' = 't', 'server' = 'h2');
create external table ds_err6 (a int) engine = datastream with ('server' = 'h', 'port' = '4444', 'table' = 't', 'recheck' = 'maybe');

drop database if exists datastream_bvt;
create database datastream_bvt;
use datastream_bvt;

-- source table that the jstfu jdbc datasource reads back out of MO
create table src_numbers (col1 int, col2 datetime, col3 varchar(50), col4 text);
insert into src_numbers values
  (1, '2020-01-01 10:00:00', 'alpha', 'first row'),
  (2, '2020-06-15 12:30:00', 'beta', 'second, with comma'),
  (3, '2021-03-10 08:45:00', 'gamma', NULL),
  (4, '2021-11-11 11:11:11', 'delta', 'fourth row'),
  (5, '2022-07-04 00:00:00', 'epsilon', 'fifth row');

-- file datasource
create external table ext_file (col1 int, col2 datetime, col3 varchar(50), col4 text) engine = datastream with ('server' = '127.0.0.1', 'port' = '4444', 'table' = 'file_numbers');
select * from ext_file order by col1;
select col1, col3 from ext_file where col2 > '2021-01-01 00:00:00' order by col1;
select count(*) from ext_file where col4 is null;
show create table ext_file;

-- jdbc datasource (round-trips through MatrixOne's own mysql surface)
create external table ext_jdbc (col1 int, col2 datetime, col3 varchar(50), col4 text) engine = datastream with ('server' = '127.0.0.1', 'port' = '4444', 'table' = 'jdbc_numbers');
select * from ext_jdbc order by col1;
select col1, col3 from ext_jdbc where col1 between 2 and 4 order by col1;
select count(*) from ext_jdbc where col2 > '2021-01-01 00:00:00' and col3 <> 'delta';

-- recheck=false trusts the server for the pushed conjuncts
create external table ext_jdbc_norecheck (col1 int, col2 datetime, col3 varchar(50), col4 text) engine = datastream with ('server' = '127.0.0.1', 'port' = '4444', 'table' = 'jdbc_numbers', 'recheck' = 'false');
select count(*) from ext_jdbc_norecheck where col1 > 2;
show create table ext_jdbc_norecheck;

-- ETL: stream into a destination table, then with disjoint filters
create table dest (col1 int, col2 datetime, col3 varchar(50), col4 text);
insert into dest select * from ext_jdbc;
select count(*) from dest;
create table dest2 (col1 int, col2 datetime, col3 varchar(50), col4 text);
insert into dest2 select * from ext_jdbc where col1 <= 2;
insert into dest2 select * from ext_jdbc where col1 > 2;
select count(*) from dest2;

-- error conditions
create external table ext_missing (a int) engine = datastream with ('server' = '127.0.0.1', 'port' = '4444', 'table' = 'no_such_source');
select * from ext_missing;
create external table ext_badfile (a int) engine = datastream with ('server' = '127.0.0.1', 'port' = '4444', 'table' = 'bad_file');
select * from ext_badfile;
create external table ext_badsql (a int) engine = datastream with ('server' = '127.0.0.1', 'port' = '4444', 'table' = 'jdbc_bad_sql');
select * from ext_badsql;
create external table ext_noserver (a int) engine = datastream with ('server' = '127.0.0.1', 'port' = '1', 'table' = 't');
select * from ext_noserver;

-- api key option: parsed and stored, ignored by an auth-disabled server, and
-- never leaked by SHOW CREATE (enforcement is covered by the e2e's keyed server)
create external table ext_keyed (col1 int, col2 datetime, col3 varchar(50), col4 text) engine = datastream with ('server' = '127.0.0.1', 'port' = '4444', 'table' = 'jdbc_numbers', 'apikey' = 'bvt-secret');
show create table ext_keyed;
select count(*) from ext_keyed;

-- external tables reject writes
insert into ext_file values (9, '2020-01-01 00:00:00', 'x', 'y');

drop database datastream_bvt;
