drop database if exists testdb;
create database testdb;
use testdb;

create account acc admin_name "root" identified by "111";
create publication pub1 database testdb account all;

create table t1 (a int);
create table t2 (a int);
create table t3 (a int);

insert into t1 select * from generate_series(1, 1000)g;
insert into t2 select * from generate_series(1, 1000)g;
insert into t3 select * from generate_series(1, 1000)g;

-- @session:id=2&user=acc:root&password=111
drop database if exists testdb_sub;
create database testdb_sub from sys publication pub1;

drop database if exists testdb_nor;
create database testdb_nor;
use testdb_nor;

create table t1 (a int);
create table t2 (a int);
create table t3 (a int);

insert into t1 select * from generate_series(1, 1001)g;
insert into t2 select * from generate_series(1, 1001)g;
insert into t3 select * from generate_series(1, 1001)g;

create table tmp(dbName varchar, tblName varchar);
insert into tmp values ("testdb_nor", "t1"), ("testdb_nor", "t2"), ("testdb_nor", "t3");
insert into tmp values ("testdb_sub", "t1"), ("testdb_sub", "t2"), ("testdb_sub", "t3");

set mo_table_stats.force_update = yes;
select mo_table_rows(dbName, tblName) from (select * from testdb_nor.tmp order by dbName, tblName asc);

insert into tmp values ("testdb_sub", "t4");
select mo_table_rows(dbName, tblName) from (select * from testdb_nor.tmp order by dbName, tblName asc);

set mo_table_stats.force_update = no;
delete from tmp where dbName = "testdb_sub" and tblName = "t4";

set mo_table_stats.use_old_impl = yes;
select mo_table_rows(dbName, tblName) from (select * from testdb_nor.tmp order by dbName, tblName asc);

insert into tmp values ("testdb_sub", "t4");
select mo_table_rows(dbName, tblName) from (select * from testdb_nor.tmp order by dbName, tblName asc);

set mo_table_stats.use_old_impl = no;

drop database testdb_nor;
drop database testdb_sub;

-- @session

drop account acc;
drop publication pub1;
drop database testdb;