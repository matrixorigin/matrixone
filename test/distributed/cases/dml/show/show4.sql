set global enable_privilege_cache = off;
drop database if exists db;
-- report error: not connect to a database
-- it is hard to get the error "not connect to a database" in bvt.
-- but, you can get it when you are executing them in a mysql shell.

-- show column_number from t1;
-- show columns from t1;
-- show create table t1;
-- show create view v1;
-- show index from t1;
-- show sequences;
-- show tables;
-- show table status;
-- show table_values from t1;
-- select * from t1;

-- report error: invalid database

show column_number from db.t1;
show column_number from t1 from db;
show column_number from db.t1 from db;

show columns from db.t1;
show columns from t1 from db;
show columns from db.t1 from db;

show create database db;
show create table db.t1;
show create view db.v1;

show index from db.t1;
show index from t1 from db;
show index from db.t1 from db;

show sequences from db;

show table_number from db;
show tables from db;

show table status from db;
show table_values from t1 from db;

use db;
select * from db.t1;

set global enable_privilege_cache = on;