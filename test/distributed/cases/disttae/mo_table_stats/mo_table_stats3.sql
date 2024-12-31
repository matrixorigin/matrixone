-- issue https://github.com/matrixorigin/MO-Cloud/issues/4678
use mo_catalog;
-- @ignore:3,5,10,11,12
show table status;

drop database if exists testdb;
create database testdb;

use testdb;

create table t1 (a int);
insert into t1 select * from generate_series(1, 100*1000)g;
-- @ignore:3,5,10,11,12
show table status;

drop database testdb;