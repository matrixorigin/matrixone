-- issue https://github.com/matrixorigin/MO-Cloud/issues/4678
use mo_catalog;
-- @ignore:3,4,5,7,9,10,11,12
show table status;

drop database if exists testdb;
create database testdb;

use testdb;

create table t1 (a int);
insert into t1 select * from generate_series(1, 100*1000)g;
-- @ignore:3,4,5,7,9,10,11,12
show table status;

drop database testdb;

-- @ignore:0
select mo_ctl("cn", "MoTableStats", "recomputing:acc.0");
-- @ignore:0
select mo_ctl("cn", "MoTableStats", "recomputing:db.1");
-- @ignore:0
select mo_ctl("cn", "MoTableStats", "recomputing:tbl.1,2,3");
-- @ignore:0
select mo_ctl("cn", "MoTableStats", "recomputing:gama.forgotten");
-- @ignore:0
select mo_ctl("cn", "MoTableStats", "recomputing:gama.clean");
-- @ignore:0
select mo_ctl("cn", "MoTableStats", "recomputing:gama.new");