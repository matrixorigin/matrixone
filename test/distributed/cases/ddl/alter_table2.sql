-- select mo_ctl('cn', 'task', 'disable');
drop database if exists db;
create database db;
use db;
drop table if exists t;
create table t (a int, b int, primary key (a));

-- 5 rows in mem
insert into t values (1, 1), (2, 2), (3, 3), (4,4), (5, 5);

-- @separator:table
select mo_ctl('dn', 'inspect', 'addc -d db -b t -n aplus -t samllint -p 2');

show columns from t;

-- reading memory, filling null for new added `aplus`
select * from t;

-- insert 2 new rows
insert into t values (6, 6,6), (7,7,7);

-- flush will generate 2 blocks, blk1 and blk2, both have seqnum [0,1,2]
-- specially, there are also ablk1 and ablk2, which are not visible for the following queries
-- @separator:table
select mo_ctl('dn', 'flush', 'db.t');

-- 5 nulls from blk1 and (6, 7) from blk2
select aplus from t;

-- test updating on blk1 and blk2
update t set aplus = 11 where a = 1;
update t set aplus = 61 where a = 6;

-- reading from memory and block
select * from t;

-- drop aplus column and add aminus column at the front
-- @separator:table
select mo_ctl('dn', 'inspect', 'dropc -d db -b t -p 2');
-- @separator:table
select mo_ctl('dn', 'inspect', 'addc -d db -b t -n aminus -t samllint -p 0');
-- @separator:table
select mo_ctl('dn', 'inspect', 'addc -d db -b t -n amul -t samllint -p 2');

show columns from t;
select attname, attr_seqnum from mo_catalog.mo_columns where att_relname = "t";
select relname, rel_version from mo_catalog.mo_tables where reldatabase = "db";

-- insert a new row, and the returned batch from logtail will be ["a", "b", "", "aminus", "amul"] with seqnum [0, 1, ?, 3, 4]
insert into t values (12, 12, 12, 12);

-- reading blk1 and blk2 will get generated null for aminus and amul. reading memory will get 12, 12
select * from t;

-- check zonmeap filter
-- blk1 and blk2 have only 3 column meta for seqnum [a-0, b-1, aplus-2] and blk1 has empty zonemap for aplus because it was all null
select a,b,aminus from t where (a > 4 and b < 10) or aminus > 10;

select * from t where aminus < 10;
select * from t where amul > 5;


-- change table name
show tables;
-- @separator:table
select mo_ctl('dn', 'inspect', 'renamet -d db -o t -n newt');

select * from t;
select * from newt;

select attname, attr_seqnum from mo_catalog.mo_columns where att_relname = "t";
select attname, attr_seqnum from mo_catalog.mo_columns where att_relname = "newt";
select relname, rel_version from mo_catalog.mo_tables where reldatabase = "db";

-- @separator:table
select mo_ctl('dn', 'inspect', 'renamet -d db -o newt -n t');

show tables;

insert into t values (100, 100, 100, 100);

select * from t;
select * from newt;

drop database db;

create database db;
use db;
show tables;

drop database db;
