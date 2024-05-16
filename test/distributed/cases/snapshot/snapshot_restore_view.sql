create account acc1 ADMIN_NAME 'admin1' IDENTIFIED BY 'test123';

-- @session:id=2&user=acc1:admin1&password=test123
create database if not exists db1;
create table db1.t1 (a int);
insert into db1.t1 values (1), (2), (3);
create snapshot sn1 for account acc1;
-- @ignore:1
show snapshots;
drop database db1;

create database view_test1;
create table view_test1.t1 (a int);
insert into view_test1.t1 values (1), (2), (4);
create view view_test1.v1 as select * from view_test1.t1;
create view view_test1.v2 as select * from db1.t1 {snapshot = 'sn1'};

create database view_test2;
create view view_test2.v3 as select view_test1.v1.a as v1a, view_test1.v2.a as v2a from view_test1.v1 join view_test1.v2 on view_test1.v1.a = view_test1.v2.a;
create view view_test2.v4 as select * from view_test2.v3;

create database view_test3;
create view view_test3.v5 as select * from view_test2.v4;
create view view_test3.v6 as select * from view_test2.v4;

select * from view_test1.t1;
select * from view_test1.v1;
select * from view_test1.v2;
select * from view_test2.v3;
select * from view_test2.v4;
select * from view_test3.v5;
select * from view_test3.v6;
-- @session

-- sys account takes a snapshot for acc1
create snapshot syssn1 for account acc1;
show databases {snapshot = 'syssn1'};
show full tables from view_test1 {snapshot = 'syssn1'};
show full tables from view_test2 {snapshot = 'syssn1'};
show full tables from view_test3 {snapshot = 'syssn1'};
select * from view_test1.t1 {snapshot = 'syssn1'};
select * from view_test1.v1 {snapshot = 'syssn1'};
select * from view_test1.v2 {snapshot = 'syssn1'};
select * from view_test2.v3 {snapshot = 'syssn1'};
select * from view_test2.v4 {snapshot = 'syssn1'};
select * from view_test3.v5 {snapshot = 'syssn1'};
select * from view_test3.v6 {snapshot = 'syssn1'};

-- @session:id=2&user=acc1:admin1&password=test123
-- acc1 drop all dbs
drop database view_test1;
drop database view_test2;
drop database view_test3;
-- @session

-- sys account restore acc1 from snapshot
restore account acc1 from snapshot syssn1 to account acc1;

-- @session:id=2&user=acc1:admin1&password=test123
show databases;
select * from view_test1.t1;
select * from view_test1.v2;
select * from view_test2.v3;
select * from view_test2.v4;
select * from view_test3.v5;
select * from view_test3.v6;

drop database view_test1;
drop database view_test2;
drop database view_test3;
drop snapshot sn1;
-- @session

drop snapshot syssn1;
drop account acc1;
