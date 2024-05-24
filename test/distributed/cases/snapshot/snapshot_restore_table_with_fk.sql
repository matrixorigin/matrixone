create account acc1 ADMIN_NAME 'admin1' IDENTIFIED BY 'test123';

-- @session:id=2&user=acc1:admin1&password=test123
show databases;
create database fk_test;
use fk_test;
create table t1 (a int primary key);
insert into t1 values (1);
create table t2 (a int primary key, b int, FOREIGN KEY (b) REFERENCES t2(a));
insert into t2 values (1, 1);
create table t3 (a int primary key, b int unique key, FOREIGN KEY (a) REFERENCES t1(a), FOREIGN KEY (b) REFERENCES t2(a));
insert into t3 values (1, 1);
create table t4 (a int primary key, b int, FOREIGN KEY (b) REFERENCES t3(b));
insert into t4 values (2, 1);
create table t5 (a int, FOREIGN KEY (a) REFERENCES t4(a));
insert into t5 values (2);
create table t6 (a int, FOREIGN KEY (a) REFERENCES t4(a));
insert into t6 values (2);

show full tables;
desc t1;
desc t2;
desc t3;
desc t4;
desc t5;
desc t6;
select * from t1;
select * from t2;
select * from t3;
select * from t4;
select * from t5;
select * from t6;

create snapshot sn1 for account acc1;
-- @ignore:1
show snapshots;

drop database fk_test;
restore account acc1 from snapshot sn1;

show databases;
use fk_test;
show full tables;
desc t1;
desc t2;
desc t3;
desc t4;
desc t5;
desc t6;
select * from t1;
select * from t2;
select * from t3;
select * from t4;
select * from t5;
select * from t6;

drop snapshot sn1;
drop database fk_test;
-- @session

drop account acc1;
