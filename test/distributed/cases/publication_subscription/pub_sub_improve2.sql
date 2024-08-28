-- pub-sub improvement test
drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '111';
drop account if exists acc03;
create account acc03 admin_name = 'test_account' identified by '111';



-- normal tenant creates the publication db, specifies the tenant to publish, verifies that the specified tenant is
-- subscribable, and displays sub_accounts in the show publications results as the specified tenant
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists db01;
create database db01;
use db01;
create table table01 (col1 int, col2 enum ('a','b','c'));
insert into table01 values(1,'a');
insert into table01 values(2, 'b');
create table table02 (col1 int, col2 enum ('a','b','c'));
insert into table02 values(1,'a');
insert into table02 values(2, 'b');
drop database if exists db02;
create database db02;
use db02;
create table index01(col1 int,key key1(col1));
insert into index01 values (1);
insert into index01 values (2);

drop publication if exists pub01;
create publication pub01 database db01 account acc02;
create publication pub02 database db02 table index01 account acc03;
-- @ignore:5,6
show publications;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub01;
create database sub01 from acc01 publication pub01;
-- @ignore:5,7
show subscriptions;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub03;
create database sub03 from acc01 publication pub02;
show databases;
use sub03;
select * from index01;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,6
show publications;
alter publication pub01 account acc02 database db02;
-- @ignore:5,6
show publications;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
show databases;
use sub01;
show tables;
select * from index01;
select count(*) from index01;
show create table index01;
-- @ignore:5,7
show subscriptions;
drop database sub01;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop publication pub01;
drop publication pub02;
drop database db01;
drop database db02;
-- @session




-- sys account publishes the db to all all. verify that show publications: the db is publishing name, the tables field is *,
-- the sub_account field is *, and the subscribed_accounts field is null. comments is a comment when the publication is
-- created. verify show subscriptions all: shows all authorized subscriptions
drop database if exists db04;
create database db04;
use db04;
drop table if exists index01;
create table index01(col1 char, col2 int, col3 binary);
insert into index01 values('a', 33, 1);
insert into index01 values('c', 231, 0);
alter table index01 add key pk(col1) comment 'primary key';
select count(*) from index01;

drop publication if exists pub04;
create publication pub04 database db04 account all comment 'pub to account all';
-- @ignore:5,6
show publications;

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists db05;
create database sub05 from sys publication pub04;
-- @ignore:5,7
show subscriptions all;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists db05;
create database sub05 from sys publication pub04;
-- @ignore:5,7
show subscriptions all;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub05;
create database sub05 from sys publication pub04;
-- @ignore:5,7
show subscriptions all;
-- @session

-- @ignore:5,6
show publications;

-- @session:id=1&user=acc01:test_account&password=111
drop database sub05;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
drop database sub05;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
drop database sub05;
-- @session

drop publication pub04;
drop database db04;




-- sys account publishes part of the tables in the db to all tenants. verify that the show publications: the name of the
-- published db, the tables field is the list of published tables, the sub_account field is *. The subscribed_accounts
-- is null, and comments is the comment publication is created. show subscriptions all: shows all subscriptions with permissions
drop database if exists db05;
create database db05;
use db05;
drop table if exists t1;
create table t1 (a text);
insert into t1 values('abcdef'),('_bcdef'),('a_cdef'),('ab_def'),('abcd_f'),('abcde_');
drop table if exists t2;
create table t2 (a datetime(0) not null, primary key(a));
insert into t2 values ('20200101000000'), ('2022-01-02'), ('2022-01-02 00:00:01'), ('2022-01-02 00:00:01.512345');
drop table if exists t3;
create table t3(col1 datetime);
insert into t3 values('2020-01-13 12:20:59.1234586153121');
insert into t3 values('2023-04-17 01:01:45');
insert into t3 values(NULL);
drop table if exists t4;
create table t4(a int unique key, b int, c int, unique key(b,c));
insert into t4 values(1,1,1);
insert into t4 values(2,3,3);
insert into t4 values(10,19,11);
drop publication if exists pub05;
create publication pub05 database db05 table t1,t3 account all comment 'publish t1、t3 to all account except sys';
-- @ignore:5,6
show publications;

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub06;
create database sub06 from sys publication pub05;
use sub06;
show tables;
select * from t1;
show create table t3;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub06;
create database sub06 from sys publication pub05;
use sub06;
show tables;
select * from t1;
show create table t3;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub06;
create database sub06 from sys publication pub05;
use sub06;
show tables;
select * from t1;
show create table t3;
-- @session

-- @ignore:5,6
show publications;

-- @session:id=1&user=acc01:test_account&password=111
drop database sub06;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
drop database sub06;
-- @session

-- @session:id=3&user=acc03:test_account&password=111
drop database sub06;
-- @session

-- @ignore:5,6
show publications;

-- modify published to all tenants to specified tenants
alter publication pub05 account acc01,acc02 database db05;
-- @ignore:5,6
show publications;

-- @session:id=3&user=acc03:test_account&password=111
-- @ignore:5,7
show subscriptions all;
-- @session

drop publication pub05;
drop database db05;




-- normal tenant is the publisher, the publisher publishes all tables, tables in show subscriptions
-- are displayed as *, then delete some tables, and the tables in show subscriptions are verified as valid tables
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists db06;
create database db06;
use db06;
drop table if exists t1;
create table t1 (a text);
insert into t1 values('abcdef'),('_bcdef'),('a_cdef'),('ab_def'),('abcd_f'),('abcde_');
drop table if exists t2;
create table t2 (a datetime(0) not null, primary key(a));
insert into t2 values ('20200101000000'), ('2022-01-02'), ('2022-01-02 00:00:01'), ('2022-01-02 00:00:01.512345');
drop table if exists t3;
create table t3(col1 datetime);
insert into t3 values('2020-01-13 12:20:59.1234586153121');
insert into t3 values('2023-04-17 01:01:45');
insert into t3 values(NULL);
drop table if exists t4;
create table t4(a int unique key, b int, c int, unique key(b,c));
insert into t4 values(1,1,1);
insert into t4 values(2,3,3);
insert into t4 values(10,19,11);
drop publication if exists pub06;
create publication pub06 database db06 account acc02 comment 'publish all tables to account acc02';
-- @ignore:5,6
show publications;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub06;
create database sub06 from acc01 publication pub06;
use sub06;
show tables;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
use db06;
drop table t1;
drop table t2;
drop table t3;
-- @ignore:5,6
show publications;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
show databases;
use sub06;
show tables;
select * from t4;
-- @ignore:5,7
show subscriptions;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop publication pub06;
drop database db06;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
drop database sub06;
-- @session




-- normal tenant is the publisher, the publisher publishes certain tables, tables in show subscriptions
-- are displayed as valid tables, then delete some tables, and the tables in show subscriptions are verified as valid tables
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists db07;
create database db07;
use db07;
drop table if exists t1;
create table t1 (a text);
insert into t1 values('abcdef'),('_bcdef'),('a_cdef'),('ab_def'),('abcd_f'),('abcde_');
drop table if exists t2;
create table t2 (a datetime(0) not null, primary key(a));
insert into t2 values ('20200101000000'), ('2022-01-02'), ('2022-01-02 00:00:01'), ('2022-01-02 00:00:01.512345');
drop table if exists t3;
create table t3(col1 datetime);
insert into t3 values('2020-01-13 12:20:59.1234586153121');
insert into t3 values('2023-04-17 01:01:45');
insert into t3 values(NULL);
drop table if exists t4;
create table t4(a int unique key, b int, c int, unique key(b,c));
insert into t4 values(1,1,1);
insert into t4 values(2,3,3);
insert into t4 values(10,19,11);
drop publication if exists pub07;
create publication pub07 database db07 table t1,t2,t3 account acc02 comment 'publish some tables to account acc02';
-- @ignore:5,6
show publications;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub07;
create database sub07 from acc01 publication pub07;
use sub07;
show tables;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
use db07;
drop table t1;
drop table t3;
-- @ignore:5,6
show publications;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
show databases;
use sub07;
show tables;
select * from t2;
-- @ignore:5,7
show subscriptions;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop publication pub07;
drop database db07;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
drop database sub07;
-- @session




-- normal tenant is the publisher, the publisher publishes certain tables, tables in show subscriptions
-- are displayed as valid tables, then delete all published tables, and the tables in show subscriptions are verified as null
-- @session:id=1&user=acc01:test_account&password=111
drop database if exists db08;
create database db08;
use db08;
drop table if exists t1;
create table t1 (a text);
insert into t1 values('abcdef'),('_bcdef'),('a_cdef'),('ab_def'),('abcd_f'),('abcde_');
drop table if exists t2;
create table t2 (a datetime(0) not null, primary key(a));
insert into t2 values ('20200101000000'), ('2022-01-02'), ('2022-01-02 00:00:01'), ('2022-01-02 00:00:01.512345');
drop table if exists t3;
create table t3(col1 datetime);
insert into t3 values('2020-01-13 12:20:59.1234586153121');
insert into t3 values('2023-04-17 01:01:45');
insert into t3 values(NULL);
drop table if exists t4;
create table t4(a int unique key, b int, c int, unique key(b,c));
insert into t4 values(1,1,1);
insert into t4 values(2,3,3);
insert into t4 values(10,19,11);
drop publication if exists pub08;
create publication pub08 database db08 table t1,t2,t3 account acc02 comment 'publish sone tables to account acc02';
-- @ignore:5,6
show publications;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub08;
create database sub08 from acc01 publication pub08;
use sub08;
show tables;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
use db08;
drop table t1;
drop table t2;
drop table t3;
-- @ignore:5,6
show publications;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
show databases;
use sub08;
show tables;
-- @ignore:5,7
show subscriptions;
-- @session

-- @session:id=1&user=acc01:test_account&password=111
drop publication pub08;
drop database db08;
-- @session

-- @session:id=2&user=acc02:test_account&password=111
drop database sub08;
-- @session




-- sys account publish db to other accounts, other accounts subscribe, then sys delete publication
drop database if exists db09;
create database db09;

drop publication if exists pub09;
create publication pub09 database db09 account all comment '发布给所有租户';
-- @ignore:5,6
show publications;

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub09;
create database sub09 from sys publication pub09;
-- @ignore:5,7
show subscriptions all;
-- @session

drop publication pub09;
-- @ignore:5,6
show publications;

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database sub09;
-- @session
drop database db09;




-- sys account publish db to all account, any account subscribe, then sys delete publication
drop database if exists db10;
create database db10;
use db10;
drop table if exists t1;
create table t1(a int not null primary key, b float, c double, d varchar(30),e decimal(20,10));
insert into t1 values(1, 3214321.213, -8392.3,'woshishei',123456789.12356);
insert into t1 values(2, 0, 38293.3332121,'12345@',-12.365);
insert into t1 values(3, -392.1, 8390232,'3***',0.456984166622488655);
drop table if exists t2;
create table t2 (
 col1 int, col2 varbinary(20) not null,
 index idx(col2)
);
insert into t2 values(1, '11111111101010101');
insert into t2 values(2, '10111111101010101');
insert into t2 values(1, '36217468721382183');
insert into t2 values(2, '22258445222388855');
insert into t2 values(2, '00000000000000000');
select * from t2;
drop table if exists t3;
create table t3(col1 tinyint unsigned, col2 binary(10) not null);
insert into t3 values(0, '2312432112');
insert into t3 values(20, '321313');
insert into t3 values(23, '2312432112');
insert into t3 values(255, '321313');
drop publication if exists pub10;
drop publication if exists pub11;
drop publication if exists pub12;
create publication pub10 database db10 account all comment 'publish to all account';
create publication pub11 database db10 table t2, t3 account acc01, acc02 comment '发布给acc01和acc02';
create publication pub12 database db10 account acc03 comment 'publish all table to acc02';
-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub10;
create database sub10 from sys publication pub10;
-- @ignore:5,7
show subscriptions;
-- @session

-- @ignore:5,6
show publications;

-- @session:id=1&user=acc01:test_account&password=111
drop database sub10;
-- @session

-- @ignore:5,6
show publications;

-- @session:id=2&user=acc02:test_account&password=111
drop database if exists sub11;
create database sub11 from sys publication pub11;
-- @ignore:5,7
show subscriptions;
-- @session

-- @ignore:5,6
show publications;

-- @session:id=2&user=acc02:test_account&password=111
drop database sub11;
-- @session

-- @ignore:5,6
show publications;

-- @session:id=3&user=acc03:test_account&password=111
drop database if exists sub12;
create database sub12 from sys publication pub12;
-- @ignore:5,7
show subscriptions;
-- @session

-- @ignore:5,6
show publications;

-- @session:id=3&user=acc03:test_account&password=111
drop database sub12;
-- @session

-- @ignore:5,6
show publications;

drop publication pub10;
drop publication pub11;
drop publication pub12;
drop database db10;




-- sys account publish db to all，normal account subscribe，delete publication，show subscriptions: status is 2
drop database if exists db13;
create database db13;
use db13;
drop table if exists t1;
create table t1(a int not null primary key, b float, c double, d varchar(30),e decimal(20,10));
insert into t1 values(1, 3214321.213, -8392.3,'woshishei',123456789.12356);
insert into t1 values(2, 0, 38293.3332121,'12345@',-12.365);
insert into t1 values(3, -392.1, 8390232,'3***',0.456984166622488655);
drop table if exists t2;
create table t2 (
 col1 int, col2 varbinary(20) not null,
 index idx(col2)
);
insert into t2 values(1, '11111111101010101');
insert into t2 values(2, '10111111101010101');
insert into t2 values(1, '36217468721382183');
insert into t2 values(2, '22258445222388855');
insert into t2 values(2, '00000000000000000');
select * from t2;
drop table if exists t3;
create table t3(col1 tinyint unsigned, col2 binary(10) not null);
insert into t3 values(0, '2312432112');
insert into t3 values(20, '321313');
insert into t3 values(23, '2312432112');
insert into t3 values(255, '321313');
drop publication if exists pub13;
drop publication if exists pub14;
drop publication if exists pub15;
create publication pub13 database db13 account all comment 'publish to all account';
create publication pub14 database db13 table t2, t3 account acc01, acc02 comment '发布给acc01和acc02';
create publication pub15 database db13 account acc03 comment 'publish all table to acc02';

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub13;
create database sub13 from sys publication pub13;
-- @ignore:5,7
show subscriptions;
-- @session

-- @ignore:5,6
show publications;
drop publication pub13;

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions;
drop database sub13;
-- @session

-- @ignore:5,6
show publications;

-- @session:id=2&user=acc02:test_account&password=111
drop database if exists sub14;
create database sub14 from sys publication pub14;
-- @ignore:5,7
show subscriptions;
-- @session

-- @ignore:5,6
show publications;
drop publication pub14;

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions;
drop database sub14;
-- @session

-- @ignore:5,6
show publications;

-- @session:id=3&user=acc03:test_account&password=111
drop database if exists sub15;
create database sub15 from sys publication pub15;
-- @ignore:5,7
show subscriptions;
-- @session

-- @ignore:5,6
show publications;
drop publication pub15;

-- @session:id=3&user=acc03:test_account&password=111
-- @ignore:5,7
show subscriptions;
drop database sub15;
-- @session

-- @ignore:5,6
show publications;

drop database db13;




-- sys account publish db to all,any account subscribe,delete publication, the publisher creates a new
-- publication with the same name, then show subscriptions: status is 0
drop database if exists db16;
create database db16;
use db16;
drop table if exists t1;
create table t1 (a int, b bit(10));
insert into t1 values (0, false);
insert into t1 values (1, true);
insert into t1 values (2, 0x2);
insert into t1 values (3, 0b11);
insert into t1 values (4, x'04');
insert into t1 values (5, b'101');
insert into t1 values (6, 'a');
drop table if exists t2;
create table t2(id int,fl float, dl double);
insert into t2 values(1,123456,123456);
insert into t2 values(2,123.456,123.456);
insert into t2 values(3,1.234567,1.234567);
insert into t2 values(4,1.234567891,1.234567891);
insert into t2 values(5,1.2345678912345678912,1.2345678912345678912);
drop table if exists t3;
create table t3 (col1 enum('red','blue','green'));
insert into t3 values ('red'),('blue'),('green');
insert into t3 values (null);
drop publication if exists pub16;
drop publication if exists pub17;
drop publication if exists pub18;
create publication pub16 database db16 account all comment 'publish to all account';
create publication pub17 database db16 table t2, t3 account acc01, acc02 comment '发布给acc01和acc02';
create publication pub18 database db16 account acc03 comment 'publish all table to acc02';

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub16;
create database sub16 from sys publication pub16;
-- @ignore:5,7
show subscriptions;
-- @session

drop publication pub16;

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions;
-- @session

create publication pub16 database db16 table t1 account all comment 'publish to all accounts';
-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions;
drop database sub16;
-- @session

-- @ignore:5,6
show publications;

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub17;
create database sub17 from sys publication pub17;
-- @ignore:5,7
show subscriptions;
-- @session

drop publication pub17;

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions;
-- @session

create publication pub17 database db16 account acc02 comment 'publish to acc02';
-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions;
use sub17;
show tables;
select * from t1;
select count(*) from t2;
show create table t3;
drop database sub17;
-- @session

-- @ignore:5,6
show publications;

-- @session:id=3&user=acc03:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database if exists sub18;
create database sub18 from sys publication pub18;
-- @ignore:5,7
show subscriptions;
-- @session

drop publication pub18;

-- @session:id=3&user=acc03:test_account&password=111
-- @ignore:5,7
show subscriptions;
-- @session

create publication pub18 database db16 table t1,t2 account acc03 comment 'publish to acc02';
-- @session:id=3&user=acc03:test_account&password=111
-- @ignore:5,7
show subscriptions;
use sub18;
show tables;
select * from t1;
select count(*) from t2;
show create table t3;
drop database sub18;
-- @session

-- @ignore:5,6
show publications;
drop publication pub16;
drop publication pub17;
drop publication pub18;
drop database db16;




-- sys account publish db to account a, a subscribe, sys account delete publication, the publisher creates a new
-- publication with the same name and publish to account b, then show subscriptions: status is 1
drop database if exists db19;
create database db19;
use db19;
drop table if exists employees;
create table employees (
      emp_no      int             NOT NULL,
      birth_date  date            NOT NULL,
      first_name  varchar(14)     NOT NULL,
      last_name   varchar(16)     NOT NULL,
      gender      varchar(5)      NOT NULL,
      hire_date   date            NOT NULL,
      primary key (emp_no)
) partition by range columns (emp_no)(
    partition p01 values less than (100001),
    partition p02 values less than (200001),
    partition p03 values less than (300001),
    partition p04 values less than (400001)
);

insert into employees values (9001,'1980-12-17', 'SMITH', 'CLERK', 'F', '2008-12-17'),
                          (9002,'1981-02-20', 'ALLEN', 'SALESMAN', 'F', '2008-02-20'),
                          (9003,'1981-02-22', 'WARD', 'SALESMAN', 'M', '2005-02-22'),
                          (9004,'1981-04-02', 'JONES', 'MANAGER', 'M', '2003-04-02'),
                          (9005,'1981-09-28', 'MARTIN', 'SALESMAN', 'F','2003-09-28'),
                          (9006,'1981-05-01', 'BLAKE', 'MANAGER', 'M', '2003-05-01'),
                          (9007,'1981-06-09', 'CLARK', 'MANAGER', 'F', '2005-06-09');

drop table if exists pri01;
create table pri01(
                      deptno int unsigned comment '部门编号',
                      dname varchar(15) comment '部门名称',
                      loc varchar(50)  comment '部门所在位置',
                      primary key(deptno)
) comment='部门表';

insert into pri01 values (10,'ACCOUNTING','NEW YORK');
insert into pri01 values (20,'RESEARCH','DALLAS');
insert into pri01 values (30,'SALES','CHICAGO');
insert into pri01 values (40,'OPERATIONS','BOSTON');

drop table if exists aff01;
create table aff01(
                      empno int unsigned auto_increment COMMENT '雇员编号',
                      ename varchar(15) comment '雇员姓名',
                      job varchar(10) comment '雇员职位',
                      mgr int unsigned comment '雇员对应的领导的编号',
                      hiredate date comment '雇员的雇佣日期',
                      sal decimal(7,2) comment '雇员的基本工资',
                      comm decimal(7,2) comment '奖金',
                      deptno int unsigned comment '所在部门',
                      primary key(empno),
                      constraint `c1` foreign key (deptno) references pri01 (deptno)
);

insert into aff01 values (7369,'SMITH','CLERK',7902,'1980-12-17',800,NULL,20);
insert into aff01 values (7499,'ALLEN','SALESMAN',7698,'1981-02-20',1600,300,30);
insert into aff01 values (7521,'WARD','SALESMAN',7698,'1981-02-22',1250,500,30);
insert into aff01 values (7566,'JONES','MANAGER',7839,'1981-04-02',2975,NULL,20);
insert into aff01 values (7654,'MARTIN','SALESMAN',7698,'1981-09-28',1250,1400,30);
insert into aff01 values (7698,'BLAKE','MANAGER',7839,'1981-05-01',2850,NULL,30);
insert into aff01 values (7782,'CLARK','MANAGER',7839,'1981-06-09',2450,NULL,10);
drop publication if exists pub19;
drop publication if exists pub20;
create publication pub19 database db19 account acc01 comment 'publish to all account';
create publication pub20 database db19 table pri01, aff01 account acc02 comment '发布给acc01和acc02';

-- @session:id=1&user=acc01:test_account&password=111
drop database if exists sub19;
create database sub19 from sys publication pub19;
-- @ignore:5,7
show subscriptions;
-- @session

drop publication pub19;

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions;
-- @session

create publication pub19 database db19 account acc02 comment 'publish to all account';

-- @session:id=1&user=acc01:test_account&password=111
-- @ignore:5,7
show subscriptions;
use sub19;
drop database sub19;
-- @session


-- @session:id=2&user=acc02:test_account&password=111
drop database if exists sub20;
create database sub20 from sys publication pub20;
-- @ignore:5,7
show subscriptions;
-- @session

drop publication pub20;

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions;
-- @session

create publication pub20 database db19 account acc01 comment 'publish to all account';

-- @session:id=2&user=acc02:test_account&password=111
-- @ignore:5,7
show subscriptions;
use sub20;
drop database sub20;
-- @session
drop publication pub19;
drop publication pub20;
drop database db19;

drop account acc01;
drop account acc02;
drop account acc03;