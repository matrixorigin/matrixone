drop database if exists test01;
drop database if exists test02;
drop database if exists test03;

create database test01;
use test01;

drop table if exists rs01;
create table rs01 (col1 int, col2 decimal(6), col3 varchar(30), col4 float default 11);
insert into rs01 values (1, null, 'database', 29.001);
insert into rs01 values (2, 38291.32132, 'database', 327.22);
insert into rs01 values (3, null, 'database management system', 3283);
insert into rs01 values (4, 10, null, -219823.1);
insert into rs01 values (1, -321.321, null, 3829);
insert into rs01 values (2, -1, null, 2819);
select count(*) from rs01;

drop snapshot if exists sp01;
create snapshot sp01 for account;
delete from rs01 where col1 = 1;

create database test02 clone test01 {snapshot = 'sp01'};
create table test01.rs02 clone test01.rs01 {snapshot = 'sp01'};
create table test01.rs02 clone test01.rs01 {snapshot = 'sp01'};
show databases;
use test02;
select * from rs01;
select count(*) from rs01;
use test01;
show tables;
select * from rs01;
select * from rs02;
drop database test01;
drop database test02;
drop snapshot sp01;

create database test03;
use test03;
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

drop snapshot if exists sp02;
create snapshot sp02 for account;

create database test04 clone test03 {snapshot = 'sp02'};
drop database if exists test05;
create database test05;
drop database if exists test06;
create database test06;
create table test05.pri01 clone test03.pri01 {snapshot = 'sp02'};
create table test06.aff01 clone test03.aff01 {snapshot = 'sp02'};
use test04;
show tables;
show create table aff01;
show create table pri01;
select * from aff01;
select * from pri01;
use test05;
show tables;
select * from pri01;
show create table pri01;
use test06;
show tables;
select * from aff01;
show create table aff01;
drop database test04;
drop database test05;
drop database test06;
drop database test03;
drop snapshot sp02;




drop database if exists db03;
create database db03;
use db03;
create table enum01 (col1 enum('red','blue','green'), col2 blob, col3 json);
insert into enum01 values ('red', 'abcdef', '{"t1":"a"}');
insert into enum01 values ('blue', 'abcdef', '{"t2":"a"}');
insert into enum01 values ('green', 'abcdef', '{"t3":"a"}');
drop snapshot if exists sp03;
create snapshot sp03 for account sys;
truncate enum01;
create database db04 clone db03 {snapshot = 'sp03'};
use db04;
select * from enum01;
insert into enum01 values ('green', 'abcdef', '{"t4":"a"}');
select count(*) from enum01;
show create table enum01;
desc enum01;
drop snapshot sp03;
drop database db03;
drop database db04;




-- table with fulltext
drop database if exists db04;
create database db04;
use db04;
create table src (id bigint primary key, body varchar, title text);
insert into src values (0, 'color is red', 't1'), (1, 'car is yellow', 'crazy car'), (2, 'sky is blue', 'no limit'), (3, 'blue is not red', 'colorful'),
(4, '遠東兒童中文是針對6到9歲的小朋友精心設計的中文學習教材，共三冊，目前已出版一、二冊。', '遠東兒童中文'),
(5, '每冊均採用近百張全幅彩圖及照片，生動活潑、自然真實，加深兒童學習印象，洋溢學習樂趣。', '遠東兒童中文'),
(6, '各個單元主題內容涵蓋中華文化及生活應用的介紹。本套教材含課本、教學指引、生字卡、學生作業本與CD，中英對照，精美大字版。本系列有繁體字及簡體字兩種版本印行。', '中文短篇小說'),
(7, '59個簡單的英文和中文短篇小說', '適合初學者'),
(8, NULL, 'NOT INCLUDED'),
(9, 'NOT INCLUDED BODY', NULL),
(10, NULL, NULL);

drop snapshot if exists sp04;
create snapshot sp04 for account;
create fulltext index ftidx on src (body, title);

drop database if exists db05;
create database db05 clone db04 {snapshot = 'sp04'};
use db05;
show create table src;
select * from src;
select count(*) from src;
truncate src;
drop database db04;
drop database db05;
drop snapshot sp04;




-- abnormal test: clone database which has already exists in current account
drop database if exists db01;
create database db01;
use db01;
drop table if exists index03;
create table index03 (
                         emp_no      int             not null,
                         birth_date  date            not null,
                         first_name  varchar(14)     not null,
                         last_name   varchar(16)     not null,
                         gender      varchar(5)      not null,
                         hire_date   date            not null,
                         primary key (emp_no)
) partition by range columns (emp_no)(
    partition p01 values less than (100001),
    partition p02 values less than (200001),
    partition p03 values less than (300001),
    partition p04 values less than (400001)
);
insert into index03 values (9001,'1980-12-17', 'SMITH', 'CLERK', 'F', '2008-12-17'),
                           (9002,'1981-02-20', 'ALLEN', 'SALESMAN', 'F', '2008-02-20'),
                           (9003,'1991-02-20', 'Bob', 'TEACHER', 'M', '2008-02-20'),
                           (9004,'1999-02-20', 'MARY', 'PROGRAMMER', 'M', '2008-02-20');
select * from index03;
drop snapshot if exists sp03;
create snapshot sp03 for account;
create database if not exists db02;
create database db02 clone db01 {snapshot = 'sp03'};
show databases;
drop database db01;
drop database db02;
drop snapshot sp03;




-- pub database
drop account if exists test_tenant_1;
create account test_tenant_1 admin_name 'test_account' identified by '111';

drop database if exists republication01;
create database republication01;
use republication01;
create publication publication01 database republication01 account test_tenant_1 comment 'republish';
create table repub01(col1 int);
insert into repub01 values (1);

-- @session:id=1&user=test_tenant_1:test_account&password=111
drop database if exists resub01;
create database resub01 from sys publication publication01;
-- @ignore:5,7
show subscriptions all;
-- @session

drop snapshot if exists sp04;
create snapshot sp04 for account;

create database test_pub clone republication01 {snapshot = 'sp04'};
-- @ignore:5,6
show publications;

-- @session:id=1&user=test_tenant_1:test_account&password=111
-- @ignore:5,7
show subscriptions all;
drop database resub01;
-- @session
drop publication publication01;
drop database republication01;
drop database test_pub;
drop snapshot sp04;




-- sub database
drop database if exists republication01;
create database republication01;
use republication01;
create publication publication01 database republication01 account test_tenant_1 comment 'republish';
create table repub01(col1 int);
insert into repub01 values (1);

-- @session:id=1&user=test_tenant_1:test_account&password=111
drop database if exists resub01;
create database resub01 from sys publication publication01;
-- @ignore:5,7
show subscriptions all;
drop snapshot if exists sp05;
create snapshot sp05 for account;
create database test_sub clone resub01 {snapshot = 'sp05'};
-- @ignore:5,7
show subscriptions all;
drop database test_sub;
drop database resub01;
drop snapshot sp05;
-- @session
drop publication publication01;
drop database republication01;




-- pub table
drop database if exists db02;
create database db02;
use db02;
create table vector_index_01(a int primary key, b vecf32(128),c int,key c_k(c));
insert into vector_index_01 values(9774 ,"[1, 0, 1, 6, 6, 17, 47, 39, 2, 0, 1, 25, 27, 10, 56, 130, 18, 5, 2, 6, 15, 2, 19, 130, 42, 28, 1, 1, 2, 1, 0, 5, 0, 2, 4, 4, 31, 34, 44, 35, 9, 3, 8, 11, 33, 12, 61, 130, 130, 17, 0, 1, 6, 2, 9, 130, 111, 36, 0, 0, 11, 9, 1, 12, 2, 100, 130, 28, 7, 2, 6, 7, 9, 27, 130, 83, 5, 0, 1, 18, 130, 130, 84, 9, 0, 0, 2, 24, 111, 24, 0, 1, 37, 24, 2, 10, 12, 62, 33, 3, 0, 0, 0, 1, 3, 16, 106, 28, 0, 0, 0, 0, 17, 46, 85, 10, 0, 0, 1, 4, 11, 4, 2, 2, 9, 14, 8, 8]",3),(9775,"[0, 1, 1, 3, 0, 3, 46, 20, 1, 4, 17, 9, 1, 17, 108, 15, 0, 3, 37, 17, 6, 15, 116, 16, 6, 1, 4, 7, 7, 7, 9, 6, 0, 8, 10, 4, 26, 129, 27, 9, 0, 0, 5, 2, 11, 129, 129, 12, 103, 4, 0, 0, 2, 31, 129, 129, 94, 4, 0, 0, 0, 3, 13, 42, 0, 15, 38, 2, 70, 129, 1, 0, 5, 10, 40, 12, 74, 129, 6, 1, 129, 39, 6, 1, 2, 22, 9, 33, 122, 13, 0, 0, 0, 0, 5, 23, 4, 11, 9, 12, 45, 38, 1, 0, 0, 4, 36, 38, 57, 32, 0, 0, 82, 22, 9, 5, 13, 11, 3, 94, 35, 3, 0, 0, 0, 1, 16, 97]",5),(9776,"[10, 3, 8, 5, 48, 26, 5, 16, 17, 0, 0, 2, 132, 53, 1, 16, 112, 6, 0, 0, 7, 2, 1, 48, 48, 15, 18, 31, 3, 0, 0, 9, 6, 10, 19, 27, 50, 46, 17, 9, 18, 1, 4, 48, 132, 23, 3, 5, 132, 9, 4, 3, 11, 0, 2, 46, 84, 12, 10, 10, 1, 0, 12, 76, 26, 22, 16, 26, 35, 15, 3, 16, 15, 1, 51, 132, 125, 8, 1, 2, 132, 51, 67, 91, 8, 0, 0, 30, 126, 39, 32, 38, 4, 0, 1, 12, 24, 2, 2, 2, 4, 7, 2, 19, 93, 19, 70, 92, 2, 3, 1, 21, 36, 58, 132, 94, 0, 0, 0, 0, 21, 25, 57, 48, 1, 0, 0, 1]",3);
insert into vector_index_01 values(9777, " [16, 15, 0, 0, 5, 46, 5, 5, 4, 0, 0, 0, 28, 118, 12, 5, 75, 44, 5, 0, 6, 32, 6, 49, 41, 74, 9, 1, 0, 0, 0, 9, 1, 9, 16, 41, 71, 80, 3, 0, 0, 4, 3, 5, 51, 106, 11, 3, 112, 28, 13, 1, 4, 8, 3, 104, 118, 14, 1, 1, 0, 0, 0, 88, 3, 27, 46, 118, 108, 49, 2, 0, 1, 46, 118, 118, 27, 12, 0, 0, 33, 118, 118, 8, 0, 0, 0, 4, 118, 95, 40, 0, 0, 0, 1, 11, 27, 38, 12, 12, 18, 29, 3, 2, 13, 30, 94, 78, 30, 19, 9, 3, 31, 45, 70, 42, 15, 1, 3, 12, 14, 22, 16, 2, 3, 17, 24, 13]",4),(9778,"[41, 0, 0, 7, 1, 1, 20, 67, 9, 0, 0, 0, 0, 31, 120, 61, 25, 0, 0, 0, 0, 10, 120, 90, 32, 0, 0, 1, 13, 11, 22, 50, 4, 0, 2, 93, 40, 15, 37, 18, 12, 2, 2, 19, 8, 44, 120, 25, 120, 5, 0, 0, 0, 2, 48, 97, 102, 14, 3, 3, 11, 9, 34, 41, 0, 0, 4, 120, 56, 3, 4, 5, 6, 15, 37, 116, 28, 0, 0, 3, 120, 120, 24, 6, 2, 0, 1, 28, 53, 90, 51, 11, 11, 2, 12, 14, 8, 6, 4, 30, 9, 1, 4, 22, 25, 79, 120, 66, 5, 0, 0, 6, 42, 120, 91, 43, 15, 2, 4, 39, 12, 9, 9, 12, 15, 5, 24, 36]",4);
create index idx01 using ivfflat on vector_index_01(b) lists=5 op_type "vector_l2_ops";
create table vector_index_04(a int primary key, b vecf32(3),c vecf32(4));
insert into vector_index_04 values(1,"[56,23,6]","[0.25,0.14,0.88,0.0001]"),(2,"[77,45,3]","[1.25,5.25,8.699,4.25]"),(3,"[8,56,3]","[9.66,5.22,1.22,7.02]");
drop publication if exists pub02;
create publication pub02 database db02 table vector_index_01, vector_index_04 account test_tenant_1;
drop snapshot if exists sp05;
create snapshot sp05 for account;
delete from vector_index_01 where a = 9777;
delete from vector_index_04 where a = 2;
-- @session:id=1&user=test_tenant_1:test_account&password=111
drop database if exists test_pub01;
create database test_pub01;
-- @session
create table test_pub01.pub_table01 clone db02.vector_index_01 {snapshot = 'sp05'} to account test_tenant_1;
drop database if exists test_pub01;
create database test_pub01;
create table test_pub01.pub_table02 clone db02.vector_index_04 {snapshot = 'sp05'};
-- @ignore:5,6
show publications;
-- @session:id=1&user=test_tenant_1:test_account&password=111
drop database if exists resub01;
create database resub01 from sys publication pub02;
use test_pub01;
show tables;
select * from pub_table01;
-- @ignore:5,7
show subscriptions all;
drop database resub01;
-- @session
drop database test_pub01;
drop publication pub02;
drop database db02;
drop snapshot sp05;
drop account test_tenant_1;




drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '111';
drop account if exists acc03;
create account acc03 admin_name = 'test_account' identified by '111';

-- restore db/table to new account
drop database if exists test05;
create database test05;
use test05;
drop table if exists rs01;
create table rs01 (col1 int, col2 decimal(6), col3 varchar(30));
insert into rs01 values (1, null, 'database');
insert into rs01 values (2, 38291.32132, 'database');
insert into rs01 values (3, null, 'database management system');
insert into rs01 values (4, 10, null);
insert into rs01 values (1, -321.321, null);
insert into rs01 values (2, -1, null);
select count(*) from rs01;

drop snapshot if exists sp07;
create snapshot sp07 for account;
delete from rs01 where col1 = 1;

create database test06 clone test05 {snapshot = 'sp07'} to account acc01;
-- @session:id=2&user=acc01:test_account&password=111
drop database if exists test05;
drop database if exists test03;
create database test05;
create database test03;
-- @session
create table test05.rs02 clone test05.rs01 {snapshot = 'sp07'} to account acc01;
create table test03.rs02 clone test05.rs01 {snapshot = 'sp07'} to account acc01;
-- @session:id=2&user=acc01:test_account&password=111
show databases;
use test05;
select * from rs02;
select count(*) from rs02;
use test03;
show tables;
select * from rs02;
drop database test03;
drop database test05;
-- @session
drop database test05;
drop snapshot sp07;




-- restore fk db/table to new account
drop database if exists test06;
create database test06;
use test06;
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

drop snapshot if exists sp07;
create snapshot sp07 for account;
-- @session:id=3&user=acc02:test_account&password=111
drop database if exists test05;
drop database if exists test06;
create database test05;
create database test06;
-- @session

create database test04 clone test06 {snapshot = 'sp07'} to account acc02;
create table test05.pri01 clone test06.pri01 {snapshot = 'sp07'} to account acc02;
create table test06.aff01 clone test06.aff01 {snapshot = 'sp07'} to account acc02;
-- @session:id=3&user=acc02:test_account&password=111
use test04;
show tables;
show create table aff01;
show create table pri01;
use test05;
show create table pri01;
use test06;
select * from aff01;
show create table aff01;
drop database test04;
drop database test05;
drop database test06;
-- @session
drop database test06;
drop snapshot sp07;
drop account acc01;
drop account acc02;
drop account acc03;




-- abnormal test: restore db to new account, db exists in new account
drop account if exists acc05;
create account acc05 admin_name = 'test_account' identified by '111';
drop database if exists test08;
create database test08;
use test08;
drop table if exists t1;
create table t1 (a timestamp(0) not null, primary key(a));
insert into t1 values ('20200101000000'), ('2022-01-02'), ('2022-01-02 00:00:01'), ('2022-01-02 00:00:01.512345');
select * from t1;
drop snapshot if exists sp08;
create snapshot sp08 for account;
update t1 set a=DATE_ADD(a ,INTERVAL 1 WEEK) where a>'20220102';
select * from t1;

-- @session:id=5&user=acc05:test_account&password=111
drop database if exists test08;
create database test08;
-- @session
create database test08 clone test08 {snapshot = 'sp08'} to account acc05;
create database test09 clone test08 {snapshot = 'sp08'} to account acc05;
drop snapshot sp08;
-- @session:id=5&user=acc05:test_account&password=111
use test09;
select * from t1;
drop database test09;
-- @session
drop database test08;
drop account acc05;




-- abnormal test: restore table to new account, table exists in new account
drop account if exists acc04;
create account acc04 admin_name = 'test_account' identified by '111';
drop database if exists test09;
create database test09;
use test09;
drop table if exists t1;
create table t1(
                   a uuid primary key,
                   b int,
                   c varchar(20),
                   d date
) comment='test uuid parimary key';
insert into t1 values ("6d1b1f73-2dbf-11ed-940f-000c29847904",12,'SMITH','1980-12-17');
insert into t1 values ("ad9f809f-2dbd-11ed-940f-000c29847904",34,'ALLEN','1981-02-20');
drop snapshot if exists sp05;
create snapshot sp05 for account;
truncate t1;
-- @session:id=4&user=acc04:test_account&password=111
drop database if exists test01;
create database test01;
use test01;
create table t1(col1 int, col2 decimal);
-- @session
create table test01.t1 clone test09.t1 {snapshot = 'sp05'} to account acc04;
create table test01.t2 clone test09.t1 {snapshot = 'sp05'} to account acc04;
-- @session:id=4&user=acc04:test_account&password=111
show databases;
use test01;
show tables;
select * from t1;
select * from t2;
truncate t2;
drop database test01;
-- @session
drop database test09;
drop snapshot sp05;
drop account acc04;




-- restore db/table to account which does not exists
drop database if exists test10;
create database test10;
use test10;

drop table if exists table10;
create table table10 (col1 int, col2 datalink);
insert into table10 values (1, 'file://$resources/load_data/time_date_1.csv');

drop snapshot if exists sp10;
create snapshot sp10 for account;

create database test11 clone test10 {snapshot = 'sp10'} to account acc100;
drop database test10;
drop snapshot sp10;




-- restore db to db,  there are multiple tables under the database
drop database if exists db10;
create database db10;
use db10;
drop table if exists text_01;
create table text_01(t1 text,t2 text,t3 text);
insert into text_01 values ('中文123abcd','',NULL);
insert into text_01 values ('yef&&190',' wwww ',983);
insert into text_01 select '',null,'中文';
insert into text_01 select '123','7834','commmmmmment';
insert into text_01 values ('789',' 23:50:00','20');

drop table if exists text_02;
create table text_02 (a float not null, primary key(a));
insert into text_02 values(-3.402823466E+38),(-1.175494351E-38),(0),(1.175494351E-38),(3.402823466E+38);

drop table if exists numtable;
create table numtable(id int,fl float, dl double);
insert into numtable values(1,123456,123456);
insert into numtable values(2,123.456,123.456);
insert into numtable values(3,1.234567,1.234567);
insert into numtable values(4,1.234567891,1.234567891);
insert into numtable values(5,1.2345678912345678912,1.2345678912345678912);

drop table if exists length02;
create table length02(col1 binary(255));
insert into length02 values('ttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt');
insert into length02 values('dehwjqbewbvhrbewrhebwjverguyw432843iuhfkuejwnfjewbhvbewh4gh3jbvrew vnbew rjjrewkfrhjewhrefrewfrwrewf432432r32r432r43rewvrewrfewfrewf432f43fewf4324r3r3rrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr');
select * from length02;

drop table if exists T1;
create table T1 (a smallint unsigned not null, primary key(a));
insert into T1 values (65535), (0xFFFC), (65534), (65533);

drop table if exists vtab32;
create table vtab32(id int primary key auto_increment,`vecf32_3` vecf32(3),`vecf32_5` vecf32(5));
insert into vtab32(vecf32_3,vecf32_5) values(NULL,NULL);

drop table if exists articles;
drop table if exists authors;
create table articles (
                          id int auto_increment primary key,
                          title varchar(255),
                          content text,
                          author_id int,
                          fulltext(content)
);
create table authors (
                         id int auto_increment primary key,
                         name varchar(100)
);
insert into authors (name) values ('John Doe'), ('Jane Smith'), ('Alice Johnson');
insert into articles (title, content, author_id) values
                                                     ('MO全文索引入门', 'MO全文索引是一种强大的工具，可以帮助你快速检索数据库中的文本数据。', 1),
                                                     ('深入理解全文索引', '全文索引不仅可以提高搜索效率，还可以通过JOIN操作与其他表结合使用。', 2),
                                                     ('MO性能优化', '本文将探讨如何优化MO数据库的性能，包括索引优化和查询优化。', 3),
                                                     ('全文索引与JOIN操作', '全文索引可以与JOIN操作结合使用，以实现跨表的全文搜索。', 1);
drop snapshot if exists sp10;
create snapshot sp10 for account;

drop table text_01;
drop table text_02;

drop snapshot if exists sp11;
create snapshot sp11 for account;

drop database if exists db11;
create database db11 clone db10 {snapshot = 'sp10'};
use db11;
show tables;
select * from text_01;
select * from text_02;
show create table numtable;
select count(*) from T1;
select * from T1 order by a desc;
delete from articles where id = 1;
select * from authors;

drop database if exists db12;
create database db12 clone db10 {snapshot = 'sp11'};
use db12;
show tables;
select * from text_01;
select * from text_02;
show create table numtable;
select count(*) from T1;
select * from T1 order by a desc;
delete from articles where id = 1;
select * from authors;

drop database db10;
drop database db11;
drop database db12;
drop snapshot sp10;
drop snapshot sp11;
drop database test03;
