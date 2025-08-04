drop database if exists test01;
create database test01;
use test01;

drop table if exists rs01;
create table rs01 (col1 int, col2 decimal(6), col3 varchar(30));
insert into rs01 values (1, null, 'database');
insert into rs01 values (2, 38291.32132, 'database');
insert into rs01 values (3, null, 'database management system');
insert into rs01 values (4, 10, null);
insert into rs01 values (1, -321.321, null);
insert into rs01 values (2, -1, null);
select count(*) from rs01;

drop snapshot if exists sp01;
create snapshot sp01 for account;
delete from rs01 where col1 = 1;

create database test02 clone test01 {snapshot = 'sp01'};
create table test01.rs02 clone test01.rs01 {snapshot = 'sp01'};
create table test01.rs02 clone test01.rs01 {snapshot = 'sp01'};
show databases;
use test02;
select * from test01;
select count(*) from test01;
drop snapshot if exists sp01;
use test01;
show tables;
select * from rs01;
select * from rs02;
drop database test01;
drop database test02;
drop snapshot sp01;



-- @bvt:issue#22297
drop database if exists test03;
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
create table test05.pri01 clone test03.pri01 {snapshot = 'sp02'};
create table test06.aff01 clone test03.aff01 {snapshot = 'sp02'};
use test04;
show tables;
show create table aff01;
show create table pri01;
select * from aff01;
select * from pri01;
drop table aff01;
drop table pri01;
drop database test04;
drop database test03;
drop snapshot sp02;
-- @bvt:issue



-- table with fulltext
drop database if exists db04;
create database db04;
use db04;
SET experimental_fulltext_index = 1;
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




-- abnormal test: clone database which has already exists
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




