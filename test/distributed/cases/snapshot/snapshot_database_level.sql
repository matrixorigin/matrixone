drop snapshot if exists sn1;
create snapshot sn1 for database db1;
-- @ignore:1
show snapshots;

drop database if exists db1;
create database if not exists db1;
create snapshot sn2 for database db1;
-- @ignore:1
show snapshots;

drop database if exists db1;
drop snapshot if exists sn2;
drop snapshot if exists sn1;
-- @ignore:1
show snapshots;

drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

-- @session:id=1&user=acc01:test_account&password=111
drop snapshot if exists sn1;
create snapshot sn1 for database db1;
-- @ignore:1
show snapshots;

drop database if exists db1;
create database if not exists db1;
create snapshot sn2 for database db1;
-- @ignore:1
show snapshots;

drop database if exists db1;
drop snapshot if exists sn2;
drop snapshot if exists sn1;
-- @session

drop account if exists acc1;

drop snapshot if exists sn1;
create snapshot sn1 for table db1 tbl1;
-- @ignore:1
show snapshots;

drop database if exists db1;
create database if not exists db1;
create table db1.tbl1 (a int);
insert into db1.tbl1 values (1), (2), (3);
create snapshot sn2 for table db1 tbl1;
-- @ignore:1
show snapshots;

drop database if exists db1;
drop snapshot if exists sn2;
drop snapshot if exists sn1;
-- @ignore:1
show snapshots;

drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

-- @session:id=2&user=acc01:test_account&password=111
drop snapshot if exists sn1;
create snapshot sn1 for table db1 tbl1;
-- @ignore:1
show snapshots;

drop database if exists db1;
create database if not exists db1;
create table db1.tbl1 (a int);
insert into db1.tbl1 values (1), (2), (3);
create snapshot sn2 for table db1 tbl1;
-- @ignore:1
show snapshots;

drop database if exists db1;
drop snapshot if exists sn2;
drop snapshot if exists sn1;
-- @session

drop account if exists acc1;

create snapshot sn1 for account;
create snapshot sn1 for account;
create snapshot if not exists sn1  for account;

drop snapshot if exists sn1;

create snapshot sn1 for database mo_catalog;
create snapshot sn1 for table mo_catalog mo_user;

drop snapshot if exists sn1;

drop snapshot if exists sn1;
create snapshot sn1 for account;
-- @ignore:1
show snapshots;
drop snapshot if exists sn1;


drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
-- @session:id=3&user=acc01:test_account&password=111
drop snapshot if exists sn1;
create snapshot sn1 for account;
-- @ignore:1
show snapshots;
drop snapshot if exists sn1;
-- @session

drop account if exists acc1;

drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '111';
-- @session:id=4&user=acc02:test_account&password=111
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

drop database if exists test04;
create database test04;
use test04;
-- @bvt:issue#16438
drop table if exists partition01;
create table partition01 (
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

insert into partition01 values (9001,'1980-12-17', 'SMITH', 'CLERK', 'F', '2008-12-17'),
                               (9002,'1981-02-20', 'ALLEN', 'SALESMAN', 'F', '2008-02-20');
-- @bvt:issue
drop database if exists test06;
create database test06;
use test06;
set experimental_fulltext_index=1;
create table src (id bigint primary key, body varchar, title text);
insert into src values (0, 'color is red', 't1'), (1, 'car is yellow', 'crazy car'), (2, 'sky is blue', 'no limit'), (3, 'blue is not red', 'colorful'),
                       (4, '遠東兒童中文是針對6到9歲的小朋友精心設計的中文學習教材，共三冊，目前已出版一、二冊。', '遠東兒童中文'),
                       (5, '每冊均採用近百張全幅彩圖及照片，生動活潑、自然真實，加深兒童學習印象，洋溢學習樂趣。', '遠東兒童中文'),
                       (6, '各個單元主題內容涵蓋中華文化及生活應用的介紹。本套教材含課本、教學指引、生字卡、學生作業本與CD，中英對照，精美大字版。本系列有繁體字及簡體字兩種版本印行。', '中文短篇小說'),
                       (7, '59個簡單的英文和中文短篇小說', '適合初學者'),
                       (8, NULL, 'NOT INCLUDED'),
                       (9, 'NOT INCLUDED BODY', NULL),
                       (10, NULL, NULL);

create fulltext index ftidx on src (body, title);
select * from src;
drop view if exists view01;
create view v01 as select * from src;
select * from v01;
show create table v01;
-- @session

drop snapshot if exists spsp02;
create snapshot spsp02 for account acc02;
drop account acc02;
restore account acc02 from snapshot spsp02;

drop snapshot if exists spsp02;
drop account if exists acc02;
