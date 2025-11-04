drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

-- restore null dropped account
drop snapshot if exists spsp01;
create snapshot spsp01 for account acc01;
-- @ignore:0,1
show snapshots;
-- @ignore:2,3,4,5,6,7,8,9
show accounts;
drop account acc01;
restore account acc01{snapshot="spsp01"};
-- @ignore:2,3,4,5,6,7,8,9
show accounts;
drop account acc01;
drop snapshot spsp01;


-- sys level snapshot restore nonsys dropped account
drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '111';
-- @session:id=1&user=acc02:test_account&password=111
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
restore account acc02{snapshot="spsp02"};
-- @ignore:2,3,4,5,6,7,8,9
show accounts;

-- @session:id=2&user=acc02:test_account&password=111
show databases;
use test01;
select * from rs01;
show create table rs01;
use test03;
select * from pri01;
select * from aff01;
delete from aff01;
use test04;
select * from partition01;
use test06;
select * from src;
truncate src;
-- @session

drop account acc02;
drop snapshot spsp02;




-- abnormal test: drop account, create the same account name with dropped account, restore dropped account
drop account if exists acc03;
create account acc03 admin_name = 'test_account' identified by '111';
-- @session:id=3&user=acc03:test_account&password=111
drop database if exists udf_db;
create database udf_db;
use udf_db;
select name, db from mo_catalog.mo_user_defined_function;
-- function add
create function `addab`(x int, y int) returns int
    language sql as
'$1 + $2';
select addab(10, 5);
select name, db from mo_catalog.mo_user_defined_function;

create function`concatenate`(str1 varchar(255), str2 varchar(255)) returns varchar(255)
    language sql as
'$1 + $2';
select concatenate('Hello, ', 'World!');

drop database if exists test01;
create database test01;
use test01;
drop table if exists EmployeeSalaries;
create table EmployeeSalaries (
                                  EmployeeID INT,
                                  EmployeeName VARCHAR(100),
                                  Salary DECIMAL(10, 2)
);
insert into EmployeeSalaries (EmployeeID, EmployeeName, Salary) VALUES
            (1, 'Alice', 70000),
            (2, 'Bob', 80000),
            (3, 'Charlie', 90000),
            (4, 'David', 65000),
            (5, 'Eva', 75000);
drop view if exists EmployeeSalaryRanking;
create view EmployeeSalaryRanking AS
select
    EmployeeID,
    EmployeeName,
    Salary,
    rank() over (order by Salary desc) as SalaryRank
from
    EmployeeSalaries;
select * from EmployeeSalaryRanking;

drop database if exists procedure_test;
create database procedure_test;
use procedure_test;

drop database if exists test02;
create database test02;
use test02;
drop table if exists tbh1;
drop table if exists tbh2;
drop table if exists tbh2;
create table tbh1(id int primary key, val int);
create table tbh2(id int primary key, val char);
create table tbh3(id int primary key, val float);

insert into tbh1(id, val) values(1,10),(2,20),(3,30);
insert into tbh2(id, val) values(1,'a'),(2,'b'),(3,'c');
insert into tbh3(id, val) values(1,1.5),(2,2.5),(3,3.5);

drop procedure if exists test_if_hit_second_elseif;
create procedure test_if_hit_second_elseif() 'begin DECLARE v1 INT; SET v1 = 4; IF v1 > 5 THEN select * from tbh1; ELSEIF v1 = 5 THEN select * from tbh2; ELSEIF v1 = 4 THEN select * from tbh2 order by id limit 1; ELSE select * from tbh3; END IF; end';
call test_if_hit_second_elseif();
-- @session

drop snapshot if exists spsp03;
create snapshot spsp03 for account acc03;

drop account acc03;
create account acc03 admin_name = 'test_account' identified by '111';

restore account acc03{snapshot="spsp03"};
drop account acc03;
drop snapshot spsp03;


-- cluster level snapshot restore cluster which contains dropped account
drop account if exists acc04;
create account acc04 admin_name = 'test_account' identified by '111';
drop account if exists acc05;
create account acc05 admin_name = 'test_account' identified by '111';
drop account if exists acc06;
create account acc06 admin_name = 'test_account' identified by '111';
-- @session:id=4&user=acc04:test_account&password=111
drop database if exists test04;
create database test04;
use test04;
drop table if exists t1;
create table t1(
                   col1 date not null,
                   col2 datetime,
                   col3 timestamp,
                   col4 bool
);
set time_zone = 'SYSTEM';
load data infile '$resources/load_data/time_date_1.csv' into table t1 fields terminated by ',';
select * from t1;
drop stage if exists stage01;
create stage stage01 url = 'file:///$resources/into_outfile/stage';
-- @ignore:0,2,5
select * from mo_catalog.mo_stages;
-- @ignore:1
show stages;
select * from t1 into outfile 'stage://stage01/local_stage_table20.csv';
truncate t1;
load data infile 'stage://stage01/local_stage_table20.csv' into table t1 fields terminated by ',' ignore 1 lines;
select * from t1;

drop table if exists vtab32;
drop table if exists vtab64;
create table vtab32(id int primary key auto_increment,`vecf32_3` vecf32(3),`vecf32_5` vecf32(5));
create table vtab64(id int primary key auto_increment,`vecf64_3` vecf64(3),`vecf64_5` vecf64(5));

insert into vtab32(vecf32_3,vecf32_5) values("[0.8166459,NULL,0.4886152]",NULL);
insert into vtab32(vecf32_3,vecf32_5) values(NULL,NULL);
insert into vtab32(vecf32_3,vecf32_5) values("[0.8166459,0.66616553,0.4886152]",NULL);
insert into vtab32(vecf32_3,vecf32_5) values("0.1726299,3.29088557,30.4330937","0.1726299,3.29088557,30.4330937");
insert into vtab32(vecf32_3,vecf32_5) values ("[0.1726299,3.29088557,30.4330937]","[0.45052445,2.19845265,9.579752,123.48039162,4635.89423394]");
insert into vtab32(vecf32_3,vecf32_5) values ("[8.5606893,6.7903588,821.977768]","[0.46323407,23.49801546,563.9229458,56.07673508,8732.9583881]");
insert into vtab64(vecf64_3,vecf64_5) values("[0.8166459,NULL,0.4886152]",NULL);
insert into vtab64(vecf64_3,vecf64_5) values(NULL,NULL);
insert into vtab64(vecf64_3,vecf64_5) values("[0.8166459,0.66616553,0.4886152]",NULL);
insert into vtab64(vecf64_3,vecf64_5) values ("[8.5606893,6.7903588,821.977768]","[0.46323407,23.49801546,563.9229458,56.07673508,8732.9583881]");
insert into vtab64(vecf64_3,vecf64_5) values ("[0.9260021,0.26637346,0.06567037]","[0.45756745,65.2996871,321.623636,3.60082066,87.58445764]");


drop table if exists t1;
create table t1 (id int,ti tinyint unsigned,si smallint,bi bigint unsigned,fl float,dl double,de decimal,ch char(20),vch varchar(20),dd date,dt datetime);
insert into t1 values(1,1,4,3,1113.32,111332,1113.32,'hello','subquery','2022-04-28','2022-04-28 22:40:11');
insert into t1 values(2,2,5,2,2252.05,225205,2252.05,'bye','sub query','2022-04-28','2022-04-28 22:40:11');
insert into t1 values(3,6,6,3,3663.21,366321,3663.21,'hi','subquery','2022-04-28','2022-04-28 22:40:11');
insert into t1 values(4,7,1,5,4715.22,471522,4715.22,'good morning','my subquery','2022-04-28','2022-04-28 22:40:11');
insert into t1 values(5,1,2,6,51.26,5126,51.26,'byebye',' is subquery?','2022-04-28','2022-04-28 22:40:11');
insert into t1 values(6,3,2,1,632.1,6321,632.11,'good night','maybe subquery','2022-04-28','2022-04-28 22:40:11');
insert into t1 values(7,4,4,3,7443.11,744311,7443.11,'yes','subquery','2022-04-28','2022-04-28 22:40:11');
insert into t1 values(8,7,5,8,8758.00,875800,8758.11,'nice to meet','just subquery','2022-04-28','2022-04-28 22:40:11');
insert into t1 values(9,8,4,9,9849.312,9849312,9849.312,'see you','subquery','2022-04-28','2022-04-28 22:40:11');
select * from (select * from t1) sub where id > 4;
-- @session

-- @session:id=5&user=acc05:test_account&password=111
drop database if exists test05;
create database test05;
use test05;
drop table if exists length01;
create table length01(col1 binary(20));
insert into length01 values('12');
insert into length01 values('372814');
insert into length01 values('&***');
select * from length01;

drop table if exists t1;
create table t1 (a blob);
insert into t1 values('abcdef');
insert into t1 values('_bcdef');
insert into t1 values('a_cdef');
insert into t1 values('ab_def');
insert into t1 values('abc_ef');
insert into t1 values('abcd_f');
insert into t1 values('abcde_');
select * from t1 where a like 'ab\_def' order by 1 asc;

drop table if exists test02;
create table test02 (col1 int, col2 datalink);
insert into test02 values (1, 'file://$resources/load_data/time_date_2.csv');
select col1, load_file(col2) from test02;
-- @session

-- @session:id=6&user=acc06:test_account&password=111
drop database if exists test06;
create database test06;
use test06;
drop table if exists table01;
create table table01 (
                         id int auto_increment primary key,
                         col1 varchar(255) not null ,
                         col2 int,
                         col3 decimal(10, 2),
                         col4 date,
                         col5 boolean,
                         col6 enum('apple', 'banana', 'orange'),
                         col7 text,
                         col8 timestamp,
                         col9 blob,
                         col10 char,
                         unique index(col8, col10)
);
insert into table01 (col1, col2, col3, col4, col5, col6, col7, col8, col9, col10) values
('Value2', 456, 78.90, '2023-10-24', false, 'banana', 'Another text', '2022-01-01 01:01:01.000', 'More binary data', 'D'),
('Value3', 789, 12.34, '2023-10-25', true, 'orange', 'Yet another text', '1979-01-01 01:01:01.123', 'Even more binary data', 'E');
drop table if exists t2;
create table t2 (a json,b int);
delete from t2;
insert into t2 values ('{"t1":"a"}',1),('{"t1":"b"}',2);
select * from t2;

drop table if exists t_timestamp;
create table t_timestamp(id timestamp(6));
insert into t_timestamp values ('2020-01-01 23:59:59.999999'), ('2022-01-02 00:00:00');
select * from t_timestamp where id = 20200102;
-- @session

drop snapshot if exists spsp06;
create snapshot spsp06 for cluster;

drop account acc04;
drop account acc05;
drop account acc06;

restore cluster from snapshot spsp06;

-- @session:id=7&user=acc04:test_account&password=111
use test04;
select * from t1;
-- @ignore:1
show stages;
select * from vtab32;
select * from vtab64;
-- @session

-- @session:id=8&user=acc05:test_account&password=111
use test05;
select * from length01;
select * from t1;
-- @ignore:1
select * from test02;
-- @session

-- @session:id=9&user=acc06:test_account&password=111
use test06;
select * from table01;
select * from t2;
select * from t_timestamp;
-- @session

drop account acc04;
drop account acc05;
drop account acc06;
drop snapshot spsp06;


-- abnormal test: nonsys restore the deleted account
drop account if exists acc07;
create account acc07 admin_name = 'test_account' identified by '111';

drop snapshot if exists spsp07;
create snapshot spsp07 for account acc07;

drop account acc07;

restore account acc07{snapshot="spsp07"};

-- @session:id=11&user=acc07:test_account&password=111
show databases;
-- @session

drop account acc07;
drop snapshot spsp07;
drop database restore_dropped_account;