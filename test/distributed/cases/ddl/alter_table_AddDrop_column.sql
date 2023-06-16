-- @suit
-- @case
-- @desc:alter table add/drop column,add comment
-- @label:bvt
drop database if exists test;
create database test;
use test;

-- add column:the default value of the new column is displayed as null
drop table if exists add01;
create table add01 (
                       col1 int not null auto_increment primary key,
                       col2 varchar(30) not null,
                       col3 varchar (20) not null,
                       col4 varchar(4) not null,
                       col5 int not null);
insert into add01 values(1,'数据库','shujuku','abcd',1);
insert into add01 values(2,'database','云原生','tb',2);
select * from add01;
alter table add01 add column col2_3 varchar(20) not null after col2;
alter table add01 add column col7 varchar(30) not null after col5;
alter table add01 add column col8 int not null;
alter table add01 add column col9 int not null first;
insert into add01 values(1,3,'nihao','hei','hu','jj',2,'varchar',1);
show create table add01;
select * from add01;
drop table add01;

-- add column:the default is after the last column
drop table if exists add02;
create table add02(col1 int,col2 binary);
insert into add02 values(1,'f');
insert into add02 values(2,'4');
alter table add02 add column col3 datetime primary key;
show create table add02;
drop table add02;

-- add column:specify after or first
drop table if exists add03;
create table add03(col1 double,col2 float);
insert into add03 values(21321.3213,239243.0);
alter table add03 add column colf int first;
show create table add03;
alter table add03 add column cola binary;
show create table add03;
alter table add03 add column colm varbinary(10) after col1;
show create table add03;
alter table add03 drop column colm;
show create table add03;
drop table add03;

-- add column:multi column
drop table if exists add02;
create table add02(f1 int);
alter table add02 add column f2 datetime not null, add column f21 date not null;
show create table add02;
insert into add02 values(1,'2000-01-01','2000-01-01');
alter table add02 add column f3 int not null;
show create table add02;

-- duplicate column name
alter table add02 add column f3 date not null;
alter table add02 add column f4 datetime not null default '2002-02-02',add column f41 date not null default '2002-02-02';
insert into add02 values(1,'2000-12-12 22:22:22','1997-01-13',13,'1997-12-12 11:11:11','2001-11-12');
select * from add02;
drop table add02;

drop table if exists t1;
create table t1 (i int unsigned auto_increment primary key);
insert into t1 values (null),(null),(null),(null);
alter table t1 add i int unsigned not null;
select * from t1;
drop table t1;


-- alter table drop column, add column
drop table if exists drop01;
create table drop01 (a TEXT, id INT, b INT);
insert into drop01 values('ahsekafe',1,2);
insert into drop01 values('efuiwojq',23,23);
show create table drop01;
alter table drop01 drop column a, add column c text first;
select * from drop01;
show create table drop01;
drop table drop01;

-- if a table contains only one column, the column cannot be dropped
drop table if exists drop01;
create table drop01(col1 int);
insert into drop01 values(1);
insert into drop01 values(2);
alter table drop01 drop column col1;
drop table drop01;

-- drop column: primary key
drop table if exists T1;
create table t1(id int PRIMARY KEY,name VARCHAR(255),age int);
insert into t1 values(1,"Abby", 24);
insert into t1 values(2,"Bob", 25);
insert into t1 values(3,"Carol", 23);
insert into t1 values(4,"Dora", 29);
create unique index Idx on t1(name);
alter table t1 drop column id;
select * from t1;
show create table t1;
drop table t1;

-- drop column: index
drop table if exists index01;
create table index01(id int,name varchar(20),unique index(id));
insert into index01 values(1,'323414');
alter table index01 drop column id;
show create table index01;
drop table index01;

drop table if exists index02;
create table index02(col1 int,col2 varchar(20),col3 char(20), index(col1,col2));
alter table index02 drop column col1;
alter table index02 drop column col2;
show create table index02;
drop table index02;

drop table if exists index03;
create table index03(col1 int,col2 binary(10),col3 text,unique key(col2));
alter table index03 drop column col2;
show create table index03;
drop table index03;


-- cluster by
drop table if exists cluster01;
create table cluster01(a int, b int, c varchar(10)) cluster by(a,b,c);
alter table cluster01 add column col1 int;
alter table cluster01 drop column c;
alter table cluster01 drop column a;
alter table cluster01 drop column b;
drop table cluster01;

-- tables with foreign keys cannot add or subtract columns
drop table if exists foreign01;
create table foreign01(col1 int primary key,
                       col2 varchar(20),
                       col3 int,
                       col4 bigint);
drop table if exists foreign02;
create table foreign02(col1 int,
                       col2 int,
                       col3 int primary key,
                       foreign key(col1) references foreign01(col1));
show create table foreign01;
show create table foreign02;
insert into foreign01 values(1,'sfhuwe',1,1);
insert into foreign01 values(2,'37829901k3d',2,2);
insert into foreign02 values(1,1,1);
insert into foreign02 values(2,2,2);
select * from foreign01;
select * from foreign02;
alter table foreign01 drop column col2;
alter table foreign02 drop column col2;
alter table foreign01 add column col4 int first;
alter table foreign02 add column col4 int;
select * from foreign01;
select * from foreign02;
drop table foreign02;
drop table foreign01;


-- add column first,after
drop table if exists test01;
create table test01(col1 int,col2 char);
insert into test01 values(1,'a');
insert into test01 values(2,'c');
alter table test01 add column col3 text first;
alter table test01 add column col4 binary after col2;
alter table test01 drop column col1;
show create table test01;
select * from test01;
drop table test01;


-- partitioned tables do not support column deletion,support column add
drop table if exists tp5;
create table tp5 (col1 INT, col2 CHAR(5), col3 DATE) partition by LINEAR key ALGORITHM = 1 (col3) PARTITIONS 5;
show create table tp5;
alter table tp5 drop column col1;
alter table tp5 add column col4 int;
drop table tp5;

-- rename table
-- update and delete
drop table if exists update01;
create table update01(col1 int, col2 int, col3 varchar(20));
insert into update01 values(1,2,'cfewquier');
insert into update01 values(2,3,'329382');
select * from update01;
alter table update01 add column col1_2 binary after col1;
alter table update01 add column col5 blob after col3;
select * from update01;
show create table update01;
-- @bvt:issue#10093
insert into update01 values(1,'1',3,'ew83u829d3qcefq','q3829ff2e3qe');
insert into update01 values(2,'3',6,'3214()_)_)','00');
select * from update01;

update update01 set col1 = 100 where col1 = 1;
update update01 set col5 = '2798u3d3frew' where col2 = 6;
delete from update01 where col1_2 is null;
drop table update01;
-- @bvt:issue

-- alter table rename
drop table if exists rename01;
create table rename01(a int,b int);
insert into rename01 values(1,1);
alter table rename01 rename to rename02;
select * from rename01;
select * from rename02;
insert into rename02 values(2,2);
update rename02 set a = 10 where a = 1;
delete from rename02 where a = 10;
create view view01 as select * from rename02;
truncate table rename02;
drop table rename02;

drop table if exists rename02;
drop table if exists rename03;
drop table if exists rename04;
create table rename02(a int primary key,b varchar(20));
create table rename03(col1 int,col2 char);
create table rename04(col1 binary,col2 text);
alter table rename02 rename to rename_02;
alter table rename03 rename to rename_03;
alter table rename04 rename to rename04;
show create table rename_02;
show create table rename_03;
show create table rename04;
drop table rename_02;
drop table rename_03;
drop table rename04;

-- consecutive rename multiple times
drop table if exists rename05;
create table rename05(col1 int,col2 text);
insert into rename05 values(1,'jfhwuief3');
insert into rename05 values(2,'ew8uif4324f');
alter table rename05 rename to rename_05;
select * from rename05;
select * from rename_05;
alter table rename_05 rename to rename05;
select * from rename05;
select * from rename_05;
drop table rename05;

-- error:wrong table name
drop table if exists rename06;
create table rename06(col1 int);
insert into rename06 values(1),(2);
alter table rename06 rename to '';
drop table rename06;

-- check if special characters work and duplicates are detected.
drop table if exists `t+1`;
drop table if exists `t+2`;
create table `t+1` (c1 INT);
alter table  `t+1` rename to `t+2`;
create table `t+1` (c1 INT);
alter table  `t+1` rename to `t+2`;
drop table `t+1`;
drop table `t+2`;

-- check if special characters as in tmp_file_prefix work
drop table if exists `#sql1`;
drop table if exists `@0023sql2`;
create table `#sql1` (c1 INT);
create table `@0023sql2` (c1 INT);
alter table `#sql1` rename to `@0023sql1`;
show create table `@0023sql1`;
alter table `@0023sql2` rename to `#sql2`;
show create table `#sql2`;
alter table `@0023sql1` rename to `#sql-1`;
alter table `#sql2` rename to `@0023sql-2`;
show create table `#sql-1`;
show create table `@0023sql-2`;
insert into `#sql-1` values (1);
insert into `@0023sql-2` values (2);
select * from `#sql-1`;
select * from `@0023sql-2`;
drop table `#sql-1`;
drop table `@0023sql-2`;

-- based on an existing table name
drop table if exists test03;
create table test03(col1 int);
insert into test03 values(100);
alter table test03 rename to test03;
drop table test03;

-- alter table add comment
drop table if exists test02;
create table test02(a int);
alter table test02 comment = "comment_1";
show create table test02;
alter table test02 comment = "comment_2", comment = "comment_3";
show create table test02;
drop table test02;

-- alter table and rename
drop table if exists t1;
create table t1 (i int unsigned not null auto_increment primary key);
alter table t1 rename to t2;
alter table t2 rename to t1;
alter table t1 add column c char(10);
alter table t1 comment = "no comment";
show create table t1;
alter table t1 comment = 'this is a comment';
show create table t1;
drop table t1;

drop database test;