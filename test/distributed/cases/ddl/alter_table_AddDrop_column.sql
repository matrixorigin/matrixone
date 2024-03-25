-- @suit
-- @case
-- @desc:alter table add/drop column,add comment
-- @label:bvt
drop database if exists test;
create database test;
use test;


-- add column:alter column of table with s3 blocks
drop table if exists s3t;
create table s3t (a int, b int, c int, primary key(a, b));
insert into s3t select result, 2, 12 from generate_series(1, 30000, 1) g;

alter table s3t add column d int after b;

insert into s3t values (300001, 34, 23, 1);
select count(*) from s3t;
select * from s3t where d = 23;

alter table s3t drop column c;

insert into s3t select result, 2, 12 from generate_series(30002, 60000, 1) g;
select count(d) from s3t;
select count(d) from s3t where d > 13;


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
alter table add01 add column col2_3 varchar(20) after col2;
alter table add01 add column col7 varchar(30) not null after col5;
alter table add01 add column col8 int not null;
alter table add01 add column col9 int not null first;
show create table add01;
insert into add01 values(1,3,'nihao','hei','hu','jj',2,'varchar',1);

insert into add01 values(2,3,'nihao',null,'hu','jj',2,'varchar',1);
insert into add01 values(3,4,'nihao','hi','hu','jj',2,'varchar',null);

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

-- add/drop columns multi times
drop table if exists drop02;
create table drop02(col1 int);
insert into drop02 values(1);
insert into drop02 values(2);
alter table drop02 add column col2 decimal(20,10);
alter table drop02 add column col3 char;
alter table drop02 add column col4 int unsigned;
select * from drop02;
alter table drop02 drop column col2;
alter table drop02 drop column col3;
alter table drop02 drop column col4;
alter table drop02 drop column col1;
drop table drop02;

-- alter table after truncate table
drop table if exists truncate01;
create table truncate01(col1 int,col2 decimal);
insert into truncate01 values(1,8934245);
insert into truncate01 values(2,-1924);
insert into truncate01 values(3,18294234);
truncate truncate01;
show create table truncate01;
select * from truncate01;
alter table truncate01 add column col3 int unsigned after col1 ;
alter table truncate01 add column colF binary first;
show create table truncate01;
alter table truncate01 drop column col3;
alter table truncate01 drop column col1;
show create table truncate01;
drop table truncate01;

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
                       constraint `c1` foreign key(col1) references foreign01(col1));
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

-- permission
drop role if exists role_r1;
drop user if exists role_u1;
create role role_r1;
create user role_u1 identified by '111' default role role_r1;
drop table if exists test01(col1 int);
insert into test01 values(1);
insert into test01 values(2);
grant create database on account * to role_r1;
grant show databases on account * to role_r1;
grant connect on account * to role_r1;
grant select on table * to role_r1;
grant show tables on database * to role_r1;

-- @session:id=2&user=sys:role_u1:role_r1&password=111
use test;
alter table test01 add column col0 int first;
-- @session
grant alter table on database * to role_r1;

-- @session:id=2&user=sys:role_u1:role_r1&password=111
use test;
alter table test01 add column col0 int first;
alter table test01 add column col3 int unsigned after col1;
show create table test01;
alter table test01 drop column col3;
alter table test01 drop column col1;
-- @session
create table t(a int);
drop table test01;
drop role role_r1;
drop user role_u1;

-- transaction: automatic
drop table if exists transaction01;
create table transaction01 (c int primary key,d int);
begin;
insert into transaction01 values(1,1);
insert into transaction01 values(2,2);
alter table transaction01 add column colf int first;
rollback;
show create table transaction01;
drop table transaction01;

-- transaction: isolation
drop table if exists transaction03;
create table transaction03 (c int primary key,d int);
insert into transaction03 values(1,1);
insert into transaction03 values(2,2);
begin;
insert into transaction03 values(3,1);
insert into transaction03 values(4,2);
alter table transaction03 add column decimal after c;
show create table transaction03;
-- @session:id=1{
use isolation;
show create table transaction03;
-- @session}
commit;
-- @session:id=1{
alter table transaction03 drop column d;
show create table transaction03;
-- @session}
drop table transaction03;

-- transcation: w-w conflict
drop table if exists transaction05;
create table transaction05(a int not null auto_increment,b varchar(25) not null,c datetime,primary key(a),key bstr (b),key cdate (c) );
insert into transaction05(b,c) values ('aaaa','2020-09-08');
insert into transaction05(b,c) values ('aaaa','2020-09-08');

begin;
alter table transaction05 rename to `conflict_test`;

-- @session:id=1{
use ww_conflict;
begin;
alter table conflict_test drop column b;
-- @session}
commit;
-- @session:id=1{
alter table conflict_test add column colf int first;
-- @session}
select * from conflict_test;
show create table conflict_test;
drop table conflict_test;

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
insert into update01 values(1,'1',3,'ew83u829d3qcefq','q3829ff2e3qe');
insert into update01 values(2,'3',6,'3214()_)_)','00');
select * from update01;

update update01 set col1 = 100 where col1 = 1;
update update01 set col5 = '2798u3d3frew' where col2 = 6;
delete from update01 where col1_2 is null;
drop table update01;

-- alter table rename
drop table if exists rename01;
drop table if exists rename02;
create table rename01(a int,b int);
insert into rename01 values(1,1);
alter table rename01 rename to rename02;
select * from rename01;
select * from rename02;
insert into rename02 values(2,2);
select * from rename02;
update rename02 set a = 10 where a = 1;
select * from rename02;
delete from rename02 where a = 10;
select * from rename02;
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

-- rename internal table name: have no privilege
alter table system.statement_info rename to statement_info01;
alter table mo_catalog.mo_account rename to mo_account01;
alter table mysql.procs_priv rename to `procs_priv01`;

-- transaction: automatic
drop table if exists transaction01;
create table transaction01 (c int primary key,d int);
begin;
insert into test_11 values(1,1);
insert into test_11 values(2,2);
alter table transaction01 rename to `test_transaction`;
rollback;
select * from transaction01;
select * from test_transaction;

drop table test_transaction;

-- transaction: isolation
drop table if exists transaction03;
create table transaction03 (c int primary key,d int);
insert into transaction03 values(1,1);
insert into transaction03 values(2,2);
begin;
insert into transaction03 values(3,1);
insert into transaction03 values(4,2);
alter table transaction03 rename to `transaction04`;
select * from transaction04;

-- @session:id=1{
use isolation;
select * from transaction04;
-- @session}
commit;

select * from transaction04;
drop table transaction04;

-- transcation: w-w conflict
drop table if exists transaction05;
create table transaction05(a int not null auto_increment,b varchar(25) not null,c datetime,primary key(a),key bstr (b),key cdate (c) );
insert into transaction05(b,c) values ('aaaa','2020-09-08');
insert into transaction05(b,c) values ('aaaa','2020-09-08');

begin;
alter table transaction05 rename to `conflict_test`;

-- @session:id=1{
use ww_conflict;
begin;
alter table conflict_test add column colf int first;
-- @session}
commit;
-- @session:id=1{
alter table conflict_test add column colf int first;
-- @session}
select * from conflict_test;
show create table conflict_test;
drop table conflict_test;

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

-- alter table add comment ''
drop table if exists test03;
create table test03(a int);
alter table test03 comment = '';
show create table test03;
alter table test03 comment = "comment_2", comment = "comment_3";
show create table test03;
drop table test03;

-- alter table add comment like Chinese, number，long characters
drop table if exists test04;
create table test04(a int);
alter table test04 comment = '数据库Database！';
show create table test04;
alter table test04 comment = "3721  98479824309284093254324532";
show create table test04;
alter table test04 comment = "#$%^&*(%$R%TYGHJHUWHDIU^&W%^&WWsUIHFW&W数据库*&()()()__";
show create table test04;
alter table test04 comment = "47382749823409243f4oir32434",comment = "f73hjkrew473982u4f32g54jjUIHFW&W数据库*&()()()__";
show create table test04;
drop table test04;

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

-- permission
drop role if exists role_r1;
drop user if exists role_u1;
create role role_r1;
create user role_u1 identified by '111' default role role_r1;
drop table if exists rename01;
create table rename01(col1 int);
insert into rename01 values(1);
insert into rename01 values(2);
grant create database on account * to role_r1;
grant show databases on account * to role_r1;
grant connect on account * to role_r1;
grant select on table * to role_r1;
grant show tables on database * to role_r1;

-- @session:id=2&user=sys:role_u1:role_r1&password=111
use test;
alter table rename01 rename to newRename;
-- @session
grant alter table on database * to role_r1;

-- @session:id=2&user=sys:role_u1:role_r1&password=111
use test;
alter table rename01 rename to newRename;
alter table newRename rename to `newRename`;
show create table newRename;
-- @session
drop table newRename;
drop role role_r1;
drop user role_u1;

drop database test;
