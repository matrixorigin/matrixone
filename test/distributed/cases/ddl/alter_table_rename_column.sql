-- @suit
-- @case
-- @desc: alter table rename column
-- @label:bvt
drop database if exists test;
create database test;
use test;

-- rename column name: the same column name in the table
drop table if exists samecolumn01;
create table samecolumn01 (col1 int, col2 char);
alter table samecolumn01 rename column col1 to newColumn;
alter table samecolumn01 rename column col2 to newcolumn;
show create table samecolumn01;
show columns from samecolumn01;
drop table samecolumn01;

-- rename column in empty table
drop table if exists rename01;
create table rename01 (col1 int, col2 decimal);
alter table rename01 rename column col1 to col1New;
show create table rename01;
show columns from rename01;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'name01' and COLUMN_NAME not like '__mo%';
drop table rename01;


-- rename the column name to the same as before
drop table if exists rename02;
create table rename02 (`colcolcol1` int, `colcolcol2` binary);
insert into rename02 values (1, '2');
insert into rename02 values (2, 'g');
alter table rename02 rename column `colcolcol1` to `colcolcol1`;
show create table rename02;
insert into rename02 (colcolcol1, colcolcol2) values (3, '7');
delete from rename02 where colcolcol1 = 1;
update rename02 set colcolcol2 = '&' where colcolcol1 = 2;
select * from rename02;
show columns from rename02;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'rename02' and COLUMN_NAME not like '__mo%';
drop table rename02;


-- rename column then update and delete
drop table if exists rename03;
create table rename03(col1 int, col2 int, col3 varchar(20));
insert into rename03 values (1,2,'cfewquier');
insert into rename03 values (2,3,'329382');
insert into rename03 values (3, 10, null);
select * from rename03;
alter table rename03 rename column col1 to col1New;
alter table rename03 rename column col3 to col3New;
show create table rename03;
insert into rename03 (col1, col2, col3) values (3,4,'121131312');
insert into rename03 (col1New, col2, col3New) values (3,4,'121131312');
select * from rename03;
update rename03 set col1New = 100 where col1New = 1;
update rename03 set col3New = '2798u3d3frew' where col3New = '6';
delete from rename03 where col3New is null;
select * from rename03;
show columns from rename03;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'rename03' and COLUMN_NAME not like '__mo%';
drop table rename03;


-- alter table rename column multi times
drop table if exists rename04;
create table rename04(a int,b int);
insert into rename04 values(1,1);
alter table rename04 rename column a to newA;
show create table rename04;
update rename04 set newA = 100 where b = 1;
select * from rename04;
alter table rename04 rename column newA to newnewA;
show create table rename04;
insert into rename04 values (1, 3);
insert into rename04 values (1289,232);
update rename04 set a = 10000 where b = 1;
update rename04 set newnewA = 10000 where b = 1;
select * from rename04;
delet from rename04 where newnewa = 10000;
select * from rename04;
show columns from rename04;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'rename04' and COLUMN_NAME not like '__mo%';
drop table rename04;


-- error: abnormal column name
drop table if exists rename06;
create table rename06(col1 int);
insert into rename06 values(1),(2);
alter table rename06 rename column col1 to '';
alter table rename06 rename column col1 to ' ';
alter table rename06 rename column col1 to 数据库系统;
alter table rename06 rename column col1 to 7327323467dhhjfkrnfe;
alter table rename06 rename column col1 to **&&^^%%^&**;
drop table rename06;


-- rename column with ``
drop table if exists rename06;
create table rename06(col1 int);
insert into rename06 values(1),(2);
alter table rename06 rename column col1 to `数据库系统`;
alter table rename06 rename column col1 to `数据操作，数据收集7327323467dhhjfkrnfe`;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'rename06' and COLUMN_NAME not like '__mo%';
show columns from rename06;
drop table rename06;

-- rename internal table column: have no privilege
alter table system.statement_info rename column role_id to role_idNew;
alter table mo_catalog.mo_database rename column dat_type to newdat_type;
alter table mysql.procs_priv rename column grantor to newGrantor;


-- rename primary key column
drop table if exists primary01;
create table primary01 (col1 int primary key , col2 decimal);
insert into primary01 values (2389324, 32784329.4309403);
insert into primary01 values (3287, 89384038);
alter table primary01 rename column col1 to col1New;
show create table primary01;
insert into primary01 values (-2839, 8239802839.00000000);
insert into primary01 (col1New, col2) values (3287, 3293892.3232);
delete from primary01 where col1New = -2839;
update primary01 set col1 = 2873892 where col1New = 2389324;
update primary01 set col1New = 2873892 where col1New = 2389324;
select * from primary01;
show columns from primary01;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'primary01' and COLUMN_NAME not like '__mo%';
drop table primary01;


-- rename foreign key column
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
alter table foreign01 rename column col1 to col1New;
alter table foreign02 rename column col1 to `Colnewcolumn`;
show create table foreign01;
show create table foreign02;
alter table foreign01 change col2 col2New varchar(100);
alter table foreign02 change col2 col2new double after col3;
insert into foreign01 values(3,'bcguwgheinwqneku678',2,2);
insert into foreign02 values(6,6,6);
delete from foreign01 where col2New = 'sfhuwe';
delete from foreign02 where col2New = 2;
update foreign01 set col2 = 'database ewueh ' where col1 = 1;
update foreign01 set col1new = 9283923 where col1new = 1;
select * from foreign01;
select * from foreign02;
show create table foreign01;
show create table foreign02;
show columns from foreign01;
show columns from foreign02;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'foreign01' and COLUMN_NAME not like '__mo%';
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'foreign02' and COLUMN_NAME not like '__mo%';
drop table foreign02;
drop table foreign01;


-- unique key
drop table if exists index01;
CREATE TABLE index01(a INTEGER not null , b CHAR(10), c date, d decimal(7,2), UNIQUE KEY(a, b));
show create table index01;
insert into index01 values(1, 'ab', '1980-12-17', 800);
insert into index01 values(2, 'ac', '1981-02-20', 1600);
insert into index01 values(3, 'ad', '1981-02-22', 500);
select * from index01;
alter table index01 rename column b to bNew;
show create table index01;
show index from index01;
insert into index01 (a, b, c, d) values (5, 'bh', '1999-01-01', 3000);
insert into index01 (a, bnew, c, d) values (5, 'bh', '1999-01-01', 3000);
select * from index01;
delete from index01 where b = 'ab';
delete from index01 where bneW = 'ab';
select * from index01;
update index01 set c = '2022-12-12' where bNew = 'ac';
select * from index01;
show columns from index01;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'index01' and COLUMN_NAME not like '__mo%';
drop table index01;


-- index
drop table if exists index02;
CREATE TABLE index02(a INTEGER PRIMARY KEY, b CHAR(10), c date, d decimal(7,2), INDEX(a, b), KEY(c));
insert into index02 values(1, 'ab', '1980-12-17', 800);
insert into index02 values(2, 'ac', '1981-02-20', 1600);
insert into index02 values(3, 'ad', '1981-02-22', 500);
select * from index02;
alter table index02 rename column b to bNewNew;
show create table index02;
insert into index02 values (4, 'ab', '2000-10-10', 10000);
insert into index02 values (5, 'gh', '1999-12-31', 20000);
delete from index02 where bnewnew = 'ab';
update index02 set bnewnew = 'database' where bnewnEW = 'ad';
select * from index02;
show index from index02;
show columns from index02;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'index02' and COLUMN_NAME not like '__mo%';
select * from index02;


-- rename cluster by table column
drop table if exists cluster01;
create table cluster01(a tinyint primary key, b smallint signed, c int unsigned,d bigint not null);
insert into cluster01 (a, b, c, d) values (1, 255, 438, 7328832832);
alter table cluster01 rename column a to `NewA`;
alter table cluster01 rename column `newa` to `NewAAAAAAAA`;
show create table cluster01;
insert into cluster01 (a, b, c, d) values (-32, 32832, 8329, 893434);
insert into cluster01 (NewAAAAAAAA, b, c, d) values (-32, 32, 8329, 893434);
select * from cluster01;
show columns from cluster01;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'cluster01' and COLUMN_NAME not like '__mo%';
drop table cluster01;


-- rename temporary table column
drop table if exists temporary01;
create table temporary01 (col1 int, col2 decimal);
insert into temporary01 (col1, col2) values (3728937, 37283.3232);
alter table temporary01 rename column col1 to `UUUYGGBBJBJ`;
insert into temporary01 (col1, col2) values (-32893, -89232);
insert into temporary01 (`UUUYGGBBJBJ`, col2) values (-32893, -89232);
select * from temporary01;
show columns from temporary01;
select table_name,COLUMN_NAME, data_type,is_nullable from information_schema.columns where table_name like 'temporary01' and COLUMN_NAME not like '__mo%';
drop table temporary01;


-- rename external table column
drop table if exists ex_table_2_1;
create external table ex_table_2_1(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19))infile{"filepath"='$resources/external_table_file/ex_table_2_1.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
alter table ex_table_2_1 rename column num_col1 to newnum_col1;
alter table ex_table_2_1 rename column num_col2 to `shurhenwjkrferveg`;
show create table ex_table_2_1;
select * from ex_table_2_1;


-- creating table, creating view, renaming the columns, view the view
drop table if exists view01;
drop table if exists view02;
drop view if exists v0;
create table view01 (a int);
insert into view01 values (1),(2);
create table view02 (a int);
insert into view02 values (1);
alter table view01 rename column a to `cwhuenwjfdwcweffcfwef`;
alter table view02 rename column a to `cwhuenwjfdwcweffcfwef`;
show columns from view01;
show columns from view02;
show create table view01;
show create table view02;
create view v0 as select view01.a, view02.a as b from view01 left join view02 using(a);
create view v0 as select view01.cwhuenwjfdwcweffcfwef, view02.cwhuenwjfdwcweffcfwef as b from view01 left join view02 using(cwhuenwjfdwcweffcfwef);
show create view v0;
drop table view01;
drop table view02;

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
alter table rename01 rename column col1 to newCol1;
-- @session
grant alter table on database * to role_r1;

-- @session:id=2&user=sys:role_u1:role_r1&password=111
use test;
alter table rename01 rename column col1 to newRename;
alter table rename01 rename column newRename to `newNewRename`;
show create table rename01;
show columns from rename01;
-- @session
drop table rename01;
drop role role_r1;
drop user role_u1;
-- rename column

-- prepare
drop table if exists prepare01;
create table prepare01(col1 int, col2 char);
insert into prepare01 values (1,'a'),(2,'b'),(3,'c');
prepare s1 from 'alter table prepare01 rename column col1 to col1dheuwhvcer';
execute s1;
show create table prepare01;
prepare s2 from 'alter table prepare01 rename column col1dheuwhvcer to col1';
execute s2;
show create table prepare01;
show columns from prepare01;
update prepare01 set col1 = 2147483647 where col2 = 'c';
delete from prepare01 where col2 = 'b';
insert into prepare01 values (42342, '3');
select * from prepare01;
drop table prepare01;

-- begin, alter table rename, commit, then select
drop table if exists table03;
begin;
create table table03(col1 int, col2 char);
alter table table03 rename to NewCol1;
commit;
select * from NewCol1;
select col1 from NewCol1;
drop table NewCol1;
drop database test;

