-- @suit
-- @case
-- @desc:alter table add/drop foreign key,alter table add/drop index
-- @label:bvt
create database test;
use test;

-- alter table drop foreign key
drop table if exists foreign01;
create table foreign01(col1 int primary key,
                       col2 varchar(20),
                       col3 int,
                       col4 bigint);
insert into foreign01 values(1,'shujuku',100,3247984);
insert into foreign01 values(2,'数据库',328932,32324423432);
drop table if exists foreign02;
create table foreign02(col1 int,
                       col2 int,
                       col3 int primary key,
                       constraint fk foreign key fk(col1) references foreign01(col1));
insert into foreign02 values(1,1,1);
insert into foreign02 values(2,2,2);
delete from foreign01 where col3 = 100;

show create table foreign02;
alter table foreign02 drop foreign key fk;
show create table foreign02;
drop table foreign01;
drop table foreign02;

-- alter table add/drop foreign key
drop table if exists ti1;
drop table if exists tm1;
drop table if exists ti2;
drop table if exists tm2;

create  table ti1(a INT not null, b INT, c INT);
create  table tm1(a INT not null, b INT, c INT);
create  table ti2(a INT primary key AUTO_INCREMENT, b INT, c INT);
create  table tm2(a INT primary key AUTO_INCREMENT, b INT, c INT);
insert into ti1 values (1,1,1), (2,2,2);
insert into ti2 values (1,1,1), (2,2,2);
insert into tm1 values (1,1,1), (2,2,2);
insert into tm2 values (1,1,1), (2,2,2);

alter table ti1 add constraint fi1 foreign key (b) references ti2(a);
alter table tm1 add constraint fm1 foreign key (b) references tm2(a);
show create table ti1;
show create table tm1;

delete from ti2 where c = 1;
delete from tm2 where c = 1;

alter table ti1 drop foreign key fi1;
alter table tm1 drop foreign key fm1;

show create table ti1;
show create table tm1;

delete from ti2 where c = 1;
delete from tm2 where c = 1;

drop table  ti1;
drop table  tm1;
drop table  ti2;
drop table  tm2;

-- alter table add/drop foreign key in temporary table
-- @bvt:issue#9282
drop table if exists foreign01;
create temporary table foreign01(col1 int primary key,
					 col2 varchar(20),
					 col3 int,
					 col4 bigint);
insert into foreign01 values(1,'shujuku',100,3247984);
insert into foreign01 values(2,'数据库',328932,32324423432);
drop table if exists foreign02;
create temporary table foreign02(col1 int,
					 col2 int,
					 col3 int primary key,
					 constraint fk foreign key fk(col1) references foreign01(col1));
insert into foreign02 values(1,1,1);
insert into foreign02 values(2,2,2);
delete from foreign01 where col3 = 100;

show create table foreign02;
alter table foreign02 drop foreign key fk;
show create table foreign02;

drop table foreign01;
drop table foreign02;

-- alter table add/drop foreign key
drop table if exists ti1;
drop table if exists tm1;
drop table if exists ti2;
drop table if exists tm2;

create temporary table ti1(a INT not null, b INT, c INT);
create temporary table tm1(a INT not null, b INT, c INT);
create temporary table ti2(a INT primary key AUTO_INCREMENT, b INT, c INT);
create temporary table tm2(a INT primary key AUTO_INCREMENT, b INT, c INT);
insert into ti1 values (1,1,1), (2,2,2);
insert into ti2 values (1,1,1), (2,2,2);
insert into tm1 values (1,1,1), (2,2,2);
insert into tm2 values (1,1,1), (2,2,2);

alter table ti1 add constraint fi1 foreign key (b) references ti2(a);
alter table tm1 add constraint fm1 foreign key (b) references tm2(a);
show create table ti1;
show create table tm1;

delete from ti2 where c = 1;
delete from tm2 where c = 1;

alter table ti1 drop foreign key fi1;
alter table tm1 drop foreign key fm1;

show create table ti1;
show create table tm1;

delete from ti2 where c = 1;
delete from tm2 where c = 1;

drop table  ti1;
drop table  tm1;
drop table  ti2;
drop table  tm2;
-- @bvt:issue

-- alter table add index
drop table if exists index01;
create table index01(col1 int,key key1(col1));
show index from index01;
alter table index01 drop index key1;
show index from index01;
alter table Index01 add index key1(col1) comment 'test';
show index from index01;
alter table index01 alter index key1 invisible;
show index from index01;
drop table index01;

drop table if exists index02;
create table index02(col1 int,key key1(col1) comment 'test');
alter table index02 drop index key1;
show index from index02;
alter table index02 add index Key1(col1);
alter table index02 alter index key1 invisible;
show index from index02;
drop table index02;

drop table if exists index03;
create table index03(col1 int, col2 int, col3 int);
alter table index03 add unique key(col1,col2) comment 'abcTest';
show index from index03;
alter table index03 alter index col1 invisible;
show index from index03;
drop table index03;

drop table if exists index03;
create table index03(fld1 int, key key1(fld1) comment 'test');
show index from index03;
alter table index03 drop index key1;
show index from index03;
drop table index03;

drop table if exists index04;
create table index04(col1 int, col2 char, col3 varchar(10),primary key(col1,col2));
show index from index04;
alter table index04 add index(col1);
alter table index04 alter index col1 invisible;
show index from index04;
alter table index04 alter index col1 visible;
show index from index04;
drop table index04;

drop table if exists index05;
create table index05(col1 int, col2 bigint, col3 decimal);
show index from index05;
alter table index05 add unique key(col1,col2);
show index from index05;
alter table index05 alter index col1 invisible;
show index from index05;
drop table index05;

drop table if exists index06;
create table index06(col1 int not null,col2 binary, col3 float,unique key(col1));
alter table index06 add unique index(col2);
show index from index06;
alter table index06 alter index col2 invisible;
show index from index06;
drop table index06;

drop table if exists index07;
CREATE TABLE index07(
                        col1 INT NOT NULL,
                        col2 DATE NOT NULL,
                        col3 VARCHAR(16) NOT NULL,
                        col4 int unsigned NOT NULL,
                        PRIMARY KEY(col1)
);

insert into index07 values(1, '1980-12-17','Abby', 21);
insert into index07 values(2, '1981-02-20','Bob', 22);
insert into index07 values(3, '1981-02-22','Carol', 23);

alter table index07 add constraint unique key (col3, col4);
alter table index07 add constraint unique key wwwww (col3, col4);
alter table index07 add constraint abctestabbc unique key zxxxxx (col3);
show index from index07;
alter table index07 add unique key idx1(col3);
show index from index07;
alter table index07 add constraint idx2 unique key (col3);
alter table index07 add constraint idx2 unique key (col4);
alter table index07 alter index wwwww invisible;
show index from index07;
drop table index07;

drop table if exists index08;
CREATE TABLE index08(
                        col1 INT NOT NULL,
                        col2 DATE NOT NULL,
                        col3 VARCHAR(16) NOT NULL,
                        col4 int unsigned NOT NULL,
                        PRIMARY KEY(col1)
);

insert into index08 values(1, '1980-12-17','Abby', 21);
insert into index08 values(2, '1981-02-20','Bob', 22);

alter table index08 add constraint unique index (col3, col4);
alter table index08 add constraint index wwwww (col3, col4);
alter table index08 add constraint unique index zxxxxx (col3);
show index from index08;
alter table index08 add index zxxxx(col3);
show index from index08;
drop table index08;

drop table if exists index09;
CREATE TABLE index09(
                        col1 INT NOT NULL,
                        col2 DATE NOT NULL,
                        col3 VARCHAR(16) NOT NULL,
                        col4 int unsigned NOT NULL,
                        UNIQUE KEY u1 (col1 DESC)
);

insert into index09 values(1, '1980-12-17','Abby', 21);
insert into index09 values(2, '1981-02-20','Bob', 22);
show index from index09;
ALTER TABLE emp ADD UNIQUE INDEX idx1 (col1 ASC, col2 DESC);
show index from index09;
drop table index09;

drop table if exists index10;
CREATE TABLE index10(
                        col1 INT NOT NULL,
                        col2 DATE NOT NULL,
                        col3 VARCHAR(16) NOT NULL,
                        col4 int unsigned NOT NULL,
                        INDEX idx1 (col1 DESC),
                        KEY idx2 (col2 DESC)
);

insert into index10 values(1, '1980-12-17','Abby', 21);
insert into index10 values(2, '1981-02-20','Bob', 22);
show index from index10;
ALTER TABLE emp ADD INDEX idx3 (col1 ASC, col2 DESC);
show index from index10;
drop table index10;

CREATE TABLE `t2` (
`a` INT DEFAULT NULL
) COMMENT='New table comment';

alter table t2 drop primary key;
alter table t2 AUTO_INCREMENT=10;
alter table t2 disable keys;

CREATE TABLE `t3` (
`a` INT NOT NULL,
PRIMARY KEY (`a`)
);
alter table t3 drop primary key;

-- duplicate values add a unique key
drop table if exists index03;
create table index03(col1 int, col2 int, col3 int);
insert into index03 values (1,2,1);
insert into index03 values (2,3,4);
insert into index03 values (1,2,10);
select count(*) from index03;
alter table index03 add unique key `tempKey`(col1,col2) comment 'abcTest';
show index from index03;
select count(*) from index03;
drop table index03;

-- varchar column add unique key
drop table if exists t4;
create table t4(col1 varchar(256) primary key, col2 int);
insert into t4 select "matrixone " || "mo " || result, 1 from generate_series (1, 500000)g;
select count(*) from t4;
alter table t4 add unique key `title01`(`col1`);
show create table t4;
select count(*) from t4;
drop table t4;

-- add unique key with parser 'using btree'
drop table if exists t5;
create table t5(col1 varchar(256) primary key, col2 int);
insert into t5 select "matrixone " || "mo " || result, 1 from generate_series (1, 500000)g;
select count(*) from t5;
alter table t5 add unique key `title01`(`col1`) using btree;
show create table t5;
select count(*) from t5;
drop table t5;

drop database test;

drop database if exists `collate`;
create database `collate`;
use `collate`;
drop table if exists t6;
create table t6(col1 int, col2 int, col3 int);
insert into t6 values (1,2,1);
alter table t6 add unique key tempKey(col1,col2) comment 'abcTest';
drop table t6;
drop database `collate`;

drop database if exists `current_time`;
create database `current_time`;
use `current_time`;
drop table if exists t7;
create table t7(col1 int, col2 int, col3 int);
insert into t7 values (1,2,1);
alter table t7 add unique index tempKey(col1,col2) comment 'unique index';
drop table t7;
drop database `current_time`;

drop database if exists `drop`;
create database `drop`;
use `drop`;
drop table if exists t8;
create table t8(col1 char, col2 int, col3 binary);
insert into t8 values('a', 33, 1);
insert into t8 values('c', 231, 0);
alter table t8 add key pk(col1) comment 'primary key';
select * from t8;
drop table t8;
drop database `drop`;




-- set lower_case_table_names = 0, alter table then show create table
-- the result of show create table is case sensitive
set global lower_case_table_names = 0;
drop database if exists test;
create database test;
use test;
drop table if exists test01;
create table test01 (col1 int, col2 decimal, col3 varchar(50));
insert into test01 values (1, 3242434.423, '3224332r32r');
insert into test01 values (2, 39304.3424, '343234343213124');
insert into test01 values (3, 372.324, '00');

alter table test01 rename column col1 to newCol;
show create table test01;
drop database test;
set global lower_case_table_names = 1;




drop database if exists test01;
create database test01;
use test01;
drop table if exists s3t;
create table s3t (a int, b int, c int, primary key(a, b));
insert into s3t select result, 2, 12 from generate_series(1, 30000, 1) g;
alter table s3t modify column b bigint;
show create table s3t;
drop database test01;




drop database if exists test;
create database test;
use test;
begin;
drop table if exists s3t;
create table s3t (a int, b int, c int, primary key(a, b));
insert into s3t select result, 2, 12 from generate_series(1, 30000, 1) g;
delete from s3t where a < 1000;
commit;
drop database test;


-- Functional test for varchar column length modification
drop database if exists varchar_test;
create database varchar_test;
use varchar_test;

-- Test 1: Simple varchar length increase
create table t1 (
    id int primary key,
    name varchar(20),
    email varchar(50)
);

insert into t1 values (1, 'Alice', 'alice@example.com');
insert into t1 values (2, 'Bob', 'bob@example.com');

-- Should use INPLACE algorithm for simple length increase
alter table t1 modify column name varchar(80);
alter table t1 modify column email varchar(150);

select * from t1;
show create table t1;

-- Test 2: Varchar length increase with NOT NULL constraint
create table t2 (
    id int primary key,
    name varchar(20) not null,
    description varchar(100)
);

insert into t2 values (1, 'Test1', 'Description1');
insert into t2 values (2, 'Test2', 'Description2');

-- Should use INPLACE algorithm for length increase with consistent NOT NULL
alter table t2 modify column name varchar(80) not null;
alter table t2 modify column description varchar(200);

select * from t2;
show create table t2;

-- Test 3: Multiple varchar columns modification in single statement
create table t3 (
    id int primary key,
    col1 varchar(10),
    col2 varchar(20) not null,
    col3 varchar(30)
);

insert into t3 values (1, 'a', 'bb', 'ccc');
insert into t3 values (2, 'aa', 'bbbb', 'cccccc');

-- Should use INPLACE algorithm for multiple varchar length increases
alter table t3 
modify column col1 varchar(50),
modify column col2 varchar(100) not null,
modify column col3 varchar(150);

select * from t3;
show create table t3;

-- Test 4: Varchar with other constraints (should still work with relaxed checks)
create table t4 (
    id int primary key,
    name varchar(20) not null,
    email varchar(50),
    status varchar(10) default 'active'
);

insert into t4 (id, name, email) values (1, 'User1', 'user1@test.com');
insert into t4 (id, name, email, status) values (2, 'User2', 'user2@test.com', 'inactive');

-- Should handle varchar length increase with NOT NULL constraint
alter table t4 modify column name varchar(100) not null;
alter table t4 modify column email varchar(200);

select * from t4;
show create table t4;

-- Test 5: Large varchar length increase
create table t5 (
    id int primary key,
    content varchar(100)
);

insert into t5 values (1, 'Short content');
insert into t5 values (2, 'This is a longer content that tests the varchar modification functionality');

-- Should handle large length increases
alter table t5 modify column content varchar(1000);

select * from t5;
show create table t5;

begin;

alter table t5 rename column content to new_content;

-- @separator:table
select mo_ctl('dn', 'flush', 'mo_catalog.mo_columns');

select sleep(0.51);

commit;

show create table t5;


set experimental_fulltext_index=1;
create table v1 (a int primary key, b int);

insert into v1 values (1, 1), (2, 1);

alter table v1 modify column b int unique key; -- dup, join dedup path

create table v2 (a int primary key, b int, c text, fulltext f01(c));

insert into v2 values (1, 1, "42"), (2, 1, "43");

alter table v2 modify column b int unique key; -- dup, fuzzy dedup path


create table v3 (a int primary key, b int, c text, unique index IdX2(b));

load data infile  '$resources/load_data/dup_load.csv' into table v3 fields terminated by ',';

insert into v3 values (1, 1, "boroborodesu"); -- dup
-- @pattern
insert into v3 values (10, 2, "boroborodesu"); -- dup

alter table v3 add column d int; -- no dup because we have skipped pk and unique index dedup

-- Cleanup
drop table t1;
drop table t2;
drop table t3;
drop table t4;
drop table t5;
drop table v1;
drop table v2;
drop table v3;
drop database varchar_test;

