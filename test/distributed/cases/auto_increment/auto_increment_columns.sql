
-- test auto_increment as primary key
drop table if exists t1;

create table t1(
a bigint primary key auto_increment,
b varchar(10)
);

show create table t1;

insert into t1(b) values ('bbb');

-- echo error msg: tae data: duplicate
insert into t1 values (1, 'ccc');

insert into t1 values (3, 'ccc');
insert into t1(b) values ('bbb1111');


select * from t1 order by a;

insert into t1 values (2, 'aaaa1111');
select * from t1 order by a;

insert into t1(b) values ('aaaa1111');

select * from t1 order by a;

insert into t1 values (100, 'xxxx');
insert into t1(b) values ('xxxx');

select * from t1 order by a;

insert into t1 values (0, 'xxxx');
insert into t1(b) values ('xxxx');

insert into t1 values (-1000, 'yyy');

select * from t1 order by a;

insert into t1 values ('-2000', 'yyy');
insert into t1 values ('200', 'yyy');

-- echo error msg;
insert into t1 values ('0', 'yyy');

select * from t1;

insert into t1 values (NULL, 'yyy');
select * from t1 order by a;

-- echo error
update t1 set a=0 where b='ccc';

update t1 set a='200' where b='ccc';

insert into t1(b) values ('xefrsdfgds');

select * from t1 order by a;

-- test bigint min value
insert into t1 values (-9223372036854775808,'xefrsdfgds');
-- echo error msg
insert into t1 values (-9223372036854775809,'xefrsdfgds');

-- test bigint max value
insert into t1 values (9223372036854775807,'xefrsdfgds');
-- echo error msg
insert into t1 values (9223372036854775808,'xefrsdfgds');
insert into t1(b) values ('eeeee');

drop table t1;


-- tet int max value and min value
drop table if exists t2;
create table t2 (
c int primary key auto_increment,
d varchar(10)
);

insert into t2 values (-2147483648, 'aaa');
select * from t2 order by c;
-- echo error
insert into t2 values (-2147483649, 'aaa');

insert into t2(d) values ('1111');
select * from t2 order by c;

insert into t2 values(2147483647, 'bbb');
-- echo error
insert into t2 values(2147483648, 'bbb');

insert into t2(d) values ('22222');
select * from t2 order by c;

drop table t2;


drop table if exists t3;
create table t3(
a int primary key auto_increment,
b varchar(10)
);

insert into t3 values (-19, 'aaa');
insert into t3(b) values ('bbb');
select * from t3 order by a;

delete from t3 where b='bbb';
insert into t3(b) values ('bbb');
select * from t3 order by a;

insert into t3 values (1, 'aaa');
-- error msg
update t3 set a=10 where b='aaa';
update t3 set a=10 where b='bbb';
select * from t3 order by a;

insert into t3 values (2,'ccc');
select * from t3 order by a;

delete from t3;
insert into t3(b) values ('bbb');
select * from t3 order by a;

drop table t3;

-- test auto_increment as not primary key
drop table if exists t4;

create table t4(
a bigint  auto_increment,
b varchar(10)
);

insert into t4(b) values ('bbb');

insert into t4 values (1, 'ccc');

insert into t4 values (3, 'ccc');
insert into t4(b) values ('bbb1111');

select * from t4 order by a;

insert into t4 values (2, 'aaaa1111');
select * from t4 order by a;

insert into t4(b) values ('aaaa1111');

select * from t4 order by a;

insert into t4 values (100, 'xxxx');
insert into t4(b) values ('xxxx');

select * from t4 order by a;

insert into t4 values (0, 'xxxx');
insert into t4(b) values ('xxxx');

insert into t4 values (-1000, 'yyy');

select * from t4 order by a;

insert into t4 values ('-2000', 'yyy');
insert into t4 values ('200', 'yyy');

-- echo error msg;
insert into t4 values ('0', 'yyy');

select * from t4 order by a;

insert into t4 values (NULL, 'yyy');
select * from t4 order by a;

-- echo error
update t4 set a=0 where b='ccc';

update t4 set a='200' where b='ccc';

insert into t4(b) values ('xefrsdfgds');

select * from t4 order by a;

-- test bigint min value
insert into t4 values (-9223372036854775808,'xefrsdfgds');
-- echo error msg
insert into t4 values (-9223372036854775809,'xefrsdfgds');

-- test bigint max value
insert into t4 values (9223372036854775807,'xefrsdfgds');
-- echo error msg
insert into t4 values (9223372036854775808,'xefrsdfgds');
insert into t4(b) values ('eeeee');

drop table t4;

-- test no primary key auto_increment columns

drop table if exists t5;
create table t5 (
c int auto_increment,
d varchar(10)
);

insert into t5 values (-2147483648, 'aaa');
select * from t5 order by c;
-- echo error
insert into t5 values (-2147483649, 'aaa');

insert into t5(d) values ('1111');
select * from t5 order by c;

insert into t5 values(2147483647, 'bbb');
-- echo error
insert into t5 values(2147483648, 'bbb');
select * from t5 order by c;

insert into t5(d) values ('22222');
select * from t5 order by c;

drop table t5;


-- test one table more auto_increment columns.

drop table if exists t6;
create table t6(
a int primary key auto_increment,
b bigint auto_increment,
c int auto_increment,
d int auto_increment,
e bigint auto_increment
);

show create table t6;

insert into t6 values (),(),(),();
select * from t6 order by a;

insert into t6 values (NULL, NULL, NULL, NULL, NULL);
select * from t6 order by a;

insert into t6(b,c,d) values (NULL,NULL,NULL);
select * from t6 order by a;

insert into t6(a,b) values (100, 400);
select * from t6 order by a;

insert into t6(c,d,e) values (200, 200, 200);
select * from t6;

insert into t6(c,d,e) values (200, 400, 600);
select * from t6;

-- echo error: duplicate
insert into t6(a,b) values (100, 400);
select * from t6 order by a;

insert into t6 values ('0','0','0','0','0');
select * from t6 order by a;

-- echo error
insert into t6 values ('a','a','a','a','a');
select * from t6 order by a;

insert into t6 values ('-1',0,0,0,0);
select * from t6 order by a;

drop table t6;

-- Test for the presence of autoincrement columns in multiple tables
drop table if exists t8;
create table t8(
a int auto_increment primary key,
b int auto_increment
);

drop table if exists t9;
create table t9(
c int auto_increment primary key,
d int auto_increment
);

insert into t8 values (),();
select * from t8 order by a;

insert into t9 values (),();
select * from t9 order by c;

insert into t8(a) values (19);
select * from t8 order by a;

insert into t9 (c) values (19);
select * from t9 order by c;

insert into t8 values (),();
select * from t8 order by a;

insert into t9 values (),();
select * from t9 order by c;

insert into t8(b) values (1);
select * from t8 order by a;

insert into t9 (d) values (1);
select * from t9 order by c;

-- echo error
insert into t8(a) values (1);
select * from t8 order by a;

insert into t9 (c) values (1);
select * from t9 order by c;

drop table t8;
drop table t9;

-- test truncate tabel,auto_increment columns whether it will be cleared.
drop table if exists t10;
create table t10(
a int auto_increment primary key,
b int auto_increment
);

insert into t10 values (10, 10);
insert into t10 values (),(),();
select * from t10 order by a;
truncate table t10;
insert into t10 values ();
select * from t10 order by a;

drop table t10;


-- test load data
drop table if exists t11;
create table t11(
a int primary key auto_increment,
b bigint auto_increment,
c varchar(25)
);

load data infile '$resources/auto_increment_columns/auto_increment_1.csv' into table t11;
select * from t11 order by a;
drop table t11;


drop table if exists t12;
create table t12(
a int primary key auto_increment,
b bigint auto_increment,
c varchar(25)
);
load data infile '$resources/auto_increment_columns/auto_increment_2.csv' into table t12;
select * from t12 order by a;

drop table t12;

drop table if exists t13;
create table t13(
a int primary key auto_increment,
b bigint auto_increment,
c varchar(25)
);

load data infile '$resources/auto_increment_columns/auto_increment_3.csv' into table t13;
select * from t13 order by a;

drop table t13;

create table t1(a int default(-1) auto_increment);
create table t1(a int primary key default(-1) auto_increment);
create table t1(a bigint default(-1) auto_increment);
create table t1(a bigint primary key default(-1) auto_increment);
create table t1(a int, b int default(10), c int auto_increment);
show create table t1;
drop table t1;

create table t1(a tinyint auto_increment);
insert into t1 values(null), (3), (null), (6), (null), (127);
select * from t1;
insert into t1 values();

drop table t1;
create table t1(a smallint auto_increment);
insert into t1 values(null), (3), (null), (6), (null), (32767);
select * from t1;
insert into t1 values();

drop table t1;
create table t1(a int auto_increment);
insert into t1 values(null), (3), (null), (6), (null), (2147483647);
select * from t1;
insert into t1 values();

drop table t1;
create table t1(a bigint auto_increment);
insert into t1 values(null), (3), (null), (6), (null), (9223372036854775807);
select * from t1;
insert into t1 values();

drop table t1;
create table t1(a tinyint unsigned auto_increment);
insert into t1 values(null), (3), (null), (6), (null), (255);
select * from t1;
insert into t1 values();

drop table t1;
create table t1(a smallint unsigned auto_increment);
insert into t1 values(null), (3), (null), (6), (null), (65535);
select * from t1;
insert into t1 values();

drop table t1;
create table t1(a int unsigned auto_increment);
insert into t1 values(null), (3), (null), (6), (null), (4294967295);
select * from t1;
insert into t1 values();

drop table t1;
create table t1(a bigint unsigned auto_increment);
insert into t1 values(null), (3), (null), (6), (null), (18446744073709551615);
select * from t1;
insert into t1 values();
drop table t1;
drop table if exists t1;
create table t1 (a int not null auto_increment, b int);
insert into t1(b) values (1);
select * from t1;

drop table t1;
create table t1(a int auto_increment primary key);
insert into t1 values();
select last_insert_id();
insert into t1 values(11);
insert into t1 values();
select last_insert_id();
create table t2(a int auto_increment primary key);
insert into t2 values();
select last_insert_id();
insert into t2 values(100);
insert into t2 values();
select last_insert_id();
insert into t1 values();
select last_insert_id();
insert into t2 values();
select last_insert_id();