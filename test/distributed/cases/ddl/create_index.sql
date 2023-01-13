drop table if exists t1;
create table t1(id int PRIMARY KEY,name VARCHAR(255),age int);
insert into t1 values(1,"Abby", 24);
insert into t1 values(2,"Bob", 25);
insert into t1 values(3,"Carol", 23);
insert into t1 values(4,"Dora", 29);
create unique index idx on t1(name);
select * from t1;
drop table t1;

create table t2 (
col1 bigint primary key,
col2 varchar(25),
col3 float,
col4 varchar(50)
);
create unique index idx on t2(col2) comment 'create varchar index';
insert into t2 values(1,"Abby", 24,'zbcvdf');
insert into t2 values(2,"Bob", 25,'zbcvdf');
insert into t2 values(3,"Carol", 23,'zbcvdf');
insert into t2 values(4,"Dora", 29,'zbcvdf');
select * from t2;
drop table t2;

create table t3 (
col1 bigint primary key,
col2 varchar(25),
col3 float,
col4 varchar(50)
);
create unique index idx on t3(col2,col3);
insert into t3 values(1,"Abby", 24,'zbcvdf');
insert into t3 values(2,"Bob", 25,'zbcvdf');
insert into t3 values(3,"Carol", 23,'zbcvdf');
insert into t3 values(4,"Dora", 29,'zbcvdf');
select * from t3;
insert into t3 values(4,"Dora", 29,'zbcvdf');
drop table t3;

create table t4(a int, b int, key(c));

create table t5(a int, b int, unique key(a));
show create table t5;
create index b on t5(b);
show create table t5;
drop index b on t5;
show create table t5;
drop table t5;

create table t6(a int, b int, unique key(a));
show create table t6;
create index b on t6(a, b);
show create table t6;
drop index b on t6;
show create table t6;
drop table t6;

create table t7(a int, b int);
create unique index x ON t7(a) comment 'x';
show create table t7;
drop table t7;

create table t8(a int, b int);
create index x ON t8(a) comment 'x';
show create table t8;
drop table t8;

create table t9(a int, b int, unique key(a) comment 'a');
show create table t9;
drop table t9;

create table t10(a int, b int, key(a) comment 'a');
show create table t10;
drop table t10;

create table t11(a int, b int, unique key(a) comment 'a');
create unique index x ON t11(a) comment 'x';
create index xx ON t11(a) comment 'xx';
show create table t11;
drop table t11;
