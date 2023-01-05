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