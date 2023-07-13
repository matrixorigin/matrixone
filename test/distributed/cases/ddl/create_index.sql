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
insert into t3 values(4,"Dora", 28,'zbcvdf');
insert into t3 values(5,"Dora", 29,'zbcvdf');

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

create table t12(a text);
create unique index x on t12(a);
create index x2 on t12(a);
drop table t12;

create table t13(a blob);
create unique index x on t13(a);
create index x2 on t13(a);
drop table t13;

create table t14(a json);
create unique index x on t14(a);
create index x2 on t14(a);
drop table t14;

create table t15(
col1 int unsigned,
col2 varchar(15),
col3 varchar(10),
col4 int unsigned,
col5 date,
col6 decimal(7,2),
col7 decimal(7,2),
col8 int unsigned,
unique index(col1,col2,col3,col6)
);
INSERT INTO t15 VALUES (7369,'SMITH','CLERK',7902,'1980-12-17',800,NULL,20);
INSERT INTO t15 VALUES (7499,'ALLEN','SALESMAN',7698,'1981-02-20',1600,300,30);
INSERT INTO t15 VALUES (7521,'WARD','SALESMAN',7698,'1981-02-22',1250,500,30);
INSERT INTO t15 VALUES (7566,'JONES','MANAGER',7839,'1981-04-02',2975,NULL,20);
INSERT INTO t15 VALUES (7654,'MARTIN','SALESMAN',7698,'1981-09-28',1250,1400,30);
INSERT INTO t15 VALUES (7698,'BLAKE','MANAGER',7839,'1981-05-01',2850,NULL,30);
INSERT INTO t15 VALUES (7782,'CLARK','MANAGER',7839,'1981-06-09',2450,NULL,10);
INSERT INTO t15 VALUES (7788,'SCOTT','ANALYST',7566,'0087-07-13',3000,NULL,20);
INSERT INTO t15 VALUES (7839,'KING','PRESIDENT',NULL,'1981-11-17',5000,NULL,10);
INSERT INTO t15 VALUES (7844,'TURNER','SALESMAN',7698,'1981-09-08',1500,0,30);
INSERT INTO t15 VALUES (7876,'ADAMS','CLERK',7788,'0087-07-13',1100,NULL,20);
INSERT INTO t15 VALUES (7900,'JAMES','CLERK',7698,'1981-12-03',950,NULL,30);
INSERT INTO t15 VALUES (7902,'FORD','ANALYST',7566,'1981-12-03',3000,NULL,20);
INSERT INTO t15 VALUES (7934,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,10);

create unique index idx_1 on t15(col1,col2,col3,col6);
select * from t15;
drop table t15;