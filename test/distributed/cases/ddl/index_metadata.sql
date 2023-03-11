drop table if exists t1;
create table t1(
                   deptno int unsigned,
                   dname varchar(15),
                   loc varchar(50),
                   unique key(deptno)
);
select * from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where relname = 't1');

drop table if exists t2;
create table t2(
                   empno int unsigned auto_increment,
                   ename varchar(15),
                   job varchar(10),
                   mgr int unsigned ,
                   hiredate date,
                   sal decimal(7,2),
                   comm decimal(7,2),
                   deptno int unsigned,
                   primary key(empno)
);
select * from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where relname = 't2');

drop table if exists t3;
create table t3(
                   empno int unsigned,
                   ename varchar(15),
                   job varchar(10),
                   mgr int unsigned ,
                   hiredate date,
                   sal decimal(7,2),
                   comm decimal(7,2),
                   deptno int unsigned,
                   unique key(empno, ename)
);
select * from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where relname = 't3');

drop table if exists t4;
create table t4(
                   empno int unsigned,
                   ename varchar(15),
                   job varchar(10),
                   mgr int unsigned ,
                   hiredate date,
                   sal decimal(7,2),
                   comm decimal(7,2),
                   deptno int unsigned,
                   index(empno, ename, job)
);
select * from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where relname = 't4');

drop table if exists t5;
create table t5(
                   empno int unsigned,
                   ename varchar(15),
                   job varchar(10),
                   mgr int unsigned ,
                   hiredate date,
                   sal decimal(7,2),
                   comm decimal(7,2),
                   deptno int unsigned,
                   primary key(empno, ename)
);
select * from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where relname = 't5');

drop table if exists t6;
create table t6(
                   empno int unsigned,
                   ename varchar(15),
                   job varchar(10),
                   mgr int unsigned ,
                   hiredate date,
                   sal decimal(7,2),
                   comm decimal(7,2),
                   deptno int unsigned
);
select * from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where relname = 't6');


drop table if exists t7;
create table t7(
                   col1 int unsigned,
                   col2 varchar(15),
                   col3 varchar(10),
                   col4 int unsigned,
                   col5 date,
                   col6 decimal(7,2),
                   col7 decimal(7,2),
                   col8 int unsigned,
                   unique index(col1,col2),
                   unique index(col3,col6)
);
select * from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where relname = 't7');

drop table if exists t8;
create table t8(
                   empno int unsigned primary key,
                   ename varchar(15),
                   job varchar(10),
                   mgr int unsigned ,
                   hiredate date,
                   sal decimal(7,2),
                   comm decimal(7,2),
                   deptno int unsigned,
                   unique key(empno, ename)
);
select * from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where relname = 't8');


drop table if exists t9;
create table t9(id int PRIMARY KEY,name VARCHAR(255),age int);
insert into t9 values(1,"Abby", 24);
insert into t9 values(2,"Bob", 25);
insert into t9 values(3,"Carol", 23);
insert into t9 values(4,"Dora", 29);
select * from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where relname = 't9');
create unique index idx on t9(name);
select * from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where relname = 't9');
select * from t9;
drop table t9;


drop table if exists t10;
create table t10 (
                     col1 bigint primary key,
                     col2 varchar(25),
                     col3 float,
                     col4 varchar(50)
);
select * from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where relname = 't10');
create unique index idx on t10(col2) comment 'create varchar index';
select * from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where relname = 't10');
insert into t10 values(1,"Abby", 24,'zbcvdf');
insert into t10 values(2,"Bob", 25,'zbcvdf');
insert into t10 values(3,"Carol", 23,'zbcvdf');
insert into t10 values(4,"Dora", 29,'zbcvdf');
select * from t10;
drop table t10;

drop table if exists t11;
create table t11(a int, b int,c varchar(20));
select * from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where relname = 't11');
create index x11 ON t11(a) comment 'xxxxxxx';
create index x12 ON t11(b, c) comment 'yyyyyyyyy';
select * from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where relname = 't11');
drop index x11 on t11;
drop index x12 on t11;
select * from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where relname = 't11');
drop table t11;

drop table if exists t12;
create table t12(a int, b int,c varchar(20), primary key(a));
create index idx_1 on t12(a, b) comment 'xxxxxxx';
select * from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where relname = 't12');
create index idx_1 on t12(a, b);
select * from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where relname = 't12');
drop index idx_1 on t12;
select * from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where relname = 't12');
drop index idx_1 on t12;
drop table t12;