drop database if exists db6;
create database db6;
use db6;

drop table if exists t1;
create table t1(
                   deptno int unsigned,
                   dname varchar(15),
                   loc varchar(50),
                   unique key(deptno)
);
select
    `idx`.`name`,
    `idx`.`type`,
    `idx`.`name`,
    `idx`.`is_visible`,
    `idx`.`hidden`,
    `idx`.`comment`,
    `tbl`.`relname`,
    `idx`.`column_name`,
    `idx`.`ordinal_position`,
    `idx`.`options`
from
    `mo_catalog`.`mo_indexes` `idx` join `mo_catalog`.`mo_tables` `tbl` on (`idx`.`table_id` = `tbl`.`rel_id`)
where  `tbl`.`relname` = 't1' and `tbl`.`reldatabase` = 'db6';

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
select
    `idx`.`name`,
    `idx`.`type`,
    `idx`.`name`,
    `idx`.`is_visible`,
    `idx`.`hidden`,
    `idx`.`comment`,
    `tbl`.`relname`,
    `idx`.`column_name`,
    `idx`.`ordinal_position`,
    `idx`.`options`
from
    `mo_catalog`.`mo_indexes` `idx` join `mo_catalog`.`mo_tables` `tbl` on (`idx`.`table_id` = `tbl`.`rel_id`)
where  `tbl`.`relname` = 't2' and `tbl`.`reldatabase` = 'db6';


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
select
    `idx`.`name`,
    `idx`.`type`,
    `idx`.`name`,
    `idx`.`is_visible`,
    `idx`.`hidden`,
    `idx`.`comment`,
    `tbl`.`relname`,
    `idx`.`column_name`,
    `idx`.`ordinal_position`,
    `idx`.`options`
from
    `mo_catalog`.`mo_indexes` `idx` join `mo_catalog`.`mo_tables` `tbl` on (`idx`.`table_id` = `tbl`.`rel_id`)
where  `tbl`.`relname` = 't3' and `tbl`.`reldatabase` = 'db6';


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
select
    `idx`.`name`,
    `idx`.`type`,
    `idx`.`name`,
    `idx`.`is_visible`,
    `idx`.`hidden`,
    `idx`.`comment`,
    `tbl`.`relname`,
    `idx`.`column_name`,
    `idx`.`ordinal_position`,
    `idx`.`options`
from
    `mo_catalog`.`mo_indexes` `idx` join `mo_catalog`.`mo_tables` `tbl` on (`idx`.`table_id` = `tbl`.`rel_id`)
where  `tbl`.`relname` = 't4' and `tbl`.`reldatabase` = 'db6';


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
select
    `idx`.`name`,
    `idx`.`type`,
    `idx`.`name`,
    `idx`.`is_visible`,
    `idx`.`hidden`,
    `idx`.`comment`,
    `tbl`.`relname`,
    `idx`.`column_name`,
    `idx`.`ordinal_position`,
    `idx`.`options`
from
    `mo_catalog`.`mo_indexes` `idx` join `mo_catalog`.`mo_tables` `tbl` on (`idx`.`table_id` = `tbl`.`rel_id`)
where  `tbl`.`relname` = 't5' and `tbl`.`reldatabase` = 'db6';

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
select
    `idx`.`name`,
    `idx`.`type`,
    `idx`.`name`,
    `idx`.`is_visible`,
    `idx`.`hidden`,
    `idx`.`comment`,
    `tbl`.`relname`,
    `idx`.`column_name`,
    `idx`.`ordinal_position`,
    `idx`.`options`
from
    `mo_catalog`.`mo_indexes` `idx` join `mo_catalog`.`mo_tables` `tbl` on (`idx`.`table_id` = `tbl`.`rel_id`)
where  `tbl`.`relname` = 't6' and `tbl`.`reldatabase` = 'db6';


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
select
    `idx`.`name`,
    `idx`.`type`,
    `idx`.`name`,
    `idx`.`is_visible`,
    `idx`.`hidden`,
    `idx`.`comment`,
    `tbl`.`relname`,
    `idx`.`column_name`,
    `idx`.`ordinal_position`,
    `idx`.`options`
from
    `mo_catalog`.`mo_indexes` `idx` join `mo_catalog`.`mo_tables` `tbl` on (`idx`.`table_id` = `tbl`.`rel_id`)
where  `tbl`.`relname` = 't7' and `tbl`.`reldatabase` = 'db6';


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
select
    `idx`.`name`,
    `idx`.`type`,
    `idx`.`name`,
    `idx`.`is_visible`,
    `idx`.`hidden`,
    `idx`.`comment`,
    `tbl`.`relname`,
    `idx`.`column_name`,
    `idx`.`ordinal_position`,
    `idx`.`options`
from
    `mo_catalog`.`mo_indexes` `idx` join `mo_catalog`.`mo_tables` `tbl` on (`idx`.`table_id` = `tbl`.`rel_id`)
where  `tbl`.`relname` = 't8' and `tbl`.`reldatabase` = 'db6';


drop table if exists t9;
create table t9(id int PRIMARY KEY,name VARCHAR(255),age int);
insert into t9 values(1,"Abby", 24);
insert into t9 values(2,"Bob", 25);
insert into t9 values(3,"Carol", 23);
insert into t9 values(4,"Dora", 29);
select
    `idx`.`name`,
    `idx`.`type`,
    `idx`.`name`,
    `idx`.`is_visible`,
    `idx`.`hidden`,
    `idx`.`comment`,
    `tbl`.`relname`,
    `idx`.`column_name`,
    `idx`.`ordinal_position`,
    `idx`.`options`
from
    `mo_catalog`.`mo_indexes` `idx` join `mo_catalog`.`mo_tables` `tbl` on (`idx`.`table_id` = `tbl`.`rel_id`)
where  `tbl`.`relname` = 't9' and `tbl`.`reldatabase` = 'db6';
create unique index idx on t9(name);
select
    `idx`.`name`,
    `idx`.`type`,
    `idx`.`name`,
    `idx`.`is_visible`,
    `idx`.`hidden`,
    `idx`.`comment`,
    `tbl`.`relname`,
    `idx`.`column_name`,
    `idx`.`ordinal_position`,
    `idx`.`options`
from
    `mo_catalog`.`mo_indexes` `idx` join `mo_catalog`.`mo_tables` `tbl` on (`idx`.`table_id` = `tbl`.`rel_id`)
where  `tbl`.`relname` = 't9';
select * from t9;
drop table t9;


drop table if exists t10;
create table t10 (
                     col1 bigint primary key,
                     col2 varchar(25),
                     col3 float,
                     col4 varchar(50)
);
select
    `idx`.`name`,
    `idx`.`type`,
    `idx`.`name`,
    `idx`.`is_visible`,
    `idx`.`hidden`,
    `idx`.`comment`,
    `tbl`.`relname`,
    `idx`.`column_name`,
    `idx`.`ordinal_position`,
    `idx`.`options`
from
    `mo_catalog`.`mo_indexes` `idx` join `mo_catalog`.`mo_tables` `tbl` on (`idx`.`table_id` = `tbl`.`rel_id`)
where  `tbl`.`relname` = 't10' and `tbl`.`reldatabase` = 'db6';
create unique index idx on t10(col2) comment 'create varchar index';
select
    `idx`.`name`,
    `idx`.`type`,
    `idx`.`name`,
    `idx`.`is_visible`,
    `idx`.`hidden`,
    `idx`.`comment`,
    `tbl`.`relname`,
    `idx`.`column_name`,
    `idx`.`ordinal_position`,
    `idx`.`options`
from
    `mo_catalog`.`mo_indexes` `idx` join `mo_catalog`.`mo_tables` `tbl` on (`idx`.`table_id` = `tbl`.`rel_id`)
where  `tbl`.`relname` = 't10' and `tbl`.`reldatabase` = 'db6';
insert into t10 values(1,"Abby", 24,'zbcvdf');
insert into t10 values(2,"Bob", 25,'zbcvdf');
insert into t10 values(3,"Carol", 23,'zbcvdf');
insert into t10 values(4,"Dora", 29,'zbcvdf');
select * from t10;
drop table t10;

drop table if exists t11;
create table t11(a int, b int,c varchar(20));
select
    `idx`.`name`,
    `idx`.`type`,
    `idx`.`name`,
    `idx`.`is_visible`,
    `idx`.`hidden`,
    `idx`.`comment`,
    `tbl`.`relname`,
    `idx`.`column_name`,
    `idx`.`ordinal_position`,
    `idx`.`options`
from
    `mo_catalog`.`mo_indexes` `idx` join `mo_catalog`.`mo_tables` `tbl` on (`idx`.`table_id` = `tbl`.`rel_id`)
where  `tbl`.`relname` = 't11' and `tbl`.`reldatabase` = 'db6';
create index x11 ON t11(a) comment 'xxxxxxx';
create index x12 ON t11(b, c) comment 'yyyyyyyyy';
select
    `idx`.`name`,
    `idx`.`type`,
    `idx`.`name`,
    `idx`.`is_visible`,
    `idx`.`hidden`,
    `idx`.`comment`,
    `tbl`.`relname`,
    `idx`.`column_name`,
    `idx`.`ordinal_position`,
    `idx`.`options`
from
    `mo_catalog`.`mo_indexes` `idx` join `mo_catalog`.`mo_tables` `tbl` on (`idx`.`table_id` = `tbl`.`rel_id`)
where  `tbl`.`relname` = 't11' and `tbl`.`reldatabase` = 'db6';
drop index x11 on t11;
drop index x12 on t11;
select
    `idx`.`name`,
    `idx`.`type`,
    `idx`.`name`,
    `idx`.`is_visible`,
    `idx`.`hidden`,
    `idx`.`comment`,
    `tbl`.`relname`,
    `idx`.`column_name`,
    `idx`.`ordinal_position`,
    `idx`.`options`
from
    `mo_catalog`.`mo_indexes` `idx` join `mo_catalog`.`mo_tables` `tbl` on (`idx`.`table_id` = `tbl`.`rel_id`)
where  `tbl`.`relname` = 't11' and `tbl`.`reldatabase` = 'db6';
drop table t11;

drop table if exists t12;
create table t12(a int, b int,c varchar(20), primary key(a));
create index idx_1 on t12(a, b) comment 'xxxxxxx';
select
    `idx`.`name`,
    `idx`.`type`,
    `idx`.`name`,
    `idx`.`is_visible`,
    `idx`.`hidden`,
    `idx`.`comment`,
    `tbl`.`relname`,
    `idx`.`column_name`,
    `idx`.`ordinal_position`,
    `idx`.`options`
from
    `mo_catalog`.`mo_indexes` `idx` join `mo_catalog`.`mo_tables` `tbl` on (`idx`.`table_id` = `tbl`.`rel_id`)
where  `tbl`.`relname` = 't12' and `tbl`.`reldatabase` = 'db6';
create index idx_1 on t12(a, b);
select
    `idx`.`name`,
    `idx`.`type`,
    `idx`.`name`,
    `idx`.`is_visible`,
    `idx`.`hidden`,
    `idx`.`comment`,
    `tbl`.`relname`,
    `idx`.`column_name`,
    `idx`.`ordinal_position`,
    `idx`.`options`
from
    `mo_catalog`.`mo_indexes` `idx` join `mo_catalog`.`mo_tables` `tbl` on (`idx`.`table_id` = `tbl`.`rel_id`)
where  `tbl`.`relname` = 't12' and `tbl`.`reldatabase` = 'db6';
drop index idx_1 on t12;
select
    `idx`.`name`,
    `idx`.`type`,
    `idx`.`name`,
    `idx`.`is_visible`,
    `idx`.`hidden`,
    `idx`.`comment`,
    `tbl`.`relname`,
    `idx`.`column_name`,
    `idx`.`ordinal_position`,
    `idx`.`options`
from
    `mo_catalog`.`mo_indexes` `idx` join `mo_catalog`.`mo_tables` `tbl` on (`idx`.`table_id` = `tbl`.`rel_id`)
where  `tbl`.`relname` = 't12' and `tbl`.`reldatabase` = 'db6';
drop index idx_1 on t12;
drop table t12;
drop database db6;