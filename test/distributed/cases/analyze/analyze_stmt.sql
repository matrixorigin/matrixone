-- prepare
drop database if exists db_analyze_stmt;
create database db_analyze_stmt;
use db_analyze_stmt;

drop table if exists t_analyze_01;
create table t_analyze_01(
    a int,
    b varchar(10)
);
insert into t_analyze_01 values
    (1, 'a'),
    (1, 'a'),
    (2, 'b'),
    (2, 'c');

drop table if exists t_analyze_02;
create table t_analyze_02(
    x int,
    y int
);
insert into t_analyze_02 values
    (1, 10),
    (2, 20),
    (3, 30),
    (4, 40);

-- ANALYZE TABLE single table (existing behavior)
analyze table t_analyze_01(a, b);

-- CHECK TABLE: returns not-supported error
check table t_analyze_01;
check table t_analyze_01 extended;
check table t_analyze_01 for upgrade;
check table t_analyze_01, t_analyze_02;
check table t_analyze_01, t_analyze_02 extended;

-- SHOW PROFILE: returns not-supported error
show profile;
show profile for query 2;
show profile limit 10;
show profile for query 2 limit 10;
show profile for query 2 limit 10 offset 5;

-- cleanup
drop table t_analyze_01;
drop table t_analyze_02;
drop database db_analyze_stmt;
