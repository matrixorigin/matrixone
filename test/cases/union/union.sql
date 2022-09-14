drop table if exists t1;
drop table if exists t2;
create table t1(a int, b int);
insert into t1 values (1, 10),(2, 20);
create table t2(aa int, bb varchar(20));
insert into t2 values (11, "aa"),(22, "bb");

(select a from t1) order by b desc;
(((select a from t1) order by b desc));
(((select a from t1))) order by b desc;
(((select a from t1 order by b desc) limit 1));
(((select a from t1 order by b desc))) limit 1;
(select a from t1 union select aa from t2) order by a desc;

(select a from t1 order by a) order by a;
(((select a from t1 order by a))) order by a;
(((select a from t1) order by a)) order by a;
(select a from t1 limit 1) limit 1;
(((select a from t1 limit 1))) limit 1;
(((select a from t1) limit 1)) limit 1;
(select a from t1 union select aa from t2) order by aa;
select a from t1 union select a from t1;
drop table if exists t3;
create table t3(
a tinyint
);
insert into t3 values (20),(10),(30),(-10);
drop table if exists t4;
create table t4(
col1 smallint,
col2 smallint unsigned,
col3 float,
col4 bool
);
insert into t4 values(100, 65535, 127.0, 1);
insert into t4 values(300, 0, 1.0, 0);
insert into t4 values(500, 100, 0.0, 0);
insert into t4 values(200, 35, 127.0, 1);
insert into t4 values(200, 35, 127.44, 1);
select a from t3 union select col3 from t4 order by a;