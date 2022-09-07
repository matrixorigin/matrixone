create table t1 (a int);
create table t2 (a int, b int);
create table t3 (a int);
create table t4 (a int not null, b int not null);
insert into t1 values (2);
insert into t2 values (1,7),(2,7);
insert into t4 values (4,8),(3,8),(5,9);
insert into t3 values (6),(7),(3);
-- @bvt:issue#3312
select * from t3 where a = (select b from t2);
-- @bvt:issue
select * from t3 where a = (select distinct b from t2);


CREATE TABLE t1 ( a char(1), b char(1));
INSERT INTO t1 VALUES ('1','1'),('2','2'),('3','3');Query OK, 0 rows affected (0.02 sec)
INSERT INTO t1 VALUES ('1','1'),('2','2'),('3','3');
SELECT a FROM t1 WHERE a > (SELECT a FROM t1 WHERE b = '2');
INSERT INTO t1 VALUES ('1','1'),('2','2'),('3','3'),('2','3');
-- @bvt:issue#3312
SELECT a FROM t1 WHERE a > (SELECT a FROM t1 WHERE b = '2');
-- @bvt:issue
