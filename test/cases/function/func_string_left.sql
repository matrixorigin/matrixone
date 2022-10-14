select left('abcde', 3) from dual;
select left('abcde', 0) from dual;
select left('abcde', 10) from dual;
select left('abcde', -1) from dual;
select left('abcde', null) from dual;
select left(null, 3) from dual;
select left(null, null) from dual;
select left('foobarbar', 5) from dual;
select left('qwerty', 1.2) from dual;
select left('qwerty', 1.5) from dual;
select left('qwerty', 1.8) from dual;
select left("是都方式快递费",3) from dual;
select left("あいうえお",3) from dual;
select left("あいうえお ",3) from dual;
select left("あいうえお  ",3) from dual;
select left("あいうえお   ",3) from dual;
select left("龔龖龗龞龡",3) from dual;
select left("龔龖龗龞龡 ",3) from dual;
select left("龔龖龗龞龡  ",3) from dual;
select left("龔龖龗龞龡   ",3) from dual;

drop table if exists t1;
CREATE TABLE t1 (str VARCHAR(100), len INT);
insert into t1 values('abcdefghijklmn',3);
insert into t1 values('ABCDEFGH123456', 3);
insert into t1 values('ABCDEFGHIJKLMN', 20);
insert into t1 values('ABCDEFGHijklmn', -1);
insert into t1 values('ABCDEFGH123456', 7);
insert into t1 values('', 3);

select left(str, len) from t1;
select * from t1 where left(str, len) = 'ABC';
select left(str, 3) from t1;
select left('sdfsdfsdfsdf', len) from t1;
drop table t1;

DROP TABLE IF EXISTS t2;
CREATE TABLE t2(
d INT,
d1 BIGINT,
d2 FLOAT,
d3 DOUBLE,
PRIMARY KEY (d)
);

INSERT INTO t2 VALUES (1,101210131014,50565056.5566,80898089.8899);
INSERT INTO t2 VALUES (2,46863515648464,9876453.3156153,6486454631564.156153489);
SELECT LEFT(d1,3), LEFT(d2,4), LEFT(d3,5) FROM t2;
drop table t2;