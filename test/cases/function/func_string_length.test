#SELECT
#0.5 MO的BINARY类型暂不支持
#CREATE TABLE t1 (s CHAR(8) BINARY);
#INSERT INTO t1 VALUES ('test');
#SELECT LENGTH(s) FROM t1;
#ALTER TABLE t1 MODIFY s CHAR(10) BINARY;
#SELECT LENGTH(s) FROM t1;
#DROP TABLE t1;

CREATE TABLE t1 (s CHAR(8));
INSERT INTO t1 VALUES ('test');
SELECT LENGTH(s) FROM t1;
CREATE TABLE t2 (s CHAR(10));
INSERT INTO t2 VALUES ('1234589002');
SELECT LENGTH(s) FROM t2;
drop table t1;
drop table t2;

#DATE TYPE

CREATE TABLE t1 (s char(8), b CHAR(100), c VARCHAR(255), d int, e float, f datetime);
INSERT INTO t1 VALUES ('test', "hhhhhhh", "12jknfjniosdjcoijcpowef", 1234, 0.14123123, "2012-02-08 12:03:23");
SELECT LENGTH(s), length(b), length(c), length(d), length(e), length(f) FROM t1;
DROP TABLE t1;


#INSERT, DISTINCT, ON CONDITION
CREATE TABLE t1 (t1_fld1 int, b varchar(255));
CREATE TABLE t2 (t2_fld1 int, b varchar(255));
CREATE TABLE t3 (t3_fld1 int, b varchar(255));
INSERT INTO t1 select LENGTH(space(300)), "abcdefg";
INSERT INTO t1 select LENGTH(space(300)), "123124141";
INSERT INTO t2 select LENGTH(space(65680)), "abcdefg";
INSERT INTO t2 select LENGTH(space(65680)), "1238193";
INSERT INTO t3 select LENGTH(space(65680)), "1238193";
INSERT INTO t3 select LENGTH(space(16777300)),"123124141";
INSERT INTO t3 select LENGTH(space(16777300)),"123asdq";
SELECT DISTINCT * from t1;
SELECT t1.t1_fld1, t2.t2_fld1, t3.t3_fld1 FROM t1 JOIN t2 JOIN t3 ON (length(t1.b) = length(t2.b));
drop table t1;
drop table t2;
drop table t3;

#EXTREME VALUE，中文
select length('\n\t\r\b\0\_\%\\');

select length(12314124);
select length(0.14123124124);

select length('1039214-#**$&#@*#(*($*');
select length(NULL);
select length("中文");




#HAVING，嵌套
CREATE TABLE t1 (a varchar(10));
INSERT INTO t1 VALUES ('abc'), ('xyz');
SELECT a, CONCAT_WS(",",a,' ',a) AS c FROM t1
HAVING LENGTH(REVERSE(c)) >0;
DROP TABLE t1;

#嵌套
select length(space(1)) as a;
select length(space(1024*1024*1024)) as a;
select length(space(1024*1024)) as a;

select length(space(1024*1024*1024)) as a;

#WHERE, 算术运算
drop table if exists t1;
create table t1(a INT,  b date);
insert into t1 values(1, "2012-10-12"),(2, "2004-04-24"),(3, "2008-12-04"),(4, "2012-03-23");
select * from t1 where length(b)-8>0;
drop table t1;

#BLOB
DROP table if exists t1;
CREATE TABLE t1 (s BLOB);
INSERT INTO t1 VALUES ('test');
SELECT LENGTH(s) FROM t1;
CREATE TABLE t2 (s BLOB);
INSERT INTO t2 VALUES ('1234589002');
SELECT LENGTH(s) FROM t2;
drop table t1;
drop table t2;