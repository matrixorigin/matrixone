// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

var (
	debugSQL = struct {
		input  string
		output string
	}{
		input:  "with t11 as (select * from (select * from t1) as t22) update t11 join t2 on t11.a = t2.a set t2.b = 666;",
		output: "with t11 as (select * from (select * from t1) as t22) update t11 inner join t2 on t11.a = t2.a set t2.b = 666",
	}
)

// character set latin1 NOT NULL default
func TestDebug(t *testing.T) {
	if debugSQL.output == "" {
		debugSQL.output = debugSQL.input
	}
	ast, err := ParseOne(debugSQL.input)
	if err != nil {
		t.Errorf("Parse(%q) err: %v", debugSQL.input, err)
		return
	}
	out := tree.String(ast, dialect.MYSQL)
	if debugSQL.output != out {
		t.Errorf("Parsing failed. \nExpected/Got:\n%s\n%s", debugSQL.output, out)
	}
}

var (
	validSQL = []struct {
		input  string
		output string
	}{{
		input:  "explain select * from emp",
		output: "explain select * from emp",
	}, {
		input:  "explain verbose select * from emp",
		output: "explain (verbose) select * from emp",
	}, {
		input:  "explain analyze select * from emp",
		output: "explain (analyze) select * from emp",
	}, {
		input:  "explain analyze verbose select * from emp",
		output: "explain (analyze,verbose) select * from emp",
	}, {
		input:  "explain (analyze true,verbose false) select * from emp",
		output: "explain (analyze true,verbose false) select * from emp",
	}, {
		input:  "explain (analyze true,verbose false,format json) select * from emp",
		output: "explain (analyze true,verbose false,format json) select * from emp",
	}, {
		input:  "with t11 as (select * from t1) update t11 join t2 on t11.a = t2.b set t11.b = 1 where t2.a > 1",
		output: "with t11 as (select * from t1) update t11 inner join t2 on t11.a = t2.b set t11.b = 1 where t2.a > 1",
	}, {
		input:  "UPDATE items,(SELECT id FROM items WHERE id IN (SELECT id FROM items WHERE retail / wholesale >= 1.3 AND quantity < 100)) AS discounted SET items.retail = items.retail * 0.9 WHERE items.id = discounted.id",
		output: "update items, (select id from items where id in (select id from items where retail / wholesale >= 1.3 and quantity < 100)) as discounted set items.retail = items.retail * 0.9 where items.id = discounted.id",
	}, {
		input:  "with t2 as (select * from t1) DELETE FROM a1, a2 USING t1 AS a1 INNER JOIN t2 AS a2 WHERE a1.id=a2.id;",
		output: "with t2 as (select * from t1) delete from a1, a2 using t1 as a1 inner join t2 as a2 where a1.id = a2.id",
	}, {
		input:  "DELETE FROM a1, a2 USING t1 AS a1 INNER JOIN t2 AS a2 WHERE a1.id=a2.id;",
		output: "delete from a1, a2 using t1 as a1 inner join t2 as a2 where a1.id = a2.id",
	}, {
		input:  "DELETE a1, a2 FROM t1 AS a1 INNER JOIN t2 AS a2 WHERE a1.id=a2.id",
		output: "delete from a1, a2 using t1 as a1 inner join t2 as a2 where a1.id = a2.id",
	}, {
		input:  "DELETE FROM t1, t2 USING t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id=t2.id AND t2.id=t3.id",
		output: "delete from t1, t2 using t1 inner join t2 inner join t3 where t1.id = t2.id and t2.id = t3.id",
	}, {
		input:  "DELETE t1, t2 FROM t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id=t2.id AND t2.id=t3.id",
		output: "delete from t1, t2 using t1 inner join t2 inner join t3 where t1.id = t2.id and t2.id = t3.id",
	}, {
		input: "select cast(false as varchar)",
	}, {
		input:  "select cast(a as timestamp)",
		output: "select cast(a as timestamp(26))",
	}, {
		input:  "select cast(\"2022-01-30\" as varchar);",
		output: "select cast(2022-01-30 as varchar)",
	}, {
		input:  "select cast(b as timestamp) from t2",
		output: "select cast(b as timestamp(26)) from t2",
	}, {
		input:  "select cast(\"2022-01-01 01:23:34\" as varchar)",
		output: "select cast(2022-01-01 01:23:34 as varchar)",
	}, {
		input:  "show schemas where 1",
		output: "show databases where 1",
	}, {
		input: "select role from t1",
	}, {
		input:  "select a || 'hello' || 'world' from t1;",
		output: "select concat(concat(a, hello), world) from t1",
	}, {
		input:  "select col || 'bar'",
		output: "select concat(col, bar)",
	}, {
		input:  "select 'foo' || 'bar'",
		output: "select concat(foo, bar)",
	}, {
		input:  "select 'a\\'b'",
		output: "select a'b",
	}, {
		input:  "select char_length('\\n\\t\\r\\b\\0\\_\\%\\\\');",
		output: "select char_length(\\n\\t\\r\\b\\0\\_\\%\\\\)",
	}, {
		input:  "select CAST('10 ' as unsigned);",
		output: "select cast(10  as unsigned)",
	}, {
		input:  "select CAST('10 ' as unsigned integer);",
		output: "select cast(10  as integer unsigned)",
	}, {
		input:  "SELECT ((+0) IN ((0b111111111111111111111111111111111111111111111111111),(rpad(1.0,2048,1)), (32767.1)));",
		output: "select ((+0) in ((0b111111111111111111111111111111111111111111111111111), (rpad(1.0, 2048, 1)), (32767.1)))",
	}, {
		input: "select 0b111111111111111111111111111111111111111111111111111",
	}, {
		input:  "select date,format,to_date(date, format) as to_date from t1;",
		output: "select date, format, to_date(date, format) as to_date from t1",
	}, {
		input:  "select date,format,concat_ws(',',to_date(date, format)) as con from t1;",
		output: "select date, format, concat_ws(,, to_date(date, format)) as con from t1",
	}, {
		input:  "select date,format,to_date(date, format) as to_date from t1;",
		output: "select date, format, to_date(date, format) as to_date from t1",
	}, {
		input:  "select date,format,concat_ws(\" \",to_date(date, format),'') as con from t1;",
		output: "select date, format, concat_ws( , to_date(date, format), ) as con from t1",
	}, {
		input:  "load data infile 'test/loadfile5' ignore INTO TABLE T.A FIELDS TERMINATED BY  ',' (@,@,c,d,e,f)",
		output: "load data infile test/loadfile5 ignore into table t.a fields terminated by , (, , c, d, e, f)",
	}, {
		input: "select schema()",
	}, {
		input: "select last_insert_id()",
	}, {
		input:  "show char set where charset = 'utf8mb4'",
		output: "show charset where charset = utf8mb4",
	}, {
		input:  "show charset where charset = 'utf8mb4'",
		output: "show charset where charset = utf8mb4",
	}, {
		input:  "show character set where charset = 'utf8mb4'",
		output: "show charset where charset = utf8mb4",
	}, {
		input: "show config where a > 1",
	}, {
		input:  "set @@a = b",
		output: "set a = b",
	}, {
		input:  "set @a = b",
		output: "set a = b",
	}, {
		input:  "CREATE TABLE t1 (datetime datetime, timestamp timestamp, date date)",
		output: "create table t1 (datetime datetime(26), timestamp timestamp(26), date date)",
	}, {
		input:  "SET timestamp=DEFAULT;",
		output: "set timestamp = default",
	}, {
		input:  "SET timestamp=UNIX_TIMESTAMP('2011-07-31 10:00:00')",
		output: "set timestamp = unix_timestamp(2011-07-31 10:00:00)",
	}, {
		input:  "select ltrim(\"a\"),rtrim(\"a\"),trim(BOTH \"\" from \"a\"),trim(BOTH \" \" from \"a\");",
		output: "select ltrim(a), rtrim(a), trim(both, , a), trim(both,  , a)",
	}, {
		input:  "select rpad('hello', -18446744073709551616, '1');",
		output: "select rpad(hello, -18446744073709551616, 1)",
	}, {
		input:  "select rpad('hello', -18446744073709551616, '1');",
		output: "select rpad(hello, -18446744073709551616, 1)",
	}, {
		input:  "SELECT CONCAT_WS(1471290948102948112341241204312904-23412412-4141, \"a\", \"b\")",
		output: "select concat_ws(1471290948102948112341241204312904 - 23412412 - 4141, a, b)",
	}, {
		input:  "SELECT * FROM t1 WHERE a = ANY ( SELECT 1 UNION ( SELECT 1 UNION SELECT 1 ) );",
		output: "select * from t1 where a = any (select 1 union (select 1 union select 1))",
	}, {
		input:  "SELECT * FROM t1 WHERE (a,b) = ANY (SELECT a, max(b) FROM t1 GROUP BY a);",
		output: "select * from t1 where (a, b) = any (select a, max(b) from t1 group by a)",
	}, {
		input:  "select  (1,2) != ALL (select * from t1);",
		output: "select (1, 2) != all (select * from t1)",
	}, {
		input:  "select s1, s1 = ANY (SELECT s1 FROM t2) from t1;",
		output: "select s1, s1 = any (select s1 from t2) from t1",
	}, {
		input:  "select * from t3 where a >= some (select b from t2);",
		output: "select * from t3 where a >= some (select b from t2)",
	}, {
		input:  "select 9999999999999999999;",
		output: "select 9999999999999999999",
	}, {
		input:  "select substring('hello', -18446744073709551616, -18446744073709551616);",
		output: "select substring(hello, -18446744073709551616, -18446744073709551616)",
	}, {
		input:  "select substring('hello', -18446744073709551616, 1);",
		output: "select substring(hello, -18446744073709551616, 1)",
	}, {
		input:  "select space(18446744073709551616);",
		output: "select space(18446744073709551616)",
	}, {
		input:  "select space(-18446744073709551616);",
		output: "select space(-18446744073709551616)",
	}, {
		input:  "select ltrim(\"a\"),rtrim(\"a\"),trim(BOTH \"\" from \"a\"),trim(BOTH \" \" from \"a\");",
		output: "select ltrim(a), rtrim(a), trim(both, , a), trim(both,  , a)",
	}, {
		input:  "SELECT (rpad(1.0, 2048,1)) IS NOT FALSE;",
		output: "select (rpad(1.0, 2048, 1)) != false",
	}, {
		input:  "SELECT FROM_UNIXTIME(99999999999999999999999999999999999999999999999999999999999999999);",
		output: "select from_unixtime(99999999999999999999999999999999999999999999999999999999999999999)",
	}, {
		input:  "SELECT FROM_UNIXTIME(2147483647) AS c1, FROM_UNIXTIME(2147483648) AS c2, FROM_UNIXTIME(2147483647.9999999) AS c3, FROM_UNIXTIME(32536771199) AS c4,FROM_UNIXTIME(32536771199.9999999) AS c5;",
		output: "select from_unixtime(2147483647) as c1, from_unixtime(2147483648) as c2, from_unixtime(2147483647.9999999) as c3, from_unixtime(32536771199) as c4, from_unixtime(32536771199.9999999) as c5",
	}, {
		input:  "select date_add(\"1997-12-31 23:59:59\",INTERVAL -100000 YEAR);",
		output: "select date_add(1997-12-31 23:59:59, interval(-100000, year))",
	}, {
		input:  "SELECT ADDDATE(DATE'2021-01-01', INTERVAL 1 DAY);",
		output: "select adddate(date(2021-01-01), interval(1, day))",
	}, {
		input:  "select '2007-01-01' + interval a day from t1;",
		output: "select 2007-01-01 + interval(a, day) from t1",
	}, {
		input:  "SELECT CAST(COALESCE(t0.c0, -1) AS UNSIGNED) IS TRUE FROM t0;",
		output: "select cast(coalesce(t0.c0, -1) as unsigned) = true from t0",
	}, {
		input:  "select Fld1, variance(Fld2) as q from t1 group by Fld1 having q is not null;",
		output: "select fld1, variance(fld2) as q from t1 group by fld1 having q is not null",
	}, {
		input:  "select variance(-99999999999999999.99999);",
		output: "select variance(-99999999999999999.99999)",
	}, {
		input:  "select Fld1, std(Fld2) from t1 group by Fld1 having variance(Fld2) is not null",
		output: "select fld1, std(fld2) from t1 group by fld1 having variance(fld2) is not null",
	}, {
		input:  "select a.f1 as a, a.f1 > b.f1 as gt, a.f1 < b.f1 as lt, a.f1<=>b.f1 as eq from t1 a, t1 b;",
		output: "select a.f1 as a, a.f1 > b.f1 as gt, a.f1 < b.f1 as lt, a.f1 <=> b.f1 as eq from t1 as a, t1 as b",
	}, {
		input:  "select var_samp(s) as '0.5', var_pop(s) as '0.25' from bug22555;",
		output: "select var_samp(s) as 0.5, var_pop(s) as 0.25 from bug22555",
	}, {
		input:  "select var_samp(s) as 'null', var_pop(s) as 'null' from bug22555;",
		output: "select var_samp(s) as null, var_pop(s) as null from bug22555",
	}, {
		input: "select cast(variance(ff) as decimal(10, 3)) from t2",
	}, {
		input:  "SELECT GROUP_CONCAT(DISTINCT 2) from t1",
		output: "select group_concat(distinct 2) from t1",
	}, {
		input: "select variance(2) from t1",
	}, {
		input:  "select SQL_BIG_RESULT bit_and(col), bit_or(col) from t1 group by col;",
		output: "select sql_big_result bit_and(col), bit_or(col) from t1 group by col",
	}, {
		input: "select sql_small_result t2.id, avg(rating + 0.0e0) from t2 group by t2.id",
	}, {
		input: "select sql_small_result t2.id, avg(rating) from t2 group by t2.id",
	}, {
		input: "select any_value(name), avg(value1), std(value1), variance(value1) from t1, t2 where t1.id = t2.id group by t1.id",
	}, {
		input: "select id, avg(value1), std(value1), variance(value1) from t1 group by id",
	}, {
		input: "select i, count(*), std(s1 / s2) from bug22555 group by i order by i",
	}, {
		input: "select i, count(*), variance(s1 / s2) from bug22555 group by i order by i",
	}, {
		input: "select i, count(*), variance(s1 / s2) from bug22555 group by i order by i",
	}, {
		input: "select name, avg(value1), std(value1), variance(value1) from t1, t2 where t1.id = t2.id group by t1.id",
	}, {
		input:  "select sum(all a),count(all a),avg(all a),std(all a),variance(all a),bit_or(all a),bit_and(all a),min(all a),max(all a),min(all c),max(all c) from t",
		output: "select sum(all a), count(all a), avg(all a), std(all a), variance(all a), bit_or(all a), bit_and(all a), min(all a), max(all a), min(all c), max(all c) from t",
	}, {
		input:  "insert into t1 values (date_add(NULL, INTERVAL 1 DAY));",
		output: "insert into t1 values (date_add(null, interval(1, day)))",
	}, {
		input:  "SELECT DATE_ADD('2022-02-28 23:59:59.9999', INTERVAL 1 SECOND) '1 second later';",
		output: "select date_add(2022-02-28 23:59:59.9999, interval(1, second)) as 1 second later",
	}, {
		input:  "SELECT sum(a) as 'hello' from t1;",
		output: "select sum(a) as hello from t1",
	}, {
		input:  "SELECT DATE_ADD(\"2017-06-15\", INTERVAL -10 MONTH);",
		output: "select date_add(2017-06-15, interval(-10, month))",
	}, {
		input:  "create table t1 (a varchar)",
		output: "create table t1 (a varchar)",
	}, {
		input:  "SELECT (CAST(0x7FFFFFFFFFFFFFFF AS char));",
		output: "select (cast(0x7fffffffffffffff as char))",
	}, {
		input:  "select cast(-19999999999999999999 as signed);",
		output: "select cast(-19999999999999999999 as signed)",
	}, {
		input:  "select cast(19999999999999999999 as signed);",
		output: "select cast(19999999999999999999 as signed)",
	}, {
		input:  "select date_sub(now(), interval 1 day) from t1;",
		output: "select date_sub(now(), interval(1, day)) from t1",
	}, {
		input:  "select date_sub(now(), interval '1' day) from t1;",
		output: "select date_sub(now(), interval(1, day)) from t1",
	}, {
		input:  "select date_add(now(), interval '1' day) from t1;",
		output: "select date_add(now(), interval(1, day)) from t1",
	}, {
		input:  "SELECT md.datname as `Database` FROM TT md",
		output: "select md.datname as Database from tt as md",
	}, {
		input:  "select * from t where a = `Hello`",
		output: "select * from t where a = Hello",
	}, {
		input:  "CREATE VIEW v AS SELECT * FROM t WHERE t.id = f(t.name);",
		output: "create view v as select * from t where t.id = f(t.name)",
	}, {
		input:  "CREATE VIEW v AS SELECT qty, price, qty*price AS value FROM t;",
		output: "create view v as select qty, price, qty * price as value from t",
	}, {
		input: "create view v_today (today) as select current_day from t",
	}, {
		input: "explain (analyze true,verbose false) select * from emp",
	}, {
		input: "select quarter from ontime limit 1",
	}, {
		input: "select month from ontime limit 1",
	}, {
		input: "with tw as (select * from t2), tf as (select * from t3) select * from tw where a > 1",
	}, {
		input: "with tw as (select * from t2) select * from tw where a > 1",
	}, {
		input:  "create table t (a double(13))  // comment",
		output: "create table t (a double(13))",
	}, {
		input: "select a as promo_revenue from (select * from r) as c_orders(c_custkey, c_count)",
	}, {
		input:  "select extract(year from l_shipdate) as l_year from t",
		output: "select extract(year, l_shipdate) as l_year from t",
	}, {
		input:  "select * from R join S on R.uid = S.uid where l_shipdate <= date '1998-12-01' - interval '112' day",
		output: "select * from r inner join s on r.uid = s.uid where l_shipdate <= date(1998-12-01) - interval(112, day)",
	}, {
		input: "create table deci_table (a decimal(10, 5))",
	}, {
		input: "create table deci_table (a decimal(20, 5))",
	}, {
		input:  "create table deci_table (a decimal)",
		output: "create table deci_table (a decimal(10))",
	}, {
		input: "create table deci_table (a decimal(20))",
	}, {
		input: "select substr(name, 5) from t1",
	}, {
		input: "select substring(name, 5) from t1",
	}, {
		input: "select substr(name, 5, 3) from t1",
	}, {
		input: "select substring(name, 5, 3) from t1",
	}, {
		input:  "select * from R join S on R.uid = S.uid",
		output: "select * from r inner join s on r.uid = s.uid",
	}, {
		input:  "create table t (a int, b char, key idx1 type zonemap (a, b))",
		output: "create table t (a int, b char, index idx1 using zonemap (a, b))",
	}, {
		input: "create table t (a int, index idx1 using zonemap (a))",
	}, {
		input: "create table t (a int, index idx1 using bsi (a))",
	}, {
		input:  "set @@sql_mode ='TRADITIONAL'",
		output: "set sql_mode = TRADITIONAL",
	}, {
		input:  "set @@session.sql_mode ='TRADITIONAL'",
		output: "set sql_mode = TRADITIONAL",
	}, {
		input:  "set session sql_mode ='TRADITIONAL'",
		output: "set sql_mode = TRADITIONAL",
	}, {
		input:  "select @session.tx_isolation",
		output: "select @session.tx_isolation",
	}, {
		input:  "select @@session.tx_isolation",
		output: "select @@tx_isolation",
	}, {
		input:  "/* mysql-connector-java-8.0.27 (Revision: e920b979015ae7117d60d72bcc8f077a839cd791) */SHOW VARIABLES;",
		output: "show variables",
	}, {
		input: "create index idx1 using bsi on a (a) ",
	}, {
		input:  "INSERT INTO pet VALUES row('Sunsweet05','Dsant05','otter','f',30.11,2), row('Sunsweet06','Dsant06','otter','m',30.11,3);",
		output: "insert into pet values (Sunsweet05, Dsant05, otter, f, 30.11, 2), (Sunsweet06, Dsant06, otter, m, 30.11, 3)",
	}, {
		input:  "INSERT INTO t1 SET f1 = -1.0e+30, f2 = 'exore', f3 = 123",
		output: "insert into t1 (f1, f2, f3) values (-1.0e+30, exore, 123)",
	}, {
		input:  "INSERT INTO t1 SET f1 = -1;",
		output: "insert into t1 (f1) values (-1)",
	}, {
		input:  "insert into t1 values (18446744073709551615), (0xFFFFFFFFFFFFFFFE), (18446744073709551613), (18446744073709551612)",
		output: "insert into t1 values (18446744073709551615), (0xfffffffffffffffe), (18446744073709551613), (18446744073709551612)",
	}, {
		input:  "create table t (a int) properties(\"host\" = \"127.0.0.1\", \"port\" = \"8239\", \"user\" = \"mysql_user\", \"password\" = \"mysql_passwd\")",
		output: "create table t (a int) properties(host = 127.0.0.1, port = 8239, user = mysql_user, password = mysql_passwd)",
	}, {
		input:  "create table t (a int) properties('a' = 'b')",
		output: "create table t (a int) properties(a = b)",
	}, {
		input:  "load data infile '/root/lineorder_flat_10.tbl' into table lineorder_flat FIELDS TERMINATED BY '' OPTIONALLY ENCLOSED BY '' LINES TERMINATED BY '';",
		output: "load data infile /root/lineorder_flat_10.tbl into table lineorder_flat fields terminated by \t optionally enclosed by \u0000 lines",
	}, {
		input: "create table t (a int, b char, check (1 + 1) enforced)",
	}, {
		input: "create table t (a int, b char, foreign key sdf (a, b) references b(a asc, b desc))",
	}, {
		input: "create table t (a int, b char, unique key idx (a, b))",
	}, {
		input: "create table t (a int, b char, index if not exists idx (a, b))",
	}, {
		input: "create table t (a int, b char, fulltext idx (a, b))",
	}, {
		input:  "create table t (a int, b char, constraint p1 primary key idx using hash (a, b))",
		output: "create table t (a int, b char, primary key p1 using none (a, b))",
	}, {
		input: "create table t (a int, b char, primary key idx (a, b))",
	}, {
		input:  "SET NAMES 'utf8mb4' COLLATE 'utf8mb4_general_ci'",
		output: "set names = utf8mb4 utf8mb4_general_ci",
	}, {
		input: "insert into cms values (null, default)",
	}, {
		input:  "create database `show`",
		output: "create database show",
	}, {
		input: "create table table16 (1a20 int, 1e int)",
	}, {
		input: "insert into t2 values (-3, 2)",
	}, {
		input:  "select spID,userID,score from t1 where spID>(userID-1);",
		output: "select spid, userid, score from t1 where spid > (userid - 1)",
	}, {
		input:  "CREATE TABLE t2(product VARCHAR(32),country_id INTEGER NOT NULL,year INTEGER,profit INTEGER)",
		output: "create table t2 (product varchar(32), country_id integer not null, year integer, profit integer)",
	}, {
		input: "insert into numtable values (255, 65535, 4294967295, 18446744073709551615)",
	}, {
		input: "create table numtable (a tinyint unsigned, b smallint unsigned, c int unsigned, d bigint unsigned)",
	}, {
		input:  "SELECT userID as user, MAX(score) as max FROM t1 GROUP BY userID order by user",
		output: "select userid as user, max(score) as max from t1 group by userid order by user",
	}, {
		input:  "load data local infile 'data' replace into table db.a (a, b, @vc, @vd) set a = @vc != 0, d = @vd != 1",
		output: "load data local infile data replace into table db.a (a, b, @vc, @vd) set a = @vc != 0, d = @vd != 1",
	}, {
		input: "load data local infile 'data' replace into table db.a lines starting by '#' terminated by '\t' ignore 2 lines",
		output: "load data local infile data replace into table db.a lines starting by # terminated by 	 ignore 2 lines",
	}, {
		input:  "load data infile 'data.txt' into table db.a fields terminated by '\t' escaped by '\t'",
		output: "load data infile data.txt into table db.a fields terminated by \t escaped by \t",
	}, {
		input:  "load data infile 'data.txt' into table db.a fields terminated by '\t' enclosed by '\t' escaped by '\t'",
		output: "load data infile data.txt into table db.a fields terminated by \t enclosed by \t escaped by \t",
	}, {
		input:  "load data infile 'data.txt' into table db.a",
		output: "load data infile data.txt into table db.a",
	}, {
		input:  "show tables from test01 where tables_in_test01 like '%t2%'",
		output: "show tables from test01 where tables_in_test01 like %t2%",
	}, {
		input:  "select userID,MAX(score) max_score from t1 where userID <2 || userID > 3 group by userID order by max_score",
		output: "select userid, max(score) as max_score from t1 where concat(userid < 2, userid > 3) group by userid order by max_score",
	}, {
		input: "select c1, -c2 from t2 order by -c1 desc",
	}, {
		input:  "select * from t1 where spID>2 AND userID <2 || userID >=2 OR userID < 2 limit 3",
		output: "select * from t1 where concat(spid > 2 and userid < 2, userid >= 2) or userid < 2 limit 3",
	}, {
		input:  "select * from t10 where (b='ba' or b='cb') and (c='dc' or c='ed');",
		output: "select * from t10 where (b = ba or b = cb) and (c = dc or c = ed)",
	}, {
		input:  "select CAST(userID AS DOUBLE) cast_double, CAST(userID AS FLOAT(3)) cast_float , CAST(userID AS REAL) cast_real, CAST(userID AS SIGNED) cast_signed, CAST(userID AS UNSIGNED) cast_unsigned from t1 limit 2",
		output: "select cast(userid as double) as cast_double, cast(userid as float(3)) as cast_float, cast(userid as real) as cast_real, cast(userid as signed) as cast_signed, cast(userid as unsigned) as cast_unsigned from t1 limit 2",
	}, {
		input: "select distinct name as name1 from t1",
	}, {
		input:  "select userID, userID DIV 2 as user_dir, userID%2 as user_percent, userID MOD 2 as user_mod from t1",
		output: "select userid, userid div 2 as user_dir, userid % 2 as user_percent, userid % 2 as user_mod from t1",
	}, {
		input:  "select sum(score) as sum from t1 where spID=6 group by score order by sum desc",
		output: "select sum(score) as sum from t1 where spid = 6 group by score order by sum desc",
	}, {
		input:  "select userID,count(score) from t1 where userID>2 group by userID having count(score)>1",
		output: "select userid, count(score) from t1 where userid > 2 group by userid having count(score) > 1",
	}, {
		input:  "SELECT product, SUM(profit),AVG(profit) FROM t2 where product<>'TV' GROUP BY product order by product asc",
		output: "select product, sum(profit), avg(profit) from t2 where product != TV group by product order by product asc",
	}, {
		input:  "SELECT product, SUM(profit),AVG(profit) FROM t2 where product='Phone' GROUP BY product order by product asc",
		output: "select product, sum(profit), avg(profit) from t2 where product = Phone group by product order by product asc",
	}, {
		input:  "select sum(col_1d),count(col_1d),avg(col_1d),min(col_1d),max(col_1d) from tbl1 group by col_1e",
		output: "select sum(col_1d), count(col_1d), avg(col_1d), min(col_1d), max(col_1d) from tbl1 group by col_1e",
	}, {
		input:  "select u.a, (select t.a from sa.t, u) from u, (select t.a, u.a from sa.t, u where t.a = u.a) as t where (u.a, u.b, u.c) in (select t.a, u.a, t.b * u.b tubb from t)",
		output: "select u.a, (select t.a from sa.t, u) from u, (select t.a, u.a from sa.t, u where t.a = u.a) as t where (u.a, u.b, u.c) in (select t.a, u.a, t.b * u.b as tubb from t)",
	}, {
		input: "select u.a, (select t.a from sa.t, u) from u",
	}, {
		input:  "select t.a, u.a, t.b * u.b from sa.t join u on t.c = u.c or t.d != u.d where t.a = u.a and t.b > u.b group by t.a, u.a, (t.a + u.b + v.b) having t.a = 11 and v.c > 1000 order by t.a desc, u.a asc, v.d asc, tubb limit 200 offset 100",
		output: "select t.a, u.a, t.b * u.b from sa.t inner join u on t.c = u.c or t.d != u.d where t.a = u.a and t.b > u.b group by t.a, u.a, (t.a + u.b + v.b) having t.a = 11 and v.c > 1000 order by t.a desc, u.a asc, v.d asc, tubb limit 200 offset 100",
	}, {
		input:  "select t.a, u.a, t.b * u.b from sa.t join u on t.c = u.c or t.d != u.d where t.a = u.a and t.b > u.b group by t.a, u.a, (t.a + u.b + v.b) having t.a = 11 and v.c > 1000",
		output: "select t.a, u.a, t.b * u.b from sa.t inner join u on t.c = u.c or t.d != u.d where t.a = u.a and t.b > u.b group by t.a, u.a, (t.a + u.b + v.b) having t.a = 11 and v.c > 1000",
	}, {
		input:  "select t.a, u.a, t.b * u.b from sa.t join u on t.c = u.c or t.d != u.d where t.a = u.a and t.b > u.b group by t.a, u.a, (t.a + u.b + v.b)",
		output: "select t.a, u.a, t.b * u.b from sa.t inner join u on t.c = u.c or t.d != u.d where t.a = u.a and t.b > u.b group by t.a, u.a, (t.a + u.b + v.b)",
	}, {
		input:  "SELECT t.a,u.a,t.b * u.b FROM sa.t join u on t.c = u.c or t.d != u.d where t.a = u.a and t.b > u.b",
		output: "select t.a, u.a, t.b * u.b from sa.t inner join u on t.c = u.c or t.d != u.d where t.a = u.a and t.b > u.b",
	}, {
		input: "select avg(u.a), count(u.b), cast(u.c as char) from u",
	}, {
		input: "select avg(u.a), count(*) from u",
	}, {
		input: "select avg(u.a), count(u.b) from u",
	}, {
		input: "select sum(col_1d) from tbl1 where col_1d < 13 group by col_1e",
	}, {
		input:  "select sum(col_1a),count(col_1b),avg(col_1c),min(col_1d),max(col_1d) from tbl1",
		output: "select sum(col_1a), count(col_1b), avg(col_1c), min(col_1d), max(col_1d) from tbl1",
	}, {
		input:  "insert into tbl1 values (0,1,5,11, \"a\")",
		output: "insert into tbl1 values (0, 1, 5, 11, a)",
	}, {
		input: "create table tbl1 (col_1a tinyint, col_1b smallint, col_1c int, col_1d bigint, col_1e char(10) not null)",
	}, {
		input: "insert into numtable values (4, 1.234567891, 1.234567891)",
	}, {
		input: "insert into numtable values (3, 1.234567, 1.234567)",
	}, {
		input: "create table numtable (id int, fl float, dl double)",
	}, {
		input: "drop table if exists numtable",
	}, {
		input:  "create table table17 (`index` int)",
		output: "create table table17 (index int)",
	}, {
		input: "create table table19$ (a int)",
	}, {
		input:  "create table `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa` (aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa int);",
		output: "create table aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa int)",
	}, {
		input:  "create table table12 (`a ` int)",
		output: "create table table12 (a  int)",
	}, {
		input:  "create table `table11 ` (a int)",
		output: "create table table11  (a int)",
	}, {
		input:  "create table table10 (a int primary key, b varchar(10)) checksum=0 COMMENT=\"asdf\"",
		output: "create table table10 (a int primary key, b varchar(10)) checksum = 0 comment = asdf",
	}, {
		input:  "create temporary table table05 ( a int, b char(10));",
		output: "create temporary table table05 (a int, b char(10))",
	}, {
		input:  "create table table15 (a varchar(5) default 'abcde')",
		output: "create table table15 (a varchar(5) default abcde)",
	}, {
		input:  "create table table01 (a TINYINT primary key, b SMALLINT SIGNED, c INT UNSIGNED, d BIGINT not null , e FLOAT unique,f DOUBLE, g CHAR(10), h VARCHAR(20))",
		output: "create table table01 (a tinyint primary key, b smallint, c int unsigned, d bigint not null, e float unique, f double, g char(10), h varchar(20))",
	}, {
		input:  "create database test04 CHARACTER SET=utf8 collate=utf8_general_ci ENCRYPTION='N'",
		output: "create database test04 character set utf8 collate utf8_general_ci encryption N",
	}, {
		input:  "create database test03 DEFAULT CHARACTER SET utf8 collate utf8_general_ci ENCRYPTION 'Y'",
		output: "create database test03 default character set utf8 collate utf8_general_ci encryption Y",
	}, {
		input: "drop database if exists t01234567890123456789012345678901234567890123456789012345678901234567890123456789",
	}, {
		input: "select distinct a from t",
	}, {
		input:  "select * from t where a like 'a%'",
		output: "select * from t where a like a%",
	}, {
		input: "select sysdate(), curtime(22) from t",
	}, {
		input: "select sysdate(), curtime from t",
	}, {
		input:  "select current_time(), current_timestamp, lacalTIMe(89), utc_time() from t",
		output: "select current_time(), current_timestamp(), lacaltime(89), utc_time() from t",
	}, {
		input:  "select current_user(), current_role(), current_date, utc_date from t",
		output: "select current_user(), current_role(), current_date(), utc_date() from t",
	}, {
		input: "select ascii(a), collation(b), hour(c), microsecond(d) from t",
	}, {
		input:  "select dayofmonth('2001-11-00'), month('2005-00-00') from t",
		output: "select dayofmonth(2001-11-00), month(2005-00-00) from t",
	}, {
		input: "select sum(distinct s) from tbl where 1",
	}, {
		input:  "select u.a, interval 1 second from t",
		output: "select u.a, interval(1, second) from t",
	}, {
		input: "select u.a, (select t.a from sa.t, u) from t where (u.a, u.b, u.c) in (select * from t)",
	}, {
		input: "select u.a, (select t.a from sa.t, u) from t where (u.a, u.b, u.c)",
	}, {
		input: "select u.a, (select t.a from sa.t, u) from u",
	}, {
		input: "select t.a from sa.t, u",
	}, {
		input: "select t.a from sa.t",
	}, {
		input: "create table a (a int) partition by key (a, b, db.t.c) (partition xx (subpartition s1, subpartition s3 max_rows = 1000 min_rows = 100))",
	}, {
		input: "create table a (a int) partition by key (a, b, db.t.c) (partition xx row_format = dynamic max_rows = 1000 min_rows = 100)",
	}, {
		input:  "create table a (a int) engine = 'innodb' row_format = dynamic comment = 'table A' compression = 'lz4' data directory = '/data' index directory = '/index' max_rows = 1000 min_rows = 100",
		output: "create table a (a int) engine = innodb row_format = dynamic comment = table A compression = lz4 data directory = /data index directory = /index max_rows = 1000 min_rows = 100",
	}, {
		input: "create table a (a int) partition by linear key algorithm = 3221 (a, b, db.t.c) (partition xx values less than (1, 2, 323), partition yy)",
	}, {
		input: "create table a (a int) partition by linear key algorithm = 3221 (a, b, db.t.c) partitions 10 subpartition by key (a, b, db.t.c) subpartitions 10",
	}, {
		input: "create table a (a int) partition by linear key algorithm = 3221 (a, b, db.t.c) partitions 10",
	}, {
		input: "create table a (a int) partition by linear hash (1 + 1234 / 32)",
	}, {
		input: "create table a (a int) partition by linear key algorithm = 31 (a, b, db.t.c)",
	}, {
		input: "create table a (a int) partition by linear key (a, b, db.t.c)",
	}, {
		input: "create table a (a int) partition by list columns (a, b, db.t.c)",
	}, {
		input: "create table a (a int) partition by list columns (a, b, db.t.c)",
	}, {
		input: "create table a (a int) partition by range columns (a, b, db.t.c)",
	}, {
		input: "create table a (a int) partition by range(1 + 21)",
	}, {
		input: "create table a (a int storage disk constraint cx check (b + c) enforced)",
	}, {
		input: "create table a (a int storage disk, b int references b(a asc, b desc) match full on delete cascade on update restrict)",
	}, {
		input: "create table a (a int storage disk, b int)",
	}, {
		input: "create table a (a int not null default 1 auto_increment unique primary key collate utf8_bin storage disk)",
	}, {
		input:  "grant all, all(a, b), create(a, b), select(a, b), super(a, b, c) on table db.A to u1, 'u2'@'h2', ''@'h3' with grant option",
		output: "grant all, all(a, b), create(a, b), select(a, b), super(a, b, c) on table db.a to u1, u2@h2, @h3 with grant option",
	}, {
		input: "grant proxy on u1 to u2, u3, u4 with grant option",
	}, {
		input: "grant proxy on u1 to u2, u3, u4",
	}, {
		input: "grant r1, r2, r3 to u1, u1, u3",
	}, {
		input:  "grant super(a, b, c) on procedure db.func to 'h3'",
		output: "grant super(a, b, c) on procedure db.func to h3",
	}, {
		input:  "revoke all, all(a, b), create(a, b), select(a, b), super(a, b, c) on table db.A from u1, 'u2'@'h2', ''@'h3'",
		output: "revoke all, all(a, b), create(a, b), select(a, b), super(a, b, c) on table db.a from u1, u2@h2, @h3",
	}, {
		input: "revoke r1, r2, r3 from u1, u2, u3",
	}, {
		input: "revoke super(a, b, c) on procedure db.func from h3",
	}, {
		input:  "revoke all on table db.A from u1, 'u2'@'h2', ''@'h3'",
		output: "revoke all on table db.a from u1, u2@h2, @h3",
	}, {
		input: "revoke all on table db.a from u1",
	}, {
		input: "set default role r1, r2, r3 to u1, u2, u3",
	}, {
		input: "set default role all to u1, u2, u3",
	}, {
		input: "set default role none to u1, u2, u3",
	}, {
		input: "set role all",
	}, {
		input: "set role none",
	}, {
		input: "set role r1, r2, r3",
	}, {
		input: "set role all except r1, r2, r3",
	}, {
		input:  "set password = password('ppp')",
		output: "set password = ppp",
	}, {
		input:  "set password for u1@h1 = password('ppp')",
		output: "set password for u1@h1 = ppp",
	}, {
		input:  "set password for u1@h1 = 'ppp'",
		output: "set password for u1@h1 = ppp",
	}, {
		input:  "set @a = 0, @b = 1",
		output: "set a = 0, b = 1",
	}, {
		input:  "set a = 0, session b = 1, @@session.c = 1, global d = 1, @@global.e = 1",
		output: "set a = 0, b = 1, c = 1, global d = 1, global e = 1",
	}, {
		input:  "set @@session.a = 1",
		output: "set a = 1",
	}, {
		input:  "set @@global.a = 1",
		output: "set global a = 1",
	}, {
		input: "set global a = 1",
	}, {
		input: "set a = 1",
	}, {
		input: "rollback",
	}, {
		input:  "rollback and chain no release",
		output: "rollback",
	}, {
		input:  "commit and chain no release",
		output: "commit",
	}, {
		input: "commit",
	}, {
		input: "start transaction read only",
	}, {
		input: "start transaction read write",
	}, {
		input: "start transaction",
	}, {
		input: "use db1",
	}, {
		input: "use",
	}, {
		input: "update a as aa set a = 3, b = 4 where a != 0 order by b limit 1",
	}, {
		input: "update a as aa set a = 3, b = 4",
	}, {
		input: "explain insert into u (a, b, c, d) values (1, 2, 3, 4), (5, 6, 7, 8)",
	}, {
		input: "explain delete from a where a != 0 order by b limit 1",
	}, {
		input: "explain select a from a union select b from b",
	}, {
		input: "explain select a from a",
	}, {
		input:  "explain (format text) select a from A",
		output: "explain (format text) select a from a",
	}, {
		input:  "explain analyze select * from t",
		output: "explain (analyze) select * from t",
	}, {
		input:  "explain format = 'tree' for connection 10",
		output: "explain format = tree for connection 10",
	}, {
		input: "explain db.a db.a.a",
	}, {
		input: "explain a",
	}, {
		input:  "alter user u1 require cipher 'xxx' subject 'yyy' with max_queries_per_hour 0 password expire interval 1 day password expire default account lock account unlock",
		output: "alter user u1 require cipher xxx subject yyy with max_queries_per_hour 0 password expire interval 1 day password expire default account lock account unlock",
	}, {
		input:  "alter user if exists user() identified by 'test'",
		output: "alter user if exists user() identified by test",
	}, {
		input: "show index from t where true",
	}, {
		input:  "show databases like 'a%'",
		output: "show databases like a%",
	}, {
		input: "show global status where 1 + 21 > 21",
	}, {
		input: "show global variables",
	}, {
		input: "show warnings",
	}, {
		input: "show errors",
	}, {
		input: "show full processlist",
	}, {
		input: "show processlist",
	}, {
		input:  "show full tables from db1 like 'a%' where a != 0",
		output: "show full tables from db1 like a% where a != 0",
	}, {
		input:  "show open tables from db1 like 'a%' where a != 0",
		output: "show open tables from db1 like a% where a != 0",
	}, {
		input:  "show tables from db1 like 'a%' where a != 0",
		output: "show tables from db1 like a% where a != 0",
	}, {
		input:  "show databases like 'a%' where a != 0",
		output: "show databases like a% where a != 0",
	}, {
		input: "show databases",
	}, {
		input:  "show extended full columns from t from db like 'a%'",
		output: "show extended full columns from t from db like a%",
	}, {
		input: "show extended full columns from t from db where a != 0",
	}, {
		input: "show columns from t from db where a != 0",
	}, {
		input: "show columns from t from db",
	}, {
		input: "show create database if not exists db",
	}, {
		input: "show create database db",
	}, {
		input: "show create table db.t1",
	}, {
		input: "show create table t1",
	}, {
		input: "drop user if exists u1, u2, u3",
	}, {
		input: "drop user u1",
	}, {
		input: "drop role r1",
	}, {
		input: "drop role if exists r1, r2, r3",
	}, {
		input: "drop index if exists idx1 on db.t",
	}, {
		input: "drop index idx1 on db.t",
	}, {
		input: "drop table if exists t1, t2, db.t",
	}, {
		input: "drop table db.t",
	}, {
		input: "drop table if exists t",
	}, {
		input: "drop database if exists t",
	}, {
		input: "drop database t",
	}, {
		input: "create user u1@'hostname'",
	}, {
		input: "create user u1",
	}, {
		input:  "create user if not exists u1 identified by 'u1', u2 require cipher 'xxx' subject 'yyy' with max_queries_per_hour 0",
		output: "create user if not exists u1 identified by u1, u2 require cipher xxx and subject yyy with max_queries_per_hour 0",
	}, {
		input:  "create role if not exists 'a'@'localhost', 'b'@'localhost'",
		output: "create role if not exists a@localhost, b@localhost",
	}, {
		input:  "create role if not exists 'webapp' @ \"identier\"",
		output: "create role if not exists webapp@identier",
	}, {
		input:  "create role 'admin', 'developer'",
		output: "create role admin, developer",
	}, {
		input:  "create index idx1 on a (a) KEY_BLOCK_SIZE 10 with parser x comment 'x' invisible",
		output: "create index idx1 on a (a) KEY_BLOCK_SIZE 10 with parser x comment x invisible",
	}, {
		input:  "create index idx1 using btree on A (a) KEY_BLOCK_SIZE 10 with parser x comment 'x' invisible",
		output: "create index idx1 using btree on a (a) KEY_BLOCK_SIZE 10 with parser x comment x invisible",
	}, {
		input: "create index idx1 on a (a)",
	}, {
		input: "create unique index idx1 using btree on a (a, b(10), (a + b), (a - b)) visible",
	}, {
		input:  "create database test_db default collate 'utf8mb4_general_ci' collate utf8mb4_general_ci",
		output: "create database test_db default collate utf8mb4_general_ci collate utf8mb4_general_ci",
	}, {
		input: "create database if not exists test_db character set geostd8",
	}, {
		input: "create database test_db default collate utf8mb4_general_ci",
	}, {
		input: "create database if not exists db",
	}, {
		input: "create database db",
	}, {
		input: "delete from a as aa",
	}, {
		input: "delete from t where a > 1 order by b limit 1 offset 2",
	}, {
		input: "delete from t where a = 1",
	}, {
		input: "insert into u partition(p1, p2) (a, b, c, d) values (1, 2, 3, 4), (5, 6, 1, 0)",
	}, {
		input:  "insert into t values ('aa', 'bb', 'cc')",
		output: "insert into t values (aa, bb, cc)",
	}, {
		input:  "insert into t() values (1, 2, 3)",
		output: "insert into t values (1, 2, 3)",
	}, {
		input: "insert into t (c1, c2, c3) values (1, 2, 3)",
	}, {
		input: "insert into t (c1, c2, c3) select c1, c2, c3 from t1",
	}, {
		input: "insert into t select c1, c2, c3 from t1",
	}, {
		input: "insert into t values (1, 3, 4)",
	}, {
		input:  "create table t1 (`show` bool(0));",
		output: "create table t1 (show bool(0))",
	}, {
		input:  "create table t1 (t bool(0));",
		output: "create table t1 (t bool(0))",
	}, {
		input: "create table t1 (t char(0))",
	}, {
		input: "create table t1 (t bool(20), b int, c char(20), d varchar(20))",
	}, {
		input: "create table t (a int(20) not null)",
	}, {
		input: "create table db.t (db.t.a int(20) null)",
	}, {
		input: "create table t (a float(20, 20) not null, b int(20) null, c int(30) null)",
	}, {
		input:  "create table t1 (t time(3) null, dt datetime(6) null, ts timestamp(1) null)",
		output: "create table t1 (t time(3) null, dt datetime(26, 6) null, ts timestamp(26, 1) null)",
	}, {
		input:  "create table t1 (a int default 1 + 1 - 2 * 3 / 4 div 7 ^ 8 << 9 >> 10 % 11)",
		output: "create table t1 (a int default 1 + 1 - 2 * 3 / 4 div 7 ^ 8 << 9 >> 10 % 11)",
	}, {
		input: "create table t1 (t bool default -1 + +1)",
	}, {
		input: "create table t (id int unique key)",
	}, {
		input: "select * from t",
	}, {
		input: "select c1, c2, c3 from t1, t as t2 where t1.c1 = 1 group by c2 having c2 > 10",
	}, {
		input: "select a from t order by a desc limit 1 offset 2",
	}, {
		input:  "select a from t order by a desc limit 1, 2",
		output: "select a from t order by a desc limit 2 offset 1",
	}, {
		input: "select * from t union select c from t1",
	}, {
		input: "select * from t union all select c from t1",
	}, {
		input: "select * from t union distinct select c from t1",
	}, {
		input: "select * from (select a from t) as t1",
	}, {
		input:  "select * from (select a from t) as t1 join t2 on 1",
		output: "select * from (select a from t) as t1 inner join t2 on 1",
	}, {
		input: "select * from (select a from t) as t1 inner join t2 using (a)",
	}, {
		input: "select * from (select a from t) as t1 cross join t2",
	}, {
		input:  "select * from t1 join t2 using (a, b, c)",
		output: "select * from t1 inner join t2 using (a, b, c)",
	}, {
		input: "select * from t1 straight_join t2 on 1 + 213",
	}, {
		input: "select * from t1 straight_join t2 on col",
	}, {
		input:  "select * from t1 right outer join t2 on 123",
		output: "select * from t1 right join t2 on 123",
	}, {
		input: "select * from t1 natural left join t2",
	}, {
		input: "select 1",
	}, {
		input: "select $ from t",
	}, {
		input:  "analyze table part (a,b )",
		output: "analyze table part(a, b)",
	}, {
		input:  "select $ from t into outfile '/Users/tmp/test'",
		output: "select $ from t into outfile /Users/tmp/test fields terminated by , enclosed by \" lines terminated by \n header true",
	}, {
		input:  "select $ from t into outfile '/Users/tmp/test' FIELDS TERMINATED BY ','",
		output: "select $ from t into outfile /Users/tmp/test fields terminated by , enclosed by \" lines terminated by \n header true",
	}, {
		input:  "select $ from t into outfile '/Users/tmp/test' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'",
		output: "select $ from t into outfile /Users/tmp/test fields terminated by , enclosed by \" lines terminated by \n header true",
	}, {
		input:  "select $ from t into outfile '/Users/tmp/test' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' header 'TRUE'",
		output: "select $ from t into outfile /Users/tmp/test fields terminated by , enclosed by \" lines terminated by \n header true",
	}, {
		input:  "select $ from t into outfile '/Users/tmp/test' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' header 'FALSE'",
		output: "select $ from t into outfile /Users/tmp/test fields terminated by , enclosed by \" lines terminated by \n header false",
	}, {
		input:  "select $ from t into outfile '/Users/tmp/test' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' header 'FALSE' MAX_FILE_SIZE 100",
		output: "select $ from t into outfile /Users/tmp/test fields terminated by , enclosed by \" lines terminated by \n header false max_file_size 102400",
	}, {
		input:  "select $ from t into outfile '/Users/tmp/test' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' header 'FALSE' MAX_FILE_SIZE 100 FORCE_QUOTE (a, b)",
		output: "select $ from t into outfile /Users/tmp/test fields terminated by , enclosed by \" lines terminated by \n header false max_file_size 102400 force_quote a, b",
	}, {
		input: "drop prepare stmt_name1",
	}, {
		input: "deallocate prepare stmt_name1",
	}, {
		input: "execute stmt_name1",
	}, {
		input: "execute stmt_name1 using @var_name,@@sys_name",
	}, {
		input: "prepare stmt_name1 from select * from t1",
	}, {
		input:  "prepare stmt_name1 from 'select * from t1'",
		output: "prepare stmt_name1 from select * from t1",
	}, {
		input: "prepare stmt_name1 from select * from t1 where a > ? or abs(b) < ?",
	}}
)

func TestValid(t *testing.T) {
	for _, tcase := range validSQL {
		if tcase.output == "" {
			tcase.output = tcase.input
		}
		ast, err := ParseOne(tcase.input)
		if err != nil {
			t.Errorf("Parse(%q) err: %v", tcase.input, err)
			continue
		}
		out := tree.String(ast, dialect.MYSQL)
		if tcase.output != out {
			t.Errorf("Parsing failed. \nExpected/Got:\n%s\n%s", tcase.output, out)
		}
	}
}

var (
	multiSQL = []struct {
		input  string
		output string
	}{{
		input:  "use db1; select * from t;",
		output: "use db1; select * from t",
	}, {
		input: "use db1; select * from t",
	}, {
		input: "use db1; select * from t; use db2; select * from t2",
	}}
)

func TestMulti(t *testing.T) {
	for _, tcase := range multiSQL {
		if tcase.output == "" {
			tcase.output = tcase.input
		}
		asts, err := Parse(tcase.input)
		if err != nil {
			t.Errorf("Parse(%q) err: %v", tcase.input, err)
			continue
		}
		var res string
		prefix := ""
		for _, ast := range asts {
			res += prefix
			out := tree.String(ast, dialect.MYSQL)
			res += out
			prefix = "; "
		}
		if tcase.output != res {
			t.Errorf("Parsing failed. \nExpected/Got:\n%s\n%s", tcase.output, res)
		}
	}
}
