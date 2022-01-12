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

package unittest

import "testing"

func TestDeDuplicationOperator(t *testing.T) {
	e, proc := newTestEngine()
	noErrors := []string{
		// data preparation
		"create table d_table1 (i1 tinyint, i2 smallint, i3 int, i4 bigint);",
		"create table d_table2 (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);",
		"create table d_table3 (f1 float, f2 double);",
		"create table d_table4 (d1 date, d2 datetime);",
		"insert into d_table1 values (1, 2, 3, 4), (1, 2, 3, 4), (-1, -2, -3, -4), (null, null, null, null);",
		"insert into d_table2 values (1, 2, 3, 4), (1, 2, 3, 4), (1, 2, 3, 4), (2, 3, 4, 5), (null, null, null, null);",
		"insert into d_table3 values (1.1, 2.2), (1.1, 2.2), (-1.1, -1.2), (-1.1, -1.2), (null, null);",
		"insert into d_table4 values ('2015-12-12', '2013-01-01 15:15:15'), ('2015-12-12', '2013-01-01 15:15:15'), (null, null);",
		// Test DeDuplication
		"select distinct i1, i2, i3, i4 from d_table1;",
		"select distinct u1, u2, u3, u4 from d_table2;",
		"select distinct f1, f2 from d_table3;",
		"select distinct d1, d2 from d_table4;",
	}
	test(t, e, proc, noErrors, nil, nil)
}

func TestLimitOperator(t *testing.T) {
	e, proc := newTestEngine()
	noErrors := []string{
		// data preparation
		"create table l_table (a int);",
		"insert into l_table values (1), (2), (3), (4), (5);",
		// Test Limit
		"select a from l_table limit 0;",
		"select a from l_table limit 3;",
		"select a from l_table limit 5;",
		"select a from l_table limit 6;",
	}
	retErrors := [][]string{
		{"select a from l_table limit a;", "[42000]Undeclared variable 'a'"},
		{"select a from l_table limit 0.5;", "[42000]Undeclared variable '0.5'"},
	}
	test(t, e, proc, noErrors, nil, retErrors)
}

func TestOffsetOperator(t *testing.T) {
	e, proc := newTestEngine()
	noErrors := []string{
		// data preparation
		"create table o_table (a int);",
		"insert into o_table values (1), (2), (3), (4), (5), (6);",
		// Test Offset
		"select a from o_table limit 0 offset 5;",
		"select a from o_table limit 6 offset 1;",
		"select a from o_table limit 0 offset 10;",
		"select a from o_table limit 1, 2;",
		"select * from o_table limit -1, -1;",
	}
	retErrors := [][]string{
		{"select a from o_table limit 0 offset a;", "[42000]Undeclared variable 'a'"},
		{"select a from o_table limit 0 offset 0.1;", "[42000]Undeclared variable '0.1'"},
	}
	test(t, e, proc, noErrors, nil, retErrors)
}

func TestOrderOperator(t *testing.T) {
	e, proc := newTestEngine()
	noErrors := []string{
		// data preparation
		"create table or_table1 (i1 tinyint, i2 smallint, i3 int, i4 bigint);",
		"create table or_table2 (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);",
		"create table or_table3 (f1 float, f2 double);",
		"create table or_table4 (d1 date, d2 datetime);",
		"insert into or_table1 values (1, 2, 3, 4), (1, 2, 3, 4), (-1, -2, -3, -4), (null, null, null, null);",
		"insert into or_table2 values (1, 2, 3, 4), (1, 2, 3, 4), (1, 2, 3, 4), (2, 3, 4, 5), (null, null, null, null);",
		"insert into or_table3 values (1.1, 2.2), (1.1, 2.2), (-1.1, -1.2), (-1.1, -1.2), (null, null);",
		"insert into or_table4 values ('2015-12-12', '2013-01-01 15:15:15'), ('2015-12-12', '2013-01-01 15:15:15'), (null, null);",
		// Test Order
		"select * from or_table1 order by i1;",
		"select * from or_table1 order by i2;",
		"select * from or_table1 order by i3;",
		"select * from or_table1 order by i4;",
		"select * from or_table2 order by u1;",
		"select * from or_table2 order by u2;",
		"select * from or_table2 order by u3;",
		"select * from or_table2 order by u4;",
		"select * from or_table3 order by f1;",
		"select * from or_table3 order by f2;",
		"select * from or_table4 order by d1;",
		"select * from or_table4 order by d2;",
		"select * from or_table1 order by i1, i2;",
	}
	retErrors := [][]string{
		{"select * from or_table1 order by i5;", "[42000]Column 'i5' doesn't exist"},
	}
	test(t, e, proc, noErrors, nil, retErrors)
}

func TestTopOperator(t *testing.T) {
	// SQL Server support top n
	// but Mysql use limit n to instead of it
}

func TestProjectionOperator(t *testing.T) {
	e, proc := newTestEngine()
	noErrors := []string{
		// data preparation
		"create table p_table1 (i1 tinyint, i2 smallint, i3 int, i4 bigint);",
		"create table p_table2 (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);",
		"create table p_table3 (f1 float, f2 double);",
		"create table p_table4 (d1 date, d2 datetime);",
		"insert into p_table1 values (1, 2, 3, 4), (1, 2, 3, 4), (-1, -2, -3, -4), (null, null, null, null);",
		"insert into p_table2 values (1, 2, 3, 4), (1, 2, 3, 4), (1, 2, 3, 4), (2, 3, 4, 5), (null, null, null, null);",
		"insert into p_table3 values (1.1, 2.2), (1.1, 2.2), (-1.1, -1.2), (-1.1, -1.2), (null, null);",
		"insert into p_table4 values ('2015-12-12', '2013-01-01 15:15:15'), ('2015-12-12', '2013-01-01 15:15:15'), (null, null);",
		// Test projection
		"select i1, i2, i3, i4 from p_table1;",
		"select u1, u2, u3, u4 from p_table2;",
		"select f1, f2 from p_table3;",
		"select d1, d2 from p_table4;",
		"select * from p_table1;",
		"select * from p_table2;",
		"select * from p_table3;",
		"select * from p_table4;",
		"select i1 as alias1, i2 as alias2, i3 as alias3, i4 as alias4 from p_table1;",
		"select u1 as alias1, u2 as alias2, u3 as alias3, u4 as alias4 from p_table2;",
		"select f1 as alias1, f2 as alias2 from p_table3;",
		"select d1 as alias1, d2 as alias2 from p_table4;",
		"select i1 * 2 from p_table1;",
	}
	retErrors := [][]string{
		{"select *, i5 from p_table1;", "[42000]Column 'i5' doesn't exist"},
		{"select i5 from p_table1;", "[42000]Column 'i5' doesn't exist"},
	}
	test(t, e, proc, noErrors, nil, retErrors)
}

func TestRestrictOperator(t *testing.T) {
	e, proc := newTestEngine()
	noErrors := []string{
		// data preparation
		"create table r_table1 (i1 tinyint, i2 smallint, i3 int, i4 bigint);",
		"create table r_table2 (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);",
		"create table r_table3 (f1 float, f2 double);",
		"insert into r_table1 values (1, 2, 3, 4), (1, 2, 3, 4), (-1, -2, -3, -4), (null, null, null, null);",
		"insert into r_table2 values (1, 2, 3, 4), (1, 2, 3, 4), (1, 2, 3, 4), (2, 3, 4, 5), (null, null, null, null);",
		"insert into r_table3 values (1.1, 2.2), (1.1, 2.2), (-1.1, -1.2), (-1.1, -1.2), (null, null);",

		"create table t1 (userID int, spID int, score int);",
		"insert into t1 values (1, 1, 30), (2, 1, 40), (3, 1, 50), (4, 2, 0), (5, 2, 100), (6, 3, 17);",
		// Test Restrict
		// 1. where
		"select * from r_table1 where i1 < 5;",
		"select * from r_table1 where i1 < 3 and i2 < 4;",
		"select * from r_table2 where u1 < 10 or u2 < 10 and u4 < 3;",
		// 2. having
		"select f1, sum(f2) from r_table3 group by f1 having sum(f2) < 5;",
		"select f1, sum(f2) from r_table3 group by f1 having sum(f2) < 5 and f1 != 1;",
		"select f2, max(f1) from r_table3 group by f2 having max(f1) > 10;",
		// 3. from compile_test.go
		"SELECT userID, MIN(score) FROM t1 GROUP BY userID;",
		"SELECT userID, MIN(score) FROM t1 GROUP BY userID ORDER BY userID asc;",
		"SELECT userID, SUM(score) FROM t1 GROUP BY userID ORDER BY userID desc;",
		"SELECT userID as a, MIN(score) as b FROM t1 GROUP BY userID;",
		"SELECT userID as user, MAX(score) as max FROM t1 GROUP BY userID order by user;",
		"SELECT userID as user, MAX(score) as max FROM t1 GROUP BY userID order by max desc;",
		"select userID,count(score) from t1 group by userID having count(score)>1;",
		"select userID,count(score) from t1 where userID>2 group by userID having count(score)>1;",
		"select userID,count(score) from t1 group by userID having count(score)>1;",
		"SELECT distinct userID, count(score) FROM t1 GROUP BY userID;",
		"select distinct sum(spID) from t1 group by userID;",
		"select distinct sum(spID) as sum from t1 group by userID order by sum asc;",
		"select distinct sum(spID) as sum from t1 where score>1 group by userID order by sum asc;",
		"select userID,MAX(score) from t1 where userID between 2 and 3 group by userID;",
		"select userID,MAX(score) from t1 where userID not between 2 and 3 group by userID order by userID desc;",
		"select sum(score) as sum from t1 where spID=6 group by score order by sum desc;",
		"select userID,MAX(score) max_score from t1 where userID <2 || userID > 3 group by userID order by max_score;",
	}
	retErrors := [][]string{
		{"select * from r_table1 where i5 < 10;", "[42000]Column 'i5' doesn't exist"},
		{"select f1, sum(f2) from r_table3 group by f1 having sum(f3) < 5;", "[42000]Column 'f3' doesn't exist"},
	}
	test(t, e, proc, noErrors, nil, retErrors)
}