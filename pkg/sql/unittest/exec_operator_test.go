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

import (
	"testing"
)

func TestDeDuplicationOperator(t *testing.T) {
	testCases := []testCase{
		{sql: "create table d_table1 (i1 tinyint, i2 smallint, i3 int, i4 bigint);"},
		{sql: "create table d_table2 (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);"},
		{sql: "create table d_table3 (f1 float, f2 double);"},
		{sql: "create table d_table4 (d1 date, d2 datetime);"},
		{sql: "create table d_table5 (c1 char(10), c2 varchar(10));"},
		{sql: "insert into d_table1 values (1, 2, 3, 4), (1, 2, 3, 4), (-1, -2, -3, -4)"},
		{sql: "insert into d_table2 values (1, 2, 3, 4), (1, 2, 3, 4), (1, 2, 3, 4), (2, 3, 4, 5);"},
		{sql: "insert into d_table3 values (1.1, 2.2), (1.1, 2.2), (-1.1, -1.2), (-1.1, -1.2);"},
		{sql: "insert into d_table4 values ('2015-12-12', '2013-01-01 15:15:15'), ('2015-12-12', '2013-01-01 15:15:15');"},
		{sql: "insert into d_table5 values ('abc', 'cba123'), ('abc', 'cba123'), ('abc ', 'cba123');"},

		{sql: "create table d_null_table0 (i int);"},
		{sql: "create table d_null_table1 (i1 tinyint, i2 smallint, i3 int, i4 bigint);"},
		{sql: "create table d_null_table2 (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);"},
		{sql: "create table d_null_table3 (f1 float, f2 double);"},
		{sql: "create table d_null_table4 (d1 date, d2 datetime);"},
		{sql: "create table d_null_table5 (c1 char(10), c2 varchar(10));"},
		{sql: "insert into d_null_table1 values (1, 2, 3, 4), (1, 2, 3, 4), (null, null, null, null), (null, null, null, null);"},
		{sql: "insert into d_null_table2 values (1, 2, 3, 4), (1, 2, 3, 4), (1, 2, 3, 4), (null, null, null, null), (null, null, null, null);"},
		{sql: "insert into d_null_table3 values (1.1, 2.2), (1.1, 2.2), (-1.1, -1.2), (-1.1, -1.2), (null, null), (null, null);"},
		{sql: "insert into d_null_table4 values ('2015-12-12', '2013-01-01 15:15:15'), ('2015-12-12', '2013-01-01 15:15:15'), (null, null), (null, null);"},
		{sql: "insert into d_null_table5 values ('abc', 'cba123'), ('abc', 'cba123'), (null, null), (null, null);"},

		{sql: "select distinct i1, i2, i3, i4 from d_table1;", res: executeResult{
			attr: []string{"i1", "i2", "i3", "i4"},
			data: [][]string{
				{"1", "2", "3", "4"},
				{"-1", "-2", "-3", "-4"},
			},
		}},
		{sql: "select distinct u1, u2, u3, u4 from d_table2;", res: executeResult{
			attr: []string{"u1", "u2", "u3", "u4"},
			data: [][]string{
				{"1", "2", "3", "4"},
				{"2", "3", "4", "5"},
			},
		}},
		{sql: "select distinct f1, f2 from d_table3;", res: executeResult{
			attr: []string{"f1", "f2"},
			data: [][]string{
				{"1.100000", "2.200000"},
				{"-1.100000", "-1.200000"},
			},
		}},
		{sql: "select distinct d1, d2 from d_table4;", res: executeResult{
			attr: []string{"d1", "d2"},
			data: [][]string{
				{"2015-12-12", "2013-01-01 15:15:15"},
			},
		}},
		{sql: "select distinct c1, c2 from d_table5;", res: executeResult{
			attr: []string{"c1", "c2"},
			data: [][]string{
				{"abc", "cba123"},
				{"abc ", "cba123"},
			},
		}},
		// todo: please add expect result for these query after deduplication supporting null values
		{sql: "select distinct i1, i2, i3, i4 from d_null_table1;"},
		{sql: "select distinct u1, u2, u3, u4 from d_null_table2;"},
		{sql: "select distinct f1, f2 from d_null_table3;"},
		{sql: "select distinct d1, d2 from d_null_table4;"},
		{sql: "select distinct c1, c2 from d_null_table5;"},
	}
	test(t, testCases)
}

func TestLimitOperator(t *testing.T) {
	testCases := []testCase{
		{sql: "create table l_table (a int);"},
		{sql: "insert into l_table values (1), (2), (3), (4), (5);"},
		{sql: "select a from l_table limit 0;", res: executeResult{
			null: true,
		}},
		{sql: "select a from l_table limit 3;", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"1"}, {"2"}, {"3"},
			},
		}},
		{sql: "select a from l_table limit 5;", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"1"}, {"2"}, {"3"}, {"4"}, {"5"},
			},
		}},
		{sql: "select a from l_table limit 6;", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"1"}, {"2"}, {"3"}, {"4"}, {"5"},
			},
		}},
		{sql: "select a from l_table limit a;", err: "[42000]Undeclared variable 'a'"},
		{sql: "select a from l_table limit 0.5;", err: "[42000]Undeclared variable '0.5'"},
	}
	test(t, testCases)
}

func TestOffsetOperator(t *testing.T) {
	testCases := []testCase{
		{sql: "create table o_table (a int);"},
		{sql: "insert into o_table values (1), (2), (3), (4), (5), (6);"},
		{sql: "select a from o_table limit 0 offset 5;", res: executeResult{
			null: true,
		}},
		{sql: "select a from o_table limit 6 offset 1;", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"2"}, {"3"}, {"4"}, {"5"}, {"6"},
			},
		}},
		{sql: "select a from o_table limit 0 offset 10;", res: executeResult{
			null: true,
		}},
		{sql: "select a from o_table limit 1, 2;", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"2"}, {"3"},
			},
		}, com: "`limit 1, 2` equals to `limit 2 offset 1`"},
		{sql: "select * from o_table limit -1, -1;", res: executeResult{
			data: [][]string{
				{"1"}, {"2"}, {"3"}, {"4"}, {"5"}, {"6"},
			},
		}, com: "in Mysql, -1 will return syntax error"}, // todo: we should return syntax error for this case
		{sql: "select a from o_table limit 0 offset a;", err: "[42000]Undeclared variable 'a'"},
		{sql: "select a from o_table limit 0 offset 0.1;", err: "[42000]Undeclared variable '0.1'"},
	}
	test(t, testCases)
}

func TestOrderOperator(t *testing.T) {
	testCases := []testCase{
		{sql: "create table or_table1 (i1 tinyint, i2 smallint, i3 int, i4 bigint);"},
		{sql: "create table or_table2 (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);"},
		{sql: "create table or_table3 (f1 float, f2 double);"},
		{sql: "create table or_table4 (d1 date, d2 datetime);"},
		{sql: "insert into or_table1 values (1, 2, 3, 4), (1, 2, 3, 4), (-1, -2, -3, -4), (null, null, null, null);"},
		{sql: "insert into or_table2 values (1, 2, 3, 4), (1, 2, 3, 4), (1, 2, 3, 4), (2, 3, 4, 5), (null, null, null, null);"},
		{sql: "insert into or_table3 values (1.1, 2.2), (1.1, 2.2), (-1.1, -1.2), (-1.1, -1.2), (null, null);"},
		{sql: "insert into or_table4 values ('2015-12-12', '2013-01-01 15:15:16'), ('2015-12-12', '2013-01-01 15:15:15'), (null, null);"},
		{sql: "select * from or_table1 order by i1 desc;", res: executeResult{
			attr: []string{"i1", "i2", "i3", "i4"},
			data: [][]string{
				{"1", "2", "3", "4"}, {"1", "2", "3", "4"}, {"null", "null", "null", "null"}, {"-1", "-2", "-3", "-4"},
			},
		}},
		{sql: "select * from or_table1 order by i2;", res: executeResult{
			attr: []string{"i1", "i2", "i3", "i4"},
			data: [][]string{
				{"-1", "-2", "-3", "-4"}, {"null", "null", "null", "null"}, {"1", "2", "3", "4"}, {"1", "2", "3", "4"},
			},
		}},
		{sql: "select * from or_table1 order by i3;", res: executeResult{
			attr: []string{"i1", "i2", "i3", "i4"},
			data: [][]string{
				{"-1", "-2", "-3", "-4"}, {"null", "null", "null", "null"}, {"1", "2", "3", "4"}, {"1", "2", "3", "4"},
			},
		}},
		{sql: "select * from or_table1 order by i4;", res: executeResult{
			attr: []string{"i1", "i2", "i3", "i4"},
			data: [][]string{
				{"-1", "-2", "-3", "-4"}, {"null", "null", "null", "null"}, {"1", "2", "3", "4"}, {"1", "2", "3", "4"},
			},
		}},
		{sql: "select * from or_table2 order by u1;", res: executeResult{
			attr: []string{"u1", "u2", "u3", "u4"},
			data: [][]string{
				{"null", "null", "null", "null"}, {"1", "2", "3", "4"}, {"1", "2", "3", "4"}, {"1", "2", "3", "4"}, {"2", "3", "4", "5"},
			},
		}},
		{sql: "select * from or_table2 order by u2;", res: executeResult{
			attr: []string{"u1", "u2", "u3", "u4"},
			data: [][]string{
				{"null", "null", "null", "null"}, {"1", "2", "3", "4"}, {"1", "2", "3", "4"}, {"1", "2", "3", "4"}, {"2", "3", "4", "5"},
			},
		}},
		{sql: "select * from or_table2 order by u3 desc;", res: executeResult{
			attr: []string{"u1", "u2", "u3", "u4"},
			data: [][]string{
				{"2", "3", "4", "5"}, {"1", "2", "3", "4"}, {"1", "2", "3", "4"}, {"1", "2", "3", "4"}, {"null", "null", "null", "null"},
			},
		}},
		{sql: "select * from or_table2 order by u4;", res: executeResult{
			attr: []string{"u1", "u2", "u3", "u4"},
			data: [][]string{
				{"null", "null", "null", "null"}, {"1", "2", "3", "4"}, {"1", "2", "3", "4"}, {"1", "2", "3", "4"}, {"2", "3", "4", "5"},
			},
		}},
		{sql: "select * from or_table3 order by f1;", res: executeResult{
			attr: []string{"f1", "f2"},
			data: [][]string{
				{"-1.100000", "-1.200000"}, {"-1.100000", "-1.200000"}, {"null", "null"}, {"1.100000", "2.200000"}, {"1.100000", "2.200000"},
			},
		}},
		{sql: "select * from or_table3 order by f2;", res: executeResult{
			attr: []string{"f1", "f2"},
			data: [][]string{
				{"-1.100000", "-1.200000"}, {"-1.100000", "-1.200000"}, {"null", "null"}, {"1.100000", "2.200000"}, {"1.100000", "2.200000"},
			},
		}},
		{sql: "select * from or_table4 order by d1;", res: executeResult{
			attr: []string{"d1", "d2"},
			data: [][]string{
				{"null", "null"}, {"2015-12-12", "2013-01-01 15:15:16"}, {"2015-12-12", "2013-01-01 15:15:15"},
			},
		}},
		{sql: "select * from or_table4 order by d2;", res: executeResult{
			attr: []string{"d1", "d2"},
			data: [][]string{
				{"null", "null"}, {"2015-12-12", "2013-01-01 15:15:15"}, {"2015-12-12", "2013-01-01 15:15:16"},
			},
		}},
		{sql: "select * from or_table1 order by i1, i2;", res: executeResult{
			attr: []string{"i1", "i2", "i3", "i4"},
			data: [][]string{
				{"-1", "-2", "-3", "-4"}, {"null", "null", "null", "null"}, {"1", "2", "3", "4"}, {"1", "2", "3", "4"},
			},
		}},
		{sql: "select * from or_table1 order by i5;", err: "[42000]Column 'i5' doesn't exist"},
	}
	test(t, testCases)
}

func TestTopOperator(t *testing.T) {
	// SQL Server support top n
	// but Mysql use limit n to instead of it
}

func TestProjectionOperator(t *testing.T) {
	testCases := []testCase{
		{sql: "create table p_table1 (i1 tinyint, i2 smallint, i3 int, i4 bigint);"},
		{sql: "create table p_table2 (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);"},
		{sql: "create table p_table3 (f1 float, f2 double);"},
		{sql: "create table p_table4 (d1 date, d2 datetime);"},
		{sql: "insert into p_table1 values (1, 2, 3, 4), (1, 2, 3, 4), (-1, -2, -3, -4), (null, null, null, null);"},
		{sql: "insert into p_table2 values (1, 2, 3, 4), (1, 2, 3, 4), (1, 2, 3, 4), (2, 3, 4, 5), (null, null, null, null);"},
		{sql: "insert into p_table3 values (1.1, 2.2), (1.1, 2.2), (-1.1, -1.2), (-1.1, -1.2), (null, null);"},
		{sql: "insert into p_table4 values ('2015-12-12', '2013-01-01 15:15:15'), ('2015-12-12', '2013-01-01 15:15:15'), (null, null);"},
		{sql: "select i1, i2, i3, i4 from p_table1;", res: executeResult{
			attr: []string{"i1", "i2", "i3", "i4"},
			data: [][]string{
				{"1", "2", "3", "4"},
				{"1", "2", "3", "4"},
				{"-1", "-2", "-3", "-4"},
				{"null", "null", "null", "null"},
			},
		}},
		{sql: "select u1, u2, u3, u4 from p_table2;", res: executeResult{
			attr: []string{"u1", "u2", "u3", "u4"},
			data: [][]string{
				{"1", "2", "3", "4"},
				{"1", "2", "3", "4"},
				{"1", "2", "3", "4"},
				{"2", "3", "4", "5"},
				{"null", "null", "null", "null"},
			},
		}},
		{sql: "select f1, f2 from p_table3;", res: executeResult{
			attr: []string{"f1", "f2"},
			data: [][]string{
				{"1.100000", "2.200000"},
				{"1.100000", "2.200000"},
				{"-1.100000", "-1.200000"},
				{"-1.100000", "-1.200000"},
				{"null", "null"},
			},
		}},
		{sql: "select d1, d2 from p_table4;", res: executeResult{
			attr: []string{"d1", "d2"},
			data: [][]string{
				{"2015-12-12", "2013-01-01 15:15:15"},
				{"2015-12-12", "2013-01-01 15:15:15"},
				{"null", "null"},
			},
		}},
		{sql: "select * from p_table1;", res: executeResult{
			attr: []string{"i1", "i2", "i3", "i4"},
			data: [][]string{
				{"1", "2", "3", "4"},
				{"1", "2", "3", "4"},
				{"-1", "-2", "-3", "-4"},
				{"null", "null", "null", "null"},
			},
		}},
		{sql: "select * from p_table2;", res: executeResult{
			attr: []string{"u1", "u2", "u3", "u4"},
			data: [][]string{
				{"1", "2", "3", "4"},
				{"1", "2", "3", "4"},
				{"1", "2", "3", "4"},
				{"2", "3", "4", "5"},
				{"null", "null", "null", "null"},
			},
		}},
		{sql: "select * from p_table3;", res: executeResult{
			attr: []string{"f1", "f2"},
			data: [][]string{
				{"1.100000", "2.200000"},
				{"1.100000", "2.200000"},
				{"-1.100000", "-1.200000"},
				{"-1.100000", "-1.200000"},
				{"null", "null"},
			},
		}},
		{sql: "select * from p_table4;", res: executeResult{
			attr: []string{"d1", "d2"},
			data: [][]string{
				{"2015-12-12", "2013-01-01 15:15:15"},
				{"2015-12-12", "2013-01-01 15:15:15"},
				{"null", "null"},
			},
		}},
		{sql: "select i1 as alias1, i2 as alias2, i3 as alias3, i4 as alias4 from p_table1;", res: executeResult{
			attr: []string{"alias1", "alias2", "alias3", "alias4"},
			data: [][]string{
				{"1", "2", "3", "4"},
				{"1", "2", "3", "4"},
				{"-1", "-2", "-3", "-4"},
				{"null", "null", "null", "null"},
			},
		}},
		{sql: "select u1 as alias1, u2 as alias2, u3 as alias3, u4 as alias4 from p_table2;", res: executeResult{
			attr: []string{"alias1", "alias2", "alias3", "alias4"},
			data: [][]string{
				{"1", "2", "3", "4"},
				{"1", "2", "3", "4"},
				{"1", "2", "3", "4"},
				{"2", "3", "4", "5"},
				{"null", "null", "null", "null"},
			},
		}},
		{sql: "select f1 as alias1, f2 as alias2 from p_table3;", res: executeResult{
			attr: []string{"alias1", "alias2"},
			data: [][]string{
				{"1.100000", "2.200000"},
				{"1.100000", "2.200000"},
				{"-1.100000", "-1.200000"},
				{"-1.100000", "-1.200000"},
				{"null", "null"},
			},
		}},
		{sql: "select d1 as alias1, d2 as alias2 from p_table4;", res: executeResult{
			attr: []string{"alias1", "alias2"},
			data: [][]string{
				{"2015-12-12", "2013-01-01 15:15:15"},
				{"2015-12-12", "2013-01-01 15:15:15"},
				{"null", "null"},
			},
		}},
		{sql: "select i1 * 2 from p_table1;", res: executeResult{
			attr: []string{"i1 * 2"},
			data: [][]string{
				{"2"}, {"2"}, {"-2"}, {"null"},
			},
		}},
		{sql: "select *, i5 from p_table1;", err: "[42000]Column 'i5' doesn't exist"},
		{sql: "select i5 from p_table1;", err: "[42000]Column 'i5' doesn't exist"},
	}
	test(t, testCases)
}

func TestRestrictOperator(t *testing.T) {
	testCases := []testCase{
		{sql: "create table r_table1 (i1 tinyint, i2 smallint, i3 int, i4 bigint);"},
		{sql: "create table r_table2 (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);"},
		{sql: "create table r_table3 (f1 float, f2 double);"},
		{sql: "insert into r_table1 values (1, 2, 3, 4), (1, 2, 3, 4), (-1, -2, -3, -4);"},
		{sql: "insert into r_table2 values (1, 2, 3, 4), (1, 2, 3, 4), (1, 2, 3, 4), (2, 3, 4, 5);"},
		{sql: "insert into r_table3 values (1.1, 2.2), (1.1, 2.2), (-1.1, -1.2), (-1.1, -1.2);"},

		{sql: "create table t1 (userID int, spID int, score int);"},
		{sql: "insert into t1 values (1, 1, 30), (2, 1, 40), (3, 1, 50), (4, 2, 0), (5, 2, 100), (6, 3, 17);"},

		{sql: "select * from r_table1 where i5 < 10;", err: "[42000]Column 'i5' doesn't exist"},
		{sql: "select f1, sum(f2) from r_table3 group by f1 having sum(f3) < 5;", err: "[42000]Column 'f3' doesn't exist"},

		// 1. where
		{sql: "select * from r_table1 where i1 < 5;", res: executeResult{
			data: [][]string{
				{"1", "2", "3", "4"},
				{"1", "2", "3", "4"},
				{"-1", "-2", "-3", "-4"},
			},
		}},
		{sql: "select * from r_table1 where i1 < 3 and i2 < 4;", res: executeResult{
			data: [][]string{
				{"1", "2", "3", "4"},
				{"1", "2", "3", "4"},
				{"-1", "-2", "-3", "-4"},
			},
		}},
		{sql: "select * from r_table2 where (u1 < 10 or u2 < 10) and u4 < 3;", res: executeResult{
			null: true,
		}},
		// 2. having
		{sql: "select f1, sum(f2) from r_table3 group by f1 having sum(f2) < 5;", res: executeResult{
			data: [][]string{
				{"1.100000", "4.400000"},
				{"-1.100000", "-2.400000"},
			},
		}},
		{sql: "select f1, sum(f2) from r_table3 group by f1 having sum(f2) < 5 and f1 != 1;", res: executeResult{
			data: [][]string{
				{"1.100000", "4.400000"}, {"-1.100000", "-2.400000"},
			},
		}},
		{sql: "select f2, max(f1) from r_table3 group by f2 having max(f1) > 10;", res: executeResult{
			null: true,
		}},
		// 3. from compile_test.go
		{sql: "SELECT userID, min(score) FROM t1 GROUP BY userID;", res: executeResult{
			attr: []string{"userID", "min(score)"},
			data: [][]string{
				{"1", "30"}, {"2", "40"}, {"3", "50"}, {"4", "0"}, {"5", "100"}, {"6", "17"},
			},
		}},
		{sql: "SELECT userID, MIN(score) FROM t1 GROUP BY userID ORDER BY userID asc;", res: executeResult{
			data: [][]string{
				{"1", "30"}, {"2", "40"}, {"3", "50"}, {"4", "0"}, {"5", "100"}, {"6", "17"},
			},
		}},
		{sql: "SELECT userID, SUM(score) FROM t1 GROUP BY userID ORDER BY userID desc;", res: executeResult{
			data: [][]string{
				{"6", "17"}, {"5", "100"}, {"4", "0"}, {"3", "50"}, {"2", "40"}, {"1", "30"},
			},
		}},
		{sql: "SELECT userID as a, MIN(score) as b FROM t1 GROUP BY userID;", res: executeResult{
			data: [][]string{
				{"1", "30"}, {"2", "40"}, {"3", "50"}, {"4", "0"}, {"5", "100"}, {"6", "17"},
			},
		}},
		{sql: "SELECT userID as user, MAX(score) as max FROM t1 GROUP BY userID order by user;", res: executeResult{
			data: [][]string{
				{"1", "30"}, {"2", "40"}, {"3", "50"}, {"4", "0"}, {"5", "100"}, {"6", "17"},
			},
		}},
		{sql: "SELECT userID as user, MAX(score) as max FROM t1 GROUP BY userID order by max desc;", res: executeResult{
			data: [][]string{
				{"5", "100"}, {"3", "50"}, {"2", "40"}, {"1", "30"}, {"6", "17"}, {"4", "0"},
			},
		}},
		{sql: "select userID,count(score) from t1 group by userID having count(score)>1;", res: executeResult{
			null: true,
		}},
		{sql: "select userID,count(score) from t1 where userID>2 group by userID having count(score)>1;", res: executeResult{
			null: true,
		}},
		{sql: "select userID,count(score) from t1 group by userID having count(score)>1;", res: executeResult{
			null: true,
		}},
		{sql: "SELECT distinct userID, count(score) FROM t1 GROUP BY userID;", res: executeResult{
			data: [][]string{
				{"1", "1"}, {"2", "1"}, {"3", "1"}, {"4", "1"}, {"5", "1"}, {"6", "1"},
			},
		}},
		{sql: "select distinct sum(spID) from t1 group by userID;", res: executeResult{
			attr: []string{"sum(spID)"},
			data: [][]string{
				{"1"}, {"2"}, {"3"},
			},
		}},
		{sql: "select distinct sum(spID) as sum from t1 group by userID order by sum asc;", res: executeResult{
			data: [][]string{
				{"1"}, {"2"}, {"3"},
			},
		}},
		{sql: "select distinct sum(spID) as sum from t1 where score>1 group by userID order by sum asc;", res: executeResult{
			data: [][]string{
				{"1"}, {"2"}, {"3"},
			},
		}},
		{sql: "select userID,MAX(score) from t1 where userID between 2 and 3 group by userID;", res: executeResult{
			data: [][]string{
				{"2", "40"}, {"3", "50"},
			},
		}},
		{sql: "select userID,MAX(score) from t1 where userID not between 2 and 3 group by userID order by userID desc;", res: executeResult{
			data: [][]string{
				{"6", "17"}, {"5", "100"}, {"4", "0"}, {"1", "30"},
			},
		}},
		{sql: "select sum(score) as sum from t1 where spID=6 group by score order by sum desc;", res: executeResult{
			null: true,
		}},
		{sql: "select userID,MAX(score) max_score from t1 where userID <2 || userID > 3 group by userID order by max_score;", res: executeResult{
			data: [][]string{
				{"4", "0"}, {"6", "17"}, {"1", "30"}, {"5", "100"},
			},
		}},
	}
	test(t, testCases)
}