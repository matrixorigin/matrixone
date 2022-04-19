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

func TestDecimalType(t *testing.T) {
	testCases := []testCase{
		{sql: "create table decimal_table (d1 decimal(10, 5));"},
		{sql: "create table decimal_table1 (d1 decimal(20, 5));"},
		{sql: "insert into decimal_table values (333.333);"},
		{sql: "insert into decimal_table1 values (333.333);"},
		{sql: "select * from decimal_table;", res: executeResult{
			attr: []string{"d1"},
			data: [][]string{{"33333300"}},
		}},
		{sql: "select * from decimal_table1;", res: executeResult{
			attr: []string{"d1"},
			data: [][]string{{"{33333300 0}"}},
		}},
	}
	test(t, testCases)
}

func TestDecimalComparison(t *testing.T) {
	testCases := []testCase{
		{sql: "create table decimal_table (d1 decimal(10, 5));"},
		{sql: "create table decimal_table1 (d1 decimal(20, 5));"},
		{sql: "insert into decimal_table values (333.333), (-1234.5), (5), (-5);"},
		{sql: "insert into decimal_table1 values (333.333), (-1234.5), (5), (-5);"},
		{sql: "select * from decimal_table where d1 > 1;", res: executeResult{
			attr: []string{"d1"},
			data: [][]string{{"33333300"}, {"500000"}},
		}},
		{sql: "select * from decimal_table1 where d1 > 1;", res: executeResult{
			attr: []string{"d1"},
			data: [][]string{{"{33333300 0}"}, {"{500000 0}"}},
		}},
		{sql: "select * from decimal_table where d1 >= 1;", res: executeResult{
			attr: []string{"d1"},
			data: [][]string{{"33333300"}, {"500000"}},
		}},
		{sql: "select * from decimal_table1 where d1 >= 1;", res: executeResult{
			attr: []string{"d1"},
			data: [][]string{{"{33333300 0}"}, {"{500000 0}"}},
		}},
		{sql: "select * from decimal_table where d1 < 1;", res: executeResult{
			attr: []string{"d1"},
			data: [][]string{{"-123450000"}, {"-500000"}},
		}},
		{sql: "select * from decimal_table1 where d1 < 1;", res: executeResult{
			attr: []string{"d1"},
			data: [][]string{[]string{"{-123450000 -1}"}, []string{"{-500000 -1}"}},
		}},
		{sql: "select * from decimal_table where d1 <= 1;", res: executeResult{
			attr: []string{"d1"},
			data: [][]string{{"-123450000"}, {"-500000"}},
		}},
		{sql: "select * from decimal_table1 where d1 <= 1;", res: executeResult{
			attr: []string{"d1"},
			data: [][]string{{"{-123450000 -1}"}, {"{-500000 -1}"}},
		}},
		{sql: "select * from decimal_table where d1 = 333.333;", res: executeResult{
			attr: []string{"d1"},
			data: [][]string{{"33333300"}},
		}},
		{sql: "select * from decimal_table1 where d1 = 333.333;", res: executeResult{
			attr: []string{"d1"},
			data: [][]string{{"{33333300 0}"}},
		}},
	}
	test(t, testCases)

}

// TestDateType will do test for Type date
func TestDateType(t *testing.T) {
	testCases := []testCase{
		{sql: "create table tdate (a date);"},
		{sql: "create table tdefdate (a date default '20211202')"},
		{sql: "create table tble (a date);"},
		{sql: "insert into tdate values ('20070210'), ('1997-02-10'), ('0001-04-28'), (20041112), ('0123-04-03');"},
		{sql: "insert into tdefdate values ();"},

		{sql: "select * from tdate;", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"2007-02-10"}, {"1997-02-10"}, {"0001-04-28"}, {"2004-11-12"}, {"0123-04-03"},
			},
		}},
		{sql: "select * from tdefdate;", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"2021-12-02"},
			},
		}},
		{sql: "insert into tble values ('20201310')", err: "[22000]Incorrect date value"},
		{sql: "insert into tble values ('20200631')", err: "[22000]Incorrect date value"},
		{sql: "insert into tble values ('-3-5-3')", err: "[22000]Incorrect date value"},
	}

	test(t, testCases)
}

// TestDateComparison tests date type comparison operation
func TestDateComparison(t *testing.T) {
	testCases := []testCase{
		{sql: "create table tdate (a date);"},
		{sql: "create table tdate2 (a date, b date);"},
		{sql: "insert into tdate values ('20070210'), ('1997-02-10'), ('2001-04-28'), (20041112), ('0123-04-03'), ('2004-01-01'), (null);"},
		{sql: "insert into tdate2 values ('20050105', '20050105'),('20050105', '20060105'), ('3001-08-25', 20041112);"},

		{sql: "select * from tdate where a = '2004-01-01';", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"2004-01-01"},
			},
		}},
		{sql: "select * from tdate where '2004-01-01' = a;", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"2004-01-01"},
			},
		}},
		{sql: "select * from tdate where a > '2004-01-01';", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"2007-02-10"}, {"2004-11-12"},
			},
		}},
		{sql: "select * from tdate where '2004-01-01' > a;", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"1997-02-10"}, {"2001-04-28"}, {"0123-04-03"},
			},
		}},
		{sql: "select * from tdate where a < '2004-01-01';", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"1997-02-10"}, {"2001-04-28"}, {"0123-04-03"},
			},
		}},
		{sql: "select * from tdate where '2004-01-01' < a;", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"2007-02-10"}, {"2004-11-12"},
			},
		}},
		{sql: "select * from tdate where a >= '2004-01-01';", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"2007-02-10"}, {"2004-11-12"}, {"2004-01-01"},
			},
		}},
		{sql: "select * from tdate where '2004-01-01' >= a;", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"1997-02-10"}, {"2001-04-28"}, {"0123-04-03"}, {"2004-01-01"},
			},
		}},
		{sql: "select * from tdate where a <= '2004-01-01';", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"1997-02-10"}, {"2001-04-28"}, {"0123-04-03"}, {"2004-01-01"},
			},
		}},
		{sql: "select * from tdate where '2004-01-01' <= a;", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"2007-02-10"}, {"2004-11-12"}, {"2004-01-01"},
			},
		}},
		{sql: "select * from tdate where a != '20070210';", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"1997-02-10"}, {"2001-04-28"}, {"2004-11-12"}, {"0123-04-03"}, {"2004-01-01"},
			},
		}},
		{sql: "select * from tdate where '20070210' != a;", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"1997-02-10"}, {"2001-04-28"}, {"2004-11-12"}, {"0123-04-03"}, {"2004-01-01"},
			},
		}},
		{sql: "select * from tdate2 where a = b or a > b;", res: executeResult{
			attr: []string{"a", "b"},
			data: [][]string{
				{"2005-01-05", "2005-01-05"}, {"3001-08-25", "2004-11-12"},
			},
		}},
		{sql: "select * from tdate2 where a < b or b >= a;", res: executeResult{
			attr: []string{"a", "b"},
			data: [][]string{
				{"2005-01-05", "2005-01-05"}, {"2005-01-05", "2006-01-05"},
			},
		}},
		{sql: "select * from tdate2 where a <= b or b != a;", res: executeResult{
			attr: []string{"a", "b"},
			data: [][]string{
				{"2005-01-05", "2005-01-05"}, {"2005-01-05", "2006-01-05"}, {"3001-08-25", "2004-11-12"},
			},
		}},
		{sql: "select * from tdate where a <= '2004-14-01';", err: "[22000]Incorrect date value"},
	}
	test(t, testCases)
}

// TestDatetimeType will do test for Type datetime
func TestDatetimeType(t *testing.T) {
	testCases := []testCase{
		{sql: "create table tbl1 (a datetime);"},
		{sql: "insert into tbl1 values ('2018-04-28 10:21:15'), ('2017-04-28 03:05:01'), (20250716163958), (20211203145633), (null);"},
		{sql: "create table tbl2 (a datetime);"},
		{sql: "insert into tbl2 values ('2018-04-28 10:21:15.123'), ('2017-04-28 03:05:01.456'), (20250716163958.567), (20211203145633.890), (null);"},
		{sql: "create table tbl3 (a datetime);"},
		{sql: "insert into tbl3 values ('20180428'), ('20170428'), (20250716), (20211203);"},
		{sql: "create table tdatetimedef (a datetime default '2015-03-03 12:12:12');"},
		{sql: "insert into tdatetimedef values ();"},
		{sql: "create table tbl4 (a datetime);"},

		{sql: "insert into tbl4 values ('-1-04-28 10:22:14');", err: "[22000]Incorrect datetime value"},
		{sql: "insert into tbl4 values ('2010-13-28 10:22:14');", err: "[22000]Incorrect datetime value"},
		{sql: "insert into tbl4 values ('2010-11-31 10:22:14');", err: "[22000]Incorrect datetime value"},
		{sql: "insert into tbl4 values ('2010-11-30 24:22:14');", err: "[22000]Incorrect datetime value"},
		{sql: "insert into tbl4 values ('2010-11-30 23:60:14');", err: "[22000]Incorrect datetime value"},
		{sql: "insert into tbl4 values ('2010-11-30 23:59:60');", err: "[22000]Incorrect datetime value"},
		{sql: "insert into tbl4 values ('1999-02-29 23:59:59');", err: "[22000]Incorrect datetime value"},
		{sql: "select * from tbl1;", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"2018-04-28 10:21:15"}, {"2017-04-28 03:05:01"}, {"2025-07-16 16:39:58"}, {"2021-12-03 14:56:33"}, {"null"},
			},
		}},
		{sql: "select * from tbl2;", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"2018-04-28 10:21:15"}, {"2017-04-28 03:05:01"}, {"2025-07-16 16:39:58"}, {"2021-12-03 14:56:33"}, {"null"},
			},
		}, com: "that is disputed. what does the msec do?"},
		{sql: "select * from tbl3;", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"2018-04-28 00:00:00"}, {"2017-04-28 00:00:00"}, {"2025-07-16 00:00:00"}, {"2021-12-03 00:00:00"},
			},
		}},
		{sql: "select * from tdatetimedef;", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"2015-03-03 12:12:12"},
			},
		}},
	}
	test(t, testCases)
}

// TestDatetimeComparison tests date type comparison operation
func TestDatetimeComparison(t *testing.T) {
	testCases := []testCase{
		{sql: "create table tdatetime (a datetime);"},
		{sql: "create table tdatetime2 (a datetime, b datetime);"},
		{sql: "insert into tdatetime values ('2018-04-28 10:21:15'), ('2017-04-28 03:05:01'), (20250716163958), (20211203145633);"},
		{sql: "insert into tdatetime2 values ('2022-01-10 15:11:16', '2022-01-10 15:08:16'), ('2021-12-10 15:11:16', '2022-01-10 15:08:16'), ('2022-01-10 15:08:16', '2022-01-10 15:08:16');"},
		{sql: "select * from tdatetime where a = '2020-01-01 10:21:15';", res: executeResult{
			null: true,
		}},
		{sql: "select * from tdatetime where '2020-01-01 10:21:15' = a;", res: executeResult{
			null: true,
		}},
		{sql: "select * from tdatetime where a > '2020-01-01 10:21:15'", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"2025-07-16 16:39:58"}, {"2021-12-03 14:56:33"},
			},
		}},
		{sql: "select * from tdatetime where '2020-01-01 10:21:15' > a", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"2018-04-28 10:21:15"}, {"2017-04-28 03:05:01"},
			},
		}},
		{sql: "select * from tdatetime where a < '2020-01-01 10:21:15'", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"2018-04-28 10:21:15"}, {"2017-04-28 03:05:01"},
			},
		}},
		{sql: "select * from tdatetime where '2020-01-01 10:21:15' < a", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"2025-07-16 16:39:58"}, {"2021-12-03 14:56:33"},
			},
		}},
		{sql: "select * from tdatetime where a >= '2021-12-03 14:56:33'", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"2025-07-16 16:39:58"}, {"2021-12-03 14:56:33"},
			},
		}},
		{sql: "select * from tdatetime where '2021-12-03 14:56:33' >= a", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"2018-04-28 10:21:15"}, {"2017-04-28 03:05:01"}, {"2021-12-03 14:56:33"},
			},
		}},
		{sql: "select * from tdatetime where a <= '2021-12-03 14:56:33';", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"2018-04-28 10:21:15"}, {"2017-04-28 03:05:01"}, {"2021-12-03 14:56:33"},
			},
		}},
		{sql: "select * from tdatetime where '2021-12-03 14:56:33' <= a", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"2025-07-16 16:39:58"}, {"2021-12-03 14:56:33"},
			},
		}},
		{sql: "select * from tdatetime where a != '2021-12-03 14:56:33'", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"2018-04-28 10:21:15"}, {"2017-04-28 03:05:01"}, {"2025-07-16 16:39:58"},
			},
		}},
		{sql: "select * from tdatetime where '2021-12-03 14:56:33' != a", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"2018-04-28 10:21:15"}, {"2017-04-28 03:05:01"}, {"2025-07-16 16:39:58"},
			},
		}},
		{sql: "select * from tdatetime where a != '2020-01-01 10:21:61';", err: "[22000]Incorrect datetime value"},
		{sql: "select * from tdatetime2 where a = b or b = a;", res: executeResult{
			attr: []string{"a", "b"},
			data: [][]string{
				{"2022-01-10 15:08:16", "2022-01-10 15:08:16"},
			},
		}},
		{sql: "select * from tdatetime2 where a > b;", res: executeResult{
			attr: []string{"a", "b"},
			data: [][]string{
				{"2022-01-10 15:11:16", "2022-01-10 15:08:16"},
			},
		}},
		{sql: "select * from tdatetime2 where b > a;", res: executeResult{
			attr: []string{"a", "b"},
			data: [][]string{
				{"2021-12-10 15:11:16", "2022-01-10 15:08:16"},
			},
		}},
		{sql: "select * from tdatetime2 where a < b;", res: executeResult{
			attr: []string{"a", "b"},
			data: [][]string{
				{"2021-12-10 15:11:16", "2022-01-10 15:08:16"},
			},
		}},
		{sql: "select * from tdatetime2 where b < a;", res: executeResult{
			attr: []string{"a", "b"},
			data: [][]string{
				{"2022-01-10 15:11:16", "2022-01-10 15:08:16"},
			},
		}},
		{sql: "select * from tdatetime2 where a >= b;", res: executeResult{
			attr: []string{"a", "b"},
			data: [][]string{
				{"2022-01-10 15:11:16", "2022-01-10 15:08:16"}, {"2022-01-10 15:08:16", "2022-01-10 15:08:16"},
			},
		}},
		{sql: "select * from tdatetime2 where b >= a;", res: executeResult{
			attr: []string{"a", "b"},
			data: [][]string{
				{"2021-12-10 15:11:16", "2022-01-10 15:08:16"}, {"2022-01-10 15:08:16", "2022-01-10 15:08:16"},
			},
		}},
		{sql: "select * from tdatetime2 where a <= b;", res: executeResult{
			attr: []string{"a", "b"},
			data: [][]string{
				{"2021-12-10 15:11:16", "2022-01-10 15:08:16"}, {"2022-01-10 15:08:16", "2022-01-10 15:08:16"},
			},
		}},
		{sql: "select * from tdatetime2 where b <= a;", res: executeResult{
			attr: []string{"a", "b"},
			data: [][]string{
				{"2022-01-10 15:11:16", "2022-01-10 15:08:16"}, {"2022-01-10 15:08:16", "2022-01-10 15:08:16"},
			},
		}},
		{sql: "select * from tdatetime2 where a != b or b != a;", res: executeResult{
			attr: []string{"a", "b"},
			data: [][]string{
				{"2022-01-10 15:11:16", "2022-01-10 15:08:16"}, {"2021-12-10 15:11:16", "2022-01-10 15:08:16"},
			},
		}},
	}
	test(t, testCases)
}
