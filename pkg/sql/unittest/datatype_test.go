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

// TestDateType will do test for Type date
func TestDateType(t *testing.T) {
	e, proc := newTestEngine()

	noError := []string{
		"create table tdate (a date);",
		"create table tdefdate (a date default '20211202')",
		"insert into tdate values ('20070210'), ('1997-02-10'), ('0001-04-28'), (20041112), ('0123-04-03');",
		"insert into tdefdate values ();",
	}
	selects := [][]string{
		{"tdate", "a\n\t[2007-02-10 1997-02-10 0001-04-28 2004-11-12 0123-04-03]-&{<nil>}\n\n"},
		{"tdefdate", "a\n\t2021-12-02\n\n"},
	}
	retError := [][]string{
		{"create table tble (a date);", ""},
		{"insert into tble values ('20201310')", "[22000]Incorrect date value"},
		{"insert into tble values ('20200631')", "[22000]Incorrect date value"},
		{"insert into tble values ('-3-5-3')", "[22000]Incorrect date value"},
	}

	test(t, e, proc, noError, selects, retError)
}

// TestDateComparison tests date type comparison operation
func TestDateComparison(t *testing.T) {
	e, proc := newTestEngine()
	noError := []string{
		"create table tdate (a date);",
		"create table tdate2 (a date, b date);",
		"insert into tdate values ('20070210'), ('1997-02-10'), ('2001-04-28'), (20041112), ('0123-04-03');",
		"insert into tdate2 values ('20050105', '20060105'), ('3001-08-25', 20041112);",
		"select * from tdate where a = '2004-01-01';",
		"select * from tdate where '2004-01-01' = a;",
		"select * from tdate where a > '2004-01-01';",
		"select * from tdate where '2004-01-01' > a;",
		"select * from tdate where a < '2004-01-01';",
		"select * from tdate where '2004-01-01' < a;",
		"select * from tdate where a >= '2004-01-01';",
		"select * from tdate where '2004-01-01' >= a;",
		"select * from tdate where a <= '2004-01-01';",
		"select * from tdate where '2004-01-01' <= a;",
		"select * from tdate where a != '20070210';",
		"select * from tdate where '20070210' != a;",
		"select * from tdate2 where a = b or a > b;",
		"select * from tdate2 where a < b or b >= a;",
		"select * from tdate2 where a <= b or b != a;",
	}
	retError := [][]string{
		{"select * from tdate where a <= '2004-14-01';", "[22000]Incorrect date value"},
	}
	test(t, e, proc, noError, nil, retError)
}

// TestDatetimeType will do test for Type datetime
func TestDatetimeType(t *testing.T) {
	e, proc := newTestEngine()

	// TestCase expected run success
	noError := []string{
		"create table tbl1 (a datetime);", // test datetime format without msec part
		"insert into tbl1 values ('2018-04-28 10:21:15'), ('2017-04-28 03:05:01'), (20250716163958), (20211203145633);",
		"create table tbl2 (a datetime);", // test datetime with msec part
		"insert into tbl2 values ('2018-04-28 10:21:15.123'), ('2017-04-28 03:05:01.456'), (20250716163958.567), (20211203145633.890);",
		"create table tbl3 (a datetime);", // test datetime without hour / minute / second
		"insert into tbl3 values ('20180428'), ('20170428'), (20250716), (20211203);",

		"create table tdatetimedef (a datetime default '2015-03-03 12:12:12');",
		"insert into tdatetimedef values ();",
	}
	// TestCase expected run failed
	retError := [][]string{
		{"create table tbl4 (a datetime);", ""},
		{"insert into tbl4 values ('-1-04-28 10:22:14');", "[22000]Incorrect datetime value"},
		{"insert into tbl4 values ('2010-13-28 10:22:14');", "[22000]Incorrect datetime value"},
		{"insert into tbl4 values ('2010-11-31 10:22:14');", "[22000]Incorrect datetime value"},
		{"insert into tbl4 values ('2010-11-30 24:22:14');", "[22000]Incorrect datetime value"},
		{"insert into tbl4 values ('2010-11-30 23:60:14');", "[22000]Incorrect datetime value"},
		{"insert into tbl4 values ('2010-11-30 23:59:60');", "[22000]Incorrect datetime value"},
		{"insert into tbl4 values ('1999-02-29 23:59:59');", "[22000]Incorrect datetime value"},
	}

	selects := [][]string{
		{"tbl1", "a\n\t[2018-04-28 10:21:15 2017-04-28 03:05:01 2025-07-16 16:39:58 2021-12-03 14:56:33]-&{<nil>}\n\n"},
		{"tbl2", "a\n\t[2018-04-28 10:21:15 2017-04-28 03:05:01 2025-07-16 16:39:58 2021-12-03 14:56:33]-&{<nil>}\n\n"}, // that is disputed. what does msec do?
		{"tbl3", "a\n\t[2018-04-28 00:00:00 2017-04-28 00:00:00 2025-07-16 00:00:00 2021-12-03 00:00:00]-&{<nil>}\n\n"},
		{"tdatetimedef", "a\n\t2015-03-03 12:12:12\n\n"},
	}

	test(t, e, proc, noError, selects, retError)
}

// TestDatetimeComparison tests date type comparison operation
func TestDatetimeComparison(t *testing.T) {
	e, proc := newTestEngine()
	noError := []string{
		"create table tdatetime (a datetime);",
		"create table tdatetime2 (a datetime, b datetime);",
		"insert into tdatetime values ('2018-04-28 10:21:15'), ('2017-04-28 03:05:01'), (20250716163958), (20211203145633);",
		"insert into tdatetime2 values ('2022-01-10 15:11:16', '2022-01-10 15:08:16'), ('2021-12-10 15:11:16', '2022-01-10 15:08:16');",
		"select * from tdatetime where a = '2020-01-01 10:21:15'",
		"select * from tdatetime where '2020-01-01 10:21:15' = a",
		"select * from tdatetime where a > '2020-01-01 10:21:15'",
		"select * from tdatetime where '2020-01-01 10:21:15' > a",
		"select * from tdatetime where a < '2020-01-01 10:21:15'",
		"select * from tdatetime where '2020-01-01 10:21:15' < a",
		"select * from tdatetime where a >= '2020-01-01 10:21:15'",
		"select * from tdatetime where '2020-01-01 10:21:15' >= a",
		"select * from tdatetime where a <= '2020-01-01 10:21:15'",
		"select * from tdatetime where '2020-01-01 10:21:15' <= a",
		"select * from tdatetime where a != '2020-01-01 10:21:15'",
		"select * from tdatetime where '2020-01-01 10:21:15' != a",
		"select * from tdatetime2 where a = b or b = a;",
		"select * from tdatetime2 where a > b;",
		"select * from tdatetime2 where b > a;",
		"select * from tdatetime2 where a < b;",
		"select * from tdatetime2 where b < a;",
		"select * from tdatetime2 where a >= b;",
		"select * from tdatetime2 where b >= a;",
		"select * from tdatetime2 where a <= b;",
		"select * from tdatetime2 where b <= a;",
		"select * from tdatetime2 where a != b or b != a;",
	}

	retError := [][]string{
		{"select * from tdatetime where a != '2020-01-01 10:21:61';", "[22000]Incorrect datetime value"},
	}
	test(t, e, proc, noError, nil, retError)
}
