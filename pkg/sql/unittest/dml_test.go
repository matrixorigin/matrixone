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

// TestInsertFunction to make sure insert and select work can run correctly
func TestInsertAndSelectFunction(t *testing.T) {
	testCases := []testCase{
		{sql: "create table iis(i1 tinyint, i2 smallint, i3 int, i4 bigint);"},
		{sql: "create table uus(u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);"},
		{sql: "create table ffs(f1 float, f2 double);"},
		{sql: "create table ccs(c1 char(10), c2 varchar(15));"},
		{sql: "create table dates(d1 date);"},
		{sql: "create table datetimes(dt1 datetime);"},
		{sql: "create table def1 (i1 int default 888, i2 int default 888, i3 int default 888);"},
		{sql: "create table def2 (id int default 1, name varchar(255) unique, age int);"},
		{sql: "create table def3 (i int default -1, v varchar(10) default 'abc', c char(10) default '', price double default 0.00);"},
		{sql: "create table def4 (d1 int, d2 int, d3 int, d4 int default 1);"},
		{sql: "insert into iis values (1, 2, 3, 4), (1+1, 2-2, 3*3, 4/4), (1 div 1, 2+2/3, 3 mod 3, 4 + 0.5), (0, 0, 0, 0);"},
		{sql: "insert into uus values (0, 0, 1, 1), (0.5, 3+4, 4-1, 2*7), (3/4, 4 div 5, 5 mod 6, 0);"},
		{sql: "insert into ffs values (1.1, 2.2), (1, 2), (1+0.5, 2.5*3.5);"},
		{sql: "insert into ccs values ('123', '34567');"},
		{sql: "insert into dates values ('1999-04-05'), ('2004-04-03');"},
		{sql: "insert into datetimes values ('1999-04-05 11:01:02'), ('2004-04-03 13:11:10');"},
		{sql: "insert into def1 values (default, default, default), (1, default, default), (default, -1, default), (default, default, 0);"},
		{sql: "insert into def2 (name, age) values ('Abby', 24);"},
		{sql: "insert into def3 () values (), ();"},
		{sql: "insert into def4 (d1, d2) values (1, 2);"},
		{sql: "create table cha1 (a char(0));"},
		{sql: "create table cha2 (a char);"},
		{sql: "insert into cha2 values ('1');"},
		{sql: "insert into cha1 values ('');"},
		{sql: "select * from iis;", res: executeResult{
			attr: []string{"i1", "i2", "i3", "i4"},
			data: [][]string{
				{"1", "2", "3", "4"},
				{"2", "0", "9", "1"},
				{"1", "3", "0", "5"},
				{"0", "0", "0", "0"},
			},
		}},
		{sql: "select i1 from iis;", res: executeResult{
			attr: []string{"i1"},
			data: [][]string{
				{"1"},
				{"2"},
				{"1"},
				{"0"},
			},
		}},
		{sql: "select * from uus;", res: executeResult{
			attr: []string{"u1", "u2", "u3", "u4"},
			data: [][]string{
				{"0", "0", "1", "1"},
				{"1", "7", "3", "14"},
				{"1", "0", "5", "0"},
			},
		}},
		{sql: "select u2 from uus;", res: executeResult{
			attr: []string{"u2"},
			data: [][]string{
				{"0"},
				{"7"},
				{"0"},
			},
		}},
		{sql: "select * from ffs;", res: executeResult{
			attr: []string{"f1", "f2"},
			data: [][]string{
				{"1.100000", "2.200000"},
				{"1.000000", "2.000000"},
				{"1.500000", "8.750000"},
			},
		}},
		{sql: "select f1 from ffs;", res: executeResult{
			attr: []string{"f1"},
			data: [][]string{
				{"1.100000"},
				{"1.000000"},
				{"1.500000"},
			},
		}},
		{sql: "select * from ccs;", res: executeResult{
			attr: []string{"c1", "c2"},
			data: [][]string{
				{"123", "34567"},
			},
		}},
		{sql: "select c1 from ccs;", res: executeResult{
			attr: []string{"c1"},
			data: [][]string{
				{"123"},
			},
		}},
		{sql: "select * from dates", res: executeResult{
			attr: []string{"d1"},
			data: [][]string{{"1999-04-05"}, {"2004-04-03"}},
		}},
		{sql: "select * from datetimes", res: executeResult{
			attr: []string{"dt1"},
			data: [][]string{{"1999-04-05 11:01:02"}, {"2004-04-03 13:11:10"}},
		}},
		{sql: "select * from def1;", res: executeResult{
			attr: []string{"i1", "i2", "i3"},
			data: [][]string{
				{"888", "888", "888"},
				{"1", "888", "888"},
				{"888", "-1", "888"},
				{"888", "888", "0"},
			},
		}},
		{sql: "select i1 from def1;", res: executeResult{
			attr: []string{"i1"},
			data: [][]string{
				{"888"},
				{"1"},
				{"888"},
				{"888"},
			},
		}},
		{sql: "select * from def2;", res: executeResult{
			attr: []string{"id", "name", "age"},
			data: [][]string{
				{"1", "Abby", "24"},
			},
		}},
		{sql: "select id from def2;", res: executeResult{
			attr: []string{"id"},
			data: [][]string{
				{"1"},
			},
		}},
		{sql: "select name from def2;", res: executeResult{
			attr: []string{"name"},
			data: [][]string{
				{"Abby"},
			},
		}},
		{sql: "select age from def2;", res: executeResult{
			attr: []string{"age"},
			data: [][]string{
				{"24"},
			},
		}},
		{sql: "select * from def3;", res: executeResult{
			attr: []string{"i", "v", "c", "price"},
			data: [][]string{
				{"-1", "abc", "", "0.000000"},
				{"-1", "abc", "", "0.000000"},
			},
		}},
		{sql: "select i from def3;", res: executeResult{
			attr: []string{"i"},
			data: [][]string{
				{"-1"},
				{"-1"},
			},
		}},
		{sql: "select v from def3;", res: executeResult{
			attr: []string{"v"},
			data: [][]string{
				{"abc"},
				{"abc"},
			},
		}},
		{sql: "select c from def3;", res: executeResult{
			attr: []string{"c"},
			data: [][]string{
				{""},
				{""},
			},
		}},
		{sql: "select price from def3;", res: executeResult{
			attr: []string{"price"},
			data: [][]string{
				{"0.000000"},
				{"0.000000"},
			},
		}},
		{sql: "select * from def4;", res: executeResult{
			attr: []string{"d1", "d2", "d3", "d4"},
			data: [][]string{
				{"1", "2", "null", "1"},
			},
		}},
		{sql: "select d1 from def4;", res: executeResult{
			attr: []string{"d1"},
			data: [][]string{
				{"1"},
			},
		}},
		{sql: "select d2 from def4;", res: executeResult{
			attr: []string{"d2"},
			data: [][]string{
				{"2"},
			},
		}},
		{sql: "select d4 from def4;", res: executeResult{
			attr: []string{"d4"},
			data: [][]string{
				{"1"},
			},
		}},

		{sql: "create table issue1660 (a int, b int);"},
		{sql: "insert into issue1660 values (0, 0), (1, 2);"},

		{sql: "create table t_issue1659 (a int, b int);"},
		{sql: "insert into t_issue1659 values (1, 2), (3, 4), (5, 6);"},

		{sql: "create table t_issue1653 (a int, b int);"},
		{sql: "insert into t_issue1653 values (1, 2), (3, 4), (5, 6);"},

		{sql: "create table t_issue1651 (a int);"},
		{sql: "insert into t_issue1651 values(1), (2), (3);"},

		{sql: "create table t_issue_1641 (id_2 int, col_varchar_2 char(511));"},
		{sql: "insert into t_issue_1641 (id_2, col_varchar_2) values (-0, 'false'), (1, '-0'), (-0, ''), (65535, 'false'), (1, ''), (-1, '-1'), (-1, '1'), (1, 'false'), (-1, '1'), (65535, ' '), (-1, '-1'), (-1, '2020-02-02 02:02:00'), (-1, '-0'), (65535, '-1'), (1, ' '), (1, '0000-00-00 00:00:00') ;"},

		{sql: "insert into cha1 values ('1');", err: "[22000]Data too long for column 'a' at row 1"},
		{sql: "insert into cha2 values ('21');", err: "[22000]Data too long for column 'a' at row 1"},
		{sql: "insert into iis (i1) values (128);", err: "[22000]Out of range value for column 'i1' at row 1"},
		{sql: "insert into iis (i1) values (-129);", err: "[22000]Out of range value for column 'i1' at row 1"},
		{sql: "insert into iis (i1) values (1), (128);", err: "[22000]Out of range value for column 'i1' at row 2"},
		{sql: "insert into iis (i2) values (32768);", err: "[22000]Out of range value for column 'i2' at row 1"},
		{sql: "insert into iis (i2) values (-32769);", err: "[22000]Out of range value for column 'i2' at row 1"},
		{sql: "insert into iis (i3) values (2147483648);", err: "[22000]Out of range value for column 'i3' at row 1"},
		{sql: "insert into iis (i3) values (-2147483649);", err: "[22000]Out of range value for column 'i3' at row 1"},
		{sql: "insert into uus (u1) values (-1);", err: "[22000]constant value out of range"},
		{sql: "insert into uus (u1) values (256);", err: "[22000]Out of range value for column 'u1' at row 1"},
		{sql: "insert into uus (u2) values (65536);", err: "[22000]Out of range value for column 'u2' at row 1"},
		{sql: "insert into uus (u3) values (4294967296);", err: "[22000]Out of range value for column 'u3' at row 1"},
	}
	test(t, testCases)
}

func TestCAQ(t *testing.T) {
	testCases := []testCase{
		// different table, equivalence-join, and the number of join-conditions is 1.
		{sql: "create table store (store_id int, store_area varchar(20), store_type int unsigned, incomes double, duration int);"},
		{sql: "create table input (store_id int, item_id int unsigned, item_num int, input_cost double);"},
		{sql: "create table output (store_id int, item_id int unsigned, guest_id int, item_num int, output_incomes double);"},
		{sql: "create table house (item_id int unsigned, item_num int);"},
		{sql: "insert into store (store_id, store_area, store_type, incomes, duration) values " +
			"(1, 'shanghai', 0, 2500, 16), (2, 'shanghai', 0, 70000, 40), (3, 'beijing', 1, 10000, 10), (4, 'shenzhen', 1, 0, 5);"},
		{sql: "insert into input (store_id, item_id, item_num, input_cost) values " +
			"(1, 100, 1000, 500), (1, 101, 30, 900), (1, 102, 40, 80), (2, 101, 500, 400), (3, 103, 20, 800), (3, 102, 5, 80), (4, 105, 1005, 2010);"},
		{sql: "insert into output (store_id, item_id, guest_id, item_num, output_incomes) values " +
			"(1, 100, 30001, 700, 700), (1, 102, 30001, 1, 20), (2, 101, 30003, 200, 500), (3, 102, 30002, 1, 10);"},
		{sql: "insert into house values (101, 5000), (102, 1000), (103, 5000), (104, 100), (105, 1);"},

		{sql: "select count(*) from store join input on store.store_id = input.store_id;", res: executeResult{
			attr: []string{"count(*)"},
			data: [][]string{
				{"7"},
			},
		}},

		{sql: "select sum(incomes) from store join input on store.store_id = input.store_id;", res: executeResult{
			attr: []string{"sum(incomes)"},
			data: [][]string{
				{"97500.000000"},
			},
		}},

		{sql: "select store_area, sum(incomes) from " +
			"store join input on store.store_id = input.store_id " +
			"group by store_area;",
			res: executeResult{
				attr: []string{"store_area", "sum(incomes)"},
				data: [][]string{
					{"shanghai", "77500.000000"},
					{"beijing", "20000.000000"},
					{"shenzhen", "0.000000"},
				},
			}},

		{sql: "select store_area, store_type, sum(incomes) from " +
			"store join input on store.store_id = input.store_id " +
			"group by store_area, store_type;",
			res: executeResult{
				attr: []string{"store_area", "store_type", "sum(incomes)"},
				data: [][]string{
					{"shanghai", "0", "77500.000000"},
					{"beijing", "1", "20000.000000"},
					{"shenzhen", "1", "0.000000"},
				},
			}},

		//{sql: "select store_area, store_type, item_id, sum(incomes) from " +
		//	"store join input on store.store_id = input.store_id " +
		//	"group by store_area, store_type, item_id;",
		//	res: executeResult{
		//	attr: []string{"store_area", "store_type", "item_id", "sum(incomes)"},
		//	data: [][]string{
		//		{"shanghai", "0", "100", "2500"},
		//		{"shanghai", "0", "101", "72500"},
		//		{"shanghai", "0", "102", "2500"},
		//		{"beijing", "1", "103", "10000.000000"},
		//		{"beijing", "1", "102", "10000.000000"},
		//		{"shenzhen", "1", "105", "0.000000"},
		//	},
		//}, com: "this will cause panic, and return `no possible`"},

		{sql: "select store_type, max(output_incomes) from" +
			" store join output on store.store_id = output.store_id" +
			" join house on house.item_id = output.item_id" +
			" group by store_type;",
			res: executeResult{
				attr: []string{"store_type", "max(output_incomes)"},
				data: [][]string{
					{"0", "500.000000"},
					{"1", "10.000000"},
				},
			}},

		{sql: "select store_type, max(output_incomes) from " +
			"store join output on store.store_id = output.store_id" +
			" join house on house.item_id = output.item_id " +
			"where store.store_id < 2 " +
			"group by store_type;",
			res: executeResult{
				attr: []string{"store_type", "max(output_incomes)"},
				data: [][]string{
					{"0", "20.000000"},
				},
			}},

		{sql: "select store_type, max(output_incomes) from " +
			"store join output on store.store_id = output.store_id " +
			"join house on house.item_id = output.item_id " +
			"group by store_type " +
			"having store_type < 1",
			res: executeResult{
				attr: []string{"store_type", "max(output_incomes)"},
				data: [][]string{
					{"0", "500.000000"},
				},
			}},
	}
	test(t, testCases)
}

func TestAQ(t *testing.T) {
	testCases := []testCase{
		{sql: "create table in_out (name varchar(40), age int unsigned, incomes int, expenses int);"},
		{sql: "insert into in_out values ('a', 20, 2500, 1300), ('b', 25, 5000, 800), ('c', 15, 0, 700), ('d', 50, 12000, 1000), ('e', 37, 22000, 7000);"},

		{sql: "select count(name) from in_out;", res: executeResult{
			attr: []string{"count(name)"},
			data: [][]string{
				{"5"},
			},
		}},

		{sql: "select count(*) from in_out;", res: executeResult{
			attr: []string{"count(*)"},
			data: [][]string{
				{"5"},
			},
		}},

		{sql: "select max(incomes), min(expenses) from in_out;", res: executeResult{
			attr: []string{"max(incomes)", "min(expenses)"},
			data: [][]string{
				{"22000", "700"},
			},
		}},

		{sql: "select min(incomes), max(expenses) from in_out;", res: executeResult{
			attr: []string{"min(incomes)", "max(expenses)"},
			data: [][]string{
				{"0", "7000"},
			},
		}},
	}
	test(t, testCases)
}
