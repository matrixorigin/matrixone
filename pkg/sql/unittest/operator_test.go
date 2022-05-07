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

// TestPlusOperator will run some sql to test + operator
func TestPlusOperator(t *testing.T) {
	testCases := []testCase{
		// data preparation
		{sql: "create table iis (i1 tinyint, i2 smallint, i3 int, i4 bigint);"},
		{sql: "create table iis2 (i11 tinyint, i12 tinyint, i21 smallint, i22 smallint, i31 int, i32 int, i41 bigint, i42 bigint);"},
		{sql: "create table ffs (f1 float, f2 double);"},
		{sql: "create table ffs2 (f11 float, f12 float, f21 double, f22 double);"},
		{sql: "create table uus (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);"},
		{sql: "create table uus2 (u11 tinyint unsigned, u12 tinyint unsigned, u21 smallint unsigned, u22 smallint unsigned, u31 int unsigned, u32 int unsigned, " +
			"u41 bigint unsigned, u42 bigint unsigned);"},
		{sql: "create table int_decimal (i1 tinyint, i2 smallint, i3 int, i4 bigint, d1 decimal(10, 5));"},
		{sql: "create table int_decimal1 (i1 tinyint, i2 smallint, i3 int, i4 bigint, d1 decimal(20, 5));"},

		{sql: "insert into iis values (1, 11, 111, 1111), (1, null, null, 1);"},
		{sql: "insert into iis2 values (1, 1, 22, 22, 333, 333, 4444, 4444);"},
		{sql: "insert into ffs values (22.2, 222.222), (null, null);"},
		{sql: "insert into ffs2 values (22.2, 22.2, 222.222, 222.222), (null, null, null, null);"},
		{sql: "insert into uus values (3, 33, 333, 3333), (null, null, null, null);"},
		{sql: "insert into uus2 values (1, 1, 22, 22, 33, 33, 444, 444);"},
		{sql: "insert into int_decimal values (1, 1, 22, 22, 333.333);"},
		{sql: "insert into int_decimal1 values (1, 1, 22, 22, 333.333);"},
		// table iis:
		// i1,	i2, i3, i4
		// 1,	11, 111, 1111
		// 1, 	null, null, 1

		// Test +
		{sql: "select i1 + i1, i1 + i2, i1 + i3, i1 + i4, i1 + 2, 3 + i1 from iis;", res: executeResult{
			null: false,
			attr: []string{"i1 + i1", "i1 + i2", "i1 + i3", "i1 + i4", "i1 + 2", "3 + i1"},
			data: [][]string{
				{"2", "12", "112", "1112", "3", "4"}, {"2", "null", "null", "2", "3", "4"},
			},
		}},
		{sql: "select i2 + i1, i2 + i2, i2 + i3, i2 + i4, i2 + 2, 3 + i2 from iis;", res: executeResult{
			null: false,
			attr: []string{"i2 + i1", "i2 + i2", "i2 + i3", "i2 + i4", "i2 + 2", "3 + i2"},
			data: [][]string{
				{"12", "22", "122", "1122", "13", "14"},
				{"null", "null", "null", "null", "null", "null"},
			},
		}},
		{sql: "select i3 + i1, i3 + i2, i3 + i3, i3 + i4, i3 + 2, 3 + i3 from iis;", res: executeResult{
			null: false,
			attr: []string{"i3 + i1", "i3 + i2", "i3 + i3", "i3 + i4", "i3 + 2", "3 + i3"},
			data: [][]string{{"112", "122", "222", "1222", "113", "114"}, {"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select i4 + i1, i4 + i2, i4 + i3, i4 + i4, i4 + 2, 3 + i4 from iis;", res: executeResult{
			null: false,
			attr: []string{"i4 + i1", "i4 + i2", "i4 + i3", "i4 + i4", "i4 + 2", "3 + i4"},
			data: [][]string{{"1112", "1122", "1222", "2222", "1113", "1114"}, {"2", "null", "null", "2", "3", "4"}},
		}},

		{sql: "select u1 + u1, u1 + u2, u1 + u3, u1 + u4, u1 + 2, 3 + u1 from uus;", res: executeResult{
			null: false,
			attr: []string{"u1 + u1", "u1 + u2", "u1 + u3", "u1 + u4", "u1 + 2", "3 + u1"},
			data: [][]string{{"6", "36", "336", "3336", "5", "6"}, {"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select u2 + u1, u2 + u2, u2 + u3, u2 + u4, u2 + 2, 3 + u2 from uus;", res: executeResult{
			null: false,
			attr: []string{"u2 + u1", "u2 + u2", "u2 + u3", "u2 + u4", "u2 + 2", "3 + u2"},
			data: [][]string{{"36", "66", "366", "3366", "35", "36"}, {"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select u3 + u1, u3 + u2, u3 + u3, u3 + u4, u3 + 2, 3 + u3 from uus;", res: executeResult{
			null: false,
			attr: []string{"u3 + u1", "u3 + u2", "u3 + u3", "u3 + u4", "u3 + 2", "3 + u3"},
			data: [][]string{{"336", "366", "666", "3666", "335", "336"}, {"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select u4 + u1, u4 + u2, u4 + u3, u4 + u4, u4 + 2, 3 + u4 from uus;", res: executeResult{
			null: false,
			attr: []string{"u4 + u1", "u4 + u2", "u4 + u3", "u4 + u4", "u4 + 2", "3 + u4"},
			data: [][]string{[]string{"3336", "3366", "3666", "6666", "3335", "3336"}, []string{"null", "null", "null", "null", "null", "null"}},
		}},

		{sql: "select f1 + f1, f1 - f1, f1 * f1, f1 / f1 from ffs;", res: executeResult{
			null: false,
			attr: []string{"f1 + f1", "f1 - f1", "f1 * f1", "f1 / f1"},
			data: [][]string{{"44.400002", "0.000000", "492.840027", "1.000000"}, {"null", "null", "null", "null"}},
		}},
		{sql: "select f1 + 1.0, 1.0 + f1, f1 - 2.0, 2.0 - f1, f1 * 3.0, 3.0 * f1, f1 / 0.5, 0.5 / f1 from ffs;", res: executeResult{
			null: false,
			attr: []string{"f1 + 1.0", "1.0 + f1", "f1 - 2.0", "2.0 - f1", "f1 * 3.0", "3.0 * f1", "f1 / 0.5", "0.5 / f1"},
			data: [][]string{{"23.200001", "23.200001", "20.200001", "-20.200001", "66.600002", "66.600002", "44.400002", "0.022523"}, {"null", "null", "null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select f2 + f2, f2 - f2, f2 * f2, f2 / f2 from ffs;", res: executeResult{
			null: false,
			attr: []string{"f2 + f2", "f2 - f2", "f2 * f2", "f2 / f2"},
			data: [][]string{{"444.444000", "0.000000", "49382.617284", "1.000000"}, []string{"null", "null", "null", "null"}},
		}},
		{sql: "select f2 + 1.0, 1.0 + f2, f2 - 2.0, 2.0 - f2, f2 * 3.0, 3.0 * f2, f2 / 0.5, 0.5 / f2 from ffs;", res: executeResult{
			null: false,
			attr: []string{"f2 + 1.0", "1.0 + f2", "f2 - 2.0", "2.0 - f2", "f2 * 3.0", "3.0 * f2", "f2 / 0.5", "0.5 / f2"},
			data: [][]string{[]string{"223.222000", "223.222000", "220.222000", "-220.222000", "666.666000", "666.666000", "444.444000", "0.002250"}, []string{"null", "null", "null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select i11 + i12, i21 + i22, i31 + i32, i41 + i42 from iis2;", res: executeResult{
			null: false,
			attr: []string{"i11 + i12", "i21 + i22", "i31 + i32", "i41 + i42"},
			data: [][]string{[]string{"2", "44", "666", "8888"}},
		}},
		{sql: "select i11 + i12, i21 + i22, i31 + i32, i41 + i42, i11, i21, i31, i41 from iis2;", res: executeResult{
			null: false,
			attr: []string{"i11 + i12", "i21 + i22", "i31 + i32", "i41 + i42", "i11", "i21", "i31", "i41"},
			data: [][]string{[]string{"2", "44", "666", "8888", "1", "22", "333", "4444"}},
		}},

		{sql: "select u11 + u12, u21 + u22, u31 + u32, u41 + u42 from uus2;", res: executeResult{
			null: false,
			attr: []string{"u11 + u12", "u21 + u22", "u31 + u32", "u41 + u42"},
			data: [][]string{[]string{"2", "44", "66", "888"}},
		}},
		{sql: "select u11 + u12, u21 + u22, u31 + u32, u41 + u42, u11, u21, u31, u41 from uus2;", res: executeResult{
			null: false,
			attr: []string{"u11 + u12", "u21 + u22", "u31 + u32", "u41 + u42", "u11", "u21", "u31", "u41"},
			data: [][]string{[]string{"2", "44", "66", "888", "1", "22", "33", "444"}},
		}},

		{sql: "select f11 + f12, f21 + f22 from ffs2;", res: executeResult{
			null: false,
			attr: []string{"f11 + f12", "f21 + f22"},
			data: [][]string{[]string{"44.400002", "444.444000"}, []string{"null", "null"}},
		}},
		{sql: "select f11 + f12, f21 + f22, f11, f21 from ffs2;", res: executeResult{
			null: false,
			attr: []string{"f11 + f12", "f21 + f22", "f11", "f21"},
			data: [][]string{[]string{"44.400002", "444.444000", "22.200001", "222.222000"}, []string{"null", "null", "null", "null"}},
		}},
		{sql: "select i1 + d1, i2 + d1, i3 + d1, i4 + d1, d1 + 1, d1 + 12.34, d1 + d1 from int_decimal;", res: executeResult{
			null: false,
			attr: []string{"i1 + d1", "i2 + d1", "i3 + d1", "i4 + d1", "d1 + 1", "d1 + 12.34", "d1 + d1"},
			data: [][]string{{"{33433300 0}", "{33433300 0}", "{35533300 0}", "{35533300 0}", "{33433300 0}", "{34567300 0}", "66666600"}},
		}},
		{sql: "select i1 + d1, i2 + d1, i3 + d1, i4 + d1, d1 + 1, d1 + 12.34, d1 + d1 from int_decimal1;", res: executeResult{
			null: false,
			attr: []string{"i1 + d1", "i2 + d1", "i3 + d1", "i4 + d1", "d1 + 1", "d1 + 12.34", "d1 + d1"},
			data: [][]string{{"{33433300 0}", "{33433300 0}", "{35533300 0}", "{35533300 0}", "{33433300 0}", "{34567300 0}", "{66666600 0}"}},
		}},
	}
	test(t, testCases)
}

// TestMinusOperator will run some sql to test - operator
func TestMinusOperator(t *testing.T) {

	testCases := []testCase{
		// data preparation
		{sql: "create table iis (i1 tinyint, i2 smallint, i3 int, i4 bigint);"},
		{sql: "create table iis2 (i11 tinyint, i12 tinyint, i21 smallint, i22 smallint, i31 int, i32 int, i41 bigint, i42 bigint);"},
		{sql: "create table ffs (f1 float, f2 double);"},
		{sql: "create table ffs2 (f11 float, f12 float, f21 double, f22 double);"},
		{sql: "create table uus (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);"},
		{sql: "create table uus2 (u11 tinyint unsigned, u12 tinyint unsigned, u21 smallint unsigned, u22 smallint unsigned, u31 int unsigned, u32 int unsigned, " +
			"u41 bigint unsigned, u42 bigint unsigned);"},
		{sql: "create table int_decimal (i1 tinyint, i2 smallint, i3 int, i4 bigint, d1 decimal(10, 5));"},
		{sql: "create table int_decimal1 (i1 tinyint, i2 smallint, i3 int, i4 bigint, d1 decimal(20, 5));"},

		{sql: "insert into iis values (1, 11, 111, 1111), (1, null, null, 1);"},
		{sql: "insert into iis2 values (1, 1, 22, 22, 333, 333, 4444, 4444);"},
		{sql: "insert into ffs values (22.2, 222.222), (null, null);"},
		{sql: "insert into ffs2 values (22.2, 22.2, 222.222, 222.222), (null, null, null, null);"},
		{sql: "insert into uus values (3, 33, 333, 3333), (null, null, null, null);"},
		{sql: "insert into uus2 values (1, 1, 22, 22, 33, 33, 444, 444);"},
		{sql: "insert into int_decimal values (1, 1, 22, 22, 333.333);"},
		{sql: "insert into int_decimal1 values (1, 1, 22, 22, 333.333);"},
		// Test -
		{sql: "select i1 - i1, i1 - i2, i1 - i3, i1 - i4, i1 - 2, 3 - i1 from iis;", res: executeResult{
			null: false,
			attr: []string{"i1 - i1", "i1 - i2", "i1 - i3", "i1 - i4", "i1 - 2", "3 - i1"},
			data: [][]string{[]string{"0", "-10", "-110", "-1110", "-1", "2"}, []string{"0", "null", "null", "0", "-1", "2"}},
		}},
		{sql: "select i2 - i1, i2 - i2, i2 - i3, i2 - i4, i2 - 2, 3 - i2 from iis;", res: executeResult{
			null: false,
			attr: []string{"i2 - i1", "i2 - i2", "i2 - i3", "i2 - i4", "i2 - 2", "3 - i2"},
			data: [][]string{[]string{"10", "0", "-100", "-1100", "9", "-8"}, []string{"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select i3 - i1, i3 - i2, i3 - i3, i3 - i4, i3 - 2, 3 - i3 from iis;", res: executeResult{
			null: false,
			attr: []string{"i3 - i1", "i3 - i2", "i3 - i3", "i3 - i4", "i3 - 2", "3 - i3"},
			data: [][]string{[]string{"110", "100", "0", "-1000", "109", "-108"}, []string{"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select i4 - i1, i4 - i2, i4 - i3, i4 - i4, i4 - 2, 3 - i4 from iis;", res: executeResult{
			null: false,
			attr: []string{"i4 - i1", "i4 - i2", "i4 - i3", "i4 - i4", "i4 - 2", "3 - i4"},
			data: [][]string{[]string{"1110", "1100", "1000", "0", "1109", "-1108"}, []string{"0", "null", "null", "0", "-1", "2"}},
		}},

		{sql: "select u1 - u1, u1 - u2, u1 - u3, u1 - u4, u1 - 2, 3 - u1 from uus;", res: executeResult{
			null: false,
			attr: []string{"u1 - u1", "u1 - u2", "u1 - u3", "u1 - u4", "u1 - 2", "3 - u1"},
			data: [][]string{[]string{"0", "65506", "4294966966", "18446744073709548286", "1", "0"}, []string{"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select u2 - u1, u2 - u2, u2 - u3, u2 - u4, u2 - 2, 3 - u2 from uus;", res: executeResult{
			null: false,
			attr: []string{"u2 - u1", "u2 - u2", "u2 - u3", "u2 - u4", "u2 - 2", "3 - u2"},
			data: [][]string{[]string{"30", "0", "4294966996", "18446744073709548316", "31", "-30"}, []string{"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select u3 - u1, u3 - u2, u3 - u3, u3 - u4, u3 - 2, 3 - u3 from uus;", res: executeResult{
			null: false,
			attr: []string{"u3 - u1", "u3 - u2", "u3 - u3", "u3 - u4", "u3 - 2", "3 - u3"},
			data: [][]string{{"330", "300", "0", "18446744073709548616", "331", "-330"}, []string{"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select u4 - u1, u4 - u2, u4 - u3, u4 - u4, u4 - 2, 3 - u4 from uus;", res: executeResult{
			null: false,
			attr: []string{"u4 - u1", "u4 - u2", "u4 - u3", "u4 - u4", "u4 - 2", "3 - u4"},
			data: [][]string{{"3330", "3300", "3000", "0", "3331", "-3330"}, []string{"null", "null", "null", "null", "null", "null"}},
		}},

		{sql: "select f11 - f12, f21 - f22 from ffs2;", res: executeResult{
			null: false,
			attr: []string{"f11 - f12", "f21 - f22"},
			data: [][]string{[]string{"0.000000", "0.000000"}, []string{"null", "null"}},
		}},
		{sql: "select f11 - f12, f21 - f22, f11, f21 from ffs2;", res: executeResult{
			null: false,
			attr: []string{"f11 - f12", "f21 - f22", "f11", "f21"},
			data: [][]string{{"0.000000", "0.000000", "22.200001", "222.222000"}, []string{"null", "null", "null", "null"}},
		}},

		{sql: "select i11 - i12, i21 - i22, i31 - i32, i41 - i42 from iis2;", res: executeResult{
			null: false,
			attr: []string{"i11 - i12", "i21 - i22", "i31 - i32", "i41 - i42"},
			data: [][]string{[]string{"0", "0", "0", "0"}},
		}},
		{sql: "select i11 - i12, i21 - i22, i31 - i32, i41 - i42, i11, i21, i31, i41 from iis2;", res: executeResult{
			null: false,
			attr: []string{"i11 - i12", "i21 - i22", "i31 - i32", "i41 - i42", "i11", "i21", "i31", "i41"},
			data: [][]string{[]string{"0", "0", "0", "0", "1", "22", "333", "4444"}},
		}},

		{sql: "select u11 - u12, u21 - u22, u31 - u32, u41 - u42 from uus2;", res: executeResult{
			null: false,
			attr: []string{"u11 - u12", "u21 - u22", "u31 - u32", "u41 - u42"},
			data: [][]string{[]string{"0", "0", "0", "0"}},
		}},
		{sql: "select u11 - u12, u21 - u22, u31 - u32, u41 - u42, u11, u21, u31, u41 from uus2;", res: executeResult{
			null: false,
			attr: []string{"u11 - u12", "u21 - u22", "u31 - u32", "u41 - u42", "u11", "u21", "u31", "u41"},
			data: [][]string{[]string{"0", "0", "0", "0", "1", "22", "33", "444"}},
		}},
		{sql: "select i1 - d1, i2 - d1, i3 - d1, i4 - d1, d1 - 1, d1 - 12.34, d1 - d1 from int_decimal;", res: executeResult{
			null: false,
			attr: []string{"i1 - d1", "i2 - d1", "i3 - d1", "i4 - d1", "d1 - 1", "d1 - 12.34", "d1 - d1"},
			data: [][]string{{"{-33233300 -1}", "{-33233300 -1}", "{-31133300 -1}", "{-31133300 -1}", "{33233300 0}", "{32099300 0}", "0"}},
		}},
		{sql: "select i1 - d1, i2 - d1, i3 - d1, i4 - d1, d1 - 1, d1 - 12.34, d1 - d1 from int_decimal1;", res: executeResult{
			null: false,
			attr: []string{"i1 - d1", "i2 - d1", "i3 - d1", "i4 - d1", "d1 - 1", "d1 - 12.34", "d1 - d1"},
			data: [][]string{{"{-33233300 -1}", "{-33233300 -1}", "{-31133300 -1}", "{-31133300 -1}", "{33233300 0}", "{32099300 0}", "{0 0}"}},
		}},
	}
	test(t, testCases)
}

// TestMultiOperator will run some sql to test * operator
func TestMultiOperator(t *testing.T) {
	testCases := []testCase{
		// data preparation
		{sql: "create table iis (i1 tinyint, i2 smallint, i3 int, i4 bigint);"},
		{sql: "create table iis2 (i11 tinyint, i12 tinyint, i21 smallint, i22 smallint, i31 int, i32 int, i41 bigint, i42 bigint);"},
		{sql: "create table ffs (f1 float, f2 double);"},
		{sql: "create table ffs2 (f11 float, f12 float, f21 double, f22 double);"},
		{sql: "create table uus (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);"},
		{sql: "create table uus2 (u11 tinyint unsigned, u12 tinyint unsigned, u21 smallint unsigned, u22 smallint unsigned, u31 int unsigned, u32 int unsigned, " +
			"u41 bigint unsigned, u42 bigint unsigned);"},
		{sql: "create table int_decimal (i1 tinyint, i2 smallint, i3 int, i4 bigint, d1 decimal(10, 5));"},
		{sql: "create table int_decimal1 (i1 tinyint, i2 smallint, i3 int, i4 bigint, d1 decimal(20, 5));"},

		{sql: "insert into iis values (1, 11, 111, 1111), (1, null, null, 1);"},
		{sql: "insert into iis2 values (1, 1, 22, 22, 333, 333, 4444, 4444);"},
		{sql: "insert into ffs values (22.2, 222.222), (null, null);"},
		{sql: "insert into ffs2 values (22.2, 22.2, 222.222, 222.222), (null, null, null, null);"},
		{sql: "insert into uus values (3, 33, 333, 3333), (null, null, null, null);"},
		{sql: "insert into uus2 values (1, 1, 22, 22, 33, 33, 444, 444);"},
		{sql: "insert into int_decimal values (1, 1, 22, 22, 333.333);"},
		{sql: "insert into int_decimal1 values (1, 1, 22, 22, 333.333);"},

		// Test *
		{sql: "select i1 * i1, i1 * i2, i1 * i3, i1 * i4, i1 * 2, 3 * i1 from iis;", res: executeResult{
			null: false,
			attr: []string{"i1 * i1", "i1 * i2", "i1 * i3", "i1 * i4", "i1 * 2", "3 * i1"},
			data: [][]string{{"1", "11", "111", "1111", "2", "3"}, {"1", "null", "null", "1", "2", "3"}},
		}},
		{sql: "select i2 * i1, i2 * i2, i2 * i3, i2 * i4, i2 * 2, 3 * i2 from iis;", res: executeResult{
			null: false,
			attr: []string{"i2 * i1", "i2 * i2", "i2 * i3", "i2 * i4", "i2 * 2", "3 * i2"},
			data: [][]string{{"11", "121", "1221", "12221", "22", "33"}, {"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select i3 * i1, i3 * i2, i3 * i3, i3 * i4, i3 * 2, 3 * i3 from iis;", res: executeResult{
			null: false,
			attr: []string{"i3 * i1", "i3 * i2", "i3 * i3", "i3 * i4", "i3 * 2", "3 * i3"},
			data: [][]string{[]string{"111", "1221", "12321", "123321", "222", "333"}, []string{"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select i4 * i1, i4 * i2, i4 * i3, i4 * i4, i4 * 2, 3 * i4 from iis;", res: executeResult{
			null: false,
			attr: []string{"i4 * i1", "i4 * i2", "i4 * i3", "i4 * i4", "i4 * 2", "3 * i4"},
			data: [][]string{{"1111", "12221", "123321", "1234321", "2222", "3333"}, {"1", "null", "null", "1", "2", "3"}},
		}},

		{sql: "select u1 * u1, u1 * u2, u1 * u3, u1 * u4, u1 * 2, 3 * u1 from uus;", res: executeResult{
			null: false,
			data: [][]string{[]string{"9", "99", "999", "9999", "6", "9"}, []string{"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select u2 * u1, u2 * u2, u2 * u3, u2 * u4, u2 * 2, 3 * u2 from uus;", res: executeResult{
			null: false,
			data: [][]string{[]string{"99", "1089", "10989", "109989", "66", "99"}, []string{"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select u3 * u1, u3 * u2, u3 * u3, u3 * u4, u3 * 2, 3 * u3 from uus;", res: executeResult{
			null: false,
			data: [][]string{[]string{"999", "10989", "110889", "1109889", "666", "999"}, []string{"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select u4 * u1, u4 * u2, u4 * u3, u4 * u4, u4 * 2, 3 * u4 from uus;", res: executeResult{
			null: false,
			data: [][]string{[]string{"9999", "109989", "1109889", "11108889", "6666", "9999"}, []string{"null", "null", "null", "null", "null", "null"}},
		}},

		{sql: "select f11 * f12, f21 * f22 from ffs2;", res: executeResult{
			null: false,
			data: [][]string{[]string{"492.840027", "49382.617284"}, []string{"null", "null"}},
		}},
		{sql: "select f11 * f12, f21 * f22, f11, f21 from ffs2;", res: executeResult{
			null: false,
			data: [][]string{[]string{"492.840027", "49382.617284", "22.200001", "222.222000"}, []string{"null", "null", "null", "null"}},
		}},

		{sql: "select i11 * i12, i21 * i22, i31 * i32, i41 * i42 from iis2;", res: executeResult{
			null: false,
			data: [][]string{[]string{"1", "484", "110889", "19749136"}},
		}},
		{sql: "select i11 * i12, i21 * i22, i31 * i32, i41 * i42, i11, i21, i31, i41 from iis2;", res: executeResult{
			null: false,
			data: [][]string{[]string{"1", "484", "110889", "19749136", "1", "22", "333", "4444"}},
		}},

		{sql: "select u11 * u12, u21 * u22, u31 * u32, u41 * u42 from uus2;", res: executeResult{
			null: false,
			data: [][]string{[]string{"1", "484", "1089", "197136"}},
		}},
		{sql: "select u11 * u12, u21 * u22, u31 * u32, u41 * u42, u11, u21, u31, u41 from uus2;", res: executeResult{
			null: false,
			data: [][]string{{"1", "484", "1089", "197136", "1", "22", "33", "444"}},
		}},
		{sql: "select i1 * d1, i2 * d1, i3 * d1, i4 * d1, d1 * 1, d1 * 12.34, d1 * d1 from int_decimal;", res: executeResult{
			null: false,
			attr: []string{"i1 * d1", "i2 * d1", "i3 * d1", "i4 * d1", "d1 * 1", "d1 * 12.34", "d1 * d1"},
			data: [][]string{{"{33333300 0}", "{33333300 0}", "{733332600 0}", "{733332600 0}", "{33333300 0}", "{41133292200 0}", "{1111108888890000 0}"}},
		}},
		{sql: "select i1 * d1, i2 * d1, i3 * d1, i4 * d1, d1 * 1, d1 * 12.34, d1 * d1 from int_decimal1;", res: executeResult{
			null: false,
			attr: []string{"i1 * d1", "i2 * d1", "i3 * d1", "i4 * d1", "d1 * 1", "d1 * 12.34", "d1 * d1"},
			data: [][]string{{"{33333300 0}", "{33333300 0}", "{733332600 0}", "{733332600 0}", "{33333300 0}", "{41133292200 0}", "{1111108888890000 0}"}},
		}},
	}
	test(t, testCases)
}

// TestDivOperator will run some sql to test / and div(integer div) operator
func TestDivOperator(t *testing.T) {
	testCases := []testCase{
		// data preparation
		{sql: "create table iis (i1 tinyint, i2 smallint, i3 int, i4 bigint);"},
		{sql: "create table iis2 (i11 tinyint, i12 tinyint, i21 smallint, i22 smallint, i31 int, i32 int, i41 bigint, i42 bigint);"},
		{sql: "create table ffs (f1 float, f2 double);"},
		{sql: "create table ffs2 (f11 float, f12 float, f21 double, f22 double);"},
		{sql: "create table uus (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);"},
		{sql: "create table uus2 (u11 tinyint unsigned, u12 tinyint unsigned, u21 smallint unsigned, u22 smallint unsigned, u31 int unsigned, u32 int unsigned, " +
			"u41 bigint unsigned, u42 bigint unsigned);"},
		{sql: "create table int_decimal (i1 tinyint, i2 smallint, i3 int, i4 bigint, d1 decimal(10, 5));"},
		{sql: "create table int_decimal1 (i1 tinyint, i2 smallint, i3 int, i4 bigint, d1 decimal(20, 5));"},

		{sql: "insert into iis values (1, 11, 111, 1111), (1, null, null, 1);"},
		{sql: "insert into iis2 values (1, 1, 22, 22, 333, 333, 4444, 4444);"},
		{sql: "insert into ffs values (22.2, 222.222), (null, null);"},
		{sql: "insert into ffs2 values (22.2, 22.2, 222.222, 222.222), (null, null, null, null);"},
		{sql: "insert into uus values (3, 33, 333, 3333), (null, null, null, null);"},
		{sql: "insert into uus2 values (1, 1, 22, 22, 33, 33, 444, 444);"},
		{sql: "insert into int_decimal values (1, 1, 22, 22, 333.333);"},
		{sql: "insert into int_decimal1 values (1, 1, 22, 22, 333.333);"},

		// Test `/` and `div`
		{sql: "select i1 / i1, i1 / i2, i1 / i3, i1 / i4, i1 / 2, 3 / i1 from iis;", res: executeResult{
			null: false,
			attr: []string{"i1 / i1", "i1 / i2", "i1 / i3", "i1 / i4", "i1 / 2", "3 / i1"},
			data: [][]string{[]string{"1.000000", "0.090909", "0.009009", "0.000900", "0.500000", "3.000000"}, []string{"1.000000", "null", "null", "1.000000", "0.500000", "3.000000"}},
		}},
		{sql: "select i2 / i1, i2 / i2, i2 / i3, i2 / i4, i2 / 2, 3 / i2 from iis;", res: executeResult{
			null: false,
			attr: []string{"i2 / i1", "i2 / i2", "i2 / i3", "i2 / i4", "i2 / 2", "3 / i2"},
			data: [][]string{[]string{"11.000000", "1.000000", "0.099099", "0.009901", "5.500000", "0.272727"}, []string{"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select i3 / i1, i3 / i2, i3 / i3, i3 / i4, i3 / 2, 3 / i3 from iis;", res: executeResult{
			null: false,
			attr: []string{"i3 / i1", "i3 / i2", "i3 / i3", "i3 / i4", "i3 / 2", "3 / i3"},
			data: [][]string{{"111.000000", "10.090909", "1.000000", "0.099910", "55.500000", "0.027027"}, []string{"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select i4 / i1, i4 / i2, i4 / i3, i4 / i4, i4 / 2, 3 / i4 from iis;", res: executeResult{
			null: false,
			attr: []string{"i4 / i1", "i4 / i2", "i4 / i3", "i4 / i4", "i4 / 2", "3 / i4"},
			data: [][]string{[]string{"1111.000000", "101.000000", "10.009009", "1.000000", "555.500000", "0.002700"}, []string{"1.000000", "null", "null", "1.000000", "0.500000", "3.000000"}},
		}},

		{sql: "select i1 div i1, i1 div i2, i1 div i3, i1 div i4, i1 div 2, 3 div i1 from iis;", res: executeResult{
			null: false,
			data: [][]string{[]string{"1", "0", "0", "0", "0", "3"}, []string{"1", "null", "null", "1", "0", "3"}},
		}},
		{sql: "select i2 div i1, i2 div i2, i2 div i3, i2 div i4, i2 div 2, 3 div i2 from iis;", res: executeResult{
			null: false,
			data: [][]string{[]string{"11", "1", "0", "0", "5", "0"}, []string{"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select i3 div i1, i3 div i2, i3 div i3, i3 div i4, i3 div 2, 3 div i3 from iis;", res: executeResult{
			null: false,
			data: [][]string{[]string{"111", "10", "1", "0", "55", "0"}, []string{"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select i4 div i1, i4 div i2, i4 div i3, i4 div i4, i4 div 2, 3 div i4 from iis;", res: executeResult{
			null: false,
			data: [][]string{[]string{"1111", "101", "10", "1", "555", "0"}, []string{"1", "null", "null", "1", "0", "3"}},
		}},

		{sql: "select u1 / u1, u1 / u2, u1 / u3, u1 / u4, u1 / 2, 3 / u1 from uus;", res: executeResult{
			null: false,
			data: [][]string{[]string{"1.000000", "0.090909", "0.009009", "0.000900", "1.500000", "1.000000"}, []string{"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select u2 / u1, u2 / u2, u2 / u3, u2 / u4, u2 / 2, 3 / u2 from uus;", res: executeResult{
			null: false,
			data: [][]string{[]string{"11.000000", "1.000000", "0.099099", "0.009901", "16.500000", "0.090909"}, []string{"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select u3 / u1, u3 / u2, u3 / u3, u3 / u4, u3 / 2, 3 / u3 from uus;", res: executeResult{
			null: false,
			data: [][]string{[]string{"111.000000", "10.090909", "1.000000", "0.099910", "166.500000", "0.009009"}, []string{"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select u4 / u1, u4 / u2, u4 / u3, u4 / u4, u4 / 2, 3 / u4 from uus;", res: executeResult{
			null: false,
			data: [][]string{[]string{"1111.000000", "101.000000", "10.009009", "1.000000", "1666.500000", "0.000900"}, []string{"null", "null", "null", "null", "null", "null"}},
		}},

		{sql: "select u1 div u1, u1 div u2, u1 div u3, u1 div u4, u1 div 2, 3 div u1 from uus;", res: executeResult{
			null: false,
			data: [][]string{[]string{"1", "0", "0", "0", "1", "1"}, []string{"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select u2 div u1, u2 div u2, u2 div u3, u2 div u4, u2 div 2, 3 div u2 from uus;", res: executeResult{
			null: false,
			data: [][]string{[]string{"11", "1", "0", "0", "16", "0"}, []string{"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select u3 div u1, u3 div u2, u3 div u3, u3 div u4, u3 div 2, 3 div u3 from uus;", res: executeResult{
			null: false,
			data: [][]string{[]string{"111", "10", "1", "0", "166", "0"}, []string{"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select u4 div u1, u4 div u2, u4 div u3, u4 div u4, u4 div 2, 3 div u4 from uus;", res: executeResult{
			null: false,
			data: [][]string{[]string{"1111", "101", "10", "1", "1666", "0"}, []string{"null", "null", "null", "null", "null", "null"}},
		}},

		{sql: "select i11 div i12, i21 div i22, i31 div i32, i41 div i42 from iis2;", res: executeResult{
			null: false,
			data: [][]string{[]string{"1", "1", "1", "1"}},
		}},
		{sql: "select i11 div i12, i21 div i22, i31 div i32, i41 div i42, i11, i21, i31, i41 from iis2;", res: executeResult{
			null: false,
			data: [][]string{[]string{"1", "1", "1", "1", "1", "22", "333", "4444"}},
		}},

		{sql: "select i11 / i12, i21 / i22, i31 / i32, i41 / i42 from iis2;", res: executeResult{
			null: false,
			data: [][]string{[]string{"1.000000", "1.000000", "1.000000", "1.000000"}},
		}},
		{sql: "select i11 / i12, i21 / i22, i31 / i32, i41 / i42, i11, i21, i31, i41 from iis2;", res: executeResult{
			null: false,
			data: [][]string{[]string{"1.000000", "1.000000", "1.000000", "1.000000", "1", "22", "333", "4444"}},
		}},

		{sql: "select u11 div u12, u21 div u22, u31 div u32, u41 div u42 from uus2;", res: executeResult{
			null: false,
			data: [][]string{[]string{"1", "1", "1", "1"}},
		}},
		{sql: "select u11 div u12, u21 div u22, u31 div u32, u41 div u42, u11, u21, u31, u41 from uus2;", res: executeResult{
			null: false,
			data: [][]string{[]string{"1", "1", "1", "1", "1", "22", "33", "444"}},
		}},

		{sql: "select u11 / u12, u21 / u22, u31 / u32, u41 / u42 from uus2;", res: executeResult{
			null: false,
			data: [][]string{[]string{"1.000000", "1.000000", "1.000000", "1.000000"}},
		}},
		{sql: "select u11 / u12, u21 / u22, u31 / u32, u41 / u42, u11, u21, u31, u41 from uus2;", res: executeResult{
			null: false,
			data: [][]string{[]string{"1.000000", "1.000000", "1.000000", "1.000000", "1", "22", "33", "444"}},
		}},

		{sql: "select f11 / f12, f21 / f22 from ffs2;", res: executeResult{
			null: false,
			data: [][]string{[]string{"1.000000", "1.000000"}, []string{"null", "null"}},
		}},
		{sql: "select f11 / f12, f21 / f22, f11, f21 from ffs2;", res: executeResult{
			null: false,
			data: [][]string{[]string{"1.000000", "1.000000", "22.200001", "222.222000"}, []string{"null", "null", "null", "null"}},
		}},

		{sql: "select f11 div f12, f21 div f22 from ffs2;", res: executeResult{
			null: false,
			data: [][]string{[]string{"1", "1"}, []string{"null", "null"}},
		}},
		{sql: "select f11 div f12, f21 div f22, f11, f21 from ffs2;", res: executeResult{
			null: false,
			data: [][]string{[]string{"1", "1", "22.200001", "222.222000"}, []string{"null", "null", "null", "null"}},
		}},
		{sql: "select i1 / d1, i2 / d1, i3 / d1, i4 / d1, d1 / 1, d1 / 12.34, d1 / d1 from int_decimal;", res: executeResult{
			null: false,
			attr: []string{"i1 / d1", "i2 / d1", "i3 / d1", "i4 / d1", "d1 / 1", "d1 / 12.34", "d1 / d1"},
			data: [][]string{[]string{"{0 0}", "{0 0}", "{0 0}", "{0 0}", "{33333300 0}", "{2701239 0}", "{100000 0}"}},
		}},
		{sql: "select i1 / d1, i2 / d1, i3 / d1, i4 / d1, d1 / 1, d1 / 12.34, d1 / d1 from int_decimal1;", res: executeResult{
			null: false,
			attr: []string{"i1 / d1", "i2 / d1", "i3 / d1", "i4 / d1", "d1 / 1", "d1 / 12.34", "d1 / d1"},
			data: [][]string{{"{0 0}", "{0 0}", "{0 0}", "{0 0}", "{33333300 0}", "{2701239 0}", "{100000 0}"}},
		}},
	}
	test(t, testCases)
}

// TestModOperator will run some sql to test %(as same as mod) operator
func TestModOperator(t *testing.T) {
	testCases := []testCase{
		{sql: "create table iis (i1 tinyint, i2 smallint, i3 int, i4 bigint);"},
		{sql: "create table iis2 (i11 tinyint, i12 tinyint, i21 smallint, i22 smallint, i31 int, i32 int, i41 bigint, i42 bigint);"},
		{sql: "create table ffs (f1 float, f2 double);"},
		{sql: "create table ffs2 (f11 float, f12 float, f21 double, f22 double);"},
		{sql: "create table uus (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);"},
		{sql: "create table uus2 (u11 tinyint unsigned, u12 tinyint unsigned, u21 smallint unsigned, u22 smallint unsigned, u31 int unsigned, u32 int unsigned, " +
			"u41 bigint unsigned, u42 bigint unsigned);"},

		{sql: "insert into iis values (1, 11, 111, 1111), (1, null, null, 1);"},
		{sql: "insert into iis2 values (1, 1, 22, 22, 333, 333, 4444, 4444);"},
		{sql: "insert into ffs values (22.2, 222.222), (null, null);"},
		{sql: "insert into ffs2 values (22.2, 22.2, 222.222, 222.222), (null, null, null, null);"},
		{sql: "insert into uus values (3, 33, 333, 3333), (null, null, null, null);"},
		{sql: "insert into uus2 values (1, 1, 22, 22, 33, 33, 444, 444);"},

		{sql: "select i1 mod i1, i1 mod i2, i1 mod i3, i1 mod i4, i1 mod 2, 3 mod i1 from iis;", res: executeResult{
			null: false,
			attr: []string{"i1 % i1", "i1 % i2", "i1 % i3", "i1 % i4", "i1 % 2", "3 % i1"},
			data: [][]string{{"0", "1", "1", "1", "1", "0"}, {"0", "null", "null", "0", "1", "0"}},
		}},
		{sql: "select i2 mod i1, i2 mod i2, i2 mod i3, i2 mod i4, i2 mod 2, 3 mod i2 from iis;", res: executeResult{
			null: false,
			attr: []string{"i2 % i1", "i2 % i2", "i2 % i3", "i2 % i4", "i2 % 2", "3 % i2"},
			data: [][]string{{"0", "0", "11", "11", "1", "3"}, {"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select i3 mod i1, i3 mod i2, i3 mod i3, i3 mod i4, i3 mod 2, 3 mod i3 from iis;", res: executeResult{
			null: false,
			data: [][]string{{"0", "1", "0", "111", "1", "3"}, {"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select i4 mod i1, i4 mod i2, i4 mod i3, i4 mod i4, i4 mod 2, 3 mod i4 from iis;", res: executeResult{
			null: false,
			data: [][]string{{"0", "0", "1", "0", "1", "3"}, {"0", "null", "null", "0", "1", "0"}},
		}},

		{sql: "select u1 mod u1, u1 mod u2, u1 mod u3, u1 mod u4, u1 mod 2, 3 mod u1 from uus;", res: executeResult{
			null: false,
			data: [][]string{{"0", "3", "3", "3", "1", "0"}, {"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select u2 mod u1, u2 mod u2, u2 mod u3, u2 mod u4, u2 mod 2, 3 mod u2 from uus;", res: executeResult{
			null: false,
			data: [][]string{{"0", "0", "33", "33", "1", "3"}, {"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select u3 mod u1, u3 mod u2, u3 mod u3, u3 mod u4, u3 mod 2, 3 mod u3 from uus;", res: executeResult{
			null: false,
			data: [][]string{{"0", "3", "0", "333", "1", "3"}, {"null", "null", "null", "null", "null", "null"}},
		}},
		{sql: "select u4 mod u1, u4 mod u2, u4 mod u3, u4 mod u4, u4 mod 2, 3 mod u4 from uus;", res: executeResult{
			null: false,
			data: [][]string{{"0", "0", "3", "0", "1", "3"}, {"null", "null", "null", "null", "null", "null"}},
		}},

		{sql: "select i11 mod i12, i21 mod i22, i31 mod i32, i41 mod i42 from iis2;", res: executeResult{
			null: false,
			data: [][]string{{"0", "0", "0", "0"}},
		}},
		{sql: "select i11 mod i12, i21 mod i22, i31 mod i32, i41 mod i42, i11, i21, i31, i41 from iis2;", res: executeResult{
			null: false,
			data: [][]string{{"0", "0", "0", "0", "1", "22", "333", "4444"}},
		}},

		{sql: "select u11 mod u12, u21 mod u22, u31 mod u32, u41 mod u42 from uus2;", res: executeResult{
			null: false,
			data: [][]string{{"0", "0", "0", "0"}},
		}},
		{sql: "select u11 mod u12, u21 mod u22, u31 mod u32, u41 mod u42, u11, u21, u31, u41 from uus2;", res: executeResult{
			null: false,
			data: [][]string{{"0", "0", "0", "0", "1", "22", "33", "444"}},
		}},

		{sql: "select f11 mod f12, f21 mod f22 from ffs2;", res: executeResult{
			null: false,
			data: [][]string{{"0.000000", "0.000000"}, {"null", "null"}},
		}},
		{sql: "select f11 mod f12, f21 mod f22, f11, f21 from ffs2;", res: executeResult{
			null: false,
			data: [][]string{{"0.000000", "0.000000", "22.200001", "222.222000"}, {"null", "null", "null", "null"}},
		}},
	}

	test(t, testCases)
}

// TestUnaryMinusOperator will run some sql to test - before a numeric, such as '-1'
func TestUnaryMinusOperator(t *testing.T) {
	testCases := []testCase{
		// data preparation
		{sql: "create table iis (i1 tinyint, i2 smallint, i3 int, i4 bigint);"},
		{sql: "create table ffs (f1 float, f2 double);"},
		{sql: "create table uus (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);"},
		{sql: "insert into iis values (1, 11, 111, 1111), (1, null, null, 1);"},
		{sql: "insert into ffs values (22.2, 222.222), (null, null);"},
		{sql: "insert into uus values (3, 33, 333, 3333), (null, null, null, null);"},

		{sql: "select -i1, -i2, -i3, -i4 from iis;", res: executeResult{
			null: false,
			attr: []string{"-i1", "-i2", "-i3", "-i4"},
			data: [][]string{{"-1", "-11", "-111", "-1111"}, {"-1", "null", "null", "-1"}},
		}},
		{sql: "select -f1, -f2 from ffs;", res: executeResult{
			null: false,
			attr: []string{"-f1", "-f2"},
			data: [][]string{{"-22.200001", "-222.222000"}, {"null", "null"}},
		}},
		{sql: "select -i1, i1 from iis;", res: executeResult{
			null: false,
			data: [][]string{{"-1", "1"}, {"-1", "1"}},
		}},
		{sql: "select -i2, i2 from iis;", res: executeResult{
			null: false,
			data: [][]string{{"-11", "11"}, {"null", "null"}},
		}},
		{sql: "select -i3, i3 from iis;", res: executeResult{
			null: false,
			data: [][]string{{"-111", "111"}, {"null", "null"}},
		}},
		{sql: "select -i4, i4 from iis;", res: executeResult{
			null: false,
			data: [][]string{{"-1111", "1111"}, {"-1", "1"}},
		}},
		{sql: "select -f1, f1 from ffs;", res: executeResult{
			null: false,
			data: [][]string{{"-22.200001", "22.200001"}, {"null", "null"}},
		}},
		{sql: "select -f2, f2 from ffs;", res: executeResult{
			null: false,
			data: [][]string{{"-222.222000", "222.222000"}, {"null", "null"}},
		}},
		{sql: "select -u1, -u2, -u3, -u4 from uus;", err: "'-' not yet implemented for TINYINT UNSIGNED"},
	}
	test(t, testCases)
}

// TestLikeOperator will run some sql to test like operator
func TestLikeOperator(t *testing.T) {
	testCase := []testCase{
		{sql: "create table tk (a varchar(10), b char(20));"},
		{sql: "insert into tk values ('a', 'a'),('abc', 'cba'),('abcd', '%'),('hello', ''),('test', 't___'),(null, null);"},
		{sql: "select a from tk where a like 'a%';", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"a"}, {"abc"}, {"abcd"},
			},
		}},
		{sql: "select a from tk where a like '%b%';", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"abc"}, {"abcd"},
			},
		}},
		{sql: "select a from tk where a like '_bc';", res: executeResult{
			data: [][]string{
				{"abc"},
			},
		}},
		{sql: "select a from tk where a like '_bc_';", res: executeResult{
			data: [][]string{
				{"abcd"},
			},
		}},
		{sql: "select a from tk where 'abc' like a;", res: executeResult{
			attr: []string{"a"},
			data: [][]string{
				{"abc"},
			},
		}},
		{sql: "select b from tk where b like '_';", res: executeResult{
			data: [][]string{
				{"a"}, {"%"},
			},
		}},
		{sql: "select b from tk where 't___' like b;", res: executeResult{
			data: [][]string{
				{"%"}, {"t___"},
			},
		}},
		{sql: "select * from tk where a like a;", res: executeResult{
			data: [][]string{
				{"a", "a"}, {"abc", "cba"}, {"abcd", "%"}, {"hello", ""}, {"test", "t___"},
			},
		}},
		{sql: "select * from tk where a like b;", res: executeResult{
			data: [][]string{
				{"a", "a"}, {"abcd", "%"}, {"test", "t___"},
			},
		}},
		{sql: "select * from tk where 'a' like 'a';", res: executeResult{
			attr: []string{"a", "b"},
			data: [][]string{
				{"a", "a"}, {"abc", "cba"}, {"abcd", "%"}, {"hello", ""}, {"test", "t___"}, {"null", "null"},
			},
		}},
		{sql: "create table issue1588 (username varchar(50));"},
		{sql: "insert into issue1588 values ('Mastermann Masterfrau');"},
		{sql: "select username from issue1588 where username like '_aster%';", res: executeResult{
			attr: []string{"username"},
			data: [][]string{
				{"Mastermann Masterfrau"},
			},
		}},
	}
	test(t, testCase)
}

// TestCastOperator will run some sql to test cast operator
func TestCastOperator(t *testing.T) {
	testCases := []testCase{
		{sql: "create table iis (i1 tinyint, i2 smallint, i3 int, i4 bigint);"},
		{sql: "create table ffs (f1 float, f2 double);"},
		{sql: "create table uus (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);"},
		{sql: "create table ccs3 (c1 char(10), c2 varchar(10));"},
		{sql: "insert into iis values (1, 11, 111, 1111), (null, null, null, null);"},
		{sql: "insert into ffs values (22.2, 222.222), (null, null);"},
		{sql: "insert into uus values (3, 33, 333, 3333), (null, null, null, null);"},
		{sql: "insert into ccs3 values ('123', '123456');"},

		{sql: "select CAST(i1 AS signed), CAST(i1 AS unsigned), CAST(i1 AS float(1)), CAST(i1 AS double), CAST(i1 AS char(10)) from iis;", res: executeResult{
			attr: []string{"cast(i1 as signed)", "cast(i1 as unsigned unsigned)", "cast(i1 as float(1))", "cast(i1 as double)", "cast(i1 as char(10))"},
			data: [][]string{
				{"1", "1", "1.000000", "1.000000", "1"}, {"null", "null", "null", "null", "null"},
			},
		}},
		{sql: "select CAST(i2 AS signed), CAST(i2 AS unsigned), CAST(i2 AS float(1)), CAST(i2 AS double), CAST(i2 AS char(10)) from iis;", res: executeResult{
			data: [][]string{
				{"11", "11", "11.000000", "11.000000", "11"}, {"null", "null", "null", "null", "null"},
			},
		}},
		{sql: "select CAST(i3 AS signed), CAST(i3 AS unsigned), CAST(i3 AS float(1)), CAST(i3 AS double), CAST(i3 AS char(10)) from iis;", res: executeResult{
			data: [][]string{
				{"111", "111", "111.000000", "111.000000", "111"}, {"null", "null", "null", "null", "null"},
			},
		}},
		{sql: "select CAST(i4 AS signed), CAST(i4 AS unsigned), CAST(i4 AS float(1)), CAST(i4 AS double), CAST(i4 AS char(10)) from iis;", res: executeResult{
			data: [][]string{
				{"1111", "1111", "1111.000000", "1111.000000", "1111"}, {"null", "null", "null", "null", "null"},
			},
		}},
		{sql: "select CAST(u1 AS signed), CAST(u1 AS unsigned), CAST(u1 AS float(1)), CAST(u1 AS double), CAST(u1 AS char(10)) from uus;", res: executeResult{
			data: [][]string{
				{"3", "3", "3.000000", "3.000000", "3"}, {"null", "null", "null", "null", "null"},
			},
		}},
		{sql: "select CAST(u2 AS signed), CAST(u2 AS unsigned), CAST(u2 AS float(1)), CAST(u2 AS double), CAST(u2 AS char(10)) from uus;", res: executeResult{
			data: [][]string{
				{"33", "33", "33.000000", "33.000000", "33"}, {"null", "null", "null", "null", "null"},
			},
		}},
		{sql: "select CAST(u3 AS signed), CAST(u3 AS unsigned), CAST(u3 AS float(1)), CAST(u3 AS double), CAST(u3 AS char(10)) from uus;", res: executeResult{
			data: [][]string{
				{"333", "333", "333.000000", "333.000000", "333"}, {"null", "null", "null", "null", "null"},
			},
		}},
		{sql: "select CAST(u4 AS signed), CAST(u4 AS unsigned), CAST(u4 AS float(1)), CAST(u4 AS double), CAST(u4 AS char(10)) from uus;", res: executeResult{
			data: [][]string{
				{"3333", "3333", "3333.000000", "3333.000000", "3333"}, {"null", "null", "null", "null", "null"},
			},
		}},
		{sql: "select CAST(f1 AS signed), CAST(f1 AS unsigned), CAST(f1 AS float(1)), CAST(f1 AS double), CAST(f1 AS char(10)) from ffs;", res: executeResult{
			data: [][]string{
				{"22", "22", "22.200001", "22.200001", "22.2"}, {"null", "null", "null", "null", "null"},
			},
		}, com: "is that suitable we show the result as `22.200001` here"},
		{sql: "select CAST(f2 AS signed), CAST(f2 AS unsigned), CAST(f2 AS float(1)), CAST(f2 AS double), CAST(f2 AS char(10)) from ffs;", res: executeResult{
			data: [][]string{
				{"222", "222", "222.222000", "222.222000", "222.222"}, {"null", "null", "null", "null", "null"},
			},
		}},
		{sql: "select CAST(c1 AS signed), CAST(c1 AS unsigned), CAST(c1 AS float(1)), CAST(c1 AS double), CAST(c1 AS char(10)) from ccs3;", res: executeResult{
			data: [][]string{
				{"123", "123", "123.000000", "123.000000", "123"},
			},
		}},
		{sql: "select CAST(c2 AS signed), CAST(c2 AS unsigned), CAST(c2 AS float(1)), CAST(c2 AS double), CAST(c2 AS char(10)) from ccs3;", res: executeResult{
			data: [][]string{
				{"123456", "123456", "123456.000000", "123456.000000", "123456"},
			},
		}},
		{sql: "select i1, CAST(i1 AS signed), CAST(i1 AS unsigned), CAST(i1 AS float(1)), CAST(i1 AS double), CAST(i1 AS char(10)) from iis;", res: executeResult{
			data: [][]string{
				{"1", "1", "1", "1.000000", "1.000000", "1"}, {"null", "null", "null", "null", "null", "null"},
			},
		}},
		{sql: "select i2, CAST(i2 AS signed), CAST(i2 AS unsigned), CAST(i2 AS float(1)), CAST(i2 AS double), CAST(i2 AS char(10)) from iis;", res: executeResult{
			data: [][]string{
				{"11", "11", "11", "11.000000", "11.000000", "11"}, {"null", "null", "null", "null", "null", "null"},
			},
		}},
		{sql: "select i3, CAST(i3 AS signed), CAST(i3 AS unsigned), CAST(i3 AS float(1)), CAST(i3 AS double), CAST(i3 AS char(10)) from iis;", res: executeResult{
			data: [][]string{
				{"111", "111", "111", "111.000000", "111.000000", "111"}, {"null", "null", "null", "null", "null", "null"},
			},
		}},
		{sql: "select i4, CAST(i4 AS signed), CAST(i4 AS unsigned), CAST(i4 AS float(1)), CAST(i4 AS double), CAST(i4 AS char(10)) from iis;", res: executeResult{
			data: [][]string{
				{"1111", "1111", "1111", "1111.000000", "1111.000000", "1111"}, {"null", "null", "null", "null", "null", "null"},
			},
		}},
		{sql: "select u1, CAST(u1 AS signed), CAST(u1 AS unsigned), CAST(u1 AS float(1)), CAST(u1 AS double), CAST(u1 AS char(10)) from uus;", res: executeResult{
			data: [][]string{
				{"3", "3", "3", "3.000000", "3.000000", "3"}, {"null", "null", "null", "null", "null", "null"},
			},
		}},
		{sql: "select u2, CAST(u2 AS signed), CAST(u2 AS unsigned), CAST(u2 AS float(1)), CAST(u2 AS double), CAST(u2 AS char(10)) from uus;", res: executeResult{
			data: [][]string{
				{"33", "33", "33", "33.000000", "33.000000", "33"}, {"null", "null", "null", "null", "null", "null"},
			},
		}},
		{sql: "select u3, CAST(u3 AS signed), CAST(u3 AS unsigned), CAST(u3 AS float(1)), CAST(u3 AS double), CAST(u3 AS char(10)) from uus;", res: executeResult{
			data: [][]string{
				{"333", "333", "333", "333.000000", "333.000000", "333"}, {"null", "null", "null", "null", "null", "null"},
			},
		}},
		{sql: "select u4, CAST(u4 AS signed), CAST(u4 AS unsigned), CAST(u4 AS float(1)), CAST(u4 AS double), CAST(u4 AS char(10)) from uus;", res: executeResult{
			data: [][]string{
				{"3333", "3333", "3333", "3333.000000", "3333.000000", "3333"}, {"null", "null", "null", "null", "null", "null"},
			},
		}},
		{sql: "select f1, CAST(f1 AS signed), CAST(f1 AS unsigned), CAST(f1 AS float(1)), CAST(f1 AS double), CAST(f1 AS char(10)) from ffs;", res: executeResult{
			data: [][]string{
				{"22.200001", "22", "22", "22.200001", "22.200001", "22.2"}, {"null", "null", "null", "null", "null", "null"},
			},
		}},
		{sql: "select f2, CAST(f2 AS signed), CAST(f2 AS unsigned), CAST(f2 AS float(1)), CAST(f2 AS double), CAST(f2 AS char(10)) from ffs;", res: executeResult{
			data: [][]string{
				{"222.222000", "222", "222", "222.222000", "222.222000", "222.222"}, {"null", "null", "null", "null", "null", "null"},
			},
		}},
		{sql: "select c1, CAST(c1 AS signed), CAST(c1 AS unsigned), CAST(c1 AS float(1)), CAST(c1 AS double), CAST(c1 AS char(10)) from ccs3;", res: executeResult{
			data: [][]string{
				{"123", "123", "123", "123.000000", "123.000000", "123"},
			},
		}},
		{sql: "select c2, CAST(c2 AS signed), CAST(c2 AS unsigned), CAST(c2 AS float(1)), CAST(c2 AS double), CAST(c2 AS char(10)) from ccs3;", res: executeResult{
			data: [][]string{
				{"123456", "123456", "123456", "123456.000000", "123456.000000", "123456"},
			},
		}},
	}
	test(t, testCases)
}

// TestNotOperator will run some sql to test not operator
func TestNotOperator(t *testing.T) {
	testCases := []testCase{
		{sql: "create table iis (i1 tinyint, i2 smallint, i3 int, i4 bigint);"},
		{sql: "create table ffs (f1 float, f2 double);"},
		{sql: "create table uus (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);"},
		{sql: "create table ccs (c1 char(10), c2 varchar(10));"},
		{sql: "create table tk (spID int,userID int,score smallint);"},
		{sql: "insert into iis values (1, 11, 111, 1111), (0, 0, 0, 0), (null, null, null, null);"},
		{sql: "insert into ffs values (22.2, 222.222), (0, 0), (null, null);"},
		{sql: "insert into uus values (3, 33, 333, 3333), (0, 0, 0, 0), (null, null, null, null);"},
		{sql: "insert into ccs values ('1.5', '2.50');"},
		{sql: "insert into tk values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4);"},

		{sql: "select not i1, not i2, not i3, not i4 from iis;", res: executeResult{
			attr: []string{"not i1", "not i2", "not i3", "not i4"},
			data: [][]string{
				{"0", "0", "0", "0"},
				{"1", "1", "1", "1"},
				{"null", "null", "null", "null"},
			},
		}},
		{sql: "select not u1, not u2, not u3, not u4 from uus;", res: executeResult{
			data: [][]string{
				{"0", "0", "0", "0"},
				{"1", "1", "1", "1"},
				{"null", "null", "null", "null"},
			},
		}},
		{sql: "select not f1, not f2 from ffs;", res: executeResult{
			data: [][]string{
				{"0", "0"},
				{"1", "1"},
				{"null", "null"},
			},
		}},
		{sql: "select not i1, not i2, not i3, not i4, i1, i2, i3, i4 from iis;", res: executeResult{
			data: [][]string{
				{"0", "0", "0", "0", "1", "11", "111", "1111"},
				{"1", "1", "1", "1", "0", "0", "0", "0"},
				{"null", "null", "null", "null", "null", "null", "null", "null"},
			},
		}},
		{sql: "select not u1, not u2, not u3, not u4, u1, u2, u3, u4 from uus;", res: executeResult{
			data: [][]string{
				{"0", "0", "0", "0", "3", "33", "333", "3333"},
				{"1", "1", "1", "1", "0", "0", "0", "0"},
				{"null", "null", "null", "null", "null", "null", "null", "null"},
			},
		}},
		{sql: "select not f1, not f2, f1, f2 from ffs;", res: executeResult{
			data: [][]string{
				{"0", "0", "22.200001", "222.222000"},
				{"1", "1", "0.000000", "0.000000"},
				{"null", "null", "null", "null"},
			},
		}},
		{sql: "select not c1, not c2 from ccs;", res: executeResult{
			data: [][]string{
				{"0", "0"},
			},
		}},
		{sql: "select * from tk where not userID < 6 or not userID;", res: executeResult{
			null: true,
		}},
		{sql: "select * from tk where userID < 6 and not userID;", res: executeResult{
			null: true,
		}},
		{sql: "select * from tk where not userID < 6 and not userID;", res: executeResult{
			null: true,
		}},
		{sql: "select * from tk where userID < 6 or not userID;", res: executeResult{
			data: [][]string{
				{"1", "1", "1"},
				{"2", "2", "2"},
				{"3", "3", "3"},
				{"4", "4", "4"},
			},
		}},
		{sql: "select * from tk where not (userID between 2 and 3);", res: executeResult{
			data: [][]string{
				{"1", "1", "1"},
				{"4", "4", "4"},
			},
		}},
		{sql: "select * from tk where not (userID = 1 and userID = 5);", res: executeResult{
			data: [][]string{
				{"1", "1", "1"},
				{"2", "2", "2"},
				{"3", "3", "3"},
				{"4", "4", "4"},
			},
		}},
		{sql: "select * from tk where not (userID = 1 or userID = 5);", res: executeResult{
			data: [][]string{
				{"2", "2", "2"},
				{"3", "3", "3"},
				{"4", "4", "4"},
			},
		}},
		{sql: "select * from tk where not not (userID = 1 or userID = 5);", res: executeResult{
			data: [][]string{
				{"1", "1", "1"},
			},
		}},
		{sql: "select * from tk where not not not (userID = 1 or userID = 5);", res: executeResult{
			data: [][]string{
				{"2", "2", "2"},
				{"3", "3", "3"},
				{"4", "4", "4"},
			},
		}},
		{sql: "select * from tk where not (userID = 1 or ( userID = 5 ) and userID = (7));", res: executeResult{
			data: [][]string{
				{"2", "2", "2"},
				{"3", "3", "3"},
				{"4", "4", "4"},
			},
		}},
	}
	test(t, testCases)
}

// TestAndOperator will run some sql to test and operator
func TestAndOperator(t *testing.T) {
	testCases := []testCase{
		{sql: "create table adt (i int, f double, c char(15));"},
		{sql: "select i, f, c from adt where i = i and f >= f;"},
		{sql: "select i, f, c from adt where i = i and f >= f and c = c;"},
		{sql: "select i, f, c from adt where i = i and f >= f and c = c and i <= 10;"},
		{sql: "select i, f, c from adt where (i = i and f >= f) and c = c and i <= 10;"},
	}
	test(t, testCases)
}

// TestOrOperator will run some sql to test or operator
func TestOrOperator(t *testing.T) {
	testCases := []testCase{
		{sql: "create table adt (i int, f double, c char(15));"},
		{sql: "select i, f, c from adt where i = i or f >= f;"},
		{sql: "select i, f, c from adt where i = i or f >= f or c = c;"},
		{sql: "select i, f, c from adt where i = i or f >= f or c = c or i <= 10;"},
		{sql: "select i, f, c from adt where (i = i or f >= f) or c = c or i <= 10;"},
	}
	test(t, testCases)
}

// TestComparisonOperator will run some sql to test =, !=, >, <, >=, <= for basic type families
// family int, family float, family string
func TestComparisonOperator(t *testing.T) {
	testCases := []testCase{
		{sql: "create table iis (i1 tinyint, i2 smallint, i3 int, i4 bigint);"},
		{sql: "create table ffs (f1 float, f2 double);"},
		{sql: "create table uus (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);"},
		{sql: "create table ccs (c1 char(10), c2 varchar(20));"},
		{sql: "create table int_decimal (i1 tinyint, i2 smallint, i3 int, i4 bigint, d1 decimal(10, 5));"},
		{sql: "create table int_decimal1 (i1 tinyint, i2 smallint, i3 int, i4 bigint, d1 decimal(20, 5));"},
		{sql: "create table dds (d1 date, d2 datetime);"},
		{sql: "insert into iis values (1, 11, 111, 1111), (1, null, null, 1);"},
		{sql: "insert into ffs values (22.2, 222.222), (null, null);"},
		{sql: "insert into uus values (3, 33, 333, 3333), (null, null, null, null);"},
		{sql: "insert into ccs values ('kpi', 'b'), ('c', 'e'), (null, null);"},
		{sql: "insert into int_decimal values (1, 1, 22, 22, 333.333);"},
		{sql: "insert into int_decimal1 values (1, 1, 22, 22, 333.333);"},
		{sql: "insert into dds values ('2015-06-30', '2007-01-01 12:13:04');"},
		// Test =, >, <, >=, <=, !=
		{sql: "select * from iis where i1 = i1 and i2 = i2 and i3 = i3 and i4 = i4;", res: executeResult{
			data: [][]string{
				{"1", "11", "111", "1111"},
			},
		}},
		{sql: "select * from iis where i1 = i2 and i1 = i3 and i1 = i4 and i2 = i1 and i3 = i1 and i4 = i1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from iis where i2 = i3 and i2 = i4 and i3 = i2 and i4 = i2;", res: executeResult{
			null: true,
		}},
		{sql: "select * from iis where i1 = 1;", res: executeResult{
			data: [][]string{
				{"1", "11", "111", "1111"},
				{"1", "null", "null", "1"},
			},
		}},
		{sql: "select * from iis where 1 = i1;", res: executeResult{
			data: [][]string{
				{"1", "11", "111", "1111"},
				{"1", "null", "null", "1"},
			},
		}},
		{sql: "select * from iis where i2 = 1 and 1 = i2 and i3 = 1 and 1 = i3 and 1 = i4 or i4 = 1;", res: executeResult{
			data: [][]string{
				{"1", "null", "null", "1"},
			},
		}},
		{sql: "select * from uus where u1 = u1 and u2 = u2 and u3 = u3 and u4 = u4;", res: executeResult{
			data: [][]string{
				{"3", "33", "333", "3333"},
			},
		}},
		{sql: "select * from uus where u1 = u2 and u1 = u3 and u1 = u4 and u2 = u1 and u3 = u1 and u4 = u1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from uus where u2 = u3 and u2 = u4 and u3 = u2 and u4 = u2;", res: executeResult{
			null: true,
		}},
		{sql: "select * from uus where u1 = 1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from uus where 1 = u1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from uus where u2 = 1 and 1 = u2 and u3 = 1 and 1 = u3 and 1 = u4 or u4 = 1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from iis where i1 >= i1 and i2 >= i2 and i3 >= i3 and i4 >= i4;", res: executeResult{
			data: [][]string{
				{"1", "11", "111", "1111"},
			},
		}},
		{sql: "select * from iis where i1 >= i2 and i1 >= i3 and i1 >= i4 and i2 >= i1 and i3 >= i1 and i4 >= i1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from iis where i2 >= i3 and i2 >= i4 and i3 >= i2 and i4 >= i2;", res: executeResult{
			null: true,
		}},
		{sql: "select * from iis where i1 >= 1;", res: executeResult{
			data: [][]string{
				{"1", "11", "111", "1111"},
				{"1", "null", "null", "1"},
			},
		}},
		{sql: "select * from iis where 1 >= i1;", res: executeResult{
			data: [][]string{
				{"1", "11", "111", "1111"},
				{"1", "null", "null", "1"},
			},
		}},
		{sql: "select * from iis where i2 >= 1 and 1 >= i2 and i3 >= 1 and 1 >= i3 and 1 >= i4 or i4 >= 1;", res: executeResult{
			data: [][]string{
				{"1", "11", "111", "1111"},
				{"1", "null", "null", "1"},
			},
		}},
		{sql: "select * from uus where u1 >= u1 and u2 >= u2 and u3 >= u3 and u4 >= u4;", res: executeResult{
			data: [][]string{
				{"3", "33", "333", "3333"},
			},
		}},
		{sql: "select * from uus where u1 >= u2 and u1 >= u3 and u1 >= u4 and u2 >= u1 and u3 >= u1 and u4 >= u1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from uus where u2 >= u3 and u2 >= u4 and u3 >= u2 and u4 >= u2;", res: executeResult{
			null: true,
		}},
		{sql: "select * from uus where u1 >= 1;", res: executeResult{
			data: [][]string{
				{"3", "33", "333", "3333"},
			},
		}},
		{sql: "select * from uus where 1 >= u1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from uus where u2 >= 1 and 1 >= u2 and u3 >= 1 and 1 >= u3 and 1 >= u4 or u4 >= 1;", res: executeResult{
			data: [][]string{
				{"3", "33", "333", "3333"},
			},
		}},
		{sql: "select * from iis where i1 <= i1 and i2 <= i2 and i3 <= i3 and i4 <= i4;", res: executeResult{
			data: [][]string{
				{"1", "11", "111", "1111"},
			},
		}},
		{sql: "select * from iis where i1 <= i2 and i1 <= i3 and i1 <= i4 and i2 <= i1 and i3 <= i1 and i4 <= i1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from iis where i2 <= i3 and i2 <= i4 and i3 <= i2 and i4 <= i2;", res: executeResult{
			null: true,
		}},
		{sql: "select * from iis where i1 <= 1;", res: executeResult{
			data: [][]string{
				{"1", "11", "111", "1111"},
				{"1", "null", "null", "1"},
			},
		}},
		{sql: "select * from iis where 1 <= i1;", res: executeResult{
			data: [][]string{
				{"1", "11", "111", "1111"},
				{"1", "null", "null", "1"},
			},
		}},
		{sql: "select * from iis where i2 <= 1 and 1 <= i2 and i3 <= 1 and 1 <= i3 and 1 <= i4 or i4 <= 1;", res: executeResult{
			data: [][]string{
				{"1", "null", "null", "1"},
			},
		}},
		{sql: "select * from uus where u1 <= u1 and u2 <= u2 and u3 <= u3 and u4 <= u4;", res: executeResult{
			data: [][]string{
				{"3", "33", "333", "3333"},
			},
		}},
		{sql: "select * from uus where u1 <= u2 and u1 <= u3 and u1 <= u4 and u2 <= u1 and u3 <= u1 and u4 <= u1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from uus where u2 <= u3 and u2 <= u4 and u3 <= u2 and u4 <= u2;", res: executeResult{
			null: true,
		}},
		{sql: "select * from uus where u1 <= 1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from uus where 1 <= u1;", res: executeResult{
			data: [][]string{
				{"3", "33", "333", "3333"},
			},
		}},
		{sql: "select * from uus where u2 <= 1 and 1 <= u2 and u3 <= 1 and 1 <= u3 and 1 <= u4 or u4 <= 1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from iis where i1 > i1 and i2 > i2 and i3 > i3 and i4 > i4;", res: executeResult{
			null: true,
		}},
		{sql: "select * from iis where i1 > i2 and i1 > i3 and i1 > i4 and i2 > i1 and i3 > i1 and i4 > i1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from iis where i2 > i3 and i2 > i4 and i3 > i2 and i4 > i2;", res: executeResult{
			null: true,
		}},
		{sql: "select * from iis where i1 > 1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from iis where 1 > i1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from iis where i2 > 1 and 1 > i2 and i3 > 1 and 1 > i3 and 1 > i4 or i4 > 1;", res: executeResult{
			data: [][]string{
				{"1", "11", "111", "1111"},
			},
		}},
		{sql: "select * from uus where u1 > u1 and u2 > u2 and u3 > u3 and u4 > u4;", res: executeResult{
			null: true,
		}},
		{sql: "select * from uus where u1 > u2 and u1 > u3 and u1 > u4 and u2 > u1 and u3 > u1 and u4 > u1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from uus where u2 > u3 and u2 > u4 and u3 > u2 and u4 > u2;", res: executeResult{
			null: true,
		}},
		{sql: "select * from uus where u1 > 1;", res: executeResult{
			data: [][]string{
				{"3", "33", "333", "3333"},
			},
		}},
		{sql: "select * from uus where 1 > u1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from uus where u2 > 1 and 1 > u2 and u3 > 1 and 1 > u3 and 1 > u4 or u4 > 1;", res: executeResult{
			data: [][]string{
				{"3", "33", "333", "3333"},
			},
		}},
		{sql: "select * from iis where i1 < i1 and i2 < i2 and i3 < i3 and i4 < i4;", res: executeResult{
			null: true,
		}},
		{sql: "select * from iis where i1 < i2 and i1 < i3 and i1 < i4 and i2 < i1 and i3 < i1 and i4 < i1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from iis where i2 < i3 and i2 < i4 and i3 < i2 and i4 < i2;", res: executeResult{
			null: true,
		}},
		{sql: "select * from iis where i1 < 1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from iis where 1 < i1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from iis where i2 < 1 and 1 < i2 and i3 < 1 and 1 < i3 and 1 < i4 or i4 < 1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from uus where u1 < u1 and u2 < u2 and u3 < u3 and u4 < u4;", res: executeResult{
			null: true,
		}},
		{sql: "select * from uus where u1 < u2 and u1 < u3 and u1 < u4 and u2 < u1 and u3 < u1 and u4 < u1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from uus where u2 < u3 and u2 < u4 and u3 < u2 and u4 < u2;", res: executeResult{
			null: true,
		}},
		{sql: "select * from uus where u1 < 1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from uus where 1 < u1;", res: executeResult{
			data: [][]string{
				{"3", "33", "333", "3333"},
			},
		}},
		{sql: "select * from uus where u2 < 1 and 1 < u2 and u3 < 1 and 1 < u3 and 1 < u4 or u4 < 1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from iis where i1 != i1 and i2 != i2 and i3 != i3 and i4 != i4;", res: executeResult{
			null: true,
		}},
		{sql: "select * from iis where i1 != i2 and i1 != i3 and i1 != i4 and i2 != i1 and i3 != i1 and i4 != i1;", res: executeResult{
			data: [][]string{
				{"1", "11", "111", "1111"},
			},
		}},
		{sql: "select * from iis where i2 != i3 and i2 != i4 and i3 != i2 and i4 != i2;", res: executeResult{
			data: [][]string{
				{"1", "11", "111", "1111"},
			},
		}},
		{sql: "select * from iis where i1 != 1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from iis where 1 != i1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from iis where i2 != 1 and 1 != i2 and i3 != 1 and 1 != i3 and 1 != i4 or i4 != 1;", res: executeResult{
			data: [][]string{
				{"1", "11", "111", "1111"},
			},
		}},
		{sql: "select * from uus where u1 != u1 and u2 != u2 and u3 != u3 and u4 != u4;", res: executeResult{
			null: true,
		}},
		{sql: "select * from uus where u1 != u2 and u1 != u3 and u1 != u4 and u2 != u1 and u3 != u1 and u4 != u1;", res: executeResult{
			data: [][]string{
				{"3", "33", "333", "3333"},
			},
		}},
		{sql: "select * from uus where u2 != u3 and u2 != u4 and u3 != u2 and u4 != u2;", res: executeResult{
			data: [][]string{
				{"3", "33", "333", "3333"},
			},
		}},
		{sql: "select * from uus where u1 != 1;", res: executeResult{
			data: [][]string{
				{"3", "33", "333", "3333"},
			},
		}},
		{sql: "select * from uus where 1 != u1;", res: executeResult{
			data: [][]string{
				{"3", "33", "333", "3333"},
			},
		}},
		{sql: "select * from uus where u2 != 1 and 1 != u2 and u3 != 1 and 1 != u3 and 1 != u4 or u4 != 1;", res: executeResult{
			data: [][]string{
				{"3", "33", "333", "3333"},
			},
		}},
		{sql: "select * from ffs where f1 = f1 and f1 = f2 and f2 = f1 and f2 = f2;", res: executeResult{
			null: true,
		}},
		{sql: "select * from ffs where f1 = 10.5;", res: executeResult{
			null: true,
		}},
		{sql: "select * from ffs where 10.5 = f1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from ffs where 22 = f2 and f2 = 22;", res: executeResult{
			null: true,
		}},
		{sql: "select * from ffs where f1 > f2 and f1 > f1 and f2 > f2 and f2 > f1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from ffs where 500 > f1;", res: executeResult{
			data: [][]string{
				{"22.200001", "222.222000"},
			},
		}},
		{sql: "select * from ffs where f1 > 500;", res: executeResult{
			null: true,
		}},
		{sql: "select * from ffs where 100 > f2 and f2 > 50;", res: executeResult{
			null: true,
		}},
		{sql: "select * from ffs where f1 < f2 and f1 < f1 and f2 < f2 and f2 < f1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from ffs where f1 < 100;", res: executeResult{
			data: [][]string{
				{"22.200001", "222.222000"},
			},
		}},
		{sql: "select * from ffs where 3.5 < f1;", res: executeResult{
			data: [][]string{
				{"22.200001", "222.222000"},
			},}},
		{sql: "select * from ffs where 1 < f2 and f2 < 30;", res: executeResult{
			null: true,
		}},
		{sql: "select * from ffs where f1 >= f2 and f1 >= f1 and f2 >= f2 and f2 >= f1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from ffs where f1 >= 22.2;", res: executeResult{
			data: [][]string{
				{"22.200001", "222.222000"},
			},
		}},
		{sql: "select * from ffs where 33.3 >= f1;", res: executeResult{
			data: [][]string{
				{"22.200001", "222.222000"},
			},
		}},
		{sql: "select * from ffs where 70 >= f1 and f1 >= -5;", res: executeResult{
			data: [][]string{
				{"22.200001", "222.222000"},
			},
		}},
		{sql: "select * from ffs where f1 <= f2 and f1 <= f1 and f2 <= f2 and f2 <= f1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from ffs where f1 <= 100;", res: executeResult{
			data: [][]string{
				{"22.200001", "222.222000"},
			},
		}},
		{sql: "select * from ffs where 100 <= f1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from ffs where -5 <= f1 and f1 <= 0.00;", res: executeResult{
			null: true,
		}},
		{sql: "select * from ffs where f1 != f2 and f1 != f1 and f2 != f2 and f2 != f1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from ffs where f1 != 5;", res: executeResult{
			data: [][]string{
				{"22.200001", "222.222000"},
			},
		}},
		{sql: "select * from ffs where 5 != f1;", res: executeResult{
			data: [][]string{
				{"22.200001", "222.222000"},
			},
		}},
		{sql: "select * from ffs where 408 != f2 and f2 != 403.3;", res: executeResult{
			data: [][]string{
				{"22.200001", "222.222000"},
			},
		}},
		{sql: "select * from ccs where c1 = 'kpi';", res: executeResult{
			data: [][]string{
				{"kpi", "b"},
			},
		}},
		{sql: "select * from ccs where c1 = '123' and '123' = c1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from ccs where c2 = '123' or 'a' = c1;", res: executeResult{
			null: true,
		}},
		{sql: "select * from ccs where c1 > c2 or c2 > c1;", res: executeResult{
			data: [][]string{
				{"kpi", "b"},
				{"c", "e"},
			},
		}},
		{sql: "select * from ccs where c1 > 'bvh';", res: executeResult{
			data: [][]string{
				{"kpi", "b"},
				{"c", "e"},
			},
		}},
		{sql: "select * from ccs where c1 > 'a' or c2 > 'a' or 'v' > c1 or 'v' > c2;", res: executeResult{
			data: [][]string{
				{"kpi", "b"},
				{"c", "e"},
			},
		}},
		{sql: "select * from ccs where c1 < c2 or c2 < c2;", res: executeResult{
			data: [][]string{
				{"c", "e"},
			},
		}},
		{sql: "select * from ccs where c1 < 'qwq';", res: executeResult{
			data: [][]string{
				{"kpi", "b"},
				{"c", "e"},
			},
		}},
		{sql: "select * from ccs where c1 < 'z' or c2 < 'z' or 'c' < c1 or 'c' < c2;", res: executeResult{
			data: [][]string{
				{"kpi", "b"},
				{"c", "e"},
			},
		}},
		{sql: "select * from ccs where c1 >= c2 or c2 >= c1;", res: executeResult{
			data: [][]string{
				{"kpi", "b"},
				{"c", "e"},
			},
		}},
		{sql: "select * from ccs where c1 >= 'ptp';", res: executeResult{
			null: true,
		}},
		{sql: "select * from ccs where c1 >= 'a' or c2 >= 'a' or 'v' >= c1 or 'v' >= c2;", res: executeResult{
			data: [][]string{
				{"kpi", "b"},
				{"c", "e"},
			},
		}},
		{sql: "select * from ccs where c1 <= c2 or c2 <= c2;", res: executeResult{
			data: [][]string{
				{"kpi", "b"},
				{"c", "e"},
			},
		}},
		{sql: "select * from ccs where c2 <= 'ccs';", res: executeResult{
			data: [][]string{
				{"kpi", "b"},
			},
		}},
		{sql: "select * from ccs where c1 <= 'z' or c2 <= 'z' or 'c' <= c1 or 'c' <= c2;", res: executeResult{
			data: [][]string{
				{"kpi", "b"},
				{"c", "e"},
			},
		}},
		{sql: "select * from ccs where c1 != c2 or c2 != c2;", res: executeResult{
			data: [][]string{
				{"kpi", "b"},
				{"c", "e"},
			},
		}},
		{sql: "select * from ccs where c2 != 'openSource';", res: executeResult{
			data: [][]string{
				{"kpi", "b"},
				{"c", "e"},
			},
		}},
		{sql: "select * from ccs where c1 != 'z' or c2 != 'z' or 'c' != c1 or 'c' != c2;", res: executeResult{
			data: [][]string{
				{"kpi", "b"},
				{"c", "e"},
			},
		}},
		{sql: "select * from ccs where c1 = c1 and c2 = c2;", res: executeResult{
			data: [][]string{
				{"kpi", "b"},
				{"c", "e"},
			},
		}},
		{sql: "select * from ccs where c1 > c1 and c2 > c2;", res: executeResult{
			null: true,
		}},
		{sql: "select * from ccs where c1 < c1 and c2 < c2;", res: executeResult{
			null: true,
		}},
		{sql: "select * from ccs where c1 >= c1 and c2 >= c2;", res: executeResult{
			data: [][]string{
				{"kpi", "b"},
				{"c", "e"},
			},
		}},
		{sql: "select * from ccs where c1 <= c1 and c2 <= c2;", res: executeResult{
			data: [][]string{
				{"kpi", "b"},
				{"c", "e"},
			},
		}},
		{sql: "select * from ccs where c1 != c1 and c2 != c2;", res: executeResult{
			null: true,
		}},
		{sql: "select d1 from dds where d1 = '2012-02-03';", res: executeResult{
			null: true,
		}},
		{sql: "select d1 from dds where d1 != '2012-02-03';", res: executeResult{
			data: [][]string{
				{"2015-06-30"},
			},
		}},
		{sql: "select d1 from dds where d1 = '2015-06-30';", res: executeResult{
			data: [][]string{
				{"2015-06-30"},
			},
		}},
		{sql: "select d1 from dds where '2015-06-30' = d1;", res: executeResult{
			data: [][]string{
				{"2015-06-30"},
			},
		}},
		{sql: "select d1 from dds where d1 < '2012-02-03';", res: executeResult{
			null: true,
		}},
		{sql: "select d1 from dds where d1 <= '2012-02-03';", res: executeResult{
			null: true,
		}},
		{sql: "select d1 from dds where d1 > '2012-02-03';", res: executeResult{
			data: [][]string{
				{"2015-06-30"},
			},
		}},
		{sql: "select d1 from dds where d1 >= '2012-02-03';", res: executeResult{
			data: [][]string{
				{"2015-06-30"},
			},
		}},
		{sql: "select d2 from dds where d2 >= '2012-02-03 10:10:10';", res: executeResult{
			null: true,
		}},
		{sql: "select d2 from dds where d2 <= '2012-02-03 10:10:10';", res: executeResult{
			data: [][]string{
				{"2007-01-01 12:13:04"},
			},
		}},
		{sql: "select d2 from dds where d2 < '2007-01-01 12:13:04';", res: executeResult{
			null: true,
		}},
		{sql: "select d2 from dds where d2 > '2007-01-01 12:13:04';", res: executeResult{
			null: true,
		}},
		{sql: "select d2 from dds where d2 != '2007-01-01 12:13:04';", res: executeResult{
			null: true,
		}},
		{sql: "select d2 from dds where d2 = '2007-01-01 12:13:04';", res: executeResult{
			data: [][]string{
				{"2007-01-01 12:13:04"},
			},
		}},
		{sql: "select d2 from dds where '2007-01-01 12:13:04' = d2;", res: executeResult{
			data: [][]string{
				{"2007-01-01 12:13:04"},
			},
		}},
		{sql: "select d2 from dds where '2012-02-03 10:10:10' >= d2;", res: executeResult{
			data: [][]string{
				{"2007-01-01 12:13:04"},
			},
		}},
		{sql: "select d1 from dds where d1 >= '2012=02=3';", err: "[22000]Incorrect date value"},
		{sql: "select d1 from dds where '2012=02=3' >= d1;", err: "[22000]Incorrect date value"},
		{sql: "select d2 from dds where d2 >= '2012=02=3';", err: "[22000]Incorrect datetime value"},
		{sql: "select d2 from dds where '2012=02=3' >= d2;", err: "[22000]Incorrect datetime value"},
		// test IN and NOT IN operator
		{sql: "select * from iis where i4 in (1);", res: executeResult{
			null: false,
			data: [][]string{{"1", "null", "null", "1"}},
		}},
		{sql: "select * from iis where i4 in (1, 1111);", res: executeResult{
			null: false,
			data: [][]string{{"1", "11", "111", "1111"}, {"1", "null", "null", "1"}},
		}},
		{sql: "select * from iis where i4 not in (1);", res: executeResult{
			null: false,
			data: [][]string{{"1", "11", "111", "1111"}},
		}},
		{sql: "select * from ffs where f2 in (222.222);", res: executeResult{
			null: false,
			data: [][]string{{"22.200001", "222.222000"}},
		}},
		{sql: "select * from uus where u1 in (3);", res: executeResult{
			null: false,
			data: [][]string{{"3", "33", "333", "3333"}},
		}},
		{sql: "select * from ccs where c1 in ('kpi');", res: executeResult{
			null: false,
			data: [][]string{{"kpi", "b"}},
		}},
		{sql: "select * from ccs where c1 not in ('kpi');", res: executeResult{
			null: false,
			data: [][]string{{"c", "e"}},
		}},
		{sql: "select * from int_decimal where d1 = d1;", res: executeResult{
			data: [][]string{{"1", "1", "22", "22", "33333300"}},
		}},
		{sql: "select * from int_decimal where i1 < d1;", res: executeResult{
			data: [][]string{{"1", "1", "22", "22", "33333300"}},
		}},
		{sql: "select * from int_decimal where i1 <= d1;", res: executeResult{
			data: [][]string{{"1", "1", "22", "22", "33333300"}},
		}},
		{sql: "select * from int_decimal where i1 > d1;", res: executeResult{}},
		{sql: "select * from int_decimal1 where d1 = d1;", res: executeResult{
			data: [][]string{{"1", "1", "22", "22", "{33333300 0}"}},
		}},
		{sql: "select * from int_decimal1 where i1 < d1;", res: executeResult{
			data: [][]string{{"1", "1", "22", "22", "{33333300 0}"}},
		}},
		{sql: "select * from int_decimal1 where i1 <= d1;", res: executeResult{
			data: [][]string{{"1", "1", "22", "22", "{33333300 0}"}},
		}},
		{sql: "select * from int_decimal1 where i1 > d1;", res: executeResult{}},
	}
	test(t, testCases)
}
