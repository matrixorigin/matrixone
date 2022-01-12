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
	e, proc := newTestEngine()
	noError := []string{
		// data preparation
		"create table iis (i1 tinyint, i2 smallint, i3 int, i4 bigint);",
		"create table iis2 (i11 tinyint, i12 tinyint, i21 smallint, i22 smallint, i31 int, i32 int, i41 bigint, i42 bigint);",
		"create table ffs (f1 float, f2 double);",
		"create table ffs2 (f11 float, f12 float, f21 double, f22 double);",
		"create table uus (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);",
		"create table uus2 (u11 tinyint unsigned, u12 tinyint unsigned, u21 smallint unsigned, u22 smallint unsigned, u31 int unsigned, u32 int unsigned, "+
			"u41 bigint unsigned, u42 bigint unsigned);",

		"insert into iis values (1, 11, 111, 1111), (1, null, null, 1);",
		"insert into iis2 values (1, 1, 22, 22, 333, 333, 4444, 4444);",
		"insert into ffs values (22.2, 222.222), (null, null);",
		"insert into ffs2 values (22.2, 22.2, 222.222, 222.222), (null, null, null, null);",
		"insert into uus values (3, 33, 333, 3333), (null, null, null, null);",
		"insert into uus2 values (1, 1, 22, 22, 33, 33, 444, 444);",
		// Test +
		"select i1 + i1, i1 + i2, i1 + i3, i1 + i4, i1 + 2, 3 + i1 from iis;",
		"select i2 + i1, i2 + i2, i2 + i3, i2 + i4, i2 + 2, 3 + i2 from iis;",
		"select i3 + i1, i3 + i2, i3 + i3, i3 + i4, i3 + 2, 3 + i3 from iis;",
		"select i4 + i1, i4 + i2, i4 + i3, i4 + i4, i4 + 2, 3 + i4 from iis;",

		"select u1 + u1, u1 + u2, u1 + u3, u1 + u4, u1 + 2, 3 + u1 from uus;",
		"select u2 + u1, u2 + u2, u2 + u3, u2 + u4, u2 + 2, 3 + u2 from uus;",
		"select u3 + u1, u3 + u2, u3 + u3, u3 + u4, u3 + 2, 3 + u3 from uus;",
		"select u4 + u1, u4 + u2, u4 + u3, u4 + u4, u4 + 2, 3 + u4 from uus;",

		"select f1 + f1, f1 - f1, f1 * f1, f1 / f1 from ffs;",
		"select f1 + 1.0, 1.0 + f1, f1 - 2.0, 2.0 - f1, f1 * 3.0, 3.0 * f1, f1 / 0.5, 0.5 / f1 from ffs;",
		"select f2 + f2, f2 - f2, f2 * f2, f2 / f2 from ffs;",
		"select f2 + 1.0, 1.0 + f2, f2 - 2.0, 2.0 - f2, f2 * 3.0, 3.0 * f2, f2 / 0.5, 0.5 / f2 from ffs;",

		"select i11 + i12, i21 + i22, i31 + i32, i41 + i42 from iis2;",
		"select i11 + i12, i21 + i22, i31 + i32, i41 + i42, i11, i21, i31, i41 from iis2;",

		"select u11 + u12, u21 + u22, u31 + u32, u41 + u42 from uus2;",
		"select u11 + u12, u21 + u22, u31 + u32, u41 + u42, u11, u21, u31, u41 from uus2;",

		"select f11 + f12, f21 + f22 from ffs2;",
		"select f11 + f12, f21 + f22, f11, f21 from ffs2;",
	}
	test(t, e, proc, noError, nil, nil)
}

// TestMinusOperator will run some sql to test - operator
func TestMinusOperator(t *testing.T) {
	e, proc := newTestEngine()
	noError := []string{
		// data preparation
		"create table iis (i1 tinyint, i2 smallint, i3 int, i4 bigint);",
		"create table iis2 (i11 tinyint, i12 tinyint, i21 smallint, i22 smallint, i31 int, i32 int, i41 bigint, i42 bigint);",
		"create table ffs (f1 float, f2 double);",
		"create table ffs2 (f11 float, f12 float, f21 double, f22 double);",
		"create table uus (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);",
		"create table uus2 (u11 tinyint unsigned, u12 tinyint unsigned, u21 smallint unsigned, u22 smallint unsigned, u31 int unsigned, u32 int unsigned, "+
			"u41 bigint unsigned, u42 bigint unsigned);",

		"insert into iis values (1, 11, 111, 1111), (1, null, null, 1);",
		"insert into iis2 values (1, 1, 22, 22, 333, 333, 4444, 4444);",
		"insert into ffs values (22.2, 222.222), (null, null);",
		"insert into ffs2 values (22.2, 22.2, 222.222, 222.222), (null, null, null, null);",
		"insert into uus values (3, 33, 333, 3333), (null, null, null, null);",
		"insert into uus2 values (1, 1, 22, 22, 33, 33, 444, 444);",
		// Test -
		"select i1 - i1, i1 - i2, i1 - i3, i1 - i4, i1 - 2, 3 - i1 from iis;",
		"select i2 - i1, i2 - i2, i2 - i3, i2 - i4, i2 - 2, 3 - i2 from iis;",
		"select i3 - i1, i3 - i2, i3 - i3, i3 - i4, i3 - 2, 3 - i3 from iis;",
		"select i4 - i1, i4 - i2, i4 - i3, i4 - i4, i4 - 2, 3 - i4 from iis;",

		"select u1 - u1, u1 - u2, u1 - u3, u1 - u4, u1 - 2, 3 - u1 from uus;",
		"select u2 - u1, u2 - u2, u2 - u3, u2 - u4, u2 - 2, 3 - u2 from uus;",
		"select u3 - u1, u3 - u2, u3 - u3, u3 - u4, u3 - 2, 3 - u3 from uus;",
		"select u4 - u1, u4 - u2, u4 - u3, u4 - u4, u4 - 2, 3 - u4 from uus;",

		"select f11 - f12, f21 - f22 from ffs2;",
		"select f11 - f12, f21 - f22, f11, f21 from ffs2;",

		"select i11 - i12, i21 - i22, i31 - i32, i41 - i42 from iis2;",
		"select i11 - i12, i21 - i22, i31 - i32, i41 - i42, i11, i21, i31, i41 from iis2;",

		"select u11 - u12, u21 - u22, u31 - u32, u41 - u42 from uus2;",
		"select u11 - u12, u21 - u22, u31 - u32, u41 - u42, u11, u21, u31, u41 from uus2;",
	}
	test(t, e, proc, noError, nil, nil)
}

// TestMultiOperator will run some sql to test * operator
func TestMultiOperator(t *testing.T) {
	e, proc := newTestEngine()
	noError := []string{
		// data preparation
		"create table iis (i1 tinyint, i2 smallint, i3 int, i4 bigint);",
		"create table iis2 (i11 tinyint, i12 tinyint, i21 smallint, i22 smallint, i31 int, i32 int, i41 bigint, i42 bigint);",
		"create table ffs (f1 float, f2 double);",
		"create table ffs2 (f11 float, f12 float, f21 double, f22 double);",
		"create table uus (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);",
		"create table uus2 (u11 tinyint unsigned, u12 tinyint unsigned, u21 smallint unsigned, u22 smallint unsigned, u31 int unsigned, u32 int unsigned, "+
			"u41 bigint unsigned, u42 bigint unsigned);",

		"insert into iis values (1, 11, 111, 1111), (1, null, null, 1);",
		"insert into iis2 values (1, 1, 22, 22, 333, 333, 4444, 4444);",
		"insert into ffs values (22.2, 222.222), (null, null);",
		"insert into ffs2 values (22.2, 22.2, 222.222, 222.222), (null, null, null, null);",
		"insert into uus values (3, 33, 333, 3333), (null, null, null, null);",
		"insert into uus2 values (1, 1, 22, 22, 33, 33, 444, 444);",
		// Test *
		"select i1 * i1, i1 * i2, i1 * i3, i1 * i4, i1 * 2, 3 * i1 from iis;",
		"select i2 * i1, i2 * i2, i2 * i3, i2 * i4, i2 * 2, 3 * i2 from iis;",
		"select i3 * i1, i3 * i2, i3 * i3, i3 * i4, i3 * 2, 3 * i3 from iis;",
		"select i4 * i1, i4 * i2, i4 * i3, i4 * i4, i4 * 2, 3 * i4 from iis;",

		"select u1 * u1, u1 * u2, u1 * u3, u1 * u4, u1 * 2, 3 * u1 from uus;",
		"select u2 * u1, u2 * u2, u2 * u3, u2 * u4, u2 * 2, 3 * u2 from uus;",
		"select u3 * u1, u3 * u2, u3 * u3, u3 * u4, u3 * 2, 3 * u3 from uus;",
		"select u4 * u1, u4 * u2, u4 * u3, u4 * u4, u4 * 2, 3 * u4 from uus;",

		"select f11 * f12, f21 * f22 from ffs2;",
		"select f11 * f12, f21 * f22, f11, f21 from ffs2;",

		"select i11 * i12, i21 * i22, i31 * i32, i41 * i42 from iis2;",
		"select i11 * i12, i21 * i22, i31 * i32, i41 * i42, i11, i21, i31, i41 from iis2;",

		"select u11 * u12, u21 * u22, u31 * u32, u41 * u42 from uus2;",
		"select u11 * u12, u21 * u22, u31 * u32, u41 * u42, u11, u21, u31, u41 from uus2;",
	}
	test(t, e, proc, noError, nil, nil)
}

// TestDivOperator will run some sql to test / and div(integer div) operator
func TestDivOperator(t *testing.T) {
	e, proc := newTestEngine()
	noError := []string{
		// data preparation
		"create table iis (i1 tinyint, i2 smallint, i3 int, i4 bigint);",
		"create table iis2 (i11 tinyint, i12 tinyint, i21 smallint, i22 smallint, i31 int, i32 int, i41 bigint, i42 bigint);",
		"create table ffs (f1 float, f2 double);",
		"create table ffs2 (f11 float, f12 float, f21 double, f22 double);",
		"create table uus (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);",
		"create table uus2 (u11 tinyint unsigned, u12 tinyint unsigned, u21 smallint unsigned, u22 smallint unsigned, u31 int unsigned, u32 int unsigned, "+
			"u41 bigint unsigned, u42 bigint unsigned);",

		"insert into iis values (1, 11, 111, 1111), (1, null, null, 1);",
		"insert into iis2 values (1, 1, 22, 22, 333, 333, 4444, 4444);",
		"insert into ffs values (22.2, 222.222), (null, null);",
		"insert into ffs2 values (22.2, 22.2, 222.222, 222.222), (null, null, null, null);",
		"insert into uus values (3, 33, 333, 3333), (null, null, null, null);",
		"insert into uus2 values (1, 1, 22, 22, 33, 33, 444, 444);",

		// Test `/` and `div`
		"select i1 / i1, i1 / i2, i1 / i3, i1 / i4, i1 / 2, 3 / i1 from iis;",
		"select i2 / i1, i2 / i2, i2 / i3, i2 / i4, i2 / 2, 3 / i2 from iis;",
		"select i3 / i1, i3 / i2, i3 / i3, i3 / i4, i3 / 2, 3 / i3 from iis;",
		"select i4 / i1, i4 / i2, i4 / i3, i4 / i4, i4 / 2, 3 / i4 from iis;",

		"select i1 div i1, i1 div i2, i1 div i3, i1 div i4, i1 div 2, 3 div i1 from iis;",
		"select i2 div i1, i2 div i2, i2 div i3, i2 div i4, i2 div 2, 3 div i2 from iis;",
		"select i3 div i1, i3 div i2, i3 div i3, i3 div i4, i3 div 2, 3 div i3 from iis;",
		"select i4 div i1, i4 div i2, i4 div i3, i4 div i4, i4 div 2, 3 div i4 from iis;",

		"select u1 / u1, u1 / u2, u1 / u3, u1 / u4, u1 / 2, 3 / u1 from uus;",
		"select u2 / u1, u2 / u2, u2 / u3, u2 / u4, u2 / 2, 3 / u2 from uus;",
		"select u3 / u1, u3 / u2, u3 / u3, u3 / u4, u3 / 2, 3 / u3 from uus;",
		"select u4 / u1, u4 / u2, u4 / u3, u4 / u4, u4 / 2, 3 / u4 from uus;",

		"select u1 div u1, u1 div u2, u1 div u3, u1 div u4, u1 div 2, 3 div u1 from uus;",
		"select u2 div u1, u2 div u2, u2 div u3, u2 div u4, u2 div 2, 3 div u2 from uus;",
		"select u3 div u1, u3 div u2, u3 div u3, u3 div u4, u3 div 2, 3 div u3 from uus;",
		"select u4 div u1, u4 div u2, u4 div u3, u4 div u4, u4 div 2, 3 div u4 from uus;",

		"select i11 div i12, i21 div i22, i31 div i32, i41 div i42 from iis2;",
		"select i11 div i12, i21 div i22, i31 div i32, i41 div i42, i11, i21, i31, i41 from iis2;",

		"select i11 / i12, i21 / i22, i31 / i32, i41 / i42 from iis2;",
		"select i11 / i12, i21 / i22, i31 / i32, i41 / i42, i11, i21, i31, i41 from iis2;",

		"select u11 div u12, u21 div u22, u31 div u32, u41 div u42 from uus2;",
		"select u11 div u12, u21 div u22, u31 div u32, u41 div u42, u11, u21, u31, u41 from uus2;",

		"select u11 / u12, u21 / u22, u31 / u32, u41 / u42 from uus2;",
		"select u11 / u12, u21 / u22, u31 / u32, u41 / u42, u11, u21, u31, u41 from uus2;",

		"select f11 / f12, f21 / f22 from ffs2;",
		"select f11 / f12, f21 / f22, f11, f21 from ffs2;",

		"select f11 div f12, f21 div f22 from ffs2;",
		"select f11 div f12, f21 div f22, f11, f21 from ffs2;",
	}

	retError := [][]string{
		{"select i1 / 0 from iis;", "[42000]division by zero"},
		{"select i2 / 0 from iis;", "[42000]division by zero"},
		{"select i3 / 0 from iis;", "[42000]division by zero"},
		{"select i4 / 0 from iis;", "[42000]division by zero"},
		{"select u1 / 0 from uus;", "[42000]division by zero"},
		{"select u2 / 0 from uus;", "[42000]division by zero"},
		{"select u3 / 0 from uus;", "[42000]division by zero"},
		{"select u4 / 0 from uus;", "[42000]division by zero"},
		{"select f1 / 0 from ffs;", "[42000]division by zero"},
		{"select f2 / 0 from ffs;", "[42000]division by zero"},
		{"select i1 div 0 from iis;", "[42000]division by zero"},
		{"select i2 div 0 from iis;", "[42000]division by zero"},
		{"select i3 div 0 from iis;", "[42000]division by zero"},
		{"select i4 div 0 from iis;", "[42000]division by zero"},
		{"select u1 div 0 from uus;", "[42000]division by zero"},
		{"select u2 div 0 from uus;", "[42000]division by zero"},
		{"select u3 div 0 from uus;", "[42000]division by zero"},
		{"select u4 div 0 from uus;", "[42000]division by zero"},
		{"select f1 div 0 from ffs;", "[42000]division by zero"},
		{"select f2 div 0 from ffs;", "[42000]division by zero"},
	}
	test(t, e, proc, noError, nil, retError)
}

// TestModOperator will run some sql to test %(as same as mod) operator
func TestModOperator(t *testing.T) {
	e, proc := newTestEngine()
	noError := []string{
		// data preparation
		"create table iis (i1 tinyint, i2 smallint, i3 int, i4 bigint);",
		"create table iis2 (i11 tinyint, i12 tinyint, i21 smallint, i22 smallint, i31 int, i32 int, i41 bigint, i42 bigint);",
		"create table ffs (f1 float, f2 double);",
		"create table ffs2 (f11 float, f12 float, f21 double, f22 double);",
		"create table uus (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);",
		"create table uus2 (u11 tinyint unsigned, u12 tinyint unsigned, u21 smallint unsigned, u22 smallint unsigned, u31 int unsigned, u32 int unsigned, "+
			"u41 bigint unsigned, u42 bigint unsigned);",

		"insert into iis values (1, 11, 111, 1111), (1, null, null, 1);",
		"insert into iis2 values (1, 1, 22, 22, 333, 333, 4444, 4444);",
		"insert into ffs values (22.2, 222.222), (null, null);",
		"insert into ffs2 values (22.2, 22.2, 222.222, 222.222), (null, null, null, null);",
		"insert into uus values (3, 33, 333, 3333), (null, null, null, null);",
		"insert into uus2 values (1, 1, 22, 22, 33, 33, 444, 444);",

		// Test mod
		"select i1 mod i1, i1 mod i2, i1 mod i3, i1 mod i4, i1 mod 2, 3 mod i1 from iis;",
		"select i2 mod i1, i2 mod i2, i2 mod i3, i2 mod i4, i2 mod 2, 3 mod i2 from iis;",
		"select i3 mod i1, i3 mod i2, i3 mod i3, i3 mod i4, i3 mod 2, 3 mod i3 from iis;",
		"select i4 mod i1, i4 mod i2, i4 mod i3, i4 mod i4, i4 mod 2, 3 mod i4 from iis;",

		"select u1 mod u1, u1 mod u2, u1 mod u3, u1 mod u4, u1 mod 2, 3 mod u1 from uus;",
		"select u2 mod u1, u2 mod u2, u2 mod u3, u2 mod u4, u2 mod 2, 3 mod u2 from uus;",
		"select u3 mod u1, u3 mod u2, u3 mod u3, u3 mod u4, u3 mod 2, 3 mod u3 from uus;",
		"select u4 mod u1, u4 mod u2, u4 mod u3, u4 mod u4, u4 mod 2, 3 mod u4 from uus;",

		"select i11 mod i12, i21 mod i22, i31 mod i32, i41 mod i42 from iis2;",
		"select i11 mod i12, i21 mod i22, i31 mod i32, i41 mod i42, i11, i21, i31, i41 from iis2;",

		"select u11 mod u12, u21 mod u22, u31 mod u32, u41 mod u42 from uus2;",
		"select u11 mod u12, u21 mod u22, u31 mod u32, u41 mod u42, u11, u21, u31, u41 from uus2;",

		"select f11 mod f12, f21 mod f22 from ffs2;",
		"select f11 mod f12, f21 mod f22, f11, f21 from ffs2;",
	}

	retError := [][]string{
		{"select i1 % 0 from iis;", "[42000]zero modulus"},
		{"select i2 % 0 from iis;", "[42000]zero modulus"},
		{"select i3 % 0 from iis;", "[42000]zero modulus"},
		{"select i4 % 0 from iis;", "[42000]zero modulus"},
		{"select u1 % 0 from uus;", "[42000]zero modulus"},
		{"select u2 % 0 from uus;", "[42000]zero modulus"},
		{"select u3 % 0 from uus;", "[42000]zero modulus"},
		{"select u4 % 0 from uus;", "[42000]zero modulus"},
		{"select f1 % 0 from ffs;", "[42000]zero modulus"},
		{"select f2 % 0 from ffs;", "[42000]zero modulus"},
	}
	test(t, e, proc, noError, nil, retError)
}

// TestUnaryMinusOperator will run some sql to test - before a numeric, such as '-1'
func TestUnaryMinusOperator(t *testing.T) {
	e, proc := newTestEngine()
	noError := []string{
		// data preparation
		"create table iis (i1 tinyint, i2 smallint, i3 int, i4 bigint);",
		"create table ffs (f1 float, f2 double);",
		"create table uus (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);",

		"insert into iis values (1, 11, 111, 1111), (1, null, null, 1);",
		"insert into ffs values (22.2, 222.222), (null, null);",
		"insert into uus values (3, 33, 333, 3333), (null, null, null, null);",
		// Test -
		"select -i1, -i2, -i3, -i4 from iis;",
		"select -f1, -f2 from ffs;",
		"select -i1, i1 from iis;",
		"select -i2, i2 from iis;",
		"select -i3, i3 from iis;",
		"select -i4, i4 from iis;",
		"select -f1, f1 from ffs;",
		"select -f2, f2 from ffs;",
	}
	retError := [][]string{
		{"select -u1, -u2, -u3, -u4 from uus;", "'-' not yet implemented for TINYINT UNSIGNED"},
	}
	test(t, e, proc, noError, nil, retError)
}

// TestLikeOperator will run some sql to test like operator
func TestLikeOperator(t *testing.T) {
	e, proc := newTestEngine()
	noError := []string{
		// data preparation
		"create table tk (a varchar(10), b char(20));",
		"insert into tk values ('a', 'a'),('abc', 'cba'),('abcd', '%'),('hello', ''),('test', 't___'),(null, null);",

		// Test Like
		"select * from tk where a like 'abc%';",
		"select * from tk where a like '_bc%';",
		"select * from tk where 'abc%' like a;",
		"select * from tk where '_bc%' like a;",
		"select * from tk where 'a' like 'a';",
		"select * from tk where a like a;",
		"select * from tk where a like b;",
		"select * from tk where b like a;",
		"select * from tk where b like b;",
		"select * from tk where b like 'abc%';",
		"select * from tk where b like '_bc%';",
		"select * from tk where 'abc%' like b;",
		"select * from tk where '_bc%' like b;",
	}
	test(t, e, proc, noError, nil, nil)
}

// TestCastOperator will run some sql to test cast operator
func TestCastOperator(t *testing.T) {
	e, proc := newTestEngine()
	noError := []string{
		// data preparation
		"create table iis (i1 tinyint, i2 smallint, i3 int, i4 bigint);",
		"create table ffs (f1 float, f2 double);",
		"create table uus (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);",
		"create table ccs3 (c1 char(10), c2 varchar(10));",
		"insert into iis values (1, 11, 111, 1111), (1, null, null, 1);",
		"insert into ffs values (22.2, 222.222), (null, null);",
		"insert into uus values (3, 33, 333, 3333), (null, null, null, null);",
		"insert into ccs3 values ('123', '123456');",

		// Test cast rule
		"select CAST(i1 AS signed), CAST(i1 AS unsigned), CAST(i1 AS float(1)), CAST(i1 AS double), CAST(i1 AS char(10)) from iis;",
		"select CAST(i2 AS signed), CAST(i2 AS unsigned), CAST(i2 AS float(1)), CAST(i2 AS double), CAST(i2 AS char(10)) from iis;",
		"select CAST(i3 AS signed), CAST(i3 AS unsigned), CAST(i3 AS float(1)), CAST(i3 AS double), CAST(i3 AS char(10)) from iis;",
		"select CAST(i4 AS signed), CAST(i4 AS unsigned), CAST(i4 AS float(1)), CAST(i4 AS double), CAST(i4 AS char(10)) from iis;",
		"select CAST(u1 AS signed), CAST(u1 AS unsigned), CAST(u1 AS float(1)), CAST(u1 AS double), CAST(u1 AS char(10)) from uus;",
		"select CAST(u2 AS signed), CAST(u2 AS unsigned), CAST(u2 AS float(1)), CAST(u2 AS double), CAST(u2 AS char(10)) from uus;",
		"select CAST(u3 AS signed), CAST(u3 AS unsigned), CAST(u3 AS float(1)), CAST(u3 AS double), CAST(u3 AS char(10)) from uus;",
		"select CAST(u4 AS signed), CAST(u4 AS unsigned), CAST(u4 AS float(1)), CAST(u4 AS double), CAST(u4 AS char(10)) from uus;",
		"select CAST(f1 AS signed), CAST(f1 AS unsigned), CAST(f1 AS float(1)), CAST(f1 AS double), CAST(f1 AS char(10)) from ffs;",
		"select CAST(f2 AS signed), CAST(f2 AS unsigned), CAST(f2 AS float(1)), CAST(f2 AS double), CAST(f2 AS char(10)) from ffs;",
		"select CAST(c1 AS signed), CAST(c1 AS unsigned), CAST(c1 AS float(1)), CAST(c1 AS double), CAST(c1 AS char(10)) from ccs3;",
		"select CAST(c2 AS signed), CAST(c2 AS unsigned), CAST(c2 AS float(1)), CAST(c2 AS double), CAST(c2 AS char(10)) from ccs3;",
		"select i1, CAST(i1 AS signed), CAST(i1 AS unsigned), CAST(i1 AS float(1)), CAST(i1 AS double), CAST(i1 AS char(10)) from iis;",
		"select i2, CAST(i2 AS signed), CAST(i2 AS unsigned), CAST(i2 AS float(1)), CAST(i2 AS double), CAST(i2 AS char(10)) from iis;",
		"select i3, CAST(i3 AS signed), CAST(i3 AS unsigned), CAST(i3 AS float(1)), CAST(i3 AS double), CAST(i3 AS char(10)) from iis;",
		"select i4, CAST(i4 AS signed), CAST(i4 AS unsigned), CAST(i4 AS float(1)), CAST(i4 AS double), CAST(i4 AS char(10)) from iis;",
		"select u1, CAST(u1 AS signed), CAST(u1 AS unsigned), CAST(u1 AS float(1)), CAST(u1 AS double), CAST(u1 AS char(10)) from uus;",
		"select u2, CAST(u2 AS signed), CAST(u2 AS unsigned), CAST(u2 AS float(1)), CAST(u2 AS double), CAST(u2 AS char(10)) from uus;",
		"select u3, CAST(u3 AS signed), CAST(u3 AS unsigned), CAST(u3 AS float(1)), CAST(u3 AS double), CAST(u3 AS char(10)) from uus;",
		"select u4, CAST(u4 AS signed), CAST(u4 AS unsigned), CAST(u4 AS float(1)), CAST(u4 AS double), CAST(u4 AS char(10)) from uus;",
		"select f1, CAST(f1 AS signed), CAST(f1 AS unsigned), CAST(f1 AS float(1)), CAST(f1 AS double), CAST(f1 AS char(10)) from ffs;",
		"select f2, CAST(f2 AS signed), CAST(f2 AS unsigned), CAST(f2 AS float(1)), CAST(f2 AS double), CAST(f2 AS char(10)) from ffs;",
		"select c1, CAST(c1 AS signed), CAST(c1 AS unsigned), CAST(c1 AS float(1)), CAST(c1 AS double), CAST(c1 AS char(10)) from ccs3;",
		"select c2, CAST(c2 AS signed), CAST(c2 AS unsigned), CAST(c2 AS float(1)), CAST(c2 AS double), CAST(c2 AS char(10)) from ccs3;",
		"select i4, CAST(i4 AS signed) i5, CAST(i4 AS unsigned), CAST(i4 AS float(1)), CAST(i4 AS double), CAST(i4 AS char(10)) from iis;",
	}
	test(t, e, proc, noError, nil, nil)
}

// TestNotOperator will run some sql to test not operator
func TestNotOperator(t *testing.T) {
	e, proc := newTestEngine()
	noError := []string{
		// data preparation
		"create table iis (i1 tinyint, i2 smallint, i3 int, i4 bigint);",
		"create table ffs (f1 float, f2 double);",
		"create table uus (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);",
		"create table ccs (c1 char(10), c2 varchar(10));",
		"create table tk (spID int,userID int,score smallint);",
		"insert into iis values (1, 11, 111, 1111), (1, null, null, 1);",
		"insert into ffs values (22.2, 222.222), (null, null);",
		"insert into uus values (3, 33, 333, 3333), (null, null, null, null);",
		"insert into ccs values ('1.5', '2.50');",
		"insert into tk values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4);",

		// Test Not
		"select not i1, not i2, not i3, not i4 from iis;",
		"select not u1, not u2, not u3, not u4 from uus;",
		"select not f1, not f2 from ffs;",
		"select not i1, not i2, not i3, not i4, i1, i2, i3, i4 from iis;",
		"select not u1, not u2, not u3, not u4, u1, u2, u3, u4 from uus;",
		"select not f1, not f2, f1, f2 from ffs;",
		"select not c1, not c2 from ccs;",
		"select * from tk where not userID < 6 or not userID;",
		"select * from tk where userID < 6 and not userID;",
		"select * from tk where not userID < 6 and not userID;",
		"select * from tk where userID < 6 or not userID;",
		"select * from tk where not userID between 2 and 3;",
		"select * from tk where not (userID = 1 and userID = 5);",
		"select * from tk where not (userID = 1 or userID = 5);",
		"select * from tk where not not (userID = 1 or userID = 5);",
		"select * from tk where not not not (userID = 1 or userID = 5);",
		"select * from tk where not (userID = 1 or ( userID = 5 ) and userID = (7));",
	}
	test(t, e, proc, noError, nil, nil)
}

// TestAndOperator will run some sql to test and operator
func TestAndOperator(t *testing.T) {
	e, proc := newTestEngine()
	noError := []string{
		// data preparation
		"create table adt (i int, f double, c char(15));",
		// Test and
		"select i, f, c from adt where i = i and f >= f;",
		"select i, f, c from adt where i = i and f >= f and c = c;",
		"select i, f, c from adt where i = i and f >= f and c = c and i <= 10;",
		"select i, f, c from adt where (i = i and f >= f) and c = c and i <= 10;",
	}
	test(t, e, proc, noError, nil, nil)
}

// TestOrOperator will run some sql to test or operator
func TestOrOperator(t *testing.T) {
	e, proc := newTestEngine()
	noError := []string{
		// data preparation
		"create table adt (i int, f double, c char(15));",
		// Test or
		"select i, f, c from adt where i = i or f >= f;",
		"select i, f, c from adt where i = i or f >= f or c = c;",
		"select i, f, c from adt where i = i or f >= f or c = c or i <= 10;",
		"select i, f, c from adt where (i = i or f >= f) or c = c or i <= 10;",
	}
	test(t, e, proc, noError, nil, nil)
}

// TestComparisonOperator will run some sql to test =, !=, >, <, >=, <= for basic type families
// family int, family float, family string
func TestComparisonOperator(t *testing.T) {
	e, proc := newTestEngine()
	noError := []string{
		// data preparation
		"create table iis (i1 tinyint, i2 smallint, i3 int, i4 bigint);",
		"create table ffs (f1 float, f2 double);",
		"create table uus (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);",
		"create table ccs (c1 char(10), c2 varchar(20));",
		"insert into iis values (1, 11, 111, 1111), (1, null, null, 1);",
		"insert into ffs values (22.2, 222.222), (null, null);",
		"insert into uus values (3, 33, 333, 3333), (null, null, null, null);",
		"insert into ccs values ('a', 'b'), ('c', 'e'), (null, null);",

		// Test =, >, <, >=, <=, !=
		"select * from iis where i1 = i1 and i2 = i2 and i3 = i3 and i4 = i4;",
		"select * from iis where i1 = i2 and i1 = i3 and i1 = i4 and i2 = i1 and i3 = i1 and i4 = i1;",
		"select * from iis where i2 = i3 and i2 = i4 and i3 = i2 and i4 = i2;",
		"select * from iis where i1 = 1;",
		"select * from iis where 1 = i1;",
		"select * from iis where i2 = 1 and 1 = i2 and i3 = 1 and 1 = i3 and 1 = i4 or i4 = 1;",
		"select * from uus where u1 = u1 and u2 = u2 and u3 = u3 and u4 = u4;",
		"select * from uus where u1 = u2 and u1 = u3 and u1 = u4 and u2 = u1 and u3 = u1 and u4 = u1;",
		"select * from uus where u2 = u3 and u2 = u4 and u3 = u2 and u4 = u2;",
		"select * from uus where u1 = 1;",
		"select * from uus where 1 = u1;",
		"select * from uus where u2 = 1 and 1 = u2 and u3 = 1 and 1 = u3 and 1 = u4 or u4 = 1;",
		"select * from iis where i1 >= i1 and i2 >= i2 and i3 >= i3 and i4 >= i4;",
		"select * from iis where i1 >= i2 and i1 >= i3 and i1 >= i4 and i2 >= i1 and i3 >= i1 and i4 >= i1;",
		"select * from iis where i2 >= i3 and i2 >= i4 and i3 >= i2 and i4 >= i2;",
		"select * from iis where i1 >= 1;",
		"select * from iis where 1 >= i1;",
		"select * from iis where i2 >= 1 and 1 >= i2 and i3 >= 1 and 1 >= i3 and 1 >= i4 or i4 >= 1;",
		"select * from uus where u1 >= u1 and u2 >= u2 and u3 >= u3 and u4 >= u4;",
		"select * from uus where u1 >= u2 and u1 >= u3 and u1 >= u4 and u2 >= u1 and u3 >= u1 and u4 >= u1;",
		"select * from uus where u2 >= u3 and u2 >= u4 and u3 >= u2 and u4 >= u2;",
		"select * from uus where u1 >= 1;",
		"select * from uus where 1 >= u1;",
		"select * from uus where u2 >= 1 and 1 >= u2 and u3 >= 1 and 1 >= u3 and 1 >= u4 or u4 >= 1;",
		"select * from iis where i1 <= i1 and i2 <= i2 and i3 <= i3 and i4 <= i4;",
		"select * from iis where i1 <= i2 and i1 <= i3 and i1 <= i4 and i2 <= i1 and i3 <= i1 and i4 <= i1;",
		"select * from iis where i2 <= i3 and i2 <= i4 and i3 <= i2 and i4 <= i2;",
		"select * from iis where i1 <= 1;",
		"select * from iis where 1 <= i1;",
		"select * from iis where i2 <= 1 and 1 <= i2 and i3 <= 1 and 1 <= i3 and 1 <= i4 or i4 <= 1;",
		"select * from uus where u1 <= u1 and u2 <= u2 and u3 <= u3 and u4 <= u4;",
		"select * from uus where u1 <= u2 and u1 <= u3 and u1 <= u4 and u2 <= u1 and u3 <= u1 and u4 <= u1;",
		"select * from uus where u2 <= u3 and u2 <= u4 and u3 <= u2 and u4 <= u2;",
		"select * from uus where u1 <= 1;",
		"select * from uus where 1 <= u1;",
		"select * from uus where u2 <= 1 and 1 <= u2 and u3 <= 1 and 1 <= u3 and 1 <= u4 or u4 <= 1;",
		"select * from iis where i1 > i1 and i2 > i2 and i3 > i3 and i4 > i4;",
		"select * from iis where i1 > i2 and i1 > i3 and i1 > i4 and i2 > i1 and i3 > i1 and i4 > i1;",
		"select * from iis where i2 > i3 and i2 > i4 and i3 > i2 and i4 > i2;",
		"select * from iis where i1 > 1;",
		"select * from iis where 1 > i1;",
		"select * from iis where i2 > 1 and 1 > i2 and i3 > 1 and 1 > i3 and 1 > i4 or i4 > 1;",
		"select * from uus where u1 > u1 and u2 > u2 and u3 > u3 and u4 > u4;",
		"select * from uus where u1 > u2 and u1 > u3 and u1 > u4 and u2 > u1 and u3 > u1 and u4 > u1;",
		"select * from uus where u2 > u3 and u2 > u4 and u3 > u2 and u4 > u2;",
		"select * from uus where u1 > 1;",
		"select * from uus where 1 > u1;",
		"select * from uus where u2 > 1 and 1 > u2 and u3 > 1 and 1 > u3 and 1 > u4 or u4 > 1;",
		"select * from iis where i1 < i1 and i2 < i2 and i3 < i3 and i4 < i4;",
		"select * from iis where i1 < i2 and i1 < i3 and i1 < i4 and i2 < i1 and i3 < i1 and i4 < i1;",
		"select * from iis where i2 < i3 and i2 < i4 and i3 < i2 and i4 < i2;",
		"select * from iis where i1 < 1;",
		"select * from iis where 1 < i1;",
		"select * from iis where i2 < 1 and 1 < i2 and i3 < 1 and 1 < i3 and 1 < i4 or i4 < 1;",
		"select * from uus where u1 < u1 and u2 < u2 and u3 < u3 and u4 < u4;",
		"select * from uus where u1 < u2 and u1 < u3 and u1 < u4 and u2 < u1 and u3 < u1 and u4 < u1;",
		"select * from uus where u2 < u3 and u2 < u4 and u3 < u2 and u4 < u2;",
		"select * from uus where u1 < 1;",
		"select * from uus where 1 < u1;",
		"select * from uus where u2 < 1 and 1 < u2 and u3 < 1 and 1 < u3 and 1 < u4 or u4 < 1;",
		"select * from iis where i1 != i1 and i2 != i2 and i3 != i3 and i4 != i4;",
		"select * from iis where i1 != i2 and i1 != i3 and i1 != i4 and i2 != i1 and i3 != i1 and i4 != i1;",
		"select * from iis where i2 != i3 and i2 != i4 and i3 != i2 and i4 != i2;",
		"select * from iis where i1 != 1;",
		"select * from iis where 1 != i1;",
		"select * from iis where i2 != 1 and 1 != i2 and i3 != 1 and 1 != i3 and 1 != i4 or i4 != 1;",
		"select * from uus where u1 != u1 and u2 != u2 and u3 != u3 and u4 != u4;",
		"select * from uus where u1 != u2 and u1 != u3 and u1 != u4 and u2 != u1 and u3 != u1 and u4 != u1;",
		"select * from uus where u2 != u3 and u2 != u4 and u3 != u2 and u4 != u2;",
		"select * from uus where u1 != 1;",
		"select * from uus where 1 != u1;",
		"select * from uus where u2 != 1 and 1 != u2 and u3 != 1 and 1 != u3 and 1 != u4 or u4 != 1;",
		"select * from ffs where f1 = f1 and f1 = f2 and f2 = f1 and f2 = f2;",
		"select * from ffs where f1 = 10.5;",
		"select * from ffs where 10.5 = f1;",
		"select * from ffs where 22 = f2 and f2 = 22;",
		"select * from ffs where f1 > f2 and f1 > f1 and f2 > f2 and f2 > f1;",
		"select * from ffs where 500 > f1;",
		"select * from ffs where f1 > 500;",
		"select * from ffs where 100 > f2 and f2 > 50;",
		"select * from ffs where f1 < f2 and f1 < f1 and f2 < f2 and f2 < f1;",
		"select * from ffs where f1 < 100;",
		"select * from ffs where 3.5 < f1;",
		"select * from ffs where 1 < f2 and f2 < 30;",
		"select * from ffs where f1 >= f2 and f1 >= f1 and f2 >= f2 and f2 >= f1;",
		"select * from ffs where f1 >= 22.2;",
		"select * from ffs where 33.3 >= f1;",
		"select * from ffs where 70 >= f1 and f1 >= -5;",
		"select * from ffs where f1 <= f2 and f1 <= f1 and f2 <= f2 and f2 <= f1;",
		"select * from ffs where f1 <= 100;",
		"select * from ffs where 100 <= f1;",
		"select * from ffs where -5 <= f1 and f1 <= 0.00;",
		"select * from ffs where f1 != f2 and f1 != f1 and f2 != f2 and f2 != f1;",
		"select * from ffs where f1 != 5;",
		"select * from ffs where 5 != f1;",
		"select * from ffs where 408 != f2 and f2 != 403.3;",
		"select * from ccs where c1 = 'kpi';",
		"select * from ccs where c1 = '123' and '123' = c1;",
		"select * from ccs where c2 = '123' or 'a' = c1;",
		"select * from ccs where c1 > c2 or c2 > c1;",
		"select * from ccs where c1 > 'bvh';",
		"select * from ccs where c1 > 'a' or c2 > 'a' or 'v' > c1 or 'v' > c2;",
		"select * from ccs where c1 < c2 or c2 < c2;",
		"select * from ccs where c1 < 'qwq';",
		"select * from ccs where c1 < 'z' or c2 < 'z' or 'c' < c1 or 'c' < c2;",
		"select * from ccs where c1 >= c2 or c2 >= c1;",
		"select * from ccs where c1 >= 'ptp';",
		"select * from ccs where c1 >= 'a' or c2 >= 'a' or 'v' >= c1 or 'v' >= c2;",
		"select * from ccs where c1 <= c2 or c2 <= c2;",
		"select * from ccs where c2 <= 'ccs';",
		"select * from ccs where c1 <= 'z' or c2 <= 'z' or 'c' <= c1 or 'c' <= c2;",
		"select * from ccs where c1 != c2 or c2 != c2;",
		"select * from ccs where c2 != 'openSource';",
		"select * from ccs where c1 != 'z' or c2 != 'z' or 'c' != c1 or 'c' != c2;",
		"select * from ccs where c1 = c1 and c2 = c2;",
		"select * from ccs where c1 > c1 and c2 > c2;",
		"select * from ccs where c1 < c1 and c2 < c2;",
		"select * from ccs where c1 >= c1 and c2 >= c2;",
		"select * from ccs where c1 <= c1 and c2 <= c2;",
		"select * from ccs where c1 != c1 and c2 != c2;",
	}
	test(t, e, proc, noError, nil, nil)
}