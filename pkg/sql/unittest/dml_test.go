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

// TestInsertFunction to make sure insert work can run normally
func TestInsertFunction(t *testing.T) {
	e, proc := newTestEngine()

	noError := []string{
		"create table iis(i1 tinyint, i2 smallint, i3 int, i4 bigint);",
		"create table uus(u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);",
		"create table ffs(f1 float, f2 double);",
		"create table ccs(c1 char(10), c2 varchar(15));",
		"create table def1 (i1 int default 888, i2 int default 888, i3 int default 888);",
		"create table def2 (id int default 1, name varchar(255) unique, age int);",
		"create table def3 (i int default -1, v varchar(10) default 'abc', c char(10) default '', price double default 0.00);",
		"create table def4 (d1 int, d2 int, d3 int, d4 int default 1);",
		"insert into iis values (1, 2, 3, 4), (1+1, 2-2, 3*3, 4/4), (1 div 1, 2+2/3, 3 mod 3, 4 + 0.5), (0, 0, 0, 0);",
		"insert into uus values (0, 0, 1, 1), (0.5, 3+4, 4-1, 2*7), (3/4, 4 div 5, 5 mod 6, 0);",
		"insert into ffs values (1.1, 2.2), (1, 2), (1+0.5, 2.5*3.5);",
		"insert into ccs values ('123', '34567');",
		"insert into def1 values (default, default, default), (1, default, default), (default, -1, default), (default, default, 0);",
		"insert into def2 (name, age) values ('Abby', 24);",
		"insert into def3 () values (), ();",
		"insert into def4 (d1, d2) values (1, 2);",
		"create table cha1 (a char(0));",
		"create table cha2 (a char);",
		"insert into cha2 values ('1');",
		"insert into cha1 values ('');",
	}
	selects := [][]string{ // First string is relation name, Second string is expected result.
		{"iis", "i1\n\t[1 2 1 0]-&{<nil>}\ni2\n\t[2 0 3 0]-&{<nil>}\ni3\n\t[3 9 0 0]-&{<nil>}\ni4\n\t[4 1 5 0]-&{<nil>}\n\n"},
		{"uus", "u1\n\t[0 1 1]-&{<nil>}\nu2\n\t[0 7 0]-&{<nil>}\nu3\n\t[1 3 5]-&{<nil>}\nu4\n\t[1 14 0]-&{<nil>}\n\n"},
		{"ffs", "f1\n\t[1.1 1 1.5]-&{<nil>}\nf2\n\t[2.2 2 8.75]-&{<nil>}\n\n"},
		{"ccs", "c1\n\t123\n\nc2\n\t34567\n\n\n"},
		{"def1", "i1\n\t[888 1 888 888]-&{<nil>}\ni2\n\t[888 888 -1 888]-&{<nil>}\ni3\n\t[888 888 888 0]-&{<nil>}\n\n"},
		{"def2", "id\n\t1\nname\n\tAbby\n\nage\n\t24\n\n"},
		{"def3", "i\n\t[-1 -1]-&{<nil>}\nv\n\t[abc abc]-&{<nil>}\nc\n\t[ ]-&{<nil>}\nprice\n\t[0 0]-&{<nil>}\n\n"},
		{"def4", "d1\n\t1\nd2\n\t2\nd3\n\tnull\nd4\n\t1\n\n"},
	}
	retError := [][]string{ // case should return error
		{"insert into cha1 values ('1');", "[22000]Data too long for column 'a' at row 1"},
		{"insert into cha2 values ('21');", "[22000]Data too long for column 'a' at row 1"},
		{"insert into iis (i1) values (128);", "[22000]Out of range value for column 'i1' at row 1"},
		{"insert into iis (i1) values (-129);", "[22000]Out of range value for column 'i1' at row 1"},
		{"insert into iis (i1) values (1), (128);", "[22000]Out of range value for column 'i1' at row 2"},
		{"insert into iis (i2) values (32768);", "[22000]Out of range value for column 'i2' at row 1"},
		{"insert into iis (i2) values (-32769);", "[22000]Out of range value for column 'i2' at row 1"},
		{"insert into iis (i3) values (2147483648);", "[22000]Out of range value for column 'i3' at row 1"},
		{"insert into iis (i3) values (-2147483649);", "[22000]Out of range value for column 'i3' at row 1"},
		{"insert into uus (u1) values (-1);", "[22000]constant value out of range"},
		{"insert into uus (u1) values (256);", "[22000]Out of range value for column 'u1' at row 1"},
		{"insert into uus (u2) values (65536);", "[22000]Out of range value for column 'u2' at row 1"},
		{"insert into uus (u3) values (4294967296);", "[22000]Out of range value for column 'u3' at row 1"},
	}

	test(t, e, proc, noError, selects, retError)
}