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
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memEngine"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
)

func newTestEngine() (engine.Engine, *process.Process) {
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	proc := process.New(mheap.New(gm))
	e := memEngine.NewTestEngine()
	return e, proc
}

// TestInsertFunction to make sure insert work can run normally
func TestInsertFunction(t *testing.T) {
	e, proc := newTestEngine()

	prepares := []string{
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
	}
	works := [][]string{ // First string is relation name, Second string is expected result.
		{"iis", "i1\n\t[1 2 1 0]-&{<nil>}\ni2\n\t[2 0 3 0]-&{<nil>}\ni3\n\t[3 9 0 0]-&{<nil>}\ni4\n\t[4 1 5 0]-&{<nil>}\n\n"},
		{"uus", "u1\n\t[0 1 1]-&{<nil>}\nu2\n\t[0 7 0]-&{<nil>}\nu3\n\t[1 3 5]-&{<nil>}\nu4\n\t[1 14 0]-&{<nil>}\n\n"},
		{"ffs", "f1\n\t[1.1 1 1.5]-&{<nil>}\nf2\n\t[2.2 2 8.75]-&{<nil>}\n\n"},
		{"ccs", "c1\n\t123\n\nc2\n\t34567\n\n\n"},
		{"def1", "i1\n\t[888 1 888 888]-&{<nil>}\ni2\n\t[888 888 -1 888]-&{<nil>}\ni3\n\t[888 888 888 0]-&{<nil>}\n\n"},
		{"def2", "id\n\t1\nname\n\tAbby\n\nage\n\t24\n\n"},
		{"def3", "i\n\t[-1 -1]-&{<nil>}\nv\n\t[abc abc]-&{<nil>}\nc\n\t[ ]-&{<nil>}\nprice\n\t[0 0]-&{<nil>}\n\n"},
		{"def4", "d1\n\t1\nd2\n\t2\nd3\n\t[0]-&{{0}}\nd4\n\t1\n\n"},
	}

	for _, p := range prepares {
		require.NoError(t, sqlRun(p, e, proc), p)
	}
	for _, s := range works {
		require.Equal(t, s[1], TempSelect(e, "test", s[0]))
	}
}

// TestDataType will do test for new Types
// For Example: date, datetime
func TestDataType(t *testing.T) {
	e, proc := newTestEngine()

	sqls := []string {
		"create table tdate (a date);",
		"create table tdefdate (a date default '20211202')",
		"insert into tdate values ('20070210'), ('1997-02-10'), ('01-04-28'), (20041112);",
		"insert into tdefdate values ();",
	}
	res := [][]string {
		{"tdate", "a\n\t[2007-02-10 1997-02-10 2001-04-28 2004-11-12]-&{<nil>}\n\n"},
		{"tdefdate", "a\n\t2021-12-02\n\n"},
	}

	for _, sql := range sqls {
		require.NoError(t, sqlRun(sql, e, proc), sql)
	}
	for _, s := range res {
		require.Equal(t, s[1], TempSelect(e, "test", s[0]))
	}
}

// TestDDLFunction to make sure DDL sql can run normally
func TestDDLFunction(t *testing.T) {
	e, proc := newTestEngine()

	ddls := []string{
		// support
		"create database d1;",
		"create table ddlt1 (a int, b int);",
		"CREATE TABLE ddlt2 (orderId varchar(100), uid INT, price FLOAT);",
		"show databases;",
		"show tables;",
		"show columns from ddlt2;",
		"show databases like 'd_';",
		"show tables like '%1';",
		"show columns from ddlt2 like 'pri%';",
		"drop table ddlt1, ddlt2;",
		// support but has problems now
		//"create table tbl(a int, b varchar(10);",
		//"create index index_name on tbl(a);",
		//"create index index_nameb using btree on tbl(a);",
		//"create index index_nameh using hash on tbl(a);",
		//"create index index_namer using rtree on tbl(a);",
		//"drop index index_name on tbl;",
	}

	for _, ddl := range ddls {
		require.NoError(t, sqlRun(ddl, e, proc))
	}
}

// TestOperators to make sure operators can be used normally on select
func TestOperators(t *testing.T) {
	e, proc := newTestEngine()

	supports := []string{
		"create table iis (i1 tinyint, i2 smallint, i3 int, i4 bigint);",
		"create table ffs (f1 float, f2 double);",
		"create table uus (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);",
		"insert into iis values (1, 11, 111, 1111);",
		"insert into ffs values (22.2, 222.222);",
		"insert into uus values (3, 33, 333, 3333);",
		"select i1 + i1, i1 - i1, i1 / i1, i1 * i1, i2 + i2, i2 - i2, i2 / i2, i2 * i2 from iis;",
		"select i3 + i3, i3 - i3, i3 / i3, i3 * i3, i4 + i4, i4 - i4, i4 / i4, i4 * i4 from iis;",
		"select -i1, -i2, -i3, -i4 from iis;",
		"select * from iis where i1 = i1 and i2 = i2 and i3 = i3 and i4 = i4;",
		"select CAST(i1 AS FLOAT(1)) ci1f1, CAST(i1 AS DOUBLE) ci1f2, CAST(i1 AS CHAR(2)) ci1c2 from iis;",
		"select f1 + f1, f1 - f1, f1 * f1, f1 / f1 from ffs;",
		"select f2 + f2, f2 - f2, f2 * f2, f2 / f2 from ffs;",
		"select -f1, -f2 from ffs;",
		"select * from ffs where f1 = f1 or f2 = f2;",
		"select * from ffs where f1 > f1 and f2 <= f2;",
		"select u1 + u1, u1 - u1, u1 * u1, u1 % u1, u1 / u1 from uus;",
		"select u2 + u2, u2 - u2, u2 * u2, u2 % u2, u2 / u2 from uus;",
		"select u3 + u3, u3 - u3, u3 * u3, u3 % u3, u3 / u3 from uus;",
		"select u4 - u4, u4 + u4, u4 / u4, u4 * u4, u4 % u4 from uus;",
		"create table ccs (c1 char(10), c2 varchar(20));",
		"select cast(c2 AS char) cc2c1 from ccs;",
	}
	supports = nil // todo: select not support complete now

	for _, sql := range supports {
		require.NoError(t, sqlRun(sql, e, proc))
	}
}

// sqlRun compile and run a sql, return error if happens
func sqlRun(sql string, e engine.Engine, proc *process.Process) error {
	c := compile.New("test", sql, "", e, proc)
	es, err := c.Build()
	if err != nil {
		return err
	}
	for _, e := range es {
		err := e.Compile(nil, func(i interface{}, batch *batch.Batch) error {
			return nil
		})
		if err != nil {
			return err
		}
		err = e.Run(0)
		if err != nil {
			return err
		}
	}
	return nil
}

// a simple select function Todo: need delete when support select * from relation.
func TempSelect(e engine.Engine, schema, name string) string {
	var buff bytes.Buffer

	db, err := e.Database(schema)
	if err != nil {
		return err.Error()
	}
	r, err := db.Relation(name)
	if err != nil {
		return err.Error()
	}
	defs := r.TableDefs()
	attrs := make([]string, 0, len(defs))
	{
		for _, def := range defs {
			if v, ok := def.(*engine.AttributeDef); ok {
				attrs = append(attrs, v.Attr.Name)
			}
		}
	}
	cs := make([]uint64, len(attrs))
	for i := range cs {
		cs[i] = 1
	}
	rd := r.NewReader(1)[0]
	{
		bat, err := rd.Read(cs, attrs)
		if err != nil {
			return err.Error()
		}
		buff.WriteString(fmt.Sprintf("%s\n", bat))
	}
	return buff.String()
}
