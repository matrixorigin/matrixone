// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConvertValue(t *testing.T) {
	kase := []struct {
		val string
		typ string
	}{
		{"1", "int"},
		{"1", "tinyint"},
		{"1", "smallint"},
		{"1", "bigint"},
		{"1", "unsigned bigint"},
		{"1", "unsigned int"},
		{"1", "unsigned tinyint"},
		{"1", "unsigned smallint"},
		{"1.1", "float"},
		{"1.1", "double"},
		{"1.1", "decimal"},
		{"asa", "varchar"},
		{"asa", "char"},
		{"asa", "text"},
		{"asa", "blob"},
		{"asa", "uuid"},
		{"asa", "json"},
		{"2021-01-01", "date"},
		{"2021-01-01 00:00:00", "datetime"},
		{"2021-01-01 00:00:00", "timestamp"},
	}
	for _, v := range kase {
		s := convertValue(makeValue(v.val), v.typ)
		switch v.typ {
		case "int", "tinyint", "smallint", "bigint", "unsigned bigint", "unsigned int", "unsigned tinyint", "unsigned smallint", "float", "double":
			require.Equal(t, v.val, s)
		default:

			require.Equal(t, fmt.Sprintf("'%v'", v.val), s)
		}
	}
}

func makeValue(val string) interface{} {
	tmp := sql.RawBytes(val)
	return &tmp
}

func TestShowCreateTable(t *testing.T) {
	kases := []struct {
		sql          string
		withNextLine bool
		res          string
	}{
		{
			sql:          "create table t1 (a int, b int)",
			withNextLine: false,
			res:          "create table t1 (a int, b int);\n",
		},
		{
			sql:          "create table t1 (a int, b int)",
			withNextLine: true,
			res:          "create table t1 (a int, b int);\n\n\n",
		},
		{
			sql:          "create table t1 (a int, b int);",
			withNextLine: false,
			res:          "create table t1 (a int, b int);\n",
		},
		{
			sql:          "create table t1 (a int, b int);",
			withNextLine: true,
			res:          "create table t1 (a int, b int);\n\n\n",
		},
	}
	old := os.Stdout
	for _, v := range kases {
		r, w, _ := os.Pipe()
		os.Stdout = w
		showCreateTable(v.sql, v.withNextLine)

		e := w.Close()
		require.Nil(t, e)
		var buf bytes.Buffer
		_, e = buf.ReadFrom(r)
		require.Nil(t, e)
		require.Equal(t, v.res, buf.String())
	}
	os.Stdout = old
}

func TestViewOrder(t *testing.T) {
	start := 1
	createTable := []string{
		"create table t1(a int);",
		"create view t4 as select * from t2, t3;",
		"create view t2 as select * from t1;",
		"create view t3 as select * from t2;",
		"create view t6 as select * from t4, t5;",
		"create view t5 as select * from t4;",
	}
	tables := []Table{
		{Name: "t1"},
		{Name: "t4"},
		{Name: "t2"},
		{Name: "t3"},
		{Name: "t6"},
		{Name: "t5"},
	}
	createTarget := []string{
		"create table t1(a int);",
		"create view t2 as select * from t1;",
		"create view t3 as select * from t2;",
		"create view t4 as select * from t2, t3;",
		"create view t5 as select * from t4;",
		"create view t6 as select * from t4, t5;",
	}
	tableTarget := []Table{
		{Name: "t1"},
		{Name: "t2"},
		{Name: "t3"},
		{Name: "t4"},
		{Name: "t5"},
		{Name: "t6"},
	}
	adjustViewOrder(createTable, tables, start)
	for i := 0; i < len(createTable); i++ {
		require.Equal(t, createTable[i], createTarget[i])
		require.Equal(t, tables[i], tableTarget[i])
	}
}
