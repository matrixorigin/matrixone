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
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"strings"
	"testing"
	"unicode/utf8"

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
		{"[1,2,3]", "vecf32"},
		{"[4,5,6]", "vecf64"},
	}
	for _, v := range kase {
		s := convertValue(makeValue(v.val), v.typ)
		switch v.typ {
		case "int", "tinyint", "smallint", "bigint", "unsigned bigint", "unsigned int", "unsigned tinyint", "unsigned smallint", "float", "double", "vecf32", "vecf64":
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

func TestConvertValue2(t *testing.T) {
	v := makeValue("\\10\\11\\12")
	v2, f := convertValue2(v, "string")
	assert.Equal(t, *(v.(*sql.RawBytes)), v2)
	assert.Equal(t, f, defaultFmt)
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

func Test_toCsvFields(t *testing.T) {
	bys1 := []byte{0x5C, 0x31, 0x30, 0x5C, 0x33, 0x36, 0x5C, 0x38, 0x36, 0x5c}
	args1 := []any{makeValue(string(bys1))}
	cols1 := []*Column{
		{
			Name: "col1",
			Type: "varchar",
		},
	}
	line := make([]string, 1)
	toCsvFields(args1, cols1, line)
	want := "\\10\\36\\86\\"
	assert.Equal(t, want, line[0])
}

func Test_toCsvLine(t *testing.T) {
	bys1 := []byte{0x5C, 0x31, 0x30, 0x5C, 0x33, 0x36, 0x5C, 0x38, 0x36, 0x5c}
	args1 := []any{makeValue(string(bys1))}
	cols1 := []*Column{
		{
			Name: "col1",
			Type: "varchar",
		},
	}
	line := make([]string, 1)
	bb := bytes.Buffer{}
	cw1 := csv.NewWriter(&bb)
	cw1.Comma = '\t'
	err := toCsvLine(cw1, args1, cols1, line)
	assert.NoError(t, err)
	want := "\\10\\36\\86\\"
	assert.Equal(t, want, line[0])
	assert.Equal(t, bb.Bytes()[:len(bys1)], bys1)
}

func Test_checkFieldDelimiter(t *testing.T) {
	type args struct {
		ctx context.Context
		s   string
	}
	tests := []struct {
		name    string
		args    args
		want    rune
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			args: args{
				ctx: nil,
				s:   "",
			},
			want: defaultFieldDelimiter,
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), "csv field delimiter is invalid utf8 character"))
				return false
			},
		},
		{
			name: "t2",
			args: args{
				ctx: nil,
				s:   "fdaf",
			},
			want: defaultFieldDelimiter,
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), "there are multiple utf8 characters for csv field delimiter."))
				return false
			},
		},
		{
			name: "t3",
			args: args{
				ctx: nil,
				s:   string([]rune{utf8.RuneError}),
			},
			want: defaultFieldDelimiter,
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), "csv field delimiter is invalid utf8 character"))
				return false
			},
		},
		{
			name: "t4",
			args: args{
				ctx: nil,
				s:   " ",
			},
			want: defaultFieldDelimiter,
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.NoError(t, err)
				return false
			},
		},
		{
			name: "t5",
			args: args{
				ctx: nil,
				s:   "中文",
			},
			want: defaultFieldDelimiter,
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), "there are multiple utf8 characters for csv field delimiter."))
				return false
			},
		},
		{
			name: "t6",
			args: args{
				ctx: nil,
				s:   "中",
			},
			want: defaultFieldDelimiter,
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.NoError(t, err)
				return false
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := checkFieldDelimiter(tt.args.ctx, tt.args.s)
			if !tt.wantErr(t, err, fmt.Sprintf("checkFieldDelimiter(%v, %v)", tt.args.ctx, tt.args.s)) {
				return
			}
			assert.Equalf(t, tt.want, got, "checkFieldDelimiter(%v, %v)", tt.args.ctx, tt.args.s)
		})
	}
}
