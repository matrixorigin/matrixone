// Copyright 2026 Matrix Origin
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

package mysql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestMultiInsertSyntaxRoundTrip(t *testing.T) {
	tests := []struct {
		input  string
		output string
	}{
		{
			input:  "insert all into t1 into t2 select * from src",
			output: "insert all into t1 into t2 select * from src",
		},
		{
			input:  "INSERT ALL INTO t1 (a, b) VALUES (x, y) INTO db2.t2 VALUES (x + 1, upper(y)) SELECT x, y FROM src WHERE x > 0",
			output: "insert all into t1 (a, b) values (x, y) into db2.t2 values (x + 1, upper(y)) select x, y from src where x > 0",
		},
		{
			input:  "insert all when region = \"EU\" then into customers_eu (id, name, region) values (id, name, region) when region = \"US\" then into customers_us (id, name, region) values (id, name, region) else into customers_other (id, name, region) values (id, name, region) select id, name, region from customers",
			output: "insert all when region = \"EU\" then into customers_eu (id, name, region) values (id, name, region) when region = \"US\" then into customers_us (id, name, region) values (id, name, region) else into customers_other (id, name, region) values (id, name, region) select id, name, region from customers",
		},
		{
			input:  "insert first when a < 10 then into small into small2 (a) values (a) when a < 100 then into medium else into large select a from src",
			output: "insert first when a < 10 then into small into small2 (a) values (a) when a < 100 then into medium else into large select a from src",
		},
		{
			input:  "with s as (select 1 as a) insert all into t1 (a) values (a) select a from s",
			output: "with s as (select 1 as a) insert all into t1 (a) values (a) select a from s",
		},
		{
			// a parenthesized source is allowed after a column list
			input:  "insert all into t1 (a) (select 1)",
			output: "insert all into t1 (a) (select 1)",
		},
		{
			input:  "insert all into t1 select 1 union all select 2",
			output: "insert all into t1 select 1 union all select 2",
		},
	}

	for _, test := range tests {
		stmt, err := ParseOne(context.Background(), test.input, 1)
		require.NoError(t, err, test.input)
		mi, ok := stmt.(*tree.MultiInsert)
		require.True(t, ok, test.input)
		require.NotNil(t, mi.Source)
		formatted := tree.StringWithOpts(stmt, dialect.MYSQL, tree.WithQuoteString(true))
		require.Equal(t, test.output, formatted)
		stmt.Free()

		roundTrip, err := ParseOne(context.Background(), formatted, 1)
		require.NoError(t, err, formatted)
		require.Equal(t, formatted, tree.StringWithOpts(roundTrip, dialect.MYSQL, tree.WithQuoteString(true)))
		roundTrip.Free()
	}
}

func TestMultiInsertSyntaxShape(t *testing.T) {
	stmt, err := ParseOne(context.Background(),
		"insert first when a < 10 then into small into tiny (a) values (a) when a < 100 then into medium else into large select a from src", 1)
	require.NoError(t, err)
	mi := stmt.(*tree.MultiInsert)
	require.True(t, mi.First)
	require.Empty(t, mi.Targets)
	require.Len(t, mi.Whens, 2)
	require.Len(t, mi.Whens[0].Targets, 2)
	require.Equal(t, "small", string(mi.Whens[0].Targets[0].Table.ObjectName))
	require.Nil(t, mi.Whens[0].Targets[0].Values)
	require.Equal(t, "tiny", string(mi.Whens[0].Targets[1].Table.ObjectName))
	require.Len(t, mi.Whens[0].Targets[1].Columns, 1)
	require.Len(t, mi.Whens[0].Targets[1].Values, 1)
	require.Len(t, mi.Whens[1].Targets, 1)
	require.Len(t, mi.Else, 1)
	require.Equal(t, "large", string(mi.Else[0].Table.ObjectName))
	require.Len(t, mi.AllTargets(), 4)
	stmt.Free()

	stmt, err = ParseOne(context.Background(), "insert all into t1 into t2 select * from src", 1)
	require.NoError(t, err)
	mi = stmt.(*tree.MultiInsert)
	require.False(t, mi.First)
	require.Len(t, mi.Targets, 2)
	require.Empty(t, mi.Whens)
	stmt.Free()
}

func TestMultiInsertSyntaxErrors(t *testing.T) {
	invalid := []string{
		// FIRST requires WHEN branches
		"insert first into t1 select 1",
		// no source query
		"insert all into t1",
		// ELSE without WHEN
		"insert all else into t1 select 1",
		// WHEN without THEN
		"insert all when a > 1 into t1 select a from s",
		// plain INSERT still requires a single target
		"insert into t1 into t2 select 1",
		// a parenthesized source directly after a bare INTO is ambiguous
		"insert all into t1 (select 1)",
	}
	for _, sql := range invalid {
		_, err := ParseOne(context.Background(), sql, 1)
		require.Error(t, err, sql)
	}
}
