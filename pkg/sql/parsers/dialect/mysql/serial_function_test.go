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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// headFuncCall parses sql as a single-item SELECT and returns the head of its
// projection, requiring a function call.
func headFuncCall(t *testing.T, sql string) *tree.FuncExpr {
	t.Helper()
	stmt, err := ParseOne(context.Background(), sql, 1)
	require.NoError(t, err, sql)
	sel, ok := stmt.(*tree.Select)
	require.True(t, ok, sql)
	sc, ok := sel.Select.(*tree.SelectClause)
	require.True(t, ok, sql)
	require.Len(t, sc.Exprs, 1, sql)
	f, ok := sc.Exprs[0].Expr.(*tree.FuncExpr)
	require.True(t, ok, sql)
	return f
}

func selectHeadExpr(t *testing.T, sql string) tree.Expr {
	t.Helper()
	stmt, err := ParseOne(context.Background(), sql, 1)
	require.NoError(t, err, sql)
	sel, ok := stmt.(*tree.Select)
	require.True(t, ok, sql)
	sc, ok := sel.Select.(*tree.SelectClause)
	require.True(t, ok, sql)
	require.Len(t, sc.Exprs, 1, sql)
	return sc.Exprs[0].Expr
}

// TestSerialGenericFunctionCalls pins the pre-existing generic-function
// spellings of unquoted serial(...). #31748 turned `serial` into a keyword for
// the SERIAL column alias while `function_call_generic` only admitted bare ID
// names, which broke every serial(...)/serial_extract(serial(...)) consumer at
// the published CI run (31 events across plan/explain/embed/issues/
// sqlintegration at bdd0818344). The keyword now also carries this twin arm of
// the ID generic call, so all write/spellings, arities and nestings below must
// keep the byte-identical FuncExpr shape they had before the keyword existed.
func TestSerialGenericFunctionCalls(t *testing.T) {
	cases := []struct {
		sql  string
		args int
	}{
		{"select serial(a, b)", 2},
		{"select serial()", 0},
		{"select serial(cast(100000 as decimal(38,0)))", 1},
	}
	for _, c := range cases {
		f := headFuncCall(t, c.sql)
		require.Equal(t, "serial", f.FuncName.Origin(), c.sql)
		require.Len(t, f.Exprs, c.args, c.sql)
	}
}

// TestSerialNestedInsideAggregateAndSerialExtract covers the two real nesting
// shapes from the failing consumer suites: aggregates over serial (composite
// unique index prefix keys) and SERIAL_EXTRACT over serial (collation and
// iceberg catalog restore).
func TestSerialNestedInsideAggregateAndSerialExtract(t *testing.T) {
	f := headFuncCall(t, "select min(serial(a, b))")
	require.Equal(t, "min", f.FuncName.Origin())
	require.Len(t, f.Exprs, 1)
	inner, ok := f.Exprs[0].(*tree.FuncExpr)
	require.True(t, ok)
	require.Equal(t, "serial", inner.FuncName.Origin())
	require.Len(t, inner.Exprs, 2)

	e := selectHeadExpr(t, "select serial_extract(serial(x'4142'),0 as binary(255))")
	require.IsType(t, &tree.SerialExtractExpr{}, e)

	// hex(serial_extract(serial(...))): the exact wrapper shape from the
	// unicode collation consumer; the outer call is hex and its single
	// argument stays the serial_extract construct.
	f = headFuncCall(t, "select hex(serial_extract(serial(x'4142'),0 as binary(255)))")
	require.Equal(t, "hex", f.FuncName.Origin())
	require.Len(t, f.Exprs, 1)
	require.IsType(t, &tree.SerialExtractExpr{}, f.Exprs[0])
}

// TestSerialFunctionBoundaryParity keeps the boundaries of the fix honest:
// untouched spellings behave exactly as before the keyword existed and no new
// call syntax is introduced by this repair.
func TestSerialFunctionBoundaryParity(t *testing.T) {
	// serial_full never was the SERIAL lexical item; plain generic call.
	f := headFuncCall(t, "select serial_full(a, b)")
	require.Equal(t, "serial_full", f.FuncName.Origin())
	require.Len(t, f.Exprs, 2)

	// Quoted `serial`(a,b) did not parse before the keyword either
	// (function_call_generic only admits bare names); this must stay so the
	// repair does not silently broaden quoted-call syntax.
	_, err := ParseOne(context.Background(), "select `serial`(a, b)", 1)
	require.Error(t, err)

	// `serial` remains usable as table and column identifier.
	stmt, err := ParseOne(context.Background(), "create table serial (a int)", 1)
	require.NoError(t, err)
	require.IsType(t, &tree.CreateTable{}, stmt)

	col := selectHeadExpr(t, "select serial from serial")
	require.IsType(t, &tree.UnresolvedName{}, col)
}
