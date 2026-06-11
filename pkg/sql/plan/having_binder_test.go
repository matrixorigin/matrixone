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

package plan

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestHavingBinderGroupConcatOrderByHiddenArgs(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()
	havingBinder := NewHavingBinder(builder, bindCtx)

	astExpr := parseGroupConcatExprForTest(t, "select group_concat(a, ':', b order by c desc, 1 asc separator '|') from bind_select")
	boundExpr, err := havingBinder.BindAggFunc(NameGroupConcat, astExpr, 0, false)
	require.NoError(t, err)
	require.NotNil(t, boundExpr.GetCol())
	require.Len(t, bindCtx.aggregates, 1)

	fn := bindCtx.aggregates[0].GetF()
	require.NotNil(t, fn)
	require.Equal(t, NameGroupConcat, fn.Func.ObjName)
	require.Len(t, fn.Args, 6)

	require.Equal(t, int32(0), fn.Args[0].GetCol().ColPos)
	require.Equal(t, ":", fn.Args[1].GetLit().GetSval())
	require.Equal(t, int32(1), fn.Args[2].GetCol().ColPos)
	require.Equal(t, int32(2), fn.Args[3].GetCol().ColPos)
	require.Equal(t, int32(0), fn.Args[4].GetCol().ColPos)

	config := []byte(fn.Args[5].GetLit().GetSval())
	require.True(t, bytes.HasPrefix(config, []byte("\x00GCORDER1")))
	pos := len("\x00GCORDER1")
	require.Equal(t, uint32(3), binary.BigEndian.Uint32(config[pos:pos+4]))
	pos += 4
	require.Equal(t, uint32(2), binary.BigEndian.Uint32(config[pos:pos+4]))
	pos += 4
	require.Equal(t, []byte{1, 0}, config[pos:pos+2])
	pos += 2
	sepLen := int(binary.BigEndian.Uint32(config[pos : pos+4]))
	pos += 4
	require.Equal(t, "|", string(config[pos:pos+sepLen]))
}

func TestHavingBinderGroupConcatOrderByPositionErrors(t *testing.T) {
	testCases := []struct {
		name string
		sql  string
	}{
		{
			name: "position outside concat args",
			sql:  "select group_concat(a, b order by 3) from bind_select",
		},
		{
			name: "zero position",
			sql:  "select group_concat(a, b order by 0) from bind_select",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			builder, bindCtx := genBuilderAndCtx()
			havingBinder := NewHavingBinder(builder, bindCtx)
			astExpr := parseGroupConcatExprForTest(t, tc.sql)

			_, err := havingBinder.BindAggFunc(NameGroupConcat, astExpr, 0, false)
			require.Error(t, err)
			require.Empty(t, bindCtx.aggregates)
		})
	}
}

func parseGroupConcatExprForTest(t *testing.T, sql string) *tree.FuncExpr {
	t.Helper()
	stmts, err := parsers.Parse(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	selectStmt := stmts[0].(*tree.Select)
	selectClause := selectStmt.Select.(*tree.SelectClause)
	funcExpr, ok := selectClause.Exprs[0].Expr.(*tree.FuncExpr)
	require.True(t, ok)
	return funcExpr
}
