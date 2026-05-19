// Copyright 2025 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func parseLimit(t *testing.T, sql string) *tree.Limit {
	t.Helper()
	stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	sel, ok := stmts[0].(*tree.Select)
	require.True(t, ok, "expected SELECT statement")
	return sel.Limit
}

func bindLimitExpr(t *testing.T, astExpr tree.Expr, isOffset bool) (*plan.Expr, error) {
	t.Helper()
	builder, bindCtx := genBuilderAndCtx()
	binder := NewLimitBinder(builder, bindCtx, isOffset)
	return binder.BindExpr(astExpr, 0, true)
}

func TestLimitBinder_Limit0(t *testing.T) {
	astLimit := parseLimit(t, "SELECT 1 LIMIT 0")
	require.NotNil(t, astLimit.Count)

	expr, err := bindLimitExpr(t, astLimit.Count, false)
	require.NoError(t, err)
	require.Equal(t, int32(types.T_uint64), expr.Typ.Id)

	lit, ok := expr.Expr.(*plan.Expr_Lit)
	require.True(t, ok)
	uval, ok := lit.Lit.Value.(*plan.Literal_U64Val)
	require.True(t, ok)
	require.Equal(t, uint64(0), uval.U64Val)
}

func TestLimitBinder_LimitPositive(t *testing.T) {
	astLimit := parseLimit(t, "SELECT 1 LIMIT 10")
	require.NotNil(t, astLimit.Count)

	expr, err := bindLimitExpr(t, astLimit.Count, false)
	require.NoError(t, err)
	require.Equal(t, int32(types.T_uint64), expr.Typ.Id)

	lit, ok := expr.Expr.(*plan.Expr_Lit)
	require.True(t, ok)
	uval, ok := lit.Lit.Value.(*plan.Literal_U64Val)
	require.True(t, ok)
	require.Equal(t, uint64(10), uval.U64Val)
}

func TestLimitBinder_Offset0(t *testing.T) {
	astLimit := parseLimit(t, "SELECT 1 LIMIT 5 OFFSET 0")
	require.NotNil(t, astLimit.Offset)

	expr, err := bindLimitExpr(t, astLimit.Offset, true)
	require.NoError(t, err)
	require.Equal(t, int32(types.T_uint64), expr.Typ.Id)

	lit, ok := expr.Expr.(*plan.Expr_Lit)
	require.True(t, ok)
	uval, ok := lit.Lit.Value.(*plan.Literal_U64Val)
	require.True(t, ok)
	require.Equal(t, uint64(0), uval.U64Val)
}

func TestLimitBinder_LimitNegative(t *testing.T) {
	astLimit := parseLimit(t, "SELECT 1 LIMIT -1")
	require.NotNil(t, astLimit.Count)

	_, err := bindLimitExpr(t, astLimit.Count, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "LIMIT must be a non-negative integer")
}

func TestLimitBinder_OffsetNegative(t *testing.T) {
	astLimit := parseLimit(t, "SELECT 1 LIMIT 1 OFFSET -5")
	require.NotNil(t, astLimit.Offset)

	_, err := bindLimitExpr(t, astLimit.Offset, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "OFFSET must be a non-negative integer")
}

func TestLimitBinder_LimitNull(t *testing.T) {
	astLimit := parseLimit(t, "SELECT 1 LIMIT NULL")
	require.NotNil(t, astLimit.Count)

	_, err := bindLimitExpr(t, astLimit.Count, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "LIMIT/OFFSET cannot be NULL")
}

func TestLimitBinder_OffsetNull(t *testing.T) {
	astLimit := parseLimit(t, "SELECT 1 LIMIT 1 OFFSET NULL")
	require.NotNil(t, astLimit.Offset)

	_, err := bindLimitExpr(t, astLimit.Offset, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "LIMIT/OFFSET cannot be NULL")
}

func TestLimitBinder_LimitString(t *testing.T) {
	astLimit := parseLimit(t, "SELECT 1 LIMIT '10'")
	require.NotNil(t, astLimit.Count)

	expr, err := bindLimitExpr(t, astLimit.Count, false)
	require.NoError(t, err)
	require.Equal(t, int32(types.T_uint64), expr.Typ.Id)
}

func TestLimitBinder_LargeUint64(t *testing.T) {
	sql := "SELECT 1 LIMIT 18446744073709551615"
	stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	sel, ok := stmts[0].(*tree.Select)
	require.True(t, ok)

	expr, err := bindLimitExpr(t, sel.Limit.Count, false)
	require.NoError(t, err)
	require.Equal(t, int32(types.T_uint64), expr.Typ.Id)
}

func TestLimitBinder_StarInLimit(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()
	binder := NewLimitBinder(builder, bindCtx, false)

	_, err := binder.BindExpr(&tree.UnqualifiedStar{}, 0, true)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "unsupported expr in limit clause"))
}
