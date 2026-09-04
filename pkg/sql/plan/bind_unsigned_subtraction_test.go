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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func unsignedSubtractionProjection(t *testing.T, mode, sql string, prepare bool) *Expr {
	t.Helper()
	ctx := NewMockCompilerContext(false)
	ctx.SetSqlModeOverride(mode)
	stmt, err := mysql.ParseOne(ctx.GetContext(), sql, 1)
	require.NoError(t, err)
	defer stmt.Free()

	built, err := BuildPlan(ctx, stmt, prepare)
	require.NoError(t, err)
	query := built.GetQuery()
	require.NotNil(t, query)
	for _, node := range query.Nodes {
		if node.NodeType == planpb.Node_PROJECT && len(node.ProjectList) > 0 {
			return node.ProjectList[0]
		}
	}
	t.Fatal("plan contains no projection")
	return nil
}

func assertUnsignedSubtractionPlan(t *testing.T, expr *Expr, resultType types.T) {
	t.Helper()
	require.Equal(t, int32(resultType), expr.Typ.Id)
	cast := expr.GetF()
	require.NotNil(t, cast)
	require.Equal(t, "cast", cast.Func.ObjName)
	require.Len(t, cast.Args, 2)

	minus := cast.Args[0].GetF()
	require.NotNil(t, minus)
	require.Equal(t, "-", minus.Func.ObjName)
	require.Len(t, minus.Args, 2)
	for _, arg := range minus.Args {
		require.Equal(t, int32(types.T_decimal128), arg.Typ.Id)
	}
}

func TestUnsignedIntegerSubtractionHonorsSQLMode(t *testing.T) {
	for _, bindMode := range bindModes {
		for _, tc := range []struct {
			name string
			mode string
			want types.T
		}{
			{name: "empty mode", mode: "", want: types.T_uint64},
			{name: "strict mode", mode: "STRICT_TRANS_TABLES", want: types.T_uint64},
			{name: "no unsigned subtraction", mode: mysql.SQLModeNoUnsignedSubtraction, want: types.T_int64},
			{name: "composed mode", mode: "STRICT_TRANS_TABLES, no_unsigned_subtraction", want: types.T_int64},
		} {
			t.Run(bindMode.name+"/"+tc.name, func(t *testing.T) {
				expr := unsignedSubtractionProjection(
					t, tc.mode, "select cast(n_nationkey as unsigned) - 1 from nation", bindMode.prepare,
				)
				assertUnsignedSubtractionPlan(t, expr, tc.want)
			})
		}
	}
}

func TestUnsignedIntegerSubtractionOperandCombinations(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{name: "unsigned left", sql: "select cast(n_nationkey as unsigned) - 1 from nation"},
		{name: "unsigned right", sql: "select n_nationkey - cast(1 as unsigned) from nation"},
		{name: "both unsigned", sql: "select cast(n_nationkey as unsigned) - cast(n_regionkey as unsigned) from nation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := unsignedSubtractionProjection(t, "", tc.sql, false)
			assertUnsignedSubtractionPlan(t, expr, types.T_uint64)
		})
	}
}

func TestUnsignedSubtractionDoesNotAffectOtherNumericDomains(t *testing.T) {
	for _, sql := range []string{
		"select cast(n_nationkey as signed) - 1 from nation",
		"select cast(n_nationkey as unsigned) - cast(1 as decimal(10, 0)) from nation",
		"select cast(n_nationkey as unsigned) - 1.0 from nation",
	} {
		expr := unsignedSubtractionProjection(t, mysql.SQLModeNoUnsignedSubtraction, sql, false)
		require.Equal(t, "-", expr.GetF().Func.ObjName, sql)
	}
}

func TestUnsignedSubtractionUnreadableSQLModeUsesDefaultDomain(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	ctx.ResolveVariableFunc = func(string, bool, bool) (interface{}, error) {
		return nil, moerr.NewInternalErrorNoCtx("variable store unavailable")
	}
	stmt, err := mysql.ParseOne(ctx.GetContext(), "select cast(n_nationkey as unsigned) - 1 from nation", 1)
	require.NoError(t, err)
	defer stmt.Free()
	built, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	assertUnsignedSubtractionPlan(t, built.GetQuery().Nodes[1].ProjectList[0], types.T_uint64)
}

func TestUnsignedIntegerSubtractionExecution(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mode      string
		sql       string
		wantType  types.T
		wantInt   int64
		wantUint  uint64
		wantError bool
		wantNull  bool
	}{
		{name: "default underflow", sql: "select cast(0 as unsigned) - 1", wantType: types.T_uint64, wantError: true},
		{name: "strict underflow", mode: "STRICT_TRANS_TABLES", sql: "select cast(0 as unsigned) - 1", wantType: types.T_uint64, wantError: true},
		{name: "mode permits negative", mode: mysql.SQLModeNoUnsignedSubtraction, sql: "select cast(0 as unsigned) - 1", wantType: types.T_int64, wantInt: -1},
		{name: "positive result", sql: "select cast(2 as unsigned) - 1", wantType: types.T_uint64, wantUint: 1},
		{name: "negative signed operand", sql: "select cast(2 as unsigned) - (-1)", wantType: types.T_uint64, wantUint: 3},
		{name: "maximum unsigned result", sql: "select cast('18446744073709551615' as unsigned) - 0", wantType: types.T_uint64, wantUint: ^uint64(0)},
		{name: "default unsigned overflow", sql: "select cast('18446744073709551615' as unsigned) - (-1)", wantType: types.T_uint64, wantError: true},
		{name: "signed mode cancellation at unsigned maximum", mode: mysql.SQLModeNoUnsignedSubtraction, sql: "select cast('18446744073709551615' as unsigned) - cast('18446744073709551615' as unsigned)", wantType: types.T_int64, wantInt: 0},
		{name: "signed mode positive overflow", mode: mysql.SQLModeNoUnsignedSubtraction, sql: "select cast('18446744073709551615' as unsigned) - 0", wantType: types.T_int64, wantError: true},
		{name: "unsigned right underflow", sql: "select 1 - cast(2 as unsigned)", wantType: types.T_uint64, wantError: true},
		{name: "both unsigned underflow in signed mode", mode: mysql.SQLModeNoUnsignedSubtraction, sql: "select cast(0 as unsigned) - cast(1 as unsigned)", wantType: types.T_int64, wantInt: -1},
		{name: "bit underflow", sql: "select cast(0 as bit(8)) - 1", wantType: types.T_uint64, wantError: true},
		{name: "bit signed mode", mode: mysql.SQLModeNoUnsignedSubtraction, sql: "select cast(0 as bit(8)) - 1", wantType: types.T_int64, wantInt: -1},
		{name: "null", sql: "select cast(null as unsigned) - 1", wantType: types.T_uint64, wantNull: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := unsignedSubtractionProjection(t, tc.mode, tc.sql, false)
			proc := testutil.NewProc(t)
			defer proc.Free()
			executor, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer executor.Free()

			result, err := executor.Eval(proc, nil, nil)
			if tc.wantError {
				require.Error(t, err)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantType, result.GetType().Oid)
			if tc.wantNull {
				require.True(t, result.GetNulls().Contains(0))
				return
			}
			if tc.wantType == types.T_int64 {
				require.Equal(t, tc.wantInt, vector.GetFixedAtNoTypeCheck[int64](result, 0))
			} else {
				require.Equal(t, tc.wantUint, vector.GetFixedAtNoTypeCheck[uint64](result, 0))
			}
		})
	}
}

func TestSQLPrepareUnsignedSubtractionHonorsSQLMode(t *testing.T) {
	for _, modeCase := range []struct {
		name string
		mode string
		want types.T
	}{
		{name: "default", want: types.T_uint64},
		{name: "no unsigned subtraction", mode: mysql.SQLModeNoUnsignedSubtraction, want: types.T_int64},
	} {
		for _, exprCase := range []struct {
			name string
			expr string
		}{
			{name: "unsigned parameter cast", expr: "cast(? as unsigned) - 1"},
			{name: "unsigned right peer", expr: "? - cast(1 as unsigned)"},
			{name: "unsigned left peer", expr: "cast(1 as unsigned) - ?"},
		} {
			t.Run(modeCase.name+"/"+exprCase.name, func(t *testing.T) {
				mock := NewMockOptimizer(false)
				mock.ctxt.SetSqlModeOverride(modeCase.mode)
				logicPlan, err := runOneStmt(mock, t,
					"prepare unsigned_sub from 'select "+exprCase.expr+"'")
				require.NoError(t, err)
				prepared := logicPlan.GetDcl().GetPrepare().Plan
				require.NotNil(t, prepared)

				projection := firstProjectionExpr(t, prepared)
				assertUnsignedSubtractionPlan(t, projection, modeCase.want)

				filled, err := FillValuesOfParamsInPlan(context.Background(), prepared, []any{int64(0)})
				require.NoError(t, err)
				assertUnsignedSubtractionPlan(t, firstProjectionExpr(t, filled), modeCase.want)
			})
		}
	}
}

func firstProjectionExpr(t *testing.T, built *Plan) *Expr {
	t.Helper()
	query := built.GetQuery()
	require.NotNil(t, query)
	for _, node := range query.Nodes {
		if node.NodeType == planpb.Node_PROJECT && len(node.ProjectList) > 0 {
			return node.ProjectList[0]
		}
	}
	t.Fatal("plan contains no projection")
	return nil
}
