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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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

// assertConstantUnsignedSubtractionPlan verifies the selected result cast is
// retained after the nested arithmetic itself has been folded to a literal.
func assertConstantUnsignedSubtractionPlan(t *testing.T, expr *Expr, resultType types.T) {
	t.Helper()
	require.Equal(t, int32(resultType), expr.Typ.Id)
	cast := expr.GetF()
	require.NotNil(t, cast)
	require.Equal(t, "cast", cast.Func.ObjName)
	require.Len(t, cast.Args, 2)
	require.Equal(t, int32(types.T_decimal128), cast.Args[0].Typ.Id)
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

// TestUnsignedIntegerSubtractionPreservesNestedIntegerDomain checks both the
// outer subtraction domain and each nested unsigned arithmetic boundary in a
// single mode/operator matrix.
func TestUnsignedIntegerSubtractionPreservesNestedIntegerDomain(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		op   string
	}{
		{name: "addition", sql: "select (cast(n_nationkey as unsigned) + 0) - 1 from nation", op: "+"},
		{name: "multiplication", sql: "select (cast(n_nationkey as unsigned) * 1) - 1 from nation", op: "*"},
		{name: "integer division", sql: "select (cast(n_nationkey as unsigned) div 1) - 1 from nation", op: "div"},
		{name: "modulo", sql: "select (cast(n_nationkey as unsigned) % 1) - 1 from nation", op: "%"},
	} {
		for _, mode := range []struct {
			name string
			mode string
			want types.T
		}{
			{name: "default", want: types.T_uint64},
			{name: "no unsigned subtraction", mode: mysql.SQLModeNoUnsignedSubtraction, want: types.T_int64},
		} {
			for _, bindMode := range bindModes {
				t.Run(tc.name+"/"+mode.name+"/"+bindMode.name, func(t *testing.T) {
					expr := unsignedSubtractionProjection(t, mode.mode, tc.sql, bindMode.prepare)
					assertUnsignedSubtractionPlan(t, expr, mode.want)
					require.True(t, hasArithmeticResultCast(expr, tc.op, types.T_uint64))
				})
			}
		}
	}
}

func hasArithmeticResultCast(expr *Expr, operator string, resultType types.T) bool {
	if expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func != nil && fn.Func.ObjName == "cast" && len(fn.Args) > 0 &&
			types.T(expr.Typ.Id) == resultType {
			if inner := fn.Args[0].GetF(); inner != nil && inner.Func != nil && inner.Func.ObjName == operator {
				return true
			}
		}
		for _, arg := range fn.Args {
			if hasArithmeticResultCast(arg, operator, resultType) {
				return true
			}
		}
	}
	return false
}

func TestUnsignedIntegerSubtractionRetainsNestedSubtractionResultDomain(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode string
		want types.T
	}{
		{name: "default", want: types.T_uint64},
		{name: "no unsigned subtraction", mode: mysql.SQLModeNoUnsignedSubtraction, want: types.T_int64},
	} {
		for _, bindMode := range bindModes {
			t.Run(tc.name+"/"+bindMode.name, func(t *testing.T) {
				expr := unsignedSubtractionProjection(
					t, tc.mode, "select (cast(n_nationkey as unsigned) - 0) - 1 from nation", bindMode.prepare,
				)
				require.Equal(t, int32(tc.want), expr.Typ.Id)
			})
		}
	}
}

func TestUnsignedIntegerSubtractionPreservesConstantFoldedNestedIntegerDomain(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{name: "addition", sql: "select (cast(0 as unsigned) + 0) - 1"},
		{name: "multiplication", sql: "select (cast(0 as unsigned) * 1) - 1"},
		{name: "integer division", sql: "select (cast(0 as unsigned) div 1) - 1"},
		{name: "modulo", sql: "select (cast(0 as unsigned) % 1) - 1"},
	} {
		for _, mode := range []struct {
			name string
			mode string
			want types.T
		}{
			{name: "default", want: types.T_uint64},
			{name: "no unsigned subtraction", mode: mysql.SQLModeNoUnsignedSubtraction, want: types.T_int64},
		} {
			for _, bindMode := range bindModes {
				t.Run(tc.name+"/"+mode.name+"/"+bindMode.name, func(t *testing.T) {
					expr := unsignedSubtractionProjection(t, mode.mode, tc.sql, bindMode.prepare)
					assertConstantUnsignedSubtractionPlan(t, expr, mode.want)
				})
			}
		}
	}
}

func TestUnsignedIntegerSubtractionTreatsYearAsUnsigned(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode string
		want types.T
	}{
		{name: "default", want: types.T_uint64},
		{name: "no unsigned subtraction", mode: mysql.SQLModeNoUnsignedSubtraction, want: types.T_int64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := unsignedSubtractionProjection(t, tc.mode, "select cast(n_nationkey as year) - 1 from nation", false)
			assertUnsignedSubtractionPlan(t, expr, tc.want)
		})
	}
}

func TestBuilderlessBindersHonorNoUnsignedSubtraction(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "select u - 1", 1)
	require.NoError(t, err)
	defer stmt.Free()
	astExpr := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr

	for _, tc := range []struct {
		name string
		mode bool
		want types.T
	}{
		{name: "default", want: types.T_uint64},
		{name: "no unsigned subtraction", mode: true, want: types.T_int64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			generated := NewGeneratedColBinder(context.Background(), []string{"u"}, []planpb.Type{{Id: int32(types.T_uint64)}})
			generated.setNoUnsignedSubtractionOverride(tc.mode)
			expr, err := generated.BindExpr(astExpr, 0, false)
			require.NoError(t, err)
			assertUnsignedSubtractionPlan(t, expr, tc.want)

			defaults := NewDefaultBinder(context.Background(), nil, nil, planpb.Type{}, nil)
			defaults.setNoUnsignedSubtractionOverride(tc.mode)
			stmt, err := mysql.ParseOne(context.Background(), "select cast(0 as unsigned) - 1", 1)
			require.NoError(t, err)
			defer stmt.Free()
			expr, err = defaults.BindExpr(stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr, 0, false)
			require.NoError(t, err)
			assertUnsignedSubtractionPlan(t, expr, tc.want)
		})
	}
}

func TestCreateTableExpressionsHonorNoUnsignedSubtraction(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode string
		want types.T
	}{
		{name: "default", want: types.T_uint64},
		{name: "no unsigned subtraction", mode: mysql.SQLModeNoUnsignedSubtraction, want: types.T_int64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewMockCompilerContext(true)
			ctx.SetSqlModeOverride(tc.mode)
			stmt, err := mysql.ParseOne(ctx.GetContext(), `create table t_ddl_mode (
				u bigint unsigned,
				g bigint generated always as (u - 1) stored,
				d bigint default (cast(0 as unsigned) - 1),
				check (u - 1 < 0)
			)`, 1)
			require.NoError(t, err)
			defer stmt.Free()

			built, err := BuildPlan(ctx, stmt, false)
			if tc.mode == "" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			table := built.GetDdl().GetCreateTable().GetTableDef()
			require.Equal(t, tc.want, subtractionResultType(t, table.Cols[1].GetGeneratedCol().GetExpr()))
			defaultLiteral := table.Cols[2].GetDefault().GetExpr().GetLit()
			require.NotNil(t, defaultLiteral)
			require.Equal(t, int64(-1), defaultLiteral.GetI64Val())
			require.Len(t, table.Checks, 1)
			require.Equal(t, tc.want, subtractionResultType(t, table.Checks[0].GetCheck()))
		})
	}
}

func subtractionResultType(t *testing.T, expr *Expr) types.T {
	t.Helper()
	if typ, ok := findSubtractionResultType(expr); ok {
		return typ
	}
	t.Fatal("expression contains no unsigned subtraction result cast")
	return types.T_any
}

func findSubtractionResultType(expr *Expr) (types.T, bool) {
	if fn := expr.GetF(); fn != nil {
		if fn.Func != nil && fn.Func.ObjName == "cast" && len(fn.Args) > 0 {
			if minus := fn.Args[0].GetF(); minus != nil && minus.Func != nil && minus.Func.ObjName == "-" {
				return types.T(expr.Typ.Id), true
			}
		}
		for _, arg := range fn.Args {
			if typ, ok := findSubtractionResultType(arg); ok {
				return typ, true
			}
		}
	}
	return types.T_any, false
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
		{name: "constant-folded nested addition underflow", sql: "select (cast(0 as unsigned) + 0) - 1", wantType: types.T_uint64, wantError: true},
		{name: "constant-folded nested multiplication underflow", sql: "select (cast(0 as unsigned) * 1) - 1", wantType: types.T_uint64, wantError: true},
		{name: "constant-folded nested integer division underflow", sql: "select (cast(0 as unsigned) div 1) - 1", wantType: types.T_uint64, wantError: true},
		{name: "constant-folded nested modulo underflow", sql: "select (cast(0 as unsigned) % 1) - 1", wantType: types.T_uint64, wantError: true},
		{name: "function folded nested addition underflow", sql: "select (cast(0 as unsigned) + abs(0)) - 1", wantType: types.T_uint64, wantError: true},
		{name: "strict underflow", mode: "STRICT_TRANS_TABLES", sql: "select cast(0 as unsigned) - 1", wantType: types.T_uint64, wantError: true},
		{name: "mode permits negative", mode: mysql.SQLModeNoUnsignedSubtraction, sql: "select cast(0 as unsigned) - 1", wantType: types.T_int64, wantInt: -1},
		{name: "mode permits constant-folded nested integer division negative", mode: mysql.SQLModeNoUnsignedSubtraction, sql: "select (cast(0 as unsigned) div 1) - 1", wantType: types.T_int64, wantInt: -1},
		{name: "mode permits function folded nested addition negative", mode: mysql.SQLModeNoUnsignedSubtraction, sql: "select (cast(0 as unsigned) + abs(0)) - 1", wantType: types.T_int64, wantInt: -1},
		{name: "positive result", sql: "select cast(2 as unsigned) - 1", wantType: types.T_uint64, wantUint: 1},
		{name: "negative signed operand", sql: "select cast(2 as unsigned) - (-1)", wantType: types.T_uint64, wantUint: 3},
		{name: "maximum unsigned result", sql: "select cast('18446744073709551615' as unsigned) - 0", wantType: types.T_uint64, wantUint: ^uint64(0)},
		{name: "default unsigned overflow", sql: "select cast('18446744073709551615' as unsigned) - (-1)", wantType: types.T_uint64, wantError: true},
		{name: "nested addition overflow cannot be cancelled", sql: "select (cast('18446744073709551615' as unsigned) + 1) - cast('18446744073709551615' as unsigned)", wantType: types.T_uint64, wantError: true},
		{name: "nested multiplication overflow cannot be cancelled", sql: "select (cast('18446744073709551615' as unsigned) * 2) - cast('18446744073709551615' as unsigned)", wantType: types.T_uint64, wantError: true},
		{name: "signed mode cancellation at unsigned maximum", mode: mysql.SQLModeNoUnsignedSubtraction, sql: "select cast('18446744073709551615' as unsigned) - cast('18446744073709551615' as unsigned)", wantType: types.T_int64, wantInt: 0},
		{name: "signed mode positive overflow", mode: mysql.SQLModeNoUnsignedSubtraction, sql: "select cast('18446744073709551615' as unsigned) - 0", wantType: types.T_int64, wantError: true},
		{name: "signed mode nested addition overflow cannot be cancelled", mode: mysql.SQLModeNoUnsignedSubtraction, sql: "select (cast('18446744073709551615' as unsigned) + 1) - cast('18446744073709551615' as unsigned)", wantType: types.T_int64, wantError: true},
		{name: "signed mode nested multiplication overflow cannot be cancelled", mode: mysql.SQLModeNoUnsignedSubtraction, sql: "select (cast('18446744073709551615' as unsigned) * 2) - cast('18446744073709551615' as unsigned)", wantType: types.T_int64, wantError: true},
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

func TestSQLPrepareUnsignedArithmeticDefersSignedMarkerDomain(t *testing.T) {
	for _, tc := range []struct {
		name      string
		sql       string
		op        string
		wantCheck bool
	}{
		{
			name: "bare signed marker remains deferred",
			sql:  "prepare unsigned_add from 'select cast(1 as unsigned) + ?'",
			op:   "+",
		},
		{
			name:      "explicit unsigned and signed casts retain addition boundary",
			sql:       "prepare unsigned_add from 'select cast(? as unsigned) + cast(1 as signed)'",
			op:        "+",
			wantCheck: true,
		},
		{
			name:      "explicit unsigned and signed casts retain multiplication boundary",
			sql:       "prepare unsigned_mul from 'select cast(? as unsigned) * cast(2 as signed)'",
			op:        "*",
			wantCheck: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			logicPlan, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			prepared := logicPlan.GetDcl().GetPrepare().Plan
			require.NotNil(t, prepared)
			// A bare marker has no fixed signedness at PREPARE time and remains
			// deferred. In contrast, CAST(? AS UNSIGNED) fixes the operand domain,
			// so its arithmetic node retains a per-node UINT64 boundary.
			require.Equal(t, tc.wantCheck,
				hasArithmeticResultCast(firstProjectionExpr(t, prepared), tc.op, types.T_uint64))
		})
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