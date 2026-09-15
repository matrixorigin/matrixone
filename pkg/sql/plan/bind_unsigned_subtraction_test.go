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
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
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
		require.Truef(t, types.T(arg.Typ.Id).IsInteger(),
			"mixed integer arithmetic must retain integer operands, got %s", types.T(arg.Typ.Id))
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
	require.Truef(t, types.T(cast.Args[0].Typ.Id).IsInteger(),
		"folded mixed integer arithmetic must retain integer result, got %s", types.T(cast.Args[0].Typ.Id))
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
				require.Equal(t, int32(64), expr.Typ.Width,
					"integer subtraction must publish BIGINT precision instead of the unconstrained default width")
				require.Equal(t, int32(-1), expr.Typ.Scale,
					"integer subtraction must publish an integer scale")
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

func TestUnsignedIntegerModuloFoldedDomainDoesNotPolluteParent(t *testing.T) {
	expr := unsignedSubtractionProjection(t, "", "select (3 % cast(2 as unsigned)) - 2", false)
	require.Equal(t, int32(types.T_decimal128), expr.Typ.Id)

	proc := testutil.NewProc(t)
	defer proc.Free()
	executor, err := colexec.NewExpressionExecutor(proc, expr)
	require.NoError(t, err)
	defer executor.Free()

	input := batch.New(nil)
	input.SetRowCount(1)
	result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, types.Decimal128FromInt64(-1), vector.GetFixedAtNoTypeCheck[types.Decimal128](result, 0))
}

func TestUnsignedIntegerModuloFunctionKeepsDividendDomain(t *testing.T) {
	for _, mode := range []struct {
		name string
		mode string
	}{
		{name: "default"},
		{name: "no unsigned subtraction", mode: mysql.SQLModeNoUnsignedSubtraction},
	} {
		for _, prepare := range bindModes {
			for _, operator := range []string{"%", "mod"} {
				t.Run(mode.name+"/"+prepare.name+"/"+operator, func(t *testing.T) {
					query := "select mod(cast(-3 as signed), cast(2 as unsigned)) - 0"
					if operator == "%" {
						query = "select (cast(-3 as signed) % cast(2 as unsigned)) - 0"
					}
					expr := unsignedSubtractionProjection(
						t,
						mode.mode,
						query,
						prepare.prepare,
					)
					require.Equal(t, int32(types.T_decimal128), expr.Typ.Id)

					proc := testutil.NewProc(t)
					defer proc.Free()
					executor, err := colexec.NewExpressionExecutor(proc, expr)
					require.NoError(t, err)
					defer executor.Free()
					input := batch.New(nil)
					input.SetRowCount(1)
					result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
					require.NoError(t, err)
					require.Equal(t, types.Decimal128FromInt64(-1), vector.GetFixedAtNoTypeCheck[types.Decimal128](result, 0))
				})
			}
		}
	}
}

func TestUnsignedIntegerModuloFunctionUnsignedDividend(t *testing.T) {
	for _, prepare := range bindModes {
		for _, operator := range []string{"%", "mod"} {
			t.Run(prepare.name+"/"+operator, func(t *testing.T) {
				query := "select cast(3 as unsigned) % -2"
				if operator == "mod" {
					query = "select mod(cast(3 as unsigned), -2)"
				}
				expr := unsignedSubtractionProjection(t, "", query, prepare.prepare)
				require.Equal(t, int32(types.T_uint64), expr.Typ.Id)

				proc := testutil.NewProc(t)
				defer proc.Free()
				executor, err := colexec.NewExpressionExecutor(proc, expr)
				require.NoError(t, err)
				defer executor.Free()
				input := batch.New(nil)
				input.SetRowCount(1)
				result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
				require.NoError(t, err)
				require.Equal(t, types.T_uint64, result.GetType().Oid)
				require.Equal(t, uint64(1), vector.GetFixedAtNoTypeCheck[uint64](result, 0))
			})
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

func TestDefaultBinderNoUnsignedSubtractionEvaluatesNegative(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "select cast(0 as unsigned) - 1", 1)
	require.NoError(t, err)
	defer stmt.Free()

	binder := NewDefaultBinder(context.Background(), nil, nil, planpb.Type{}, nil)
	binder.setNoUnsignedSubtractionOverride(true)
	expr, err := binder.BindExpr(stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr, 0, false)
	require.NoError(t, err)

	proc := testutil.NewProc(t)
	defer proc.Free()
	executor, err := colexec.NewExpressionExecutor(proc, expr)
	require.NoError(t, err)
	defer executor.Free()

	input := batch.New(nil)
	input.SetRowCount(1)
	result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, types.T_int64, result.GetType().Oid)
	require.Equal(t, int64(-1), vector.GetFixedAtNoTypeCheck[int64](result, 0))
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

func TestUnsignedIntegerArithmeticExecution(t *testing.T) {
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
		{name: "explicit unsigned cast hides bit source", sql: "select cast(cast(0 as bit(8)) as unsigned) - 1", wantType: types.T_uint64, wantError: true},
		{name: "explicit unsigned cast hides bit source in signed mode", mode: mysql.SQLModeNoUnsignedSubtraction, sql: "select cast(cast(0 as bit(8)) as unsigned) - 1", wantType: types.T_int64, wantInt: -1},
		{name: "function result does not inherit bit source", sql: "select cast(0 as unsigned) - bit_count(cast(1 as bit(8)))", wantType: types.T_uint64, wantError: true},
		{name: "bitwise result establishes unsigned domain", sql: "select (cast(0 as bit(8)) | 0) - 1", wantType: types.T_uint64, wantError: true},
		{name: "unary bitwise result establishes unsigned domain", sql: "select ~cast(0 as bit(8)) + 1", wantType: types.T_uint64, wantError: true},
		{name: "integer division result establishes unsigned domain", sql: "select (cast(0 as bit(8)) div 1) - 1", wantType: types.T_uint64, wantError: true},
		{name: "integer division result keeps signed mode", mode: mysql.SQLModeNoUnsignedSubtraction, sql: "select (cast(0 as bit(8)) div 1) - 1", wantType: types.T_int64, wantInt: -1},
		// BIT arithmetic is resolved by the native numeric resolver as
		// DECIMAL128. It must not be rewritten to the unsigned subtraction
		// boundary, even when NO_UNSIGNED_SUBTRACTION is enabled.
		{name: "bit underflow uses decimal domain", sql: "select cast(0 as bit(8)) - 1", wantType: types.T_decimal128, wantInt: -1},
		{name: "bit signed mode keeps decimal domain", mode: mysql.SQLModeNoUnsignedSubtraction, sql: "select cast(0 as bit(8)) - 1", wantType: types.T_decimal128, wantInt: -1},
		{name: "bit addition uses decimal domain", sql: "select cast(255 as bit(8)) + 1", wantType: types.T_decimal128, wantInt: 256},
		{name: "bit multiplication uses decimal domain", sql: "select cast(255 as bit(8)) * 2", wantType: types.T_decimal128, wantInt: 510},
		{name: "bit modulo uses decimal domain", sql: "select cast(255 as bit(8)) % 2", wantType: types.T_decimal128, wantInt: 1},
		{name: "null", sql: "select cast(null as unsigned) - 1", wantType: types.T_uint64, wantNull: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := unsignedSubtractionProjection(t, tc.mode, tc.sql, false)
			proc := testutil.NewProc(t)
			defer proc.Free()
			executor, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer executor.Free()

			input := batch.New(nil)
			input.SetRowCount(1)
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
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
			if tc.wantType == types.T_decimal128 {
				require.Equal(t, types.Decimal128FromInt64(tc.wantInt), vector.GetFixedAtNoTypeCheck[types.Decimal128](result, 0))
			} else if tc.wantType == types.T_int64 {
				require.Equal(t, tc.wantInt, vector.GetFixedAtNoTypeCheck[int64](result, 0))
			} else {
				require.Equal(t, tc.wantUint, vector.GetFixedAtNoTypeCheck[uint64](result, 0))
			}
		})
	}
}

func TestUnsignedIntegerModuloPreservesDividendSignedness(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want types.T
	}{
		// Negative numeric literals are represented as signed DECIMAL in the
		// planner; the important invariant is that the unsigned divisor must not
		// force the result into UINT64.
		{name: "signed dividend with unsigned divisor", sql: "select -3 % cast(2 as unsigned)", want: types.T_decimal128},
		{name: "unsigned dividend with signed divisor", sql: "select cast(3 as unsigned) % -2", want: types.T_uint64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := unsignedSubtractionProjection(t, "", tc.sql, false)
			require.Equal(t, int32(tc.want), expr.Typ.Id)
		})
	}
}

func TestUnsignedIntegerModuloExecutionPreservesDividendSignedness(t *testing.T) {
	for _, tc := range []struct {
		name     string
		sql      string
		wantType types.T
		wantInt  int64
		wantUint uint64
	}{
		{name: "signed dividend with unsigned divisor", sql: "select -3 % cast(2 as unsigned)", wantType: types.T_decimal128, wantInt: -1},
		{name: "unsigned dividend with signed divisor", sql: "select cast(3 as unsigned) % -2", wantType: types.T_uint64, wantUint: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := unsignedSubtractionProjection(t, "", tc.sql, false)
			proc := testutil.NewProc(t)
			defer proc.Free()
			executor, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer executor.Free()

			input := batch.New(nil)
			input.SetRowCount(1)
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, tc.wantType, result.GetType().Oid)
			if tc.wantType == types.T_decimal128 {
				require.Equal(t, types.Decimal128FromInt64(tc.wantInt), vector.GetFixedAtNoTypeCheck[types.Decimal128](result, 0))
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
		name                  string
		sql                   string
		op                    string
		wantCheck             bool
		wantDeferredBoundary  bool
		wantStrictBoundary    bool
		wantNativeBitBoundary bool
	}{
		{
			name:                 "bare signed marker remains deferred",
			sql:                  "prepare unsigned_add from 'select cast(1 as unsigned) + ?'",
			op:                   "+",
			wantDeferredBoundary: true,
		},
		{
			name:                 "nested deferred arithmetic requires strict intermediate check",
			sql:                  "prepare unsigned_add from 'select (cast(1 as unsigned) + ?) - 1'",
			op:                   "+",
			wantDeferredBoundary: true,
			wantStrictBoundary:   true,
		},
		{
			name:                 "ABS wrapper remains deferred",
			sql:                  "prepare unsigned_add from 'select cast(1 as unsigned) + abs(?)'",
			op:                   "+",
			wantDeferredBoundary: true,
		},
		{
			name:                 "COALESCE integer branches remain deferred",
			sql:                  "prepare unsigned_add from 'select cast(1 as unsigned) + coalesce(?, 0)'",
			op:                   "+",
			wantDeferredBoundary: true,
		},
		{
			name:                 "CASE integer branches remain deferred",
			sql:                  "prepare unsigned_add from 'select cast(1 as unsigned) + case when 1 then ? else 0 end'",
			op:                   "+",
			wantDeferredBoundary: true,
		},
		{
			name: "fractional COALESCE branch stays non-integer",
			sql:  "prepare unsigned_add from 'select cast(1 as unsigned) + coalesce(?, 0.5)'",
			op:   "+",
		},
		{
			name:                 "explicit unsigned and signed casts retain addition boundary",
			sql:                  "prepare unsigned_add from 'select cast(? as unsigned) + cast(1 as signed)'",
			op:                   "+",
			wantCheck:            true,
			wantDeferredBoundary: true,
		},
		{
			name:                 "explicit unsigned and signed casts retain multiplication boundary",
			sql:                  "prepare unsigned_mul from 'select cast(? as unsigned) * cast(2 as signed)'",
			op:                   "*",
			wantCheck:            true,
			wantDeferredBoundary: true,
		},
		{
			name: "signed MOD dividend keeps native signed result",
			sql:  "prepare signed_mod from 'select cast(-3 as signed) % ?'",
			op:   "%",
		},
		{
			name:                  "BIT arithmetic keeps native decimal contract",
			sql:                   "prepare bit_add from 'select cast(0 as bit(8)) + ?'",
			op:                    "+",
			wantNativeBitBoundary: true,
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
			require.Equal(t, tc.wantDeferredBoundary,
				hasPreparedDeferredArithmeticBoundary(firstProjectionExpr(t, prepared), tc.op),
				"prepare plan must retain a runtime boundary marker without freezing the bare marker domain")
			require.Equal(t, tc.wantStrictBoundary,
				hasPreparedStrictArithmeticBoundary(firstProjectionExpr(t, prepared), tc.op),
				"only nested deferred arithmetic may install the strict intermediate check")
			require.Equal(t, tc.wantNativeBitBoundary,
				hasPreparedNativeBitArithmeticBoundary(firstProjectionExpr(t, prepared), tc.op),
				"prepare plan must retain the native BIT arithmetic contract")
		})
	}
}

func hasPreparedStrictArithmeticBoundary(expr *Expr, operator string) bool {
	if expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func != nil && fn.Func.ObjName == operator &&
			expr.GetPreparedNumeric().GetStrictUnsignedArithmeticBoundary() {
			return true
		}
		for _, arg := range fn.Args {
			if hasPreparedStrictArithmeticBoundary(arg, operator) {
				return true
			}
		}
	}
	return false
}

func hasPreparedDeferredArithmeticBoundary(expr *Expr, operator string) bool {
	if expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func != nil && fn.Func.ObjName == operator &&
			expr.GetPreparedNumeric().GetDeferredUnsignedArithmeticBoundary() {
			return true
		}
		for _, arg := range fn.Args {
			if hasPreparedDeferredArithmeticBoundary(arg, operator) {
				return true
			}
		}
	}
	return false
}

func hasPreparedNativeBitArithmeticBoundary(expr *Expr, operator string) bool {
	if expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func != nil && fn.Func.ObjName == operator &&
			expr.GetPreparedNumeric().GetNativeBitArithmeticBoundary() {
			return true
		}
		for _, arg := range fn.Args {
			if hasPreparedNativeBitArithmeticBoundary(arg, operator) {
				return true
			}
		}
	}
	return false
}

func TestSQLPrepareBitArithmeticKeepsDecimalRuntimeDomain(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t,
		"prepare bit_add from 'select cast(0 as bit(8)) + ?'")
	require.NoError(t, err)
	prepared := logicPlan.GetDcl().GetPrepare().Plan
	require.NotNil(t, prepared)

	fallback, _, err := FillValuesOfParamsInPlanWithSpecialization(
		context.Background(), prepared, []any{ParamValue{
			Value:            "-1",
			PrepareParamKind: vector.PrepareParamInteger,
		}},
	)
	require.NoError(t, err)
	projection := firstProjectionExpr(t, fallback)
	require.Equal(t, int32(types.T_decimal128), projection.Typ.Id)

	proc := testutil.NewProc(t)
	defer proc.Free()
	executor, err := colexec.NewExpressionExecutor(proc, projection)
	require.NoError(t, err)
	defer executor.Free()
	input := batch.New(nil)
	input.SetRowCount(1)
	result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, types.Decimal128FromInt64(-1), vector.GetFixedAtNoTypeCheck[types.Decimal128](result, 0))
}

func TestSQLPrepareNestedBitArithmeticPreservesValue(t *testing.T) {
	for _, tc := range []struct {
		name  string
		sql   string
		param string
		want  int64
	}{
		{name: "nested addition", sql: "prepare bit_nested from 'select (cast(1 as bit(8)) + 5) + ?'", param: "2", want: 8},
		{name: "nested multiplication", sql: "prepare bit_nested from 'select (cast(2 as bit(8)) * 3) + ?'", param: "4", want: 10},
		{name: "nested unary minus", sql: "prepare bit_nested from 'select (-(cast(1 as bit(8)) + 5)) + ?'", param: "2", want: -4},
		{name: "nested modulo", sql: "prepare bit_nested from 'select (cast(7 as bit(8)) % 4) + ?'", param: "2", want: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			logicPlan, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			prepared := logicPlan.GetDcl().GetPrepare().Plan
			filled, _, err := FillValuesOfParamsInPlanWithSpecialization(
				context.Background(), prepared, []any{ParamValue{
					Value:            tc.param,
					PrepareParamKind: vector.PrepareParamInteger,
				}},
			)
			require.NoError(t, err)
			projection := firstProjectionExpr(t, filled)
			require.Equal(t, types.T_decimal128, types.T(projection.Typ.Id))

			proc := testutil.NewProc(t)
			defer proc.Free()
			executor, err := colexec.NewExpressionExecutor(proc, projection)
			require.NoError(t, err)
			defer executor.Free()
			input := batch.New(nil)
			input.SetRowCount(1)
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, types.Decimal128FromInt64(tc.want), vector.GetFixedAtNoTypeCheck[types.Decimal128](result, 0))
		})
	}
}

func TestSQLPrepareBitArithmeticClearsStaleRuntimeEnvelope(t *testing.T) {
	for _, tc := range []struct {
		name     string
		param    ParamValue
		wantNull bool
	}{
		{name: "decimal marker", param: ParamValue{Value: "0.5", PrepareParamKind: vector.PrepareParamDecimal}},
		{name: "float marker", param: ParamValue{Value: "0.5", PrepareParamKind: vector.PrepareParamFloat}},
		{name: "null marker", param: ParamValue{Value: nil, RuntimeType: types.T_int64.ToType(), HasRuntimeType: true}, wantNull: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			logicPlan, err := runOneStmt(mock, t,
				"prepare bit_add_boundary from 'select cast(0 as bit(8)) + ?'")
			require.NoError(t, err)
			prepared := logicPlan.GetDcl().GetPrepare().Plan
			filled, _, err := FillValuesOfParamsInPlanWithSpecialization(
				context.Background(), prepared, []any{tc.param})
			require.NoError(t, err)
			projection := firstProjectionExpr(t, filled)
			require.NotEqual(t, int32(types.T_uint64), projection.Typ.Id)

			proc := testutil.NewProc(t)
			defer proc.Free()
			executor, err := colexec.NewExpressionExecutor(proc, projection)
			require.NoError(t, err)
			defer executor.Free()
			input := batch.New(nil)
			input.SetRowCount(1)
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			if tc.wantNull {
				require.True(t, result.GetNulls().Contains(0))
			} else {
				switch tc.name {
				case "decimal marker":
					require.Equal(t, types.T_decimal128, result.GetType().Oid)
					want, parseErr := types.ParseDecimal128("0.5", result.GetType().Width, result.GetType().Scale)
					require.NoError(t, parseErr)
					require.Equal(t, want, vector.GetFixedAtNoTypeCheck[types.Decimal128](result, 0))
				case "float marker":
					require.Equal(t, types.T_float64, result.GetType().Oid)
					require.Equal(t, 0.5, vector.GetFixedAtNoTypeCheck[float64](result, 0))
				}
			}
		})
	}
}

func TestSQLPrepareUnsignedModuloUsesDividendDomain(t *testing.T) {
	for _, tc := range []struct {
		name     string
		operator string
		want     types.T
	}{
		{name: "signed dividend", operator: "%", want: types.T_decimal128},
		{name: "unsigned dividend", operator: "%", want: types.T_uint64},
		{name: "signed dividend function", operator: "mod", want: types.T_decimal128},
		{name: "unsigned dividend function", operator: "mod", want: types.T_uint64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leftCast, rightCast := "signed", "unsigned"
			if strings.Contains(tc.name, "unsigned dividend") {
				leftCast, rightCast = "unsigned", "signed"
			}
			query := fmt.Sprintf("prepare modulo from 'select cast(? as %s) %s cast(? as %s)'", leftCast, tc.operator, rightCast)
			if tc.operator == "mod" {
				query = fmt.Sprintf("prepare modulo from 'select mod(cast(? as %s), cast(? as %s))'", leftCast, rightCast)
			}
			mock := NewMockOptimizer(false)
			logicPlan, err := runOneStmt(mock, t, query)
			require.NoError(t, err)
			prepared := logicPlan.GetDcl().GetPrepare().Plan
			require.NotNil(t, prepared)
			require.Equal(t, int32(tc.want), firstProjectionExpr(t, prepared).Typ.Id)
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
