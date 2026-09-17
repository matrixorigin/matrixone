// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"fmt"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestPreparedExportSetNullDomainHistory(t *testing.T) {
	for _, shape := range []string{"direct", "arithmetic", "subquery-arithmetic", "scalar", "derived"} {
		scalar := shape != "derived"
		sql := `select export_set(coalesce((select ?),1.5),'Y','N','',4)`
		if shape == "direct" {
			sql = `select export_set(coalesce(?,1.5),'Y','N','',4)`
		}
		if shape == "arithmetic" {
			sql = `select export_set(coalesce(?+0,1.5),'Y','N','',4)`
		}
		if shape == "subquery-arithmetic" {
			sql = `select export_set(coalesce((select ?)+0,1.5),'Y','N','',4)`
		}
		if !scalar {
			sql = `select export_set((select coalesce(x,1.5) from (select max(?) as x from tpch.nation) d),'Y','N','',4)`
		}
		t.Run(sql, func(t *testing.T) {
			ses, stmt, cw, execCtx := newPreparedExecuteEnvForSQLWithCompilerContext(t, 391, sql, plan2.NewMockCompilerContext(true))
			t.Cleanup(func() { cw.proc.SetPrepareParams(nil); stmt.Close() })
			require.Equal(t, []int32{0}, stmt.exportSetParamPositions)
			cached := stmt.PreparePlan.GetDcl().GetPrepare().Plan
			before := cached.String()
			for _, tc := range []struct {
				value    string
				null     bool
				mysql    defines.MysqlType
				want     string
				decimal  bool
				floating bool
				wantErr  bool
				rebuild  bool
			}{
				{null: true, mysql: defines.MYSQL_TYPE_NULL, want: "YNNN"},
				{value: "2.5", mysql: defines.MYSQL_TYPE_NEWDECIMAL, want: "YYNN", decimal: true},
				{value: "invalid", mysql: defines.MYSQL_TYPE_NEWDECIMAL, wantErr: true},
				{null: true, mysql: defines.MYSQL_TYPE_NULL, want: "NYNN", decimal: true},
				{value: "2.5", mysql: defines.MYSQL_TYPE_DOUBLE, want: "NYNN", floating: true},
				{null: true, mysql: defines.MYSQL_TYPE_NULL, want: "NYNN", floating: true},
				{value: "2.5", mysql: defines.MYSQL_TYPE_NEWDECIMAL, want: "NYNN", floating: true},
				{value: "2.5", mysql: defines.MYSQL_TYPE_VAR_STRING, want: "NYNN", floating: true},
				{null: true, mysql: defines.MYSQL_TYPE_NULL, want: "NYNN", floating: true},
				{null: true, mysql: defines.MYSQL_TYPE_NULL, want: "YNNN", rebuild: true},
			} {
				if tc.rebuild && !scalar {
					continue
				}
				cw.proc.SetPrepareParams(nil)
				if stmt.params != nil {
					stmt.params.Free(cw.proc.Mp())
					stmt.params = nil
				}
				params := vector.NewVec(types.T_text.ToType())
				stmt.params = params
				require.NoError(t, vector.AppendBytes(params, []byte(tc.value), tc.null, cw.proc.Mp()))
				stmt.ParamTypes = []byte{byte(tc.mysql), 0}
				history := append([]types.Type(nil), stmt.exportSetParamTypes...)
				if tc.rebuild && scalar {
					stmt.needsRebuild = true
				}
				_, filled, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, stmt.Name)
				if tc.wantErr {
					require.Error(t, err)
					require.Equal(t, history, stmt.exportSetParamTypes, "decoder failure must not publish history")
					require.Equal(t, before, cached.String())
					continue
				}
				require.NoError(t, err)
				if tc.rebuild && scalar {
					require.False(t, stmt.needsRebuild)
					require.NotSame(t, cached, stmt.PreparePlan.GetDcl().GetPrepare().Plan)
				}
				var export *plan.Expr
				require.NoError(t, plan.VisitExpressionsInOwner(filled, func(root *plan.Expr) error {
					return plan.VisitExprTree(root, func(expr *plan.Expr) error {
						if fn := expr.GetF(); fn != nil && fn.Func != nil && fn.Func.ObjName == "export_set" {
							export = expr
						}
						return nil
					})
				}))
				require.NotNil(t, export)
				if tc.decimal {
					require.True(t, types.T(export.GetF().Args[0].Typ.Id).IsDecimal(), export.String())
				}
				if tc.floating {
					source := export.GetF().Args[0]
					if source.Typ.Id == int32(types.T_int64) {
						require.NotNil(t, source.GetF())
						_, overload := function.DecodeOverloadID(source.GetF().Func.Obj)
						require.Equal(t, int32(4), overload)
						source = source.GetF().Args[0]
					}
					require.Equal(t, int32(types.T_float64), source.Typ.Id, export.String())
				}
				if scalar {
					result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, export, []*batch.Batch{batch.EmptyForConstFoldBatch})
					require.NoError(t, err)
					func() {
						defer free()
						want := tc.want
						if (shape == "direct" || shape == "arithmetic" || shape == "subquery-arithmetic") && want == "YNNN" {
							want = "NYNN"
						}
						require.Equal(t, want, result.GetStringAt(0), "params=%+v expr=%s", cw.paramVals, export.String())
					}()
				}
				require.Equal(t, before, cached.String())
			}
		})
	}
}

func TestPreparedExportSetResolvedStringAndIntegerBindings(t *testing.T) {
	for _, shape := range []string{"scalar", "direct", "subquery-arithmetic", "derived-coalesce"} {
		direct := shape == "direct"
		sql := `select export_set(coalesce((select ?),2.5),'Y','N','',4)`
		if shape == "subquery-arithmetic" {
			sql = `select export_set(coalesce((select ?)+0,2.5),'Y','N','',4)`
		}
		if direct {
			sql = `select export_set(coalesce(?,2.5),'Y','N','',4)`
		}
		if shape == "derived-coalesce" {
			sql = `select export_set(coalesce(x,2.5),'Y','N','',4) from (select ? as x) d`
		}
		_, stmt, cw, _ := newPreparedExecuteEnvForSQL(t, 393, sql)
		func() {
			defer stmt.Close()
			cached := stmt.PreparePlan.GetDcl().GetPrepare().Plan
			before := cached.String()
			decimal := types.New(types.T_decimal64, 4, 1)
			text := types.T_text.ToType()
			for i, tc := range []struct {
				value any
				typ   types.Type
				want  string
			}{
				{"2.5", text, "NYNN"},
				{nil, text, "NYNN"},
				{"2.5", decimal, "YYNN"},
				{"2.49", text, "NYNN"},
				{"abc", text, "NNNN"},
				{int64(100000), types.T_int64.ToType(), "NNNN"},
				{true, types.T_bool.ToType(), "YNNN"},
				{nil, text, "YYNN"},
				{2.5, types.T_float64.ToType(), "NYNN"},
				{"2.5", decimal, "NYNN"},
				{"2.5", text, "NYNN"},
				{nil, text, "NYNN"},
			} {
				values := []any{plan2.ParamValue{Value: tc.value, SourceType: tc.typ, HasSourceType: true, EnableNumericPrefix: true}}
				stmt.applyExportSetNullRuntimeTypes(values)
				filled, err := plan2.FillValuesOfParamsInPlan(cw.proc.Ctx, cached, values)
				require.NoError(t, err)
				q := filled.GetQuery()
				expr := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0]
				result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
				require.NoError(t, err)
				func() {
					defer free()
					want := tc.want
					if direct && i < 2 {
						want = "YYNN"
					}
					require.Equal(t, want, result.GetStringAt(0), "step=%d shape=%s expr=%s", i, shape, expr.String())
				}()
				require.Equal(t, before, cached.String())
			}
		}()
	}
}

func TestPreparedExportSetBareActualAndResolvedDomains(t *testing.T) {
	for _, source := range []string{"?", "(select ?)"} {
		for _, binary := range []bool{false, true} {
			sql := `select export_set(` + source + `,'Y','N','',4)`
			_, stmt, cw, _ := newPreparedExecuteEnvForSQL(t, 394, sql)
			func() {
				defer stmt.Close()
				cached := stmt.PreparePlan.GetDcl().GetPrepare().Plan
				before := cached.String()
				text := types.T_text.ToType()
				decimal := types.New(types.T_decimal64, 4, 1)
				for _, tc := range []struct {
					value any
					typ   types.Type
					want  string
				}{
					{"2.5", text, "YYNN"},
					{"2.5", decimal, "YYNN"},
					{2.5, types.T_float64.ToType(), "NYNN"},
					{"2.5", decimal, "YYNN"},
					{"2.5", text, "NYNN"},
					{"1.5x", text, "YNNN"},
					{"9223372036854775808.5", types.New(types.T_decimal256, 65, 1), "YYYY"},
					{"2.5", decimal, "YYNN"},
					{nil, text, ""},
				} {
					values := []any{plan2.ParamValue{Value: tc.value, SourceType: tc.typ, HasSourceType: !binary,
						RuntimeType: tc.typ, HasRuntimeType: binary, IsBinaryProtocol: binary, EnableNumericPrefix: true}}
					stmt.applyExportSetNullRuntimeTypes(values)
					filled, err := plan2.FillValuesOfParamsInPlan(cw.proc.Ctx, cached, values)
					require.NoError(t, err)
					q := filled.GetQuery()
					expr := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0]
					result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
					require.NoError(t, err)
					func() {
						defer free()
						if tc.value == nil {
							require.True(t, result.GetNulls().Contains(0))
						} else {
							want := tc.want
							if source != "?" && tc.typ.Oid.IsMySQLString() && want == "YYNN" {
								want = "NYNN"
							}
							require.Equal(t, want, result.GetStringAt(0), "source=%s binary=%t type=%s expr=%s", source, binary, tc.typ, expr.String())
						}
					}()
					require.Equal(t, before, cached.String())
				}
			}()
		}
	}
}

func TestPreparedExportSetFoldedDerivedSourceDomain(t *testing.T) {
	_, stmt, cw, _ := newPreparedExecuteEnvForSQL(t, 396,
		`select export_set(x,'Y','N','',4) from (select ? as x) d`)
	defer stmt.Close()
	require.False(t, stmt.exportSetBareParams[0], "a folded derived projection is still a producer")
	cached := stmt.PreparePlan.GetDcl().GetPrepare().Plan
	before := cached.String()
	text := types.T_text.ToType()
	decimal := types.New(types.T_decimal64, 4, 1)
	for step, tc := range []struct {
		value any
		typ   types.Type
		want  string
		null  bool
	}{
		{"2.5", text, "NYNN", false},
		{"2.5", decimal, "YYNN", false},
		{2.5, types.T_float64.ToType(), "NYNN", false},
		{"2.5", decimal, "NYNN", false},
		{"2.5", text, "NYNN", false},
		{nil, text, "", true},
	} {
		values := []any{plan2.ParamValue{Value: tc.value, SourceType: tc.typ, HasSourceType: true, EnableNumericPrefix: true}}
		stmt.applyExportSetNullRuntimeTypes(values)
		filled, err := plan2.FillValuesOfParamsInPlan(cw.proc.Ctx, cached, values)
		require.NoError(t, err)
		q := filled.GetQuery()
		expr := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0]
		result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
		require.NoError(t, err)
		func() {
			defer free()
			if tc.null {
				require.True(t, result.GetNulls().Contains(0), "step=%d", step)
			} else {
				require.Equal(t, tc.want, result.GetStringAt(0), "step=%d expr=%s", step, expr.String())
			}
		}()
		require.Equal(t, before, cached.String())
	}
}

func TestPreparedExportSetNestedFoldedDerivedConsumers(t *testing.T) {
	for _, consumer := range []string{"round(x,0)", "abs(x)"} {
		_, stmt, cw, _ := newPreparedExecuteEnvForSQL(t, 397,
			`select export_set(`+consumer+`,'Y','N','',4) from (select ? as x) d`)
		func() {
			defer stmt.Close()
			require.False(t, stmt.exportSetBareParams[0])
			require.Equal(t, types.T_text, stmt.exportSetParamTypes[0].Oid)
			cached := stmt.PreparePlan.GetDcl().GetPrepare().Plan
			before := cached.String()
			for step, tc := range []struct {
				value any
				typ   types.Type
				want  string
				null  bool
			}{
				{"2.5", types.T_text.ToType(), "NYNN", false},
				{"2.5", types.New(types.T_decimal64, 4, 1), "YYNN", false},
				{2.5, types.T_float64.ToType(), "NYNN", false},
				{"2.5", types.New(types.T_decimal64, 4, 1), "NYNN", false},
				{nil, types.T_text.ToType(), "", true},
			} {
				values := []any{plan2.ParamValue{Value: tc.value, SourceType: tc.typ, HasSourceType: true, EnableNumericPrefix: true}}
				stmt.applyExportSetNullRuntimeTypes(values)
				filled, err := plan2.FillValuesOfParamsInPlan(cw.proc.Ctx, cached, values)
				require.NoError(t, err)
				q := filled.GetQuery()
				expr := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0]
				result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
				require.NoError(t, err)
				func() {
					defer free()
					if tc.null {
						require.True(t, result.GetNulls().Contains(0), "consumer=%s step=%d", consumer, step)
					} else {
						require.Equal(t, tc.want, result.GetStringAt(0), "consumer=%s step=%d expr=%s", consumer, step, expr.String())
					}
				}()
				require.Equal(t, before, cached.String())
			}
		}()
	}
}

func TestPreparedExportSetNestedPrecisionConsumerRole(t *testing.T) {
	for _, tc := range []struct {
		consumer string
		want     string
	}{
		{"round(15.5,x)", "NNNN"},
		{"truncate(15.5,x)", "YYYY"},
	} {
		_, stmt, cw, _ := newPreparedExecuteEnvForSQL(t, 398,
			`select export_set(`+tc.consumer+`,'Y','N','',4) from (select ? as x) d`)
		func() {
			defer stmt.Close()
			cached := stmt.PreparePlan.GetDcl().GetPrepare().Plan
			before := cached.String()
			for step, binding := range []struct {
				value any
				typ   types.Type
			}{
				{"-0.5", types.T_text.ToType()},
				{-0.5, types.T_float64.ToType()},
				{"-0.5", types.T_text.ToType()},
				{"-0.5", types.New(types.T_decimal64, 2, 1)},
			} {
				values := []any{plan2.ParamValue{Value: binding.value, SourceType: binding.typ, HasSourceType: true,
					EnableNumericPrefix: true}}
				stmt.applyExportSetNullRuntimeTypes(values)
				filled, err := plan2.FillValuesOfParamsInPlan(cw.proc.Ctx, cached, values)
				require.NoError(t, err)
				q := filled.GetQuery()
				expr := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0]
				result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, expr,
					[]*batch.Batch{batch.EmptyForConstFoldBatch})
				require.NoError(t, err)
				func() {
					defer free()
					require.Equal(t, tc.want, result.GetStringAt(0), "consumer=%s step=%d expr=%s", tc.consumer, step, expr.String())
				}()
				require.Equal(t, before, cached.String())
			}
		}()
	}
}

func TestPreparedExportSetIndependentNumericPrecisionDomains(t *testing.T) {
	for _, tc := range []struct {
		name     string
		consumer string
		value    any
		typ      types.Type
		want     string
	}{
		{"round decimal half", "round(15.5,x)", "-0.5", types.New(types.T_decimal64, 4, 1), "NNYN"},
		{"truncate decimal half", "truncate(15.5,x)", "-0.5", types.New(types.T_decimal64, 4, 1), "NYNY"},
		{"round real fraction", "round(15.5,x)", -0.6, types.T_float64.ToType(), "NNYN"},
		{"truncate real fraction", "truncate(15.5,x)", -0.6, types.T_float64.ToType(), "NYNY"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, stmt, cw, _ := newPreparedExecuteEnvForSQL(t, 399,
				`select export_set(`+tc.consumer+`,'Y','N','',4) from (select ? as x) d`)
			defer stmt.Close()
			cached := stmt.PreparePlan.GetDcl().GetPrepare().Plan
			values := []any{plan2.ParamValue{Value: tc.value, SourceType: tc.typ, HasSourceType: true,
				EnableNumericPrefix: true}}
			stmt.applyExportSetNullRuntimeTypes(values)
			filled, err := plan2.FillValuesOfParamsInPlan(cw.proc.Ctx, cached, values)
			require.NoError(t, err)
			q := filled.GetQuery()
			expr := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0]
			result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, expr,
				[]*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			defer free()
			require.Equal(t, tc.want, result.GetStringAt(0), "expr=%s", expr.String())
		})
	}
}

func TestPreparedExportSetPrecisionExpressionRoles(t *testing.T) {
	for _, tc := range []struct {
		name        string
		precision   string
		value       any
		typ         types.Type
		want        string
		materialize bool
	}{
		{"abs integer", "abs(x)", int64(1), types.T_int64.ToType(), "NNNN", false},
		{"round integer", "round(x,0)", int64(1), types.T_int64.ToType(), "NNNN", false},
		{"sign integer", "sign(x)", int64(1), types.T_int64.ToType(), "NNNN", false},
		{"ifnull real", "ifnull(x,0)", 0.5, types.T_float64.ToType(), "YYYY", true},
		{"bitwise real", "x & 1", 0.5, types.T_float64.ToType(), "YYYY", true},
		{"nested bitwise real", "abs(x & 1)", 0.5, types.T_float64.ToType(), "YYYY", true},
		{"ifnull bitwise producer", "ifnull(x & 1,0)", 0.5, types.T_float64.ToType(), "YYYY", true},
		{"bitwise real saturation", "x & 1", 1e100, types.T_float64.ToType(), "NNNN", true},
		{"coalesce text", "coalesce(x,'0')", "0.5", types.New(types.T_decimal64, 2, 1), "YYYY", true},
		{"greatest coalesce text", "greatest(coalesce(x,'0'),'0')", "0.5", types.New(types.T_decimal64, 2, 1), "YYYY", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, stmt, cw, _ := newPreparedExecuteEnvForSQL(t, 401,
				`select export_set(truncate(15.5,`+tc.precision+`),'Y','N','',4) from (select ? as x) d`)
			defer stmt.Close()
			cached := stmt.PreparePlan.GetDcl().GetPrepare().Plan
			values := []any{plan2.ParamValue{Value: tc.value, SourceType: tc.typ, HasSourceType: true,
				EnableNumericPrefix: true}}
			stmt.applyExportSetNullRuntimeTypes(values)
			filled, err := plan2.FillValuesOfParamsInPlan(cw.proc.Ctx, cached, values)
			require.NoError(t, err)
			q := filled.GetQuery()
			expr := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0]
			inputs := []*batch.Batch{batch.EmptyForConstFoldBatch}
			if tc.materialize {
				input := batch.NewWithSize(1)
				input.Vecs[0] = vector.NewVec(tc.typ)
				if tc.typ.IsDecimal() {
					require.NoError(t, vector.AppendFixed(input.Vecs[0], types.Decimal64(5), false, cw.proc.Mp()))
				} else {
					require.NoError(t, vector.AppendFixed(input.Vecs[0], tc.value.(float64), false, cw.proc.Mp()))
				}
				input.SetRowCount(1)
				defer input.Clean(cw.proc.Mp())
				inputs = []*batch.Batch{input}
			}
			result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, expr, inputs)
			require.NoError(t, err)
			defer free()
			require.Equal(t, tc.want, result.GetStringAt(0), "expr=%s", expr.String())
		})
	}
}

func TestPreparedExportSetPrecisionDecimalHistory(t *testing.T) {
	_, stmt, cw, _ := newPreparedExecuteEnvForSQL(t, 402,
		`select export_set(round(15.5,x),'Y','N','',4) from (select ? as x) d`)
	defer stmt.Close()
	cached := stmt.PreparePlan.GetDcl().GetPrepare().Plan
	decimal := types.New(types.T_decimal64, 2, 1)
	for step, binding := range []struct {
		value any
		typ   types.Type
	}{
		{"-0.5", decimal},
		{"-0.5", types.T_text.ToType()},
	} {
		values := []any{plan2.ParamValue{Value: binding.value, SourceType: binding.typ, HasSourceType: true,
			EnableNumericPrefix: true}}
		stmt.applyExportSetNullRuntimeTypes(values)
		filled, err := plan2.FillValuesOfParamsInPlan(cw.proc.Ctx, cached, values)
		require.NoError(t, err)
		q := filled.GetQuery()
		expr := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0]
		result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, expr,
			[]*batch.Batch{batch.EmptyForConstFoldBatch})
		require.NoError(t, err)
		func() {
			defer free()
			require.Equal(t, "NNYN", result.GetStringAt(0), "step=%d expr=%s", step, expr.String())
		}()
	}
}

func TestPreparedExportSetPrecisionDecimalBitwiseSaturation(t *testing.T) {
	for _, value := range []string{"9223372036854775808", "18446744073709551616"} {
		t.Run(value, func(t *testing.T) {
			_, stmt, cw, _ := newPreparedExecuteEnvForSQL(t, 403,
				`select export_set(truncate(15.5,x & 1),'Y','N','',4) from (select ? as x) d`)
			defer stmt.Close()
			width := int32(len(value))
			decimalType := types.New(types.T_decimal128, width, 0)
			decimalValue, err := types.ParseDecimal128(value, width, 0)
			require.NoError(t, err)
			values := []any{plan2.ParamValue{Value: value, SourceType: decimalType, HasSourceType: true,
				EnableNumericPrefix: true}}
			stmt.applyExportSetNullRuntimeTypes(values)
			filled, err := plan2.FillValuesOfParamsInPlan(cw.proc.Ctx,
				stmt.PreparePlan.GetDcl().GetPrepare().Plan, values)
			require.NoError(t, err)
			q := filled.GetQuery()
			expr := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0]
			input := batch.NewWithSize(1)
			input.Vecs[0] = vector.NewVec(decimalType)
			defer input.Clean(cw.proc.Mp())
			require.NoError(t, vector.AppendFixed(input.Vecs[0], decimalValue, false, cw.proc.Mp()))
			input.SetRowCount(1)
			result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, expr, []*batch.Batch{input})
			require.NoError(t, err)
			defer free()
			require.Equal(t, "NNNN", result.GetStringAt(0), "expr=%s", expr.String())
		})
	}
}

func TestPreparedExportSetBitwiseCastUsesActualOperandDomain(t *testing.T) {
	for _, query := range []string{
		`select export_set(x,'Y','N','',4), y & 1 from (select ? x, ? y) d`,
		`select export_set(truncate(15.5,y & 1),'Y','N','',4) from (select coalesce(x,'0') y from (select ? x) d limit 1) p`,
		`select export_set(truncate(15.5,y & 1),'Y','N','',4) from (select coalesce(?,'0') y union all select '0') p`,
		`select export_set(truncate(15.5,y & 1),'Y','N','',4) from (select max(coalesce(x,'0')) y from (select ? x) d) p`,
		`select export_set(truncate(15.5,y & 1),'Y','N','',4) from (select first_value(coalesce(x,'0')) over () y from (select ? x) d) p`,
	} {
		t.Run(query, func(t *testing.T) {
			_, stmt, cw, _ := newPreparedExecuteEnvForSQL(t, 406, query)
			defer stmt.Close()
			values := []any{plan2.ParamValue{Value: "0.5", SourceType: types.New(types.T_decimal64, 2, 1), HasSourceType: true, EnableNumericPrefix: true}}
			if len(stmt.PreparePlan.GetDcl().GetPrepare().ParamTypes) == 2 {
				values = append(values, plan2.ParamValue{Value: "0.5", SourceType: types.T_text.ToType(), HasSourceType: true, EnableNumericPrefix: true})
			}
			stmt.applyExportSetNullRuntimeTypes(values)
			filled, err := plan2.FillValuesOfParamsInPlan(cw.proc.Ctx, stmt.PreparePlan.GetDcl().GetPrepare().Plan, values)
			require.NoError(t, err)
			err = plan.VisitExpressionsInOwner(filled, func(expr *plan.Expr) error {
				err := plan.VisitExprTree(expr, func(e *plan.Expr) error {
					if fn := e.GetF(); fn != nil && fn.Func != nil {
						id, overload := function.DecodeOverloadID(fn.Func.Obj)
						if id == function.CAST && overload == 5 {
							require.True(t, types.T(fn.Args[0].Typ.Id).ToType().IsDecimal(), "CAST5 source must be DECIMAL: %s", e.String())
						}
					}
					return nil
				})
				return err
			})
			require.NoError(t, err)
		})
	}
}

func TestPreparedExportSetPrecisionTextBitwiseProducer(t *testing.T) {
	_, stmt, cw, _ := newPreparedExecuteEnvForSQL(t, 404,
		`select export_set(truncate(15.5,ifnull(concat(x,''),'0') & 1),'Y','N','',4) from (select ? as x) d`)
	defer stmt.Close()
	values := []any{plan2.ParamValue{Value: "0.5", SourceType: types.New(types.T_decimal64, 2, 1),
		HasSourceType: true, EnableNumericPrefix: true}}
	stmt.applyExportSetNullRuntimeTypes(values)
	filled, err := plan2.FillValuesOfParamsInPlan(cw.proc.Ctx,
		stmt.PreparePlan.GetDcl().GetPrepare().Plan, values)
	require.NoError(t, err)
	q := filled.GetQuery()
	expr := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0]
	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.T_text.ToType())
	defer input.Clean(cw.proc.Mp())
	require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte("0.5"), false, cw.proc.Mp()))
	input.SetRowCount(1)
	result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, expr, []*batch.Batch{input})
	require.NoError(t, err)
	defer free()
	require.Equal(t, "YYYY", result.GetStringAt(0), "expr=%s", expr.String())
}

func TestPreparedExportSetUnaryBitwiseDomain(t *testing.T) {
	for _, tc := range []struct {
		value any
		typ   types.Type
		want  string
		query string
	}{
		{float64(0.5), types.T_float64.ToType(), "YYYY", `select export_set(~?,'Y','N','',4)`},
		{"-0.5", types.New(types.T_decimal64, 2, 1), "NNNN", `select export_set(~?,'Y','N','',4)`},
		{float64(0.5), types.T_float64.ToType(), "YYYY", `select export_set(~x,'Y','N','',4) from (select ? x)d`},
		{"-0.5", types.New(types.T_decimal64, 2, 1), "NNNN", `select export_set(~x,'Y','N','',4) from (select ? x)d`},
	} {
		t.Run(tc.typ.String()+tc.query, func(t *testing.T) {
			_, stmt, cw, _ := newPreparedExecuteEnvForSQL(t, 413, tc.query)
			defer stmt.Close()
			require.True(t, plan2.PreparedPlanNeedsRuntimeSpecialization(stmt.PreparePlan.GetDcl().GetPrepare().Plan), "%s", stmt.PreparePlan.String())
			values := []any{plan2.ParamValue{Value: tc.value, SourceType: tc.typ, HasSourceType: true, EnableNumericPrefix: true}}
			stmt.applyExportSetNullRuntimeTypes(values)
			filled, err := plan2.FillValuesOfParamsInPlan(cw.proc.Ctx, stmt.PreparePlan.GetDcl().GetPrepare().Plan, values)
			require.NoError(t, err)
			q := filled.GetQuery()
			expr := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0]
			result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			defer free()
			require.Equal(t, tc.want, result.GetStringAt(0), "expr=%s", expr.String())
		})
	}
}

func TestPreparedExportSetDirectGreatestContext(t *testing.T) {
	for _, tc := range []struct{ query, want string }{
		{`select export_set(greatest(?,1),'Y','N','',4)`, "YNNN"},
		{`select export_set(greatest(x,1),'Y','N','',4) from (select ? x)d`, "NNNN"},
	} {
		t.Run(tc.query, func(t *testing.T) {
			_, stmt, cw, _ := newPreparedExecuteEnvForSQL(t, 412, tc.query)
			defer stmt.Close()
			values := []any{plan2.ParamValue{Value: "abc", SourceType: types.T_text.ToType(), HasSourceType: true, EnableNumericPrefix: true}}
			stmt.applyExportSetNullRuntimeTypes(values)
			filled, err := plan2.FillValuesOfParamsInPlan(cw.proc.Ctx, stmt.PreparePlan.GetDcl().GetPrepare().Plan, values)
			require.NoError(t, err)
			q := filled.GetQuery()
			expr := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0]
			result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			defer free()
			require.Equal(t, tc.want, result.GetStringAt(0))
		})
	}
}

func TestPreparedExportSetSharedTextNumericConsumers(t *testing.T) {
	for _, tc := range []struct {
		expr string
		want float64
	}{{"abs(x)", 2.5}, {"floor(x)", 2}, {"ceil(x)", 3}, {"x+0", 2.5}} {
		t.Run(tc.expr, func(t *testing.T) {
			_, stmt, cw, _ := newPreparedExecuteEnvForSQL(t, 411, `select export_set(x,'Y','N','',4),`+tc.expr+` from (select ? x)d`)
			defer stmt.Close()
			values := []any{plan2.ParamValue{Value: "2.5", SourceType: types.T_text.ToType(), HasSourceType: true, EnableNumericPrefix: true}}
			stmt.applyExportSetNullRuntimeTypes(values)
			filled, err := plan2.FillValuesOfParamsInPlan(cw.proc.Ctx, stmt.PreparePlan.GetDcl().GetPrepare().Plan, values)
			require.NoError(t, err)
			q := filled.GetQuery()
			expr := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[1]
			input := batch.NewWithSize(1)
			input.Vecs[0] = vector.NewVec(types.T_text.ToType())
			defer input.Clean(cw.proc.Mp())
			require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte("2.5"), false, cw.proc.Mp()))
			input.SetRowCount(1)
			result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, expr, []*batch.Batch{input})
			require.NoError(t, err)
			defer free()
			require.Equal(t, types.T_float64, result.GetType().Oid)
			require.Equal(t, tc.want, vector.GetFixedAtNoTypeCheck[float64](result, 0))
		})
	}
}

func TestPreparedExportSetSharedTextAggregateDomain(t *testing.T) {
	for _, value := range []any{"2.5", nil} {
		t.Run(fmt.Sprint(value), func(t *testing.T) {
			_, stmt, cw, _ := newPreparedExecuteEnvForSQL(t, 410,
				`select export_set(x,'Y','N','',4),sum(x),avg(x) from (select ? x) d group by x`)
			defer stmt.Close()
			values := []any{plan2.ParamValue{Value: value, SourceType: types.T_text.ToType(), HasSourceType: true, EnableNumericPrefix: true}}
			stmt.applyExportSetNullRuntimeTypes(values)
			filled, err := plan2.FillValuesOfParamsInPlan(cw.proc.Ctx, stmt.PreparePlan.GetDcl().GetPrepare().Plan, values)
			require.NoError(t, err)
			seen := 0
			for _, node := range filled.GetQuery().Nodes {
				for _, agg := range node.AggList {
					if fn := agg.GetF(); fn != nil && (fn.Func.ObjName == "sum" || fn.Func.ObjName == "avg") {
						require.Equal(t, int32(types.T_float64), fn.Args[0].Typ.Id)
						seen++
					}
				}
			}
			require.Equal(t, 2, seen)
		})
	}
}

func TestPreparedExportSetUnsignedPrecisionDomain(t *testing.T) {
	for _, precision := range []string{"x | 1", "x ^ 0", "x << 1"} {
		t.Run(precision, func(t *testing.T) {
			_, stmt, cw, _ := newPreparedExecuteEnvForSQL(t, 408,
				`select export_set(truncate(13.5,`+precision+`),'Y','N','',4) from (select ? x) d`)
			defer stmt.Close()
			decimal := types.New(types.T_decimal64, 2, 1)
			values := []any{plan2.ParamValue{Value: "-0.5", SourceType: decimal, HasSourceType: true, EnableNumericPrefix: true}}
			stmt.applyExportSetNullRuntimeTypes(values)
			filled, err := plan2.FillValuesOfParamsInPlan(cw.proc.Ctx, stmt.PreparePlan.GetDcl().GetPrepare().Plan, values)
			require.NoError(t, err)
			q := filled.GetQuery()
			expr := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0]
			input := batch.NewWithSize(1)
			input.Vecs[0] = vector.NewVec(decimal)
			defer input.Clean(cw.proc.Mp())
			negativeHalf := int64(-5)
			require.NoError(t, vector.AppendFixed(input.Vecs[0], types.Decimal64(negativeHalf), false, cw.proc.Mp()))
			input.SetRowCount(1)
			result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, expr, []*batch.Batch{input})
			require.NoError(t, err)
			defer free()
			require.Equal(t, "NYYY", result.GetStringAt(0), "expr=%s", expr.String())
		})
	}
}

func TestPreparedExportSetConditionalDecimalSource(t *testing.T) {
	for _, tc := range []struct {
		value float64
		want  string
	}{{2.5, "YYNN"}, {-0.5, "YYYY"}} {
		t.Run(fmt.Sprint(tc.value), func(t *testing.T) {
			_, stmt, cw, _ := newPreparedExecuteEnvForSQL(t, 414,
				`select export_set(if(true,cast(? as decimal(2,1)),0e0),'Y','N','',4)`)
			defer stmt.Close()
			values := []any{plan2.ParamValue{Value: tc.value, SourceType: types.T_float64.ToType(), HasSourceType: true}}
			stmt.applyExportSetNullRuntimeTypes(values)
			filled, err := plan2.FillValuesOfParamsInPlan(cw.proc.Ctx, stmt.PreparePlan.GetDcl().GetPrepare().Plan, values)
			require.NoError(t, err)
			q := filled.GetQuery()
			expr := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0]
			result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			defer free()
			require.Equal(t, tc.want, result.GetStringAt(0))
		})
	}
}

func TestPreparedExportSetDirectRealOverflowSaturates(t *testing.T) {
	for _, tc := range []struct {
		consumer  string
		wantError bool
	}{
		{"?", false}, {"(select ?)", false}, {"if(true,?,0e0)", false},
		{"case when true then ? else 0e0 end", false}, {"abs(?)", true},
		{"if(true,abs(?),0e0)", true}, {"if(false,abs(?),1e100)", false},
		{"case when true then abs(?) else 0e0 end", true},
		{"coalesce(?,0e0)", true}, {"ifnull(?,0e0)", true}, {"(select abs(?))", true},
		{"if(true,ifnull(?,0e0),0e0)", true}, {"ifnull(if(true,?,0e0),0e0)", true},
		{"(select ifnull(?,0e0))", true},
	} {
		consumer := tc.consumer
		t.Run(consumer, func(t *testing.T) {
			_, stmt, cw, _ := newPreparedExecuteEnvForSQL(t, 409,
				`select export_set(`+consumer+`,'Y','N','',4)`)
			defer stmt.Close()
			values := []any{plan2.ParamValue{Value: float64(1e100), SourceType: types.T_float64.ToType(), HasSourceType: true, EnableNumericPrefix: true}}
			stmt.applyExportSetNullRuntimeTypes(values)
			encoded, err := stmt.PreparePlan.GetDcl().GetPrepare().Plan.Marshal()
			require.NoError(t, err)
			var restored plan.Plan
			require.NoError(t, restored.Unmarshal(encoded))
			filled, err := plan2.FillValuesOfParamsInPlan(cw.proc.Ctx, &restored, values)
			require.NoError(t, err)
			q := filled.GetQuery()
			expr := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0]
			result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
			if free != nil {
				defer free()
			}
			if tc.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "YYYY", result.GetStringAt(0))
		})
	}
}

func TestPreparedExportSetSiblingNumericConsumerValue(t *testing.T) {
	for _, tc := range []struct {
		consumer string
		want     string
	}{
		{"floor(x)", "2"}, {"ceil(x)", "3"}, {"round(x,1)", "2.5"}, {"x+0", "2.5"},
		{"floor(cast(x as signed))", "2"}, {"round(cast(x as signed),1)", "2"},
		{"export_set(2,'Y','N','',x)", "NY"},
	} {
		t.Run(tc.consumer, func(t *testing.T) {
			_, stmt, cw, _ := newPreparedExecuteEnvForSQL(t, 407,
				`select export_set(x,'Y','N','',4), `+tc.consumer+` from (select ? x) d`)
			defer stmt.Close()
			values := []any{plan2.ParamValue{Value: float64(2.5), SourceType: types.T_float64.ToType(), HasSourceType: true, EnableNumericPrefix: true}}
			stmt.applyExportSetNullRuntimeTypes(values)
			filled, err := plan2.FillValuesOfParamsInPlan(cw.proc.Ctx, stmt.PreparePlan.GetDcl().GetPrepare().Plan, values)
			require.NoError(t, err)
			q := filled.GetQuery()
			expr := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[1]
			target := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar), Width: 65535}, Expr: &plan.Expr_T{T: &plan.TargetType{}}}
			text, err := plan2.BindFuncExprImplByPlanExpr(cw.proc.Ctx, "cast", []*plan.Expr{expr, target})
			require.NoError(t, err)
			input := batch.NewWithSize(1)
			input.Vecs[0] = vector.NewVec(types.T_float64.ToType())
			defer input.Clean(cw.proc.Mp())
			require.NoError(t, vector.AppendFixed(input.Vecs[0], float64(2.5), false, cw.proc.Mp()))
			input.SetRowCount(1)
			result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, text, []*batch.Batch{input})
			require.NoError(t, err)
			defer free()
			require.Equal(t, tc.want, result.GetStringAt(0), "expr=%s", expr.String())
		})
	}
}

func TestPreparedExportSetSiblingAbsKeepsDecimalDomain(t *testing.T) {
	_, stmt, cw, _ := newPreparedExecuteEnvForSQL(t, 405,
		`select export_set(x,'Y','N','',4),abs(x) from (select ? as x) d`)
	defer stmt.Close()
	values := []any{plan2.ParamValue{Value: "2.5", SourceType: types.New(types.T_decimal64, 2, 1),
		HasSourceType: true, EnableNumericPrefix: true}}
	stmt.applyExportSetNullRuntimeTypes(values)
	filled, err := plan2.FillValuesOfParamsInPlan(cw.proc.Ctx,
		stmt.PreparePlan.GetDcl().GetPrepare().Plan, values)
	require.NoError(t, err)
	q := filled.GetQuery()
	root := q.Nodes[q.Steps[len(q.Steps)-1]]
	require.Len(t, root.ProjectList, 2)
	abs := root.ProjectList[1]
	require.Equal(t, int32(types.T_decimal64), abs.Typ.Id, "expr=%s", abs.String())
	require.Equal(t, int32(types.T_decimal64), abs.GetF().Args[0].Typ.Id)
	require.Nil(t, abs.GetF().Args[0].GetF(), "ABS must consume the refreshed producer directly")
}

func TestPreparedExportSetPrecisionExpressionOverflow(t *testing.T) {
	_, stmt, cw, _ := newPreparedExecuteEnvForSQL(t, 400,
		`select export_set(round(15.5,abs(x)),'Y','N','',4) from (select ? as x) d`)
	defer stmt.Close()
	cached := stmt.PreparePlan.GetDcl().GetPrepare().Plan
	values := []any{plan2.ParamValue{Value: "1e100", SourceType: types.T_text.ToType(), HasSourceType: true,
		EnableNumericPrefix: true}}
	stmt.applyExportSetNullRuntimeTypes(values)
	filled, err := plan2.FillValuesOfParamsInPlan(cw.proc.Ctx, cached, values)
	if err != nil {
		require.Error(t, err)
		return
	}
	q := filled.GetQuery()
	expr := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0]
	_, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, expr,
		[]*batch.Batch{batch.EmptyForConstFoldBatch})
	if free != nil {
		defer free()
	}
	require.Error(t, err)
}

func TestPreparedExportSetMaterializedRealSaturation(t *testing.T) {
	_, stmt, cw, _ := newPreparedExecuteEnvForSQLWithCompilerContext(t, 395,
		`select export_set((select max(?) from tpch.nation),'Y','N','',4)`, plan2.NewMockCompilerContext(true))
	defer stmt.Close()
	values := []any{plan2.ParamValue{Value: 2.5, SourceType: types.T_float64.ToType(), HasSourceType: true}}
	stmt.applyExportSetNullRuntimeTypes(values)
	filled, err := plan2.FillValuesOfParamsInPlan(cw.proc.Ctx, stmt.PreparePlan.GetDcl().GetPrepare().Plan, values)
	require.NoError(t, err)
	q := filled.GetQuery()
	expr := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0]
	conversion := expr.GetF().Args[0]
	require.NotNil(t, conversion.GetF())
	_, overload := function.DecodeOverloadID(conversion.GetF().Func.Obj)
	require.Equal(t, int32(4), overload)
	source := conversion.GetF().Args[0]
	require.NotNil(t, source.GetCol())
	require.Equal(t, int32(0), source.GetCol().ColPos)
	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.T_float64.ToType())
	defer input.Clean(cw.proc.Mp())
	for i, value := range []float64{2.5, math.Exp2(63), -math.Exp2(64), 0} {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], value, i == 3, cw.proc.Mp()))
	}
	input.SetRowCount(4)
	result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, expr, []*batch.Batch{input})
	require.NoError(t, err)
	defer free()
	for i, want := range []string{"NYNN", "YYYY", "NNNN"} {
		require.Equal(t, want, result.GetStringAt(i))
	}
	require.True(t, result.GetNulls().Contains(3))

	// Physical columns have no prepared-parameter lineage, but own the same
	// saturating val_int contract. The binder must protect them independently.
	physicalSource := plan2.DeepCopyExpr(source)
	physicalSource.PreparedNumeric = nil
	args := append([]*plan.Expr(nil), expr.GetF().Args...)
	args[0] = physicalSource
	physical, err := plan2.BindFuncExprImplByPlanExpr(cw.proc.Ctx, "export_set", args)
	require.NoError(t, err)
	physicalResult, physicalFree, err := colexec.GetReadonlyResultFromExpression(cw.proc, physical, []*batch.Batch{input})
	require.NoError(t, err)
	defer physicalFree()
	for i, want := range []string{"NYNN", "YYYY", "NNNN"} {
		require.Equal(t, want, physicalResult.GetStringAt(i))
	}
	require.True(t, physicalResult.GetNulls().Contains(3))
}

func TestPreparedExportSetRebuildTypeOwnership(t *testing.T) {
	_, prepared, _, _ := newPreparedExecuteEnvForSQL(t, 392, `select export_set(coalesce((select ?),1.5),?,?,'',4)`)
	t.Cleanup(prepared.Close)
	cached := prepared.PreparePlan.GetDcl().GetPrepare().Plan
	decimal := types.New(types.T_decimal256, 65, 1)
	prepared.exportSetParamTypes = []types.Type{decimal, types.T_float64.ToType(), types.T_float64.ToType()}
	prepared.refreshExportSetParamPositions(cached, 3)
	require.Equal(t, types.T_any, prepared.exportSetParamTypes[0].Oid)
	require.Equal(t, types.T_any, prepared.exportSetParamTypes[1].Oid)
	require.Equal(t, types.T_any, prepared.exportSetParamTypes[2].Oid)
	prepared.refreshExportSetParamPositions(cached, 4)
	require.Len(t, prepared.exportSetParamTypes, 4)
	require.Equal(t, types.T_any, prepared.exportSetParamTypes[0].Oid)
	prepared.exportSetParamTypes = []types.Type{decimal}
	prepared.refreshExportSetParamPositions(nil, 1)
	require.Nil(t, prepared.exportSetParamPositions)
	require.Nil(t, prepared.exportSetParamTypes)
}

func TestPreparedExportSetNullTypeStateBoundaries(t *testing.T) {
	stmt := &PrepareStmt{exportSetParamPositions: []int32{-1, 0, 2, 5}}
	t.Cleanup(stmt.Close)
	decimal := types.New(types.T_decimal256, 65, 1)
	values := []any{plan2.ParamValue{Value: "2.5", SourceType: decimal, HasSourceType: true}, plan2.ParamValue{Value: 7}, nil}
	stmt.applyExportSetNullRuntimeTypes(values)
	require.Equal(t, decimal, stmt.exportSetParamTypes[0])
	require.Equal(t, types.T_any, stmt.exportSetParamTypes[1].Oid)
	require.Equal(t, types.T_text, values[2].(plan2.ParamValue).RuntimeType.Oid, "first NULL remains in the default text domain")
	values[0] = plan2.ParamValue{Value: nil, IsBinaryProtocol: true}
	stmt.applyExportSetNullRuntimeTypes(values)
	current := values[0].(plan2.ParamValue)
	require.Nil(t, current.Value)
	require.True(t, current.IsBinaryProtocol)
	require.Equal(t, decimal, current.RuntimeType)
	values[0] = plan2.ParamValue{Value: "text", SourceType: types.T_text.ToType(), HasSourceType: true}
	stmt.applyExportSetNullRuntimeTypes(values)
	require.Equal(t, decimal, stmt.exportSetParamTypes[0])
	values[0] = nil
	stmt.applyExportSetNullRuntimeTypes(values)
	require.Equal(t, decimal, values[0].(plan2.ParamValue).RuntimeType)
	values[2] = plan2.ParamValue{Value: nil, HasRuntimeType: true, RuntimeType: types.T_blob.ToType()}
	stmt.applyExportSetNullRuntimeTypes(values)
	require.Equal(t, types.T_text, values[2].(plan2.ParamValue).RuntimeType.Oid)
	stmt.Close()
	require.Nil(t, stmt.exportSetParamTypes)
	require.Nil(t, stmt.exportSetParamPositions)
	stmt.applyExportSetNullRuntimeTypes(values)
	require.Nil(t, stmt.exportSetParamTypes)
}
