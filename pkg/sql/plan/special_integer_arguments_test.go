// Copyright 2026 Matrix Origin
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
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestSpecialIntegerArgumentEvaluation(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		sql, want      string
		null, overflow bool
	}{
		{"format(2.5,0)", "3", false, false},
		{"format(2.5e0,0)", "2", false, false},
		{"format('2.5',0)", "2", false, false},
		{"format(1.125,1.5)", "1.13", false, false},
		{"format(1.125,2.5e0)", "1.13", false, false},
		{"format(1.125,cast(1.5 as double))", "1.1", false, false},
		{"format(1.125,'1.9tail')", "1.1", false, false},
		{"format(1234.5,2,'de_DE')", "1.234,50", false, false},
		{"format(2.5,-1,null)", "3", false, false},
		{"format(1,9223372036854775807)", "1.000000000000000000000000000000", false, false},
		{"format(null,cast('9223372036854775808' as unsigned))", "", false, true},
		{"format(1,null)", "", true, false},
		{"makedate(0,1)", "2000-01-01", false, false},
		{"makedate(69,1)", "2069-01-01", false, false},
		{"makedate(70,1)", "1970-01-01", false, false},
		{"makedate(99,1)", "1999-01-01", false, false},
		{"makedate(100,1)", "0100-01-01", false, false},
		{"makedate(2024,366)", "2024-12-31", false, false},
		{"makedate(2024,367)", "2025-01-01", false, false},
		{"makedate(9999,365)", "9999-12-31", false, false},
		{"makedate(9999,366)", "", true, false},
		{"makedate(10000,1)", "", true, false},
		{"makedate(-1,1)", "", true, false},
		{"makedate(2024,0)", "", true, false},
		{"makedate(2024,4294967297)", "", true, false},
		{"makedate(2024,9223372036854775807)", "", true, false},
		{"makedate(2023.5,1.5)", "2024-01-02", false, false},
		{"makedate(2024,'1.9')", "2024-01-01", false, false},
		{"makedate(2024,cast('9223372036854775808' as unsigned))", "", false, true},
		{"makedate(null,1)", "", true, false},
		{"cast(maketime(1.0,1,cast('1.5' as binary)) as varchar)", "01:01:01.500000", false, false},
		{"cast(maketime(1.0,1,cast(null as binary)) as varchar)", "", true, false},
		{"cast(maketime(1.0,1,X'01') as varchar)", "01:01:01", false, false},
		{"cast(maketime(2.5e0,1.5,3.125) as varchar)", "02:02:03.125", false, false},
		{"cast(maketime(cast(2.5 as double),2.5,3.125) as varchar)", "02:03:03.125", false, false},
		{"cast(maketime(-12.5,59,59.9999996) as varchar)", "-14:00:00.000000", false, false},
		{"cast(maketime(9223372036854775807,0,0) as varchar)", "838:59:59", false, false},
		{"cast(maketime(1,0,cast('18446744073709551615' as unsigned)) as varchar)", "", true, false},
		{"cast(maketime(null,0,1.25) as varchar)", "", true, false},
		{"cast(maketime(1,60,0) as varchar)", "", true, false},
		{"cast(maketime(cast('9223372036854775808' as unsigned),0,0) as varchar)", "", false, true},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(proc.Ctx, dialect.MYSQL, "select "+tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			bound, err := NewDefaultBinder(proc.Ctx, nil, nil, planpb.Type{}, nil).BindExpr(ast, 0, false)
			require.NoError(t, err)
			for _, fold := range []bool{false, true} {
				t.Run(fmt.Sprintf("fold=%t", fold), func(t *testing.T) {
					expr := DeepCopyExpr(bound)
					if fold {
						expr, err = ConstantFold(batch.EmptyForConstFoldBatch, expr, proc, false, true)
						if err != nil {
							require.True(t, tc.overflow)
							require.ErrorContains(t, err, "out of range")
							return
						}
					}
					result, free, err := colexec.GetReadonlyResultFromExpression(proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
					if free != nil {
						defer free()
					}
					if tc.overflow {
						require.ErrorContains(t, err, "out of range")
						return
					}
					require.NoError(t, err)
					require.Equal(t, tc.null, result.IsConstNull() || result.IsNull(0))
					if !tc.null {
						require.Equal(t, tc.want, result.GetStringAt(0))
					}
				})
			}
		})
	}
}

func TestMakeTimeBinarySecondsColumns(t *testing.T) {
	proc := testutil.NewProcess(t)
	stmt, err := parsers.ParseOne(proc.Ctx, dialect.MYSQL, "select cast(maketime(1.0,1,s) as varchar)", 1)
	require.NoError(t, err)
	defer stmt.Free()
	ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
	for _, oid := range []types.T{types.T_binary, types.T_varbinary, types.T_blob} {
		t.Run(oid.String(), func(t *testing.T) {
			typ := oid.ToType()
			expr, err := NewGeneratedColBinder(proc.Ctx, []string{"s"}, []planpb.Type{makePlan2Type(&typ)}).BindExpr(ast, 0, false)
			require.NoError(t, err)
			input := batch.NewWithSize(1)
			defer input.Clean(proc.Mp())
			input.Vecs[0] = vector.NewVec(typ)
			for _, seconds := range []string{"1.5", "59.5", ""} {
				require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte(seconds), seconds == "", proc.Mp()))
			}
			input.SetRowCount(3)
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, expr, []*batch.Batch{input})
			if free != nil {
				defer free()
			}
			require.NoError(t, err)
			require.Equal(t, "01:01:01.500000", result.GetStringAt(0))
			require.Equal(t, "01:01:59.500000", result.GetStringAt(1))
			require.True(t, result.IsNull(2))
		})
	}
}

func TestSpecialIntegerArgumentPreparedReuse(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		sql  string
		want [3]string
	}{
		{"format(1.125,?)", [3]string{"1.1", "1.13", "1.125"}},
		{"makedate(2024,?)", [3]string{"2024-01-01", "2024-01-02", "2024-01-03"}},
		{"makedate(?,1)", [3]string{"2001-01-01", "2002-01-01", "2003-01-01"}},
		{"cast(maketime(?,0,1.25) as varchar)", [3]string{"01:00:01.25", "02:00:01.25", "03:00:01.25"}},
		{"cast(maketime(1,?,1.25) as varchar)", [3]string{"01:01:01.25", "01:02:01.25", "01:03:01.25"}},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t, "prepare special_integer from 'select "+tc.sql+"'")
			require.NoError(t, err)
			original := prepared.GetDcl().GetPrepare().Plan
			snapshot := proto.Clone(original)
			require.Equal(t, []int32{0}, PreparedPlanNumericFallbackParamPositions(original))
			for _, source := range []struct {
				value    any
				typ      types.T
				index    int
				overflow bool
			}{
				{nil, types.T_float64, -1, false}, {"2.5", types.T_float64, 1, false},
				{"1.9tail", types.T_varchar, 0, false}, {"9223372036854775808", types.T_uint64, 0, true},
				{"3", types.T_int64, 2, false},
			} {
				t.Run(fmt.Sprintf("%s/%v", source.typ, source.value), func(t *testing.T) {
					bound, changed, err := FillValuesOfParamsInPlanWithPreparedNumericOverload(proc.Ctx, original, []any{ParamValue{Value: source.value, IsBinaryProtocol: true, RuntimeType: source.typ.ToType(), HasRuntimeType: true}})
					require.NoError(t, err)
					require.True(t, changed)
					params := vector.NewVec(types.T_text.ToType())
					defer func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) }()
					require.NoError(t, vector.AppendBytes(params, []byte(fmt.Sprint(source.value)), source.value == nil, proc.Mp()))
					proc.SetPrepareParams(params)
					query := bound.GetQuery()
					result, free, err := colexec.GetReadonlyResultFromExpression(proc, query.Nodes[query.Steps[0]].ProjectList[0], []*batch.Batch{batch.EmptyForConstFoldBatch})
					if free != nil {
						defer free()
					}
					if source.overflow {
						require.ErrorContains(t, err, "out of range")
						return
					}
					require.NoError(t, err)
					if source.index < 0 {
						require.True(t, result.IsConstNull() || result.IsNull(0))
						return
					}
					require.Equal(t, tc.want[source.index], result.GetStringAt(0))
				})
				require.True(t, proto.Equal(snapshot, original))
			}
		})
	}
}

func TestPersistedFormatPrecisionCompatibility(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct{ precision, want string }{
		{"1.5", "1.1"}, {"if(a>0,1.5,2.5e0)", "1.1"},
		{"case when a>0 then cast(1.5 as double) else 2.5 end", "1.1"},
		{"nullif(1.5,a)", "1.1"}, {"cast(1.5 as signed)", "1.12"},
	} {
		t.Run(tc.precision, func(t *testing.T) {
			stmt, err := parsers.ParseOne(proc.Ctx, dialect.MYSQL, "select format(a,"+tc.precision+")", 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			typ := types.New(types.T_decimal64, 10, 3)
			expr, err := NewGeneratedColBinder(proc.Ctx, []string{"a"}, []planpb.Type{makePlan2Type(&typ)}).BindExpr(ast, 0, false)
			require.NoError(t, err)
			require.NoError(t, preservePersistedFormatCompatibility(proc.Ctx, expr))
			features, err := planpb.RequiredRemoteExpressionFeatures(expr)
			require.NoError(t, err)
			require.False(t, features.SpecialIntegerConsumers)
			require.False(t, features.IntegerParameterCoercion)
			require.False(t, features.FormatNumericArguments)
			data, err := expr.Marshal()
			require.NoError(t, err)
			restored := new(planpb.Expr)
			require.NoError(t, restored.Unmarshal(data))
			input := batch.NewWithSize(1)
			defer input.Clean(proc.Mp())
			input.Vecs[0] = vector.NewVec(typ)
			require.NoError(t, vector.AppendFixed(input.Vecs[0], types.Decimal64(1125), false, proc.Mp()))
			input.SetRowCount(1)
			executor, err := colexec.NewExpressionExecutor(proc, restored)
			require.NoError(t, err)
			defer executor.Free()
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, result.GetStringAt(0))
		})
	}
	// A nested function owns its own argument contract, not FORMAT's bridge.
	stmt, err := parsers.ParseOne(proc.Ctx, dialect.MYSQL, "select format('1.125',substring_index('2.9','. ',2.5))", 1)
	require.NoError(t, err)
	defer stmt.Free()
	expr, err := NewDefaultBinder(proc.Ctx, nil, nil, planpb.Type{}, nil).BindExpr(stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr, 0, false)
	require.NoError(t, err)
	require.NoError(t, preservePersistedFormatCompatibility(proc.Ctx, expr))
	version, err := RequiredPersistedExpressionProtocolVersion(expr)
	require.NoError(t, err)
	require.Equal(t, int64(defines.MORPCVersion85), version)
	catalogExpr, err := NewDefaultBinder(proc.Ctx, nil, nil, planpb.Type{}, nil).bindPersistedExpr(stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr, 0, false)
	require.NoError(t, err)
	version, err = RequiredPersistedExpressionProtocolVersion(catalogExpr)
	require.NoError(t, err)
	require.Equal(t, int64(defines.MORPCVersion85), version)
}

func TestSpecialIntegerLegacyExecutionIdentities(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		name         string
		id, overload int32
		args         []*planpb.Expr
		result       types.T
		want         string
	}{
		{"format", function.FORMAT, 0, []*planpb.Expr{makePlan2StringConstExprWithType("2.5"), makePlan2StringConstExprWithType("0")}, types.T_varchar, "2"},
		{"makedate", function.MAKEDATE, 0, []*planpb.Expr{makePlan2StringConstExprWithType("2024"), makePlan2StringConstExprWithType("1.9")}, types.T_varchar, "2024-01-01"},
		{"maketime", function.MAKETIME, 2, []*planpb.Expr{makePlan2Int64ConstExprWithType(12), makePlan2Int64ConstExprWithType(1), makePlan2Int64ConstExprWithType(2)}, types.T_time, "12:01:02"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.id == function.MAKETIME {
				for i := range tc.args {
					var err error
					tc.args[i], err = appendCastBeforeExpr(proc.Ctx, tc.args[i], planpb.Type{Id: int32(types.T_float64)})
					require.NoError(t, err)
				}
			}
			expr := &planpb.Expr{Typ: planpb.Type{Id: int32(tc.result)}, Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(tc.id, tc.overload), ObjName: tc.name}, Args: tc.args,
			}}}
			var err error
			expr, err = appendCastBeforeExpr(proc.Ctx, expr, planpb.Type{Id: int32(types.T_varchar), Width: 64})
			require.NoError(t, err)
			data, err := expr.Marshal()
			require.NoError(t, err)
			restored := new(planpb.Expr)
			require.NoError(t, restored.Unmarshal(data))
			features, err := planpb.RequiredRemoteExpressionFeatures(restored)
			require.NoError(t, err)
			require.False(t, features.SpecialIntegerConsumers)
			require.False(t, features.IntegerParameterCoercion)
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, restored, []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			defer free()
			require.Equal(t, tc.want, result.GetStringAt(0))
		})
	}
}
