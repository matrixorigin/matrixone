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

package plan

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// Each position has an independent result oracle; expected values do not use the registry or conversion helpers.
var stringIntegerConsumers = []struct {
	name, expression string
	position         int
	want             [3]string // Results for integer arguments 1, 2, and 3.
}{
	{"left", "left('abcdef',%s)", 1, [3]string{"a", "ab", "abc"}},
	{"right", "right('abcdef',%s)", 1, [3]string{"f", "ef", "def"}},
	{"substring_position", "substring('abcdef',%s)", 1, [3]string{"abcdef", "bcdef", "cdef"}},
	{"substring_length", "substring('abcdef',2,%s)", 2, [3]string{"b", "bc", "bcd"}},
	{"substr_position", "substr('abcdef',%s,2)", 1, [3]string{"ab", "bc", "cd"}},
	{"substr_length", "substr('abcdef',2,%s)", 2, [3]string{"b", "bc", "bcd"}},
	{"mid_position", "mid('abcdef',%s)", 1, [3]string{"abcdef", "bcdef", "cdef"}},
	{"mid_length", "mid('abcdef',2,%s)", 2, [3]string{"b", "bc", "bcd"}},
	{"lpad", "lpad('x',%s,'.')", 1, [3]string{"x", ".x", "..x"}},
	{"rpad", "rpad('x',%s,'.')", 1, [3]string{"x", "x.", "x.."}},
	{"insert_position", "insert('abcdef',%s,1,'X')", 1, [3]string{"Xbcdef", "aXcdef", "abXdef"}},
	{"insert_length", "insert('abcdef',2,%s,'X')", 2, [3]string{"aXcdef", "aXdef", "aXef"}},
	{"locate", "locate('a','baaa',%s)", 2, [3]string{"2", "2", "3"}},
	{"repeat", "repeat('x',%s)", 1, [3]string{"x", "xx", "xxx"}},
	{"space", "space(%s)", 0, [3]string{" ", "  ", "   "}},
	{"elt", "elt(%s,'a','b','c')", 0, [3]string{"a", "b", "c"}},
}

func TestStringIntegerArgumentSourceEvaluation(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, consumer := range stringIntegerConsumers {
		for _, source := range []struct {
			sql     string
			integer int
		}{
			{"2.5", 3}, {"2.5e0", 2}, {"cast(1.5 as double)", 1},
			{"'1.9tail'", 1}, {"cast(2 as unsigned)", 2}, {"b'10'", 2},
			{"if(true,2.5,2.5e0)", 3}, {"case when false then 2.5 else 2.5e0 end", 2},
			{"if(true,1,cast('9223372036854775808' as unsigned))", 1},
			{"null", 0},
		} {
			t.Run(consumer.name+"/"+source.sql, func(t *testing.T) {
				stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "select "+fmt.Sprintf(consumer.expression, source.sql), 1)
				require.NoError(t, err)
				defer stmt.Free()
				ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
				bound, err := NewDefaultBinder(ctx, nil, nil, planpb.Type{}, nil).BindExpr(ast, 0, false)
				require.NoError(t, err)
				require.Equal(t, int32(types.T_int64), bound.GetF().Args[consumer.position].Typ.Id)
				for _, fold := range []bool{false, true} {
					t.Run(fmt.Sprintf("fold=%t", fold), func(t *testing.T) {
						expr := DeepCopyExpr(bound)
						if fold {
							expr, err = ConstantFold(batch.EmptyForConstFoldBatch, expr, proc, false, true)
							require.NoError(t, err)
						}
						result, free, err := colexec.GetReadonlyResultFromExpression(proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
						require.NoError(t, err)
						defer free()
						if source.integer == 0 {
							require.True(t, result.IsConstNull() || result.IsNull(0))
							return
						}
						require.False(t, result.IsConstNull() || result.IsNull(0))
						want := consumer.want[source.integer-1]
						if consumer.name == "locate" {
							require.Equal(t, want, fmt.Sprint(vector.GetFixedAtNoTypeCheck[int64](result, 0)))
						} else {
							require.Equal(t, want, result.GetStringAt(0))
						}
					})
				}
			})
		}
	}
}

func TestStringIntegerArgumentOverflow(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, consumer := range stringIntegerConsumers {
		for _, source := range []string{"cast('9223372036854775808' as unsigned)", "'-9223372036854775809'", "cast('9223372036854775807.5' as decimal(38,1))"} {
			t.Run(consumer.name+"/"+source, func(t *testing.T) {
				stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "select "+fmt.Sprintf(consumer.expression, source), 1)
				require.NoError(t, err)
				defer stmt.Free()
				ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
				bound, err := NewDefaultBinder(ctx, nil, nil, planpb.Type{}, nil).BindExpr(ast, 0, false)
				require.NoError(t, err)
				_, free, err := colexec.GetReadonlyResultFromExpression(proc, bound, []*batch.Batch{batch.EmptyForConstFoldBatch})
				if free != nil {
					defer free()
				}
				require.ErrorContains(t, err, "out of range")
			})
		}
	}
}

func TestStringIntegerArgumentPreparedReuse(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, consumer := range stringIntegerConsumers {
		t.Run(consumer.name, func(t *testing.T) {
			sql := "select " + fmt.Sprintf(strings.ReplaceAll(consumer.expression, "'", "\""), "?")
			prepared, err := runOneStmt(NewMockOptimizer(false), t, "prepare string_integer from '"+sql+"'")
			require.NoError(t, err)
			original := prepared.GetDcl().GetPrepare().Plan
			snapshot := proto.Clone(original)
			require.Equal(t, []int32{0}, PreparedPlanNumericFallbackParamPositions(original))
			for _, source := range []struct {
				value    any
				typ      types.T
				integer  int
				overflow bool
			}{
				{nil, types.T_float64, 0, false}, {"2.5", types.T_float64, 2, false},
				{"1.9tail", types.T_varchar, 1, false}, {"9223372036854775808", types.T_uint64, 0, true},
				{"3", types.T_int64, 3, false},
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
					if source.integer == 0 {
						require.True(t, result.IsConstNull() || result.IsNull(0))
						return
					}
					require.False(t, result.IsConstNull() || result.IsNull(0))
					want := consumer.want[source.integer-1]
					if consumer.name == "locate" {
						require.Equal(t, want, fmt.Sprint(vector.GetFixedAtNoTypeCheck[int64](result, 0)))
					} else {
						require.Equal(t, want, result.GetStringAt(0))
					}
				})
				require.True(t, proto.Equal(snapshot, original), "specialization must not mutate the cached template")
			}
		})
	}
}

func TestSpaceLegacyExecutionIdentities(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, tc := range []struct {
		overload int32
		source   types.T
	}{
		{0, types.T_uint64}, {2, types.T_decimal64}, {3, types.T_decimal128}, {4, types.T_decimal256},
	} {
		t.Run(tc.source.String(), func(t *testing.T) {
			_, err := function.GetFunctionByNameWithOverload(ctx, "space", []types.Type{tc.source.ToType()}, tc.overload)
			require.ErrorContains(t, err, "legacy execution only")
			source, err := appendCastBeforeExpr(ctx, makePlan2Int64ConstExprWithType(2), planpb.Type{Id: int32(tc.source), Width: 20})
			require.NoError(t, err)
			expr := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{Obj: int64(function.SPACE)<<32 | int64(tc.overload), ObjName: "space"},
				Args: []*Expr{source},
			}}}
			data, err := expr.Marshal()
			require.NoError(t, err)
			restored := new(planpb.Expr)
			require.NoError(t, restored.Unmarshal(data))
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, restored, []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			defer free()
			require.Equal(t, "  ", result.GetStringAt(0))
		})
	}
}

func TestStringIntegerArgumentColumnBinding(t *testing.T) {
	ctx := context.Background()
	for _, consumer := range stringIntegerConsumers {
		t.Run(consumer.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "select "+fmt.Sprintf(consumer.expression, "2"), 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			bound, err := NewDefaultBinder(ctx, nil, nil, planpb.Type{}, nil).BindExpr(ast, 0, false)
			require.NoError(t, err)
			for _, source := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256, types.T_float64, types.T_uint64, types.T_varchar} {
				column := &planpb.Expr{Typ: planpb.Type{Id: int32(source), Width: 20, Scale: 1}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
				args := append([]*Expr(nil), bound.GetF().Args...)
				args[consumer.position] = column
				rebound, err := BindFuncExprImplByPlanExpr(ctx, bound.GetF().Func.ObjName, args)
				require.NoError(t, err)
				cast := rebound.GetF().Args[consumer.position]
				require.Equal(t, int32(types.T_int64), cast.Typ.Id)
				fid, overload := function.DecodeOverloadID(cast.GetF().Func.Obj)
				require.Equal(t, int32(function.CAST), fid)
				require.Equal(t, function.IntegerArgumentCastOverload, overload)
				require.Equal(t, column, cast.GetF().Args[0])
				require.Same(t, column, args[consumer.position])
			}
		})
	}
}
