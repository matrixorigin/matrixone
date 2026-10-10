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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func sumAvgTestExpression(t *testing.T, p *planpb.Plan, name string) *planpb.Expr {
	t.Helper()
	if expr := findPlanFunctionExpr(p, name); expr != nil {
		return expr
	}
	for _, node := range p.GetQuery().Nodes {
		for _, expr := range node.WinSpecList {
			if window := expr.GetW(); window != nil && window.WindowFunc.GetF().GetFunc().GetObjName() == name {
				return window.WindowFunc
			}
		}
	}
	t.Fatalf("plan contains no %s aggregate", name)
	return nil
}

func TestSumAvgSQLBindingCoercesNumericOperands(t *testing.T) {
	for _, name := range []string{"sum", "avg"} {
		for _, source := range []types.Type{
			types.T_varchar.ToType(), types.T_date.ToType(),
			types.T_time.ToTypeWithScale(6), types.T_datetime.ToTypeWithScale(6),
			types.T_timestamp.ToTypeWithScale(6),
		} {
			for _, shape := range []struct {
				name, sql string
			}{
				{"ordinary", "select " + name + "(n_name) from nation"},
				{"distinct", "select " + name + "(distinct n_name) from nation"},
				{"grouped", "select " + name + "(n_name) from nation group by n_regionkey"},
				{"window", "select " + name + "(n_name) over (order by n_nationkey rows between unbounded preceding and current row) from nation"},
			} {
				t.Run(name+"/"+source.Oid.String()+"/"+shape.name, func(t *testing.T) {
					mock := NewMockOptimizer(false, newPlanTestProcess(t))
					table := DeepCopyTableDef(mock.ctxt.tables["nation"], true)
					table.Cols[1].Typ = makePlan2Type(&source)
					mock.ctxt.tables["nation"] = table
					p, err := runOneStmt(mock, t, shape.sql)
					require.NoError(t, err)
					agg := sumAvgTestExpression(t, p, name)
					require.Len(t, agg.GetF().Args, 1)
					cast := agg.GetF().Args[0]
					if shape.name == "distinct" {
						// DISTINCT optimization moves the numeric cast onto the deduplication key.
						require.NotNil(t, cast.GetCol())
						keyCast := findPlanFunctionExpr(p, "cast")
						require.NotNil(t, keyCast)
						require.Equal(t, cast.Typ, keyCast.Typ)
						cast = keyCast
					}
					require.True(t, isCastOverload(cast, 0))
					require.Equal(t, int32(source.Oid), cast.GetF().Args[0].Typ.Id)
					if source.Oid == types.T_varchar {
						require.Equal(t, int32(types.T_float64), cast.Typ.Id)
						require.Equal(t, int32(types.T_float64), agg.Typ.Id)
					} else {
						require.Equal(t, int32(types.T_decimal128), cast.Typ.Id)
						require.Equal(t, int32(38), cast.Typ.Width)
						require.Equal(t, source.Scale, cast.Typ.Scale)
						require.Equal(t, int32(types.T_decimal256), agg.Typ.Id)
						width, scale := int32(60), source.Scale
						if name == "avg" {
							width, scale = 42, scale+4
						}
						require.Equal(t, width, agg.Typ.Width)
						require.Equal(t, scale, agg.Typ.Scale)
					}
				})
			}
		}
	}
}

func TestSumAvgDistinctRewriteKeepsNumericCast(t *testing.T) {
	for _, name := range []string{"sum", "avg"} {
		for _, source := range []types.Type{types.T_varchar.ToType(), types.T_time.ToTypeWithScale(6)} {
			t.Run(name+"/"+source.Oid.String(), func(t *testing.T) {
				column := distinctAggTestCol(source.Oid, 1, 1, 100)
				column.Typ = makePlan2Type(&source)
				bound, err := BindFuncExprImplByPlanExpr(t.Context(), name, []*planpb.Expr{column})
				require.NoError(t, err)
				bound.GetF().Func.Obj = int64(uint64(bound.GetF().Func.Obj) | function.Distinct)
				builder, outer := newDistinctAggTestBuilder(1_000, 10, 100, []*planpb.Expr{bound})
				require.NoError(t, builder.optimizeDistinctAgg(1))
				require.Len(t, builder.qry.Nodes, 3)
				inner := builder.qry.Nodes[2]
				require.Len(t, inner.GroupBy, 2)
				key := inner.GroupBy[1]
				require.True(t, isCastOverload(key, 0), "DISTINCT must compare converted numbers, not source values")
				require.Equal(t, int32(source.Oid), key.GetF().Args[0].Typ.Id)
				require.Equal(t, key.Typ, outer.AggList[0].GetF().Args[0].Typ)
				require.Zero(t, uint64(outer.AggList[0].GetF().Func.Obj)&function.Distinct)
			})
		}
	}
}

func TestSumAvgPreparedExecutionBinaryNumericLiteral(t *testing.T) {
	for _, name := range []string{"sum", "avg"} {
		t.Run(name, func(t *testing.T) {
			proc := newPlanTestProcess(t)
			mock := NewMockOptimizer(false, proc)
			params := vector.NewVec(types.T_text.ToType())
			t.Cleanup(func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) })
			require.NoError(t, vector.AppendBytes(params, []byte("1"), false, proc.Mp()))
			proc.SetPrepareParams(params)
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
				"select cast("+name+"(x'20000000000001') as decimal(38,0)) from nation where n_nationkey <= ?", 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)
			bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt,
				[]PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()}},
				[]any{ParamValue{Value: "1", SourceType: types.T_int64.ToType(), HasSourceType: true}})
			require.NoError(t, err)
			agg := sumAvgTestExpression(t, bound.Plan, name)
			require.Equal(t, int32(types.T_uint64), agg.GetF().Args[0].Typ.Id)
			folded, err := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(agg.GetF().Args[0]), proc, false, true)
			require.NoError(t, err)
			require.Equal(t, uint64(9007199254740993), folded.GetLit().GetU64Val())
		})
	}
}

func TestSumAvgBinaryNumericLiteralBinding(t *testing.T) {
	for _, name := range []string{"sum", "avg"} {
		for _, operand := range []struct {
			sql    string
			domain types.T
		}{
			{"x'20000000000000'", types.T_uint64},
			{"x'20000000000001'", types.T_uint64},
			{"x'ffffffffffffffff'", types.T_uint64},
			{"b'100000000000000000000000000000000000000000000000000001'", types.T_uint64},
			{"cast(x'20000000000001' as unsigned)", types.T_uint64},
			{"cast('12' as binary)", types.T_float64},
			{"_binary '12'", types.T_float64},
		} {
			for _, window := range []string{"", " over ()"} {
				t.Run(name+"/"+operand.sql+window, func(t *testing.T) {
					mock := NewMockOptimizer(false, newPlanTestProcess(t))
					p, err := runOneStmt(mock, t, "select "+name+"("+operand.sql+")"+window+" from nation")
					require.NoError(t, err)
					agg := sumAvgTestExpression(t, p, name)
					require.Equal(t, int32(operand.domain), agg.GetF().Args[0].Typ.Id)
					if operand.domain == types.T_uint64 {
						require.NotEqual(t, int32(types.T_float64), agg.Typ.Id)
					}
				})
			}
		}
		for _, sql := range []string{
			"select " + name + "(0x20000000000001)",
			"select cast(" + name + "(0x20000000000001) as decimal(38,0)) from nation where n_nationkey <= ?",
			"select cast(" + name + "(x''20000000000001'') as decimal(38,0)) from nation where n_nationkey <= ?",
		} {
			t.Run(name+"/prepared/"+sql, func(t *testing.T) {
				prepared := buildPreparedAggregatePlan(t, sql)
				agg := sumAvgTestExpression(t, prepared.Plan, name)
				require.Equal(t, int32(types.T_uint64), agg.GetF().Args[0].Typ.Id)
				if len(prepared.ParamTypes) > 0 {
					for _, value := range []int64{1, 2} {
						filled, _, err := FillValuesOfParamsInPlanWithSpecialization(t.Context(), prepared.Plan, []any{value})
						require.NoError(t, err)
						bound := sumAvgTestExpression(t, filled, name)
						require.Equal(t, int32(types.T_uint64), bound.GetF().Args[0].Typ.Id)
						folded, err := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(bound.GetF().Args[0]), newPlanTestProcess(t), false, true)
						require.NoError(t, err)
						require.Equal(t, uint64(9007199254740993), folded.GetLit().GetU64Val())
					}
				}
			})
		}
	}
}
