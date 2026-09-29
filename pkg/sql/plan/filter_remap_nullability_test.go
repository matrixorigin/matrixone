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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestFilterAndAssertRemapRefreshesPredicateNullability(t *testing.T) {
	for _, nodeType := range []planpb.Node_NodeType{planpb.Node_FILTER, planpb.Node_ASSERT} {
		for _, nullable := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/nullable_%t", nodeType, nullable), func(t *testing.T) {
				builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, false)
				tag := builder.GenNewBindTag()
				childType := planpb.Type{Id: int32(types.T_int64), NotNullable: !nullable}
				staleType := childType
				staleType.NotNullable = true
				abs, err := BindFuncExprImplByPlanExpr(context.Background(), "abs", []*planpb.Expr{
					GetColExpr(staleType, tag, 0),
				})
				require.NoError(t, err)
				greater, err := BindFuncExprImplByPlanExpr(context.Background(), ">", []*planpb.Expr{
					abs, makePlan2Int64ConstExprWithType(1),
				})
				require.NoError(t, err)
				isNull, err := BindFuncExprImplByPlanExpr(context.Background(), "isnull", []*planpb.Expr{
					GetColExpr(staleType, tag, 0),
				})
				require.NoError(t, err)

				builder.qry.Nodes = []*planpb.Node{
					{
						NodeId: 0, NodeType: planpb.Node_TABLE_SCAN,
						BindingTags: []int32{tag},
						TableDef:    &planpb.TableDef{Cols: []*planpb.ColDef{{Name: "x", Typ: childType}}},
					},
					{
						NodeId: 1, NodeType: nodeType, Children: []int32{0},
						FilterList: []*planpb.Expr{greater, isNull},
					},
				}
				_, err = builder.remapAllColRefs(1, 0,
					make(map[[2]int32]int), make(map[[2]int32]bool), make(map[[2]int32]int))
				require.NoError(t, err)

				actual := builder.qry.Nodes[1].FilterList[0]
				actualAbs := actual.GetF().Args[0]
				actualCol := actualAbs.GetF().Args[0]
				require.Equal(t, int32(0), actualCol.GetCol().RelPos)
				require.Equal(t, childType.NotNullable, actualCol.Typ.NotNullable)
				require.Equal(t, childType.NotNullable, actualAbs.Typ.NotNullable)
				require.Equal(t, childType.NotNullable, actual.Typ.NotNullable)
				require.True(t, builder.qry.Nodes[1].FilterList[1].Typ.NotNullable,
					"IS NULL must remain non-null even for a nullable input")
			})
		}
	}
}

func BenchmarkRefreshExprNullabilityFromInputs(b *testing.B) {
	abs, err := function.GetFunctionByName(context.Background(), "abs", []types.Type{types.T_int64.ToType()})
	if err != nil {
		b.Fatal(err)
	}
	for _, depth := range []int{32, 128, 512, 1024} {
		b.Run(fmt.Sprintf("depth_%d", depth), func(b *testing.B) {
			typ := planpb.Type{Id: int32(types.T_int64), NotNullable: true}
			expr := GetColExpr(typ, 0, 0)
			for range depth {
				expr = &planpb.Expr{Typ: typ, Expr: &planpb.Expr_F{F: &planpb.Function{
					Func: &planpb.ObjectRef{Obj: abs.GetEncodedOverloadID(), ObjName: "abs"},
					Args: []*planpb.Expr{expr},
				}}}
			}
			input := []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_int64)}}}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				refreshExprNullabilityFromInputs(expr, input)
			}
		})
	}
}
