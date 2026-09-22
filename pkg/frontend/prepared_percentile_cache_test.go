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

package frontend

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestShouldCachePrepareCompileRejectsPercentileParameter(t *testing.T) {
	value := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}
	percentile := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_float64)},
		Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
	}
	prepared := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
		Nodes: []*plan.Node{{AggList: []*plan.Expr{{
			Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: plan2.NamePercentileDisc},
				Args: []*plan.Expr{value, percentile},
			}},
		}}}},
	}}}
	require.False(t, shouldCachePrepareCompile(prepared))
	require.False(t, shouldCachePreparedRuntimeSpecialization(prepared))

	prepared.GetQuery().Nodes[0].AggList[0].GetF().Args[1] =
		plan2.MakePlan2Float64ConstExprWithType(0.5)
	require.True(t, shouldCachePrepareCompile(prepared))
	require.True(t, shouldCachePreparedRuntimeSpecialization(prepared))
}

func TestPreparedPercentileDisablesMixedRuntimeSpecializationCache(t *testing.T) {
	percentile := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_float64)},
		Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
	}
	runtimeNumericMarker := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_any)},
		Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 1}},
	}
	prepared := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
		Nodes: []*plan.Node{{
			ProjectList: []*plan.Expr{{
				Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "abs"},
					Args: []*plan.Expr{runtimeNumericMarker},
				}},
			}},
			AggList: []*plan.Expr{{
				Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: plan2.NamePercentileDisc},
					Args: []*plan.Expr{
						{Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
						percentile,
					},
				}},
			}},
		}},
	}}}
	require.False(t, shouldCachePreparedRuntimeSpecialization(prepared))
}
