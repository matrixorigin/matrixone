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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
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
	require.True(t, plan2.PreparedPlanHasPercentileParams(prepared))

	prepared.GetQuery().Nodes[0].AggList[0].GetF().Args[1] =
		plan2.MakePlan2Float64ConstExprWithType(0.5)
	require.True(t, shouldCachePrepareCompile(prepared))
}

func TestPreparedPercentileDisablesMixedRuntimeSpecializationCache(t *testing.T) {
	ses, prepared, cw, execCtx := newPreparedExecuteEnvForSQL(t, 29464,
		"select percentile_disc(?) within group (order by n), abs(?) from (select 1 as n union all select 3) t")
	t.Cleanup(func() { cw.proc.SetPrepareParams(nil); prepared.Close() })
	prepared.params = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(prepared.params, []byte("0.25"), false, cw.proc.Mp()))
	require.NoError(t, vector.AppendBytes(prepared.params, []byte("-2"), false, cw.proc.Mp()))
	prepared.ParamTypes = []byte{byte(defines.MYSQL_TYPE_DOUBLE), 0, byte(defines.MYSQL_TYPE_LONGLONG), 0}
	for _, percentile := range []string{"0.25", "0.75"} {
		require.NoError(t, vector.SetStringAt(prepared.params, 0, percentile, cw.proc.Mp()))
		comp, p, stmt, _, owned, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepared.Name)
		if owned && stmt != nil {
			stmt.Free()
		}
		require.NoError(t, err)
		require.NotNil(t, p)
		require.True(t, prepared.hasPercentileParams)
		require.Nil(t, comp, "a value-bound percentile must not reuse a compile")
		require.Nil(t, cw.runtimeCacheTarget, "mixed ABS specialization must not admit the percentile compile")
	}
}
