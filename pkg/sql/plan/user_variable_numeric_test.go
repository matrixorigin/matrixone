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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func collectVarEffectiveTypes(expr *planpb.Expr, inherited planpb.Type, result map[string]planpb.Type) {
	if expr == nil {
		return
	}
	if variable := expr.GetV(); variable != nil {
		typ := inherited
		if typ.Id == 0 {
			typ = expr.Typ
		}
		result[variable.Name] = typ
		return
	}
	if function := expr.GetF(); function != nil {
		childType := inherited
		if function.Func != nil && function.Func.ObjName == "cast" {
			childType = expr.Typ
		} else if childType.Id == 0 && types.T(expr.Typ.Id).ToType().IsNumeric() {
			childType = expr.Typ
		}
		for _, arg := range function.Args {
			collectVarEffectiveTypes(arg, childType, result)
		}
		return
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			collectVarEffectiveTypes(item, inherited, result)
		}
	}
}

func userVariableEffectiveTypes(plan *planpb.Plan) map[string]planpb.Type {
	result := make(map[string]planpb.Type)
	for _, node := range plan.GetQuery().GetNodes() {
		for _, exprs := range [][]*planpb.Expr{node.ProjectList, node.AggList, node.GroupBy, node.WinSpecList} {
			for _, expr := range exprs {
				collectVarEffectiveTypes(expr, planpb.Type{}, result)
			}
		}
	}
	return result
}

func TestUserVariablesUseNumericContextInArithmetic(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(false), t, "select @int_var + @float_var")
	require.NoError(t, err)

	varTypes := userVariableEffectiveTypes(logicPlan)
	require.Contains(t, varTypes, "int_var")
	require.Contains(t, varTypes, "float_var")
	require.True(t, types.T(varTypes["int_var"].Id).ToType().IsNumeric())
	require.True(t, types.T(varTypes["float_var"].Id).ToType().IsNumeric())
}

func TestPreparedParametersUseDefaultNumericContextInArithmetic(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(false), t, "prepare ps_count from 'select ? + ? as sum_val'")
	require.NoError(t, err)
	prepare := logicPlan.GetDcl().GetPrepare()
	require.NotNil(t, prepare)

	paramTypes := preparedEffectiveParamTypes(t, prepare)
	require.Len(t, paramTypes, 2)
	require.True(t, types.T(paramTypes[0].Id).ToType().IsNumeric())
	require.True(t, types.T(paramTypes[1].Id).ToType().IsNumeric())
}
