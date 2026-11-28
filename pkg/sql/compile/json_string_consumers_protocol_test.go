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

package compile

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func bindJSONStringConsumerExpr(t *testing.T, name string) *planpb.Expr {
	t.Helper()
	jsonColumn := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_json)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}
	stringConstant := func(value string) *planpb.Expr {
		return plan2.MakePlan2StringConstExprWithType(value)
	}

	if name == "binary_cast" {
		target := planpb.Type{Id: int32(types.T_json)}
		expr, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "cast", []*planpb.Expr{
			{Typ: planpb.Type{Id: int32(types.T_blob)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
			{Typ: target, Expr: &planpb.Expr_T{T: &planpb.TargetType{}}},
		})
		require.NoError(t, err)
		return expr
	}
	var args []*planpb.Expr
	switch name {
	case "json_row":
		args = []*planpb.Expr{jsonColumn}
	case "json_schema_valid", "json_schema_validation_report":
		args = []*planpb.Expr{stringConstant(`{"type":"string","maxLength":95}`), jsonColumn}
	case "concat":
		args = []*planpb.Expr{jsonColumn, stringConstant("!")}
	case "concat_ws":
		args = []*planpb.Expr{stringConstant("|"), jsonColumn, stringConstant("!")}
	case "elt":
		args = []*planpb.Expr{plan2.MakePlan2Int64ConstExprWithType(1), jsonColumn, stringConstant("!")}
	default:
		t.Fatalf("unsupported JSON string consumer %q", name)
	}

	expr, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), name, args)
	require.NoError(t, err)
	_, overloadID := function.DecodeOverloadID(expr.GetF().Func.Obj)
	wantOverload := int32(0)
	if name == "elt" {
		wantOverload = 1
	}
	if name == "concat" || name == "concat_ws" || name == "elt" {
		require.Equal(t, wantOverload, overloadID)
	}
	return expr
}

// Existing stored opaque values need no new CAST/ELT producer in the plan.
func TestJSONOpaqueExistingConsumerFences(t *testing.T) {
	for _, name := range []string{"json_row", "json_schema_valid", "json_schema_validation_report"} {
		t.Run(name, func(t *testing.T) {
			expr := bindJSONStringConsumerExpr(t, name)
			features, err := planpb.RequiredRemoteExpressionFeatures(expr)
			require.NoError(t, err)
			require.True(t, features.JSONOpaqueTextContracts)
			floor, err := plan2.RequiredPersistedExpressionProtocolVersion(expr)
			require.NoError(t, err)
			require.Equal(t, defines.MORPCVersion129, floor)
		})
	}
}
