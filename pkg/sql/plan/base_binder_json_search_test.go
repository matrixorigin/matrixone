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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func jsonSearchBindArgs() []*planpb.Expr {
	return []*planpb.Expr{
		makePlan2StringConstExprWithType(`{"a":"x"}`),
		makePlan2StringConstExprWithType("one"),
		makePlan2StringConstExprWithType("x"),
	}
}

func makeJSONSearchStringNullExpr() *planpb.Expr {
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
			Isnull: true,
			Value:  &planpb.Literal_Sval{Sval: ""},
		}},
	}
}

func TestBindJSONSearchContract(t *testing.T) {
	ctx := context.Background()
	bound, err := BindFuncExprImplByPlanExpr(ctx, "json_search", jsonSearchBindArgs())
	require.NoError(t, err)
	require.Equal(t, int32(types.T_json), bound.Typ.Id)
	id, overload := function.DecodeOverloadID(bound.GetF().GetFunc().GetObj())
	require.Equal(t, int32(function.JSON_SEARCH), id)
	require.Zero(t, overload)

	withCustomEscape := append(jsonSearchBindArgs(), makePlan2StringConstExprWithType("!"))
	_, err = BindFuncExprImplByPlanExpr(ctx, "json_search", withCustomEscape)
	require.NoError(t, err)

	withNullEscape := append(jsonSearchBindArgs(), makeJSONSearchStringNullExpr())
	_, err = BindFuncExprImplByPlanExpr(ctx, "json_search", withNullEscape)
	require.NoError(t, err)

	_, err = BindFuncExprImplByPlanExpr(ctx, "json_search", []*planpb.Expr{
		makePlan2StringConstExprWithType(`{"a":"x"}`),
		makePlan2Int64ConstExprWithType(1),
		makePlan2StringConstExprWithType("x"),
	})
	require.Error(t, err)
}

func TestBindJSONSearchEscapeMustBeConstant(t *testing.T) {
	ctx := context.Background()

	columnEscape := append(jsonSearchBindArgs(), &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			ColPos: 0,
		}},
	})
	_, err := BindFuncExprImplByPlanExpr(ctx, "json_search", columnEscape)
	require.ErrorContains(t, err, "Incorrect arguments to json_search")

	paramEscape := append(jsonSearchBindArgs(), &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_any)},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	})
	_, err = BindFuncExprImplByPlanExpr(ctx, "json_search", paramEscape)
	require.ErrorContains(t, err, "Incorrect arguments to json_search")

	multiRuneEscape := append(jsonSearchBindArgs(), makePlan2StringConstExprWithType("界!"))
	_, err = BindFuncExprImplByPlanExpr(ctx, "json_search", multiRuneEscape)
	require.ErrorContains(t, err, "Incorrect arguments to json_search")
}

func TestBindJSONSearchEmptyEscapeHonorsSQLMode(t *testing.T) {
	proc := testutil.NewProcess(t)
	args := append(jsonSearchBindArgs(), makePlan2StringConstExprWithType(""))

	proc.GetSessionInfo().SqlMode = ""
	_, err := bindFuncExprAndConstFoldInternal(
		proc.Ctx, proc, "json_search", args, true, false, nil)
	require.NoError(t, err)

	proc.GetSessionInfo().SqlMode = "NO_BACKSLASH_ESCAPES"
	_, err = bindFuncExprAndConstFoldInternal(
		proc.Ctx, proc, "json_search", args, true, false, nil)
	require.ErrorContains(t, err, "Incorrect arguments to json_search")

	proc.GetSessionInfo().SqlMode = "NO_BACKSLASH_ESCAPES"
	nullEscape := append(jsonSearchBindArgs(), makeJSONSearchStringNullExpr())
	_, err = bindFuncExprAndConstFoldInternal(
		proc.Ctx, proc, "json_search", nullEscape, true, false, nil)
	require.NoError(t, err)
}
