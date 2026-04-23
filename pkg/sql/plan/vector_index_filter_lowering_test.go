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
	"encoding/json"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestVectorIndexBackendLoweringUsesLogicalColumnNames(t *testing.T) {
	_, _, scanNode, _, _ := newIvfIncludeModeTestBuilder(t)
	scanTag := scanNode.BindingTags[0]
	categoryGE := makeIvfHelperFnExpr(
		">=",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperColExpr(scanTag, 3, scanNode.TableDef),
		MakePlan2Int32ConstExprWithType(20),
	)

	hnswPayload, hnswPushdown, hnswRemaining, err := lowerFiltersToHnswPayload([]*Expr{categoryGE}, scanNode, 1)
	require.NoError(t, err)
	require.Len(t, hnswPushdown, 1)
	require.Empty(t, hnswRemaining)
	require.NotContains(t, hnswPayload, "RelPos")
	require.NotContains(t, hnswPayload, "ColPos")
	require.NotContains(t, hnswPayload, "rel_pos")
	require.NotContains(t, hnswPayload, "col_pos")

	var hnswPayloadObj hnswFilterPayload
	require.NoError(t, json.Unmarshal([]byte(hnswPayload), &hnswPayloadObj))
	require.Equal(t, "logical_name", hnswPayloadObj.ColumnMode)
	require.Equal(t, "category", hnswPayloadObj.Exprs[0].Args[0].Column)

	cagraJSON, cagraPushdown, cagraRemaining, err := lowerFiltersToCagraJSON([]*Expr{categoryGE}, scanNode, 1)
	require.NoError(t, err)
	require.Len(t, cagraPushdown, 1)
	require.Empty(t, cagraRemaining)
	require.JSONEq(t, `[{"col":"category","op":">=","val":20}]`, cagraJSON)
	require.NotContains(t, cagraJSON, "RelPos")
	require.NotContains(t, cagraJSON, "ColPos")
	require.NotContains(t, cagraJSON, "rel_pos")
	require.NotContains(t, cagraJSON, "col_pos")
}

func TestCagraLoweringKeepsUnsupportedBooleanTreesResidual(t *testing.T) {
	_, _, scanNode, _, _ := newIvfIncludeModeTestBuilder(t)
	scanTag := scanNode.BindingTags[0]
	categoryGE := makeIvfHelperFnExpr(
		">=",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperColExpr(scanTag, 3, scanNode.TableDef),
		MakePlan2Int32ConstExprWithType(20),
	)
	titleEq := makeIvfHelperFnExpr(
		"=",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperColExpr(scanTag, 2, scanNode.TableDef),
		makePlan2StringConstExprWithType("alpha"),
	)
	orExpr := makeIvfHelperFnExpr("or", Type{Id: int32(types.T_bool)}, categoryGE, titleEq)

	payload, pushdown, remaining, err := lowerFiltersToCagraJSON([]*planpb.Expr{orExpr}, scanNode, 1)
	require.NoError(t, err)
	require.Empty(t, payload)
	require.Empty(t, pushdown)
	require.Equal(t, []*planpb.Expr{orExpr}, remaining)
}

func TestCagraLoweringSupportsAndBetweenAndIn(t *testing.T) {
	_, _, scanNode, _, _ := newIvfIncludeModeTestBuilder(t)
	scanTag := scanNode.BindingTags[0]
	betweenExpr := makeIvfHelperFnExpr(
		"between",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperColExpr(scanTag, 3, scanNode.TableDef),
		MakePlan2Int32ConstExprWithType(10),
		MakePlan2Int32ConstExprWithType(30),
	)
	inExpr := makeIvfHelperFnExpr(
		"in",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperColExpr(scanTag, 2, scanNode.TableDef),
		makePlan2StringConstExprWithType("alpha"),
		makePlan2StringConstExprWithType("beta"),
	)
	andExpr := makeIvfHelperFnExpr("and", Type{Id: int32(types.T_bool)}, betweenExpr, inExpr)

	payload, pushdown, remaining, err := lowerFiltersToCagraJSON([]*planpb.Expr{andExpr}, scanNode, 1)
	require.NoError(t, err)
	require.Len(t, pushdown, 1)
	require.Empty(t, remaining)
	require.JSONEq(t, `[
		{"col":"category","op":"between","lo":10,"hi":30},
		{"col":"title","op":"in","vals":["alpha","beta"]}
	]`, payload)
}

func TestPrepareCagraFilterPushdownUsesIncludedColumnsAndKeepsResiduals(t *testing.T) {
	_, _, scanNode, _, multiTableIndex := newIvfIncludeModeTestBuilder(t)
	scanTag := scanNode.BindingTags[0]
	categoryGE := makeIvfHelperFnExpr(
		">=",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperColExpr(scanTag, 3, scanNode.TableDef),
		MakePlan2Int32ConstExprWithType(20),
	)
	titleEq := makeIvfHelperFnExpr(
		"=",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperColExpr(scanTag, 2, scanNode.TableDef),
		makePlan2StringConstExprWithType("alpha"),
	)
	noteEq := makeIvfHelperFnExpr(
		"=",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperColExpr(scanTag, 4, scanNode.TableDef),
		makePlan2StringConstExprWithType("memo"),
	)
	orCovered := makeIvfHelperFnExpr("or", Type{Id: int32(types.T_bool)}, titleEq, categoryGE)
	filters := []*planpb.Expr{noteEq, orCovered, categoryGE}

	payload, pushdown, remaining, err := prepareCagraFilterPushdown(filters, scanNode, multiTableIndex, 1)
	require.NoError(t, err)
	require.Equal(t, []*planpb.Expr{categoryGE}, pushdown)
	require.Equal(t, []*planpb.Expr{noteEq, orCovered}, remaining)
	require.JSONEq(t, `[{"col":"category","op":">=","val":20}]`, payload)
}

func TestBuildCagraTableFuncArgsMakesFilterJSONOptional(t *testing.T) {
	vecLitArg := &planpb.Expr{
		Typ: Type{Id: int32(types.T_array_float32)},
		Expr: &planpb.Expr_Lit{
			Lit: &planpb.Literal{
				Value: &planpb.Literal_VecVal{
					VecVal: "[0,1,0]",
				},
			},
		},
	}

	argsWithoutFilter := buildCagraTableFuncArgs(`{"index":"idx_products"}`, vecLitArg, "")
	require.Len(t, argsWithoutFilter, 2)
	require.Equal(t, `{"index":"idx_products"}`, argsWithoutFilter[0].GetLit().GetSval())
	require.Equal(t, "[0,1,0]", argsWithoutFilter[1].GetLit().GetVecVal())
	require.NotSame(t, vecLitArg, argsWithoutFilter[1])

	argsWithFilter := buildCagraTableFuncArgs(
		`{"index":"idx_products"}`,
		vecLitArg,
		`[{"col":"category","op":"=","val":2}]`,
	)
	require.Len(t, argsWithFilter, 3)
	require.Equal(t, `[{"col":"category","op":"=","val":2}]`, argsWithFilter[2].GetLit().GetSval())
}
