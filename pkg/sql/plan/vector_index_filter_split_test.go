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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestSplitFiltersByVectorIndexCoverageBooleanTreesAndDistance(t *testing.T) {
	_, _, scanNode, _, _ := newIvfIncludeModeTestBuilder(t)
	scanTag := scanNode.BindingTags[0]

	idEq := makeIvfHelperFnExpr(
		"=",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperColExpr(scanTag, 0, scanNode.TableDef),
		makePlan2Int64ConstExprWithType(1),
	)
	titleEq := makeIvfHelperFnExpr(
		"=",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperColExpr(scanTag, 2, scanNode.TableDef),
		makePlan2StringConstExprWithType("alpha"),
	)
	categoryEq := makeIvfHelperFnExpr(
		"=",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperColExpr(scanTag, 3, scanNode.TableDef),
		MakePlan2Int32ConstExprWithType(20),
	)
	noteEq := makeIvfHelperFnExpr(
		"=",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperColExpr(scanTag, 4, scanNode.TableDef),
		makePlan2StringConstExprWithType("memo"),
	)
	crossRelTitleEq := makeIvfHelperFnExpr(
		"=",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperColExpr(scanTag+1, 2, scanNode.TableDef),
		makePlan2StringConstExprWithType("alpha"),
	)
	distanceFilter := makeIvfHelperFnExpr(
		"<",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperFnExpr(
			"l2_distance",
			Type{Id: int32(types.T_float64)},
			makeIvfHelperColExpr(scanTag, 1, scanNode.TableDef),
			&Expr{
				Typ: Type{Id: int32(types.T_array_float32)},
				Expr: &planpb.Expr_Lit{
					Lit: &planpb.Literal{Value: &planpb.Literal_VecVal{VecVal: "[1,1,1]"}},
				},
			},
		),
		MakePlan2Float64ConstExprWithType(0.5),
	)

	andCovered := makeIvfHelperFnExpr("and", Type{Id: int32(types.T_bool)}, titleEq, categoryEq)
	andPartial := makeIvfHelperFnExpr("and", Type{Id: int32(types.T_bool)}, titleEq, noteEq)
	orCovered := makeIvfHelperFnExpr("or", Type{Id: int32(types.T_bool)}, titleEq, categoryEq)
	orPartial := makeIvfHelperFnExpr("or", Type{Id: int32(types.T_bool)}, titleEq, noteEq)
	notCovered := makeIvfHelperFnExpr("not", Type{Id: int32(types.T_bool)}, categoryEq)
	notPartial := makeIvfHelperFnExpr("not", Type{Id: int32(types.T_bool)}, noteEq)

	filters := []*Expr{
		idEq,
		titleEq,
		categoryEq,
		andCovered,
		orCovered,
		notCovered,
		noteEq,
		crossRelTitleEq,
		distanceFilter,
		andPartial,
		orPartial,
		notPartial,
	}
	pushdown, remaining := splitFiltersByVectorIndexCoverage(filters, scanNode, []string{"title", "category"}, 1)

	require.Equal(t, []*Expr{idEq, titleEq, categoryEq, andCovered, orCovered, notCovered}, pushdown)
	require.Equal(t, []*Expr{noteEq, crossRelTitleEq, distanceFilter, andPartial, orPartial, notPartial}, remaining)
}

func TestVectorIndexCoverageDoesNotTreatCompositePkHiddenColumnAsCovered(t *testing.T) {
	_, _, scanNode, _, _ := newIvfIncludeModeTestBuilder(t)
	scanTag := scanNode.BindingTags[0]
	tableDef := DeepCopyTableDef(scanNode.TableDef, true)
	cpkeyPos := int32(len(tableDef.Cols))
	tableDef.Cols = append(tableDef.Cols, &ColDef{
		Name: catalog.CPrimaryKeyColName,
		Typ:  Type{Id: int32(types.T_varchar)},
	})
	tableDef.Name2ColIndex[catalog.CPrimaryKeyColName] = cpkeyPos
	tableDef.Pkey = &PrimaryKeyDef{
		PkeyColName: catalog.CPrimaryKeyColName,
		Names:       []string{"id", "category"},
	}

	compositeScan := &Node{
		NodeType:    planpb.Node_TABLE_SCAN,
		TableDef:    tableDef,
		BindingTags: []int32{scanTag},
	}
	cpkeyEq := makeIvfHelperFnExpr(
		"=",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperColExpr(scanTag, cpkeyPos, tableDef),
		makePlan2StringConstExprWithType("encoded"),
	)
	categoryEq := makeIvfHelperFnExpr(
		"=",
		Type{Id: int32(types.T_bool)},
		makeIvfHelperColExpr(scanTag, 3, tableDef),
		MakePlan2Int32ConstExprWithType(20),
	)

	pushdown, remaining := splitFiltersByVectorIndexCoverage([]*Expr{cpkeyEq, categoryEq}, compositeScan, []string{"title", "category"}, 1)
	require.Equal(t, []*Expr{categoryEq}, pushdown)
	require.Equal(t, []*Expr{cpkeyEq}, remaining)
	require.False(t, canDoIndexOnlyScan(map[string]struct{}{catalog.CPrimaryKeyColName: {}}, tableDef, []string{"title", "category"}))
}
