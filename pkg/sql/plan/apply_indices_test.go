// Copyright 2024 Matrix Origin
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
	"reflect"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSuspendScanProtection_RestoresExactCount(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	const scanID int32 = 42

	builder.protectedScans[scanID] = 3
	restore := builder.suspendScanProtection(scanID)

	assert.False(t, builder.isScanProtected(scanID))

	restore()

	assert.Equal(t, 3, builder.protectedScans[scanID])
}

func TestSuspendScanProtection_NoExistingProtection(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	const scanID int32 = 24

	restore := builder.suspendScanProtection(scanID)
	assert.False(t, builder.isScanProtected(scanID))

	restore()

	_, exists := builder.protectedScans[scanID]
	assert.False(t, exists)
}

func TestSuspendScanProtection_DoesNotDeleteNewProtection(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	const scanID int32 = 88

	restore := builder.suspendScanProtection(scanID)
	builder.protectedScans[scanID] = 1

	restore()

	assert.Equal(t, 1, builder.protectedScans[scanID])
}

func TestWithSuspendedScanProtection_RestoresAfterPanic(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	const scanID int32 = 64

	builder.protectedScans[scanID] = 2

	recovered := false
	func() {
		defer func() {
			if recover() != nil {
				recovered = true
			}
		}()

		builder.withSuspendedScanProtection(scanID, func() {
			assert.False(t, builder.isScanProtected(scanID))
			panic("boom")
		})
	}()

	assert.True(t, recovered)
	assert.Equal(t, 2, builder.protectedScans[scanID])
}

func TestFullTextJoinRewriteLeftChild(t *testing.T) {
	builder, joinID, leftScanID, rightScanID := buildFullTextJoinRewriteTestPlan(t, true, false, false)

	newID, err := builder.applyIndicesForJoins(joinID, builder.qry.Nodes[joinID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, joinID, newID)

	joinNode := builder.qry.Nodes[joinID]
	require.NotEqual(t, leftScanID, joinNode.Children[0])
	require.Equal(t, rightScanID, joinNode.Children[1])
	require.Equal(t, planpb.Node_JOIN, builder.qry.Nodes[joinNode.Children[0]].NodeType)
	require.Equal(t, 1, countFullTextFunctionScans(builder, joinNode.Children[0]))
	require.False(t, nodeHasFullTextMatchFilter(builder.qry.Nodes[leftScanID]))
	require.Len(t, joinNode.OnList, 1)
}

func TestFullTextJoinRewriteRightChild(t *testing.T) {
	builder, joinID, leftScanID, rightScanID := buildFullTextJoinRewriteTestPlan(t, false, true, false)

	newID, err := builder.applyIndicesForJoins(joinID, builder.qry.Nodes[joinID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, joinID, newID)

	joinNode := builder.qry.Nodes[joinID]
	require.Equal(t, leftScanID, joinNode.Children[0])
	require.NotEqual(t, rightScanID, joinNode.Children[1])
	require.Equal(t, planpb.Node_JOIN, builder.qry.Nodes[joinNode.Children[1]].NodeType)
	require.Equal(t, 1, countFullTextFunctionScans(builder, joinNode.Children[1]))
	require.False(t, nodeHasFullTextMatchFilter(builder.qry.Nodes[rightScanID]))
	require.Len(t, joinNode.OnList, 1)
}

func TestFullTextJoinRewriteFallsBackToScanContextWhenJoinContextIsNil(t *testing.T) {
	builder, joinID, leftScanID, _ := buildFullTextJoinRewriteTestPlan(t, true, false, false)
	builder.ctxByNode[joinID] = nil

	newID, err := builder.applyIndicesForJoins(joinID, builder.qry.Nodes[joinID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, joinID, newID)
	require.NotEqual(t, leftScanID, builder.qry.Nodes[joinID].Children[0])
	require.Equal(t, 1, countFullTextFunctionScans(builder, builder.qry.Nodes[joinID].Children[0]))
	require.False(t, nodeHasFullTextMatchFilter(builder.qry.Nodes[leftScanID]))
}

func TestFullTextJoinRewriteBothChildren(t *testing.T) {
	builder, joinID, leftScanID, rightScanID := buildFullTextJoinRewriteTestPlan(t, true, true, false)

	newID, err := builder.applyIndicesForJoins(joinID, builder.qry.Nodes[joinID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, joinID, newID)

	joinNode := builder.qry.Nodes[joinID]
	require.NotEqual(t, leftScanID, joinNode.Children[0])
	require.NotEqual(t, rightScanID, joinNode.Children[1])
	require.Equal(t, 1, countFullTextFunctionScans(builder, joinNode.Children[0]))
	require.Equal(t, 1, countFullTextFunctionScans(builder, joinNode.Children[1]))
	require.False(t, nodeHasFullTextMatchFilter(builder.qry.Nodes[leftScanID]))
	require.False(t, nodeHasFullTextMatchFilter(builder.qry.Nodes[rightScanID]))
}

func TestFullTextJoinRewriteSkipsOuterJoins(t *testing.T) {
	tests := []struct {
		name          string
		joinType      planpb.Node_JoinType
		leftFullText  bool
		rightFullText bool
	}{
		{
			name:         "left join preserved left child",
			joinType:     planpb.Node_LEFT,
			leftFullText: true,
		},
		{
			name:          "right join preserved right child",
			joinType:      planpb.Node_RIGHT,
			rightFullText: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder, joinID, leftScanID, rightScanID := buildFullTextJoinRewriteTestPlan(t, tt.leftFullText, tt.rightFullText, false)
			joinNode := builder.qry.Nodes[joinID]
			joinNode.JoinType = tt.joinType

			newID, err := builder.applyIndicesForJoins(joinID, joinNode, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
			require.NoError(t, err)
			require.Equal(t, joinID, newID)
			require.Equal(t, leftScanID, joinNode.Children[0])
			require.Equal(t, rightScanID, joinNode.Children[1])
			require.Equal(t, 0, countFullTextFunctionScans(builder, joinID))

			if tt.leftFullText {
				require.True(t, nodeHasFullTextMatchFilter(builder.qry.Nodes[leftScanID]))
			}
			if tt.rightFullText {
				require.True(t, nodeHasFullTextMatchFilter(builder.qry.Nodes[rightScanID]))
			}
		})
	}
}

func TestFullTextJoinRewritePreservesNonFullTextFilter(t *testing.T) {
	builder, joinID, leftScanID, _ := buildFullTextJoinRewriteTestPlan(t, true, false, true)

	newID, err := builder.applyIndicesForJoins(joinID, builder.qry.Nodes[joinID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, joinID, newID)

	leftScan := builder.qry.Nodes[leftScanID]
	require.Len(t, leftScan.FilterList, 1)
	require.Equal(t, "=", leftScan.FilterList[0].GetF().Func.ObjName)
	require.False(t, nodeHasFullTextMatchFilter(leftScan))
	require.Equal(t, 1, countFullTextFunctionScans(builder, builder.qry.Nodes[joinID].Children[0]))
}

func TestFullTextScanProtectionSkipsRegularIndexRule(t *testing.T) {
	builder, joinID, leftScanID, _ := buildFullTextJoinRewriteTestPlan(t, true, false, true)
	leftScan := builder.qry.Nodes[leftScanID]
	leftScan.TableDef.Indexes = append(leftScan.TableDef.Indexes, &planpb.IndexDef{
		IndexName:      "idx_base_id",
		IndexTableName: "__mo_idx_base_id",
		Parts:          []string{"base_id", "id"},
		TableExist:     true,
	})
	registerFullTextJoinRegularIndexTable(builder, "__mo_idx_base_id")

	got := builder.applyIndicesForFilters(leftScanID, leftScan, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.Equal(t, leftScanID, got)
	require.True(t, builder.scanHasMatchedFullTextFilter(leftScan))

	newID, err := builder.applyIndicesForJoins(joinID, builder.qry.Nodes[joinID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, joinID, newID)
	require.False(t, nodeHasFullTextMatchFilter(leftScan))
	require.Len(t, leftScan.FilterList, 1)
}

func TestFindMatchFullTextIndexRequiresScanBindingAndLiteralPattern(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	ftDef := makeFullTextJoinTestTableDef("ft", true)
	ftTag := builder.genNewBindTag()
	baseTag := builder.genNewBindTag()
	scan := makeFullTextJoinTestScan(ftDef, ftTag, nil)

	matched := builder.findMatchFullTextIndex(makeFullTextMatchExpr("hello", 0, ftDef, ftTag, []int32{2, 3}).GetF(), scan)
	require.NotNil(t, matched)

	crossTableExpr := makeFullTextMatchExpr("hello", 0, ftDef, ftTag, []int32{2})
	crossTableExpr.GetF().Args = append(crossTableExpr.GetF().Args, &planpb.Expr{
		Typ: ftDef.Cols[3].Typ,
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: baseTag,
			ColPos: 3,
			Name:   "body",
		}},
	})
	require.Nil(t, builder.findMatchFullTextIndex(crossTableExpr.GetF(), scan))

	dynamicPatternExpr := makeFullTextMatchExpr("hello", 0, ftDef, ftTag, []int32{2, 3})
	textTyp := types.T_text.ToType()
	dynamicPatternExpr.GetF().Args[0] = &planpb.Expr{
		Typ:  makePlan2Type(&textTyp),
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	}
	require.Nil(t, builder.findMatchFullTextIndex(dynamicPatternExpr.GetF(), scan))
}

func buildFullTextJoinRewriteTestPlan(t *testing.T, leftFullText, rightFullText, leftExtraFilter bool) (*QueryBuilder, int32, int32, int32) {
	t.Helper()

	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	ctx := NewBindContext(builder, nil)

	leftTag := builder.genNewBindTag()
	rightTag := builder.genNewBindTag()
	leftDef := makeFullTextJoinTestTableDef("ft_left", leftFullText)
	rightDef := makeFullTextJoinTestTableDef("ft_right", rightFullText)

	var leftFilters []*planpb.Expr
	if leftFullText {
		leftFilters = append(leftFilters, makeFullTextMatchExpr("hello", 0, leftDef, leftTag, []int32{2, 3}))
	}
	if leftExtraFilter {
		leftFilters = append(leftFilters, ftjMakeEqExpr(t, ftjColExpr(leftDef, leftTag, 1), makePlan2StringConstExprWithType("b1", false)))
	}

	var rightFilters []*planpb.Expr
	if rightFullText {
		rightFilters = append(rightFilters, makeFullTextMatchExpr("hello", 0, rightDef, rightTag, []int32{2, 3}))
	}

	leftScanID := builder.appendNode(makeFullTextJoinTestScan(leftDef, leftTag, leftFilters), ctx)
	rightScanID := builder.appendNode(makeFullTextJoinTestScan(rightDef, rightTag, rightFilters), ctx)
	joinCond := ftjMakeEqExpr(t, ftjColExpr(leftDef, leftTag, 1), ftjColExpr(rightDef, rightTag, 0))
	joinID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_JOIN,
		Children: []int32{leftScanID, rightScanID},
		JoinType: planpb.Node_INNER,
		OnList:   []*planpb.Expr{joinCond},
	}, ctx)

	return builder, joinID, leftScanID, rightScanID
}

type fullTextJoinMockCompilerContext struct {
	*MockCompilerContext
}

func newFullTextJoinMockCompilerContext() *fullTextJoinMockCompilerContext {
	return &fullTextJoinMockCompilerContext{MockCompilerContext: NewMockCompilerContext(true)}
}

func (m *fullTextJoinMockCompilerContext) ResolveVariable(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
	if varName == "ft_relevancy_algorithm" {
		return "", nil
	}
	return m.MockCompilerContext.ResolveVariable(varName, isSystemVar, isGlobalVar)
}

func (m *fullTextJoinMockCompilerContext) GetProcess() *process.Process {
	proc := m.MockCompilerContext.GetProcess()
	proc.SetResolveVariableFunc(m.ResolveVariable)
	return proc
}

func registerFullTextJoinRegularIndexTable(builder *QueryBuilder, indexTableName string) {
	mockCtx := builder.compCtx.(*fullTextJoinMockCompilerContext)
	key := strings.ToLower(indexTableName)
	mockCtx.objects[key] = &planpb.ObjectRef{
		SchemaName: "test",
		ObjName:    indexTableName,
	}
	mockCtx.tables[key] = &planpb.TableDef{
		Name: indexTableName,
		Cols: []*planpb.ColDef{
			{Name: catalog.IndexTableIndexColName, Typ: planpb.Type{Id: int32(types.T_varchar), Width: 191}},
			{Name: catalog.IndexTablePrimaryColName, Typ: planpb.Type{Id: int32(types.T_varchar), Width: 191}},
		},
		Name2ColIndex: map[string]int32{
			catalog.IndexTableIndexColName:   0,
			catalog.IndexTablePrimaryColName: 1,
		},
	}
}

func makeFullTextJoinTestTableDef(name string, withFullTextIndex bool) *planpb.TableDef {
	tableDef := &planpb.TableDef{
		Name: name,
		Cols: []*planpb.ColDef{
			{Name: "id", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 191}},
			{Name: "base_id", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 191}},
			{Name: "title", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 512}},
			{Name: "body", Typ: planpb.Type{Id: int32(types.T_text)}},
		},
		Name2ColIndex: map[string]int32{
			"id":      0,
			"base_id": 1,
			"title":   2,
			"body":    3,
		},
		Pkey: &planpb.PrimaryKeyDef{
			PkeyColName: "id",
			Names:       []string{"id"},
		},
	}
	if withFullTextIndex {
		tableDef.Indexes = append(tableDef.Indexes, &planpb.IndexDef{
			IndexName:      "ft_idx_" + name,
			IndexAlgo:      catalog.MOIndexFullTextAlgo.ToString(),
			IndexTableName: "__mo_fts_idx_" + name,
			Parts:          []string{"title", "body"},
			TableExist:     true,
		})
	}
	return tableDef
}

func makeFullTextJoinTestScan(tableDef *planpb.TableDef, tag int32, filters []*planpb.Expr) *planpb.Node {
	return &planpb.Node{
		NodeType:    planpb.Node_TABLE_SCAN,
		TableDef:    tableDef,
		ObjRef:      &planpb.ObjectRef{SchemaName: "test", ObjName: tableDef.Name},
		BindingTags: []int32{tag},
		FilterList:  filters,
		Stats: &planpb.Stats{
			TableCnt:    1000,
			Outcnt:      100,
			Selectivity: 0.1,
			Cost:        1000,
		},
	}
}

func makeFullTextMatchExpr(pattern string, mode int64, tableDef *planpb.TableDef, tag int32, colPositions []int32) *planpb.Expr {
	args := []*planpb.Expr{
		makePlan2StringConstExprWithType(pattern, false),
		makePlan2Int64ConstExprWithType(mode),
	}
	for _, pos := range colPositions {
		args = append(args, ftjColExpr(tableDef, tag, pos))
	}
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: "fulltext_match"},
			Args: args,
		}},
	}
}

func ftjColExpr(tableDef *planpb.TableDef, tag, pos int32) *planpb.Expr {
	return &planpb.Expr{
		Typ: tableDef.Cols[pos].Typ,
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: tag,
			ColPos: pos,
			Name:   tableDef.Cols[pos].Name,
		}},
	}
}

func ftjMakeEqExpr(t *testing.T, left, right *planpb.Expr) *planpb.Expr {
	t.Helper()

	expr, err := BindFuncExprImplByPlanExpr(context.Background(), "=", []*planpb.Expr{left, right})
	require.NoError(t, err)
	return expr
}

func countFullTextFunctionScans(builder *QueryBuilder, nodeID int32) int {
	node := builder.qry.Nodes[nodeID]
	if node == nil {
		return 0
	}

	count := 0
	if node.NodeType == planpb.Node_FUNCTION_SCAN &&
		node.TableDef != nil &&
		node.TableDef.TblFunc != nil &&
		node.TableDef.TblFunc.Name == fulltext_index_scan_func_name {
		count++
	}
	for _, childID := range node.Children {
		count += countFullTextFunctionScans(builder, childID)
	}
	return count
}

func nodeHasFullTextMatchFilter(node *planpb.Node) bool {
	for _, expr := range node.FilterList {
		fn := expr.GetF()
		if fn != nil && fn.Func.ObjName == "fulltext_match" {
			return true
		}
	}
	return false
}

func TestCalculatePostFilterOverFetchFactor(t *testing.T) {
	tests := []struct {
		name          string
		originalLimit uint64
		expectedMin   float64
		expectedMax   float64
	}{
		// Small limits (< 10): should return 5.0x
		{
			name:          "Very small limit - 1",
			originalLimit: 1,
			expectedMin:   5.0,
			expectedMax:   5.0,
		},
		{
			name:          "Very small limit - 3",
			originalLimit: 3,
			expectedMin:   5.0,
			expectedMax:   5.0,
		},
		{
			name:          "Small limit - 5",
			originalLimit: 5,
			expectedMin:   5.0,
			expectedMax:   5.0,
		},
		{
			name:          "Small limit boundary - 9",
			originalLimit: 9,
			expectedMin:   5.0,
			expectedMax:   5.0,
		},

		// Medium limits (10-49): should return 2.0x
		{
			name:          "Medium limit lower boundary - 10",
			originalLimit: 10,
			expectedMin:   2.0,
			expectedMax:   2.0,
		},
		{
			name:          "Medium limit - 20",
			originalLimit: 20,
			expectedMin:   2.0,
			expectedMax:   2.0,
		},
		{
			name:          "Medium limit - 30",
			originalLimit: 30,
			expectedMin:   2.0,
			expectedMax:   2.0,
		},
		{
			name:          "Medium limit upper boundary - 49",
			originalLimit: 49,
			expectedMin:   2.0,
			expectedMax:   2.0,
		},

		// Large limits (50-99): should return 1.5x
		{
			name:          "Large limit lower boundary - 50",
			originalLimit: 50,
			expectedMin:   1.5,
			expectedMax:   1.5,
		},
		{
			name:          "Large limit - 75",
			originalLimit: 75,
			expectedMin:   1.5,
			expectedMax:   1.5,
		},
		{
			name:          "Large limit upper boundary - 99",
			originalLimit: 99,
			expectedMin:   1.5,
			expectedMax:   1.5,
		},

		// Very large limits (100-199): should return 1.3x
		{
			name:          "Very large limit lower boundary - 100",
			originalLimit: 100,
			expectedMin:   1.3,
			expectedMax:   1.3,
		},
		{
			name:          "Very large limit - 150",
			originalLimit: 150,
			expectedMin:   1.3,
			expectedMax:   1.3,
		},
		{
			name:          "Very large limit upper boundary - 199",
			originalLimit: 199,
			expectedMin:   1.3,
			expectedMax:   1.3,
		},

		// Huge limits (200+): should return 1.2x
		{
			name:          "Huge limit lower boundary - 200",
			originalLimit: 200,
			expectedMin:   1.2,
			expectedMax:   1.2,
		},
		{
			name:          "Huge limit - 500",
			originalLimit: 500,
			expectedMin:   1.2,
			expectedMax:   1.2,
		},
		{
			name:          "Huge limit - 1000",
			originalLimit: 1000,
			expectedMin:   1.2,
			expectedMax:   1.2,
		},
		{
			name:          "Huge limit - 10000",
			originalLimit: 10000,
			expectedMin:   1.2,
			expectedMax:   1.2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculatePostFilterOverFetchFactor(tt.originalLimit)

			if result < tt.expectedMin || result > tt.expectedMax {
				t.Errorf("calculatePostFilterOverFetchFactor(%d) = %f, want between %f and %f",
					tt.originalLimit, result, tt.expectedMin, tt.expectedMax)
			}

			// Verify the result is positive
			if result <= 0 {
				t.Errorf("calculatePostFilterOverFetchFactor(%d) = %f, want positive value",
					tt.originalLimit, result)
			}

			// Verify the result is at least 1.0 (must fetch at least original limit)
			if result < 1.0 {
				t.Errorf("calculatePostFilterOverFetchFactor(%d) = %f, want >= 1.0",
					tt.originalLimit, result)
			}
		})
	}
}

// Test the actual over-fetch calculation results
func TestCalculatePostFilterOverFetchFactor_ActualValues(t *testing.T) {
	testCases := []struct {
		limit         uint64
		expectedFetch uint64 // expected number of rows to fetch
	}{
		// Small limits (5x factor)
		{limit: 3, expectedFetch: 15},  // 3 * 5 = 15
		{limit: 5, expectedFetch: 25},  // 5 * 5 = 25
		{limit: 10, expectedFetch: 20}, // 10 * 2 = 20 (crosses boundary)

		// Medium limits (2x factor)
		{limit: 20, expectedFetch: 40}, // 20 * 2 = 40
		{limit: 30, expectedFetch: 60}, // 30 * 2 = 60
		{limit: 49, expectedFetch: 98}, // 49 * 2 = 98
		{limit: 50, expectedFetch: 75}, // 50 * 1.5 = 75 (crosses boundary)

		// Large limits (1.5x factor)
		{limit: 80, expectedFetch: 120},  // 80 * 1.5 = 120
		{limit: 99, expectedFetch: 148},  // 99 * 1.5 = 148.5, truncated to 148
		{limit: 100, expectedFetch: 130}, // 100 * 1.3 = 130 (crosses boundary)

		// Very large limits (1.3x factor)
		{limit: 150, expectedFetch: 195}, // 150 * 1.3 = 195
		{limit: 199, expectedFetch: 258}, // 199 * 1.3 = 258.7, truncated to 258
		{limit: 200, expectedFetch: 240}, // 200 * 1.2 = 240 (crosses boundary)

		// Huge limits (1.2x factor)
		{limit: 250, expectedFetch: 300},   // 250 * 1.2 = 300
		{limit: 500, expectedFetch: 600},   // 500 * 1.2 = 600
		{limit: 1000, expectedFetch: 1200}, // 1000 * 1.2 = 1200
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			factor := calculatePostFilterOverFetchFactor(tc.limit)
			actualFetch := uint64(float64(tc.limit) * factor)

			if actualFetch != tc.expectedFetch {
				t.Errorf("For limit %d: got fetch %d, want %d (factor: %f)",
					tc.limit, actualFetch, tc.expectedFetch, factor)
			}
		})
	}
}

func makeTestRegularIndexPrefixEq(t *testing.T, numArgs int) *planpb.Expr {
	t.Helper()
	args := make([]*planpb.Expr, 0, numArgs)
	for i := 0; i < numArgs; i++ {
		args = append(args, &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_int32)},
			Expr: &planpb.Expr_Lit{
				Lit: &planpb.Literal{
					Value: &planpb.Literal_I32Val{I32Val: int32(i + 1)},
				},
			},
		})
	}
	serialExpr, err := BindFuncExprImplByPlanExpr(context.Background(), "serial", args)
	require.NoError(t, err)
	prefixExpr, err := BindFuncExprImplByPlanExpr(context.Background(), "prefix_eq", []*planpb.Expr{
		GetColExpr(planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}, 100, 0),
		serialExpr,
	})
	require.NoError(t, err)
	return prefixExpr
}

func makeTestRegularIndexProjectBuilder(
	t *testing.T,
	prefixArgCount int,
	projectExpr *planpb.Expr,
	sortFlag planpb.OrderBySpec_OrderByFlag,
) (*QueryBuilder, int32) {
	t.Helper()

	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	builder.nameByColRef[[2]int32{200, 0}] = "id"

	scanNode := &planpb.Node{
		NodeType: planpb.Node_TABLE_SCAN,
		NodeId:   0,
		TableDef: &planpb.TableDef{
			Cols: []*planpb.ColDef{
				{
					Name: catalog.IndexTableIndexColName,
					Typ:  planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
				},
				{
					Name: catalog.IndexTablePrimaryColName,
					Typ:  planpb.Type{Id: int32(types.T_int64)},
				},
			},
			Indexes: []*planpb.IndexDef{{IndexName: "idx_user_active"}},
		},
		BindingTags: []int32{100},
		FilterList:  []*planpb.Expr{makeTestRegularIndexPrefixEq(t, prefixArgCount)},
		IndexScanInfo: planpb.IndexScanInfo{
			IsIndexScan:    true,
			IndexName:      "idx_user_active",
			BelongToTable:  "events",
			Parts:          []string{"user_id", "is_active", "id"},
			IsUnique:       false,
			IndexTableName: "__mo_index_secondary_idx_user_active",
		},
	}

	sortProjectNode := &planpb.Node{
		NodeType:    planpb.Node_PROJECT,
		NodeId:      1,
		BindingTags: []int32{200},
		Children:    []int32{0},
		ProjectList: []*planpb.Expr{projectExpr},
	}

	sortNode := &planpb.Node{
		NodeType: planpb.Node_SORT,
		NodeId:   2,
		Children: []int32{1},
		OrderBy: []*planpb.OrderBySpec{
			{
				Expr: GetColExpr(planpb.Type{Id: int32(types.T_int64)}, 200, 0),
				Flag: sortFlag,
			},
		},
		Limit: &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_uint64)},
			Expr: &planpb.Expr_Lit{
				Lit: &planpb.Literal{
					Value: &planpb.Literal_U64Val{U64Val: 20},
				},
			},
		},
	}

	projNode := &planpb.Node{
		NodeType: planpb.Node_PROJECT,
		NodeId:   3,
		Children: []int32{2},
	}

	builder.qry.Nodes = []*planpb.Node{scanNode, sortProjectNode, sortNode, projNode}
	return builder, 3
}

func makeTestRegularIndexMessageBuilder(
	t *testing.T,
	prefixArgCount int,
	sortColPos int32,
	sortFlag planpb.OrderBySpec_OrderByFlag,
) (*QueryBuilder, int32) {
	t.Helper()

	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)

	scanNode := &planpb.Node{
		NodeType: planpb.Node_TABLE_SCAN,
		NodeId:   0,
		TableDef: &planpb.TableDef{
			Cols: []*planpb.ColDef{
				{
					Name: catalog.IndexTableIndexColName,
					Typ:  planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
				},
				{
					Name: catalog.IndexTablePrimaryColName,
					Typ:  planpb.Type{Id: int32(types.T_int64)},
				},
			},
		},
		BindingTags: []int32{100},
		FilterList:  []*planpb.Expr{makeTestRegularIndexPrefixEq(t, prefixArgCount)},
		IndexScanInfo: planpb.IndexScanInfo{
			IsIndexScan:    true,
			IndexName:      "idx_user_active",
			BelongToTable:  "events",
			Parts:          []string{"user_id", "is_active", "__mo_alias_id"},
			IsUnique:       false,
			IndexTableName: "__mo_index_secondary_idx_user_active",
		},
	}

	sortExpr := GetColExpr(scanNode.TableDef.Cols[sortColPos].Typ, 100, sortColPos)
	sortExpr.GetCol().Name = scanNode.TableDef.Cols[sortColPos].Name

	sortNode := &planpb.Node{
		NodeType: planpb.Node_SORT,
		NodeId:   1,
		Children: []int32{0},
		OrderBy: []*planpb.OrderBySpec{
			{
				Expr: sortExpr,
				Flag: sortFlag,
			},
		},
		Limit: &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_uint64)},
			Expr: &planpb.Expr_Lit{
				Lit: &planpb.Literal{
					Value: &planpb.Literal_U64Val{U64Val: 20},
				},
			},
		},
	}

	projNode := &planpb.Node{
		NodeType: planpb.Node_PROJECT,
		NodeId:   2,
		Children: []int32{1},
	}

	builder.qry.Nodes = []*planpb.Node{scanNode, sortNode, projNode}
	return builder, 2
}

func TestApplyIndicesForProjectPushesTopValueThroughRegularIndexPKOrder(t *testing.T) {
	builder, rootNodeID := makeTestRegularIndexProjectBuilder(
		t,
		2,
		GetColExpr(planpb.Type{Id: int32(types.T_int64)}, 100, 1),
		planpb.OrderBySpec_DESC,
	)

	_, err := builder.applyIndicesForProject(rootNodeID, builder.qry.Nodes[rootNodeID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)

	scanNode := builder.qry.Nodes[0]
	sortProjectNode := builder.qry.Nodes[1]
	sortNode := builder.qry.Nodes[2]

	require.Len(t, sortNode.SendMsgList, 1)
	assert.Equal(t, int32(message.MsgTopValue), sortNode.SendMsgList[0].MsgType)
	require.Len(t, scanNode.RecvMsgList, 1)
	assert.Equal(t, sortNode.SendMsgList[0], scanNode.RecvMsgList[0])

	require.Len(t, scanNode.OrderBy, 1)
	scanOrderCol := scanNode.OrderBy[0].Expr.GetCol()
	require.NotNil(t, scanOrderCol)
	assert.Equal(t, int32(100), scanOrderCol.RelPos)
	assert.Equal(t, int32(0), scanOrderCol.ColPos)
	assert.Equal(t, catalog.IndexTableIndexColName, scanOrderCol.Name)
	assert.Equal(t, planpb.OrderBySpec_DESC, scanNode.OrderBy[0].Flag)
	require.Len(t, scanNode.BlockOrderBy, 1)
	require.NotNil(t, scanNode.BlockLimit)
	assert.Equal(t, uint64(20), scanNode.BlockLimit.GetLit().GetU64Val())
	blockOrderCol := scanNode.BlockOrderBy[0].Expr.GetCol()
	require.NotNil(t, blockOrderCol)
	assert.Equal(t, int32(100), blockOrderCol.RelPos)
	assert.Equal(t, int32(0), blockOrderCol.ColPos)
	assert.Equal(t, catalog.IndexTableIndexColName, blockOrderCol.Name)
	assert.Equal(t, planpb.OrderBySpec_DESC, scanNode.BlockOrderBy[0].Flag)

	sortOrderCol := sortNode.OrderBy[0].Expr.GetCol()
	require.NotNil(t, sortOrderCol)
	assert.Equal(t, int32(200), sortOrderCol.RelPos)
	assert.Equal(t, int32(1), sortOrderCol.ColPos)

	require.Len(t, sortProjectNode.ProjectList, 2)
	hiddenKeyProjectCol := sortProjectNode.ProjectList[1].GetCol()
	require.NotNil(t, hiddenKeyProjectCol)
	assert.Equal(t, int32(100), hiddenKeyProjectCol.RelPos)
	assert.Equal(t, int32(0), hiddenKeyProjectCol.ColPos)
	assert.Equal(t, "id", builder.nameByColRef[[2]int32{200, 1}])
}

func TestApplyIndicesForProjectPushesTopValueThroughRegularIndexPKOrderAsc(t *testing.T) {
	builder, rootNodeID := makeTestRegularIndexProjectBuilder(
		t,
		2,
		GetColExpr(planpb.Type{Id: int32(types.T_int64)}, 100, 1),
		0,
	)

	_, err := builder.applyIndicesForProject(rootNodeID, builder.qry.Nodes[rootNodeID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)

	scanNode := builder.qry.Nodes[0]
	sortNode := builder.qry.Nodes[2]

	require.Len(t, sortNode.SendMsgList, 1)
	require.Len(t, scanNode.OrderBy, 1)
	assert.Equal(t, planpb.OrderBySpec_OrderByFlag(0), sortNode.OrderBy[0].Flag)
	assert.Equal(t, planpb.OrderBySpec_OrderByFlag(0), scanNode.OrderBy[0].Flag)
	assert.Equal(t, catalog.IndexTableIndexColName, scanNode.OrderBy[0].Expr.GetCol().Name)
	require.Len(t, scanNode.BlockOrderBy, 1)
	assert.Equal(t, planpb.OrderBySpec_OrderByFlag(0), scanNode.BlockOrderBy[0].Flag)
	assert.Equal(t, catalog.IndexTableIndexColName, scanNode.BlockOrderBy[0].Expr.GetCol().Name)
}

func TestHandleMessageFromTopToScanRewritesRegularIndexPKOrderToHiddenKey(t *testing.T) {
	builder, rootNodeID := makeTestRegularIndexMessageBuilder(t, 2, 1, planpb.OrderBySpec_DESC)

	builder.handleMessageFromTopToScan(rootNodeID)

	scanNode := builder.qry.Nodes[0]
	sortNode := builder.qry.Nodes[1]

	require.Len(t, sortNode.SendMsgList, 1)
	require.Len(t, scanNode.RecvMsgList, 1)
	require.Len(t, scanNode.OrderBy, 1)

	sortOrderCol := sortNode.OrderBy[0].Expr.GetCol()
	require.NotNil(t, sortOrderCol)
	assert.Equal(t, int32(100), sortOrderCol.RelPos)
	assert.Equal(t, int32(0), sortOrderCol.ColPos)
	assert.Equal(t, catalog.IndexTablePrimaryColName, sortOrderCol.Name)

	scanOrderCol := scanNode.OrderBy[0].Expr.GetCol()
	require.NotNil(t, scanOrderCol)
	assert.Equal(t, int32(100), scanOrderCol.RelPos)
	assert.Equal(t, int32(0), scanOrderCol.ColPos)
	assert.Equal(t, catalog.IndexTableIndexColName, scanOrderCol.Name)

	require.Len(t, scanNode.BlockOrderBy, 1)
	require.NotNil(t, scanNode.BlockLimit)
	assert.Equal(t, uint64(20), scanNode.BlockLimit.GetLit().GetU64Val())
	blockOrderCol := scanNode.BlockOrderBy[0].Expr.GetCol()
	require.NotNil(t, blockOrderCol)
	assert.Equal(t, int32(100), blockOrderCol.RelPos)
	assert.Equal(t, int32(0), blockOrderCol.ColPos)
	assert.Equal(t, catalog.IndexTableIndexColName, blockOrderCol.Name)
}

func TestHandleMessageFromTopToScanSkipsBlockTopWithOffsetOrRank(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*planpb.Node)
	}{
		{
			name: "offset",
			setup: func(sortNode *planpb.Node) {
				sortNode.Offset = &planpb.Expr{
					Typ: planpb.Type{Id: int32(types.T_uint64)},
					Expr: &planpb.Expr_Lit{
						Lit: &planpb.Literal{
							Value: &planpb.Literal_U64Val{U64Val: 10},
						},
					},
				}
			},
		},
		{
			name: "rank",
			setup: func(sortNode *planpb.Node) {
				sortNode.RankOption = &planpb.RankOption{}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder, rootNodeID := makeTestRegularIndexMessageBuilder(t, 2, 1, planpb.OrderBySpec_DESC)
			sortNode := builder.qry.Nodes[1]
			tc.setup(sortNode)

			builder.handleMessageFromTopToScan(rootNodeID)

			scanNode := builder.qry.Nodes[0]
			require.Len(t, sortNode.SendMsgList, 1)
			require.Len(t, scanNode.RecvMsgList, 1)
			require.Len(t, scanNode.OrderBy, 1)
			assert.Equal(t, catalog.IndexTableIndexColName, scanNode.OrderBy[0].Expr.GetCol().Name)
			assert.Empty(t, scanNode.BlockOrderBy)
			assert.Nil(t, scanNode.BlockLimit)
		})
	}
}

func TestHandleMessageFromTopToScanKeepsPKOrderWhenPrefixIncomplete(t *testing.T) {
	builder, rootNodeID := makeTestRegularIndexMessageBuilder(t, 1, 1, planpb.OrderBySpec_DESC)

	builder.handleMessageFromTopToScan(rootNodeID)

	scanNode := builder.qry.Nodes[0]
	sortNode := builder.qry.Nodes[1]

	require.Len(t, sortNode.SendMsgList, 1)
	require.Len(t, scanNode.RecvMsgList, 1)
	require.Len(t, scanNode.OrderBy, 1)

	sortOrderCol := sortNode.OrderBy[0].Expr.GetCol()
	require.NotNil(t, sortOrderCol)
	assert.Equal(t, int32(1), sortOrderCol.ColPos)
	assert.Equal(t, catalog.IndexTablePrimaryColName, sortOrderCol.Name)

	scanOrderCol := scanNode.OrderBy[0].Expr.GetCol()
	require.NotNil(t, scanOrderCol)
	assert.Equal(t, int32(1), scanOrderCol.ColPos)
	assert.Equal(t, catalog.IndexTablePrimaryColName, scanOrderCol.Name)
	assert.Empty(t, scanNode.BlockOrderBy)
	assert.Nil(t, scanNode.BlockLimit)
}

func TestApplyIndicesForProjectSkipsRegularIndexPKOrderWithoutFullPrefixEquality(t *testing.T) {
	builder, rootNodeID := makeTestRegularIndexProjectBuilder(
		t,
		1,
		GetColExpr(planpb.Type{Id: int32(types.T_int64)}, 100, 1),
		planpb.OrderBySpec_DESC,
	)

	_, err := builder.applyIndicesForProject(rootNodeID, builder.qry.Nodes[rootNodeID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)

	scanNode := builder.qry.Nodes[0]
	sortProjectNode := builder.qry.Nodes[1]
	sortNode := builder.qry.Nodes[2]

	assert.Empty(t, sortNode.SendMsgList)
	assert.Empty(t, scanNode.RecvMsgList)
	assert.Empty(t, scanNode.OrderBy)
	require.Len(t, sortProjectNode.ProjectList, 1)
}

func TestApplyIndicesForProjectSkipsRegularIndexPKOrderForNonPKSortColumn(t *testing.T) {
	builder, rootNodeID := makeTestRegularIndexProjectBuilder(
		t,
		2,
		GetColExpr(planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}, 100, 0),
		planpb.OrderBySpec_DESC,
	)

	_, err := builder.applyIndicesForProject(rootNodeID, builder.qry.Nodes[rootNodeID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)

	scanNode := builder.qry.Nodes[0]
	sortProjectNode := builder.qry.Nodes[1]
	sortNode := builder.qry.Nodes[2]

	assert.Empty(t, sortNode.SendMsgList)
	assert.Empty(t, scanNode.RecvMsgList)
	assert.Empty(t, scanNode.OrderBy)
	require.Len(t, sortProjectNode.ProjectList, 1)
}

// Benchmark the function to ensure it's fast
func BenchmarkCalculatePostFilterOverFetchFactor(b *testing.B) {
	limits := []uint64{1, 5, 10, 20, 50, 100, 200, 500, 1000, 10000}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, limit := range limits {
			_ = calculatePostFilterOverFetchFactor(limit)
		}
	}
}

// Test edge case: zero limit (defensive programming)
func TestCalculatePostFilterOverFetchFactor_EdgeCases(t *testing.T) {
	// Test with zero - should still work (though not expected in real usage)
	result := calculatePostFilterOverFetchFactor(0)
	if result != 5.0 {
		t.Errorf("calculatePostFilterOverFetchFactor(0) = %f, want 5.0", result)
	}

	// Test with max uint64 value
	result = calculatePostFilterOverFetchFactor(^uint64(0))
	if result != 1.2 {
		t.Errorf("calculatePostFilterOverFetchFactor(max_uint64) = %f, want 1.2", result)
	}
}

// Test that the factor decreases as limit increases (monotonic property)
func TestCalculatePostFilterOverFetchFactor_MonotonicDecrease(t *testing.T) {
	testLimits := []uint64{1, 5, 10, 20, 50, 100, 200, 500, 1000}

	var prevFactor float64 = 10.0 // Start with a high value

	for _, limit := range testLimits {
		currentFactor := calculatePostFilterOverFetchFactor(limit)

		if currentFactor > prevFactor {
			t.Errorf("Factor should decrease as limit increases: limit=%d factor=%f > previous factor=%f",
				limit, currentFactor, prevFactor)
		}

		prevFactor = currentFactor
	}
}

func TestTryMatchMoreLeadingFiltersRequiresContiguousPrefix(t *testing.T) {
	idxDef := &IndexDef{
		Parts: []string{"uid", "typ", "flag", "__mo_alias_id"},
	}
	node := &planpb.Node{
		TableDef: &planpb.TableDef{
			Name2ColIndex: map[string]int32{
				"uid":  1,
				"typ":  2,
				"flag": 3,
				"id":   0,
			},
		},
		// Filters only on uid and flag, missing typ.
		FilterList: []*planpb.Expr{
			makeEqFilterExpr(1),
			makeEqFilterExpr(3),
		},
	}

	leadingPos := tryMatchMoreLeadingFilters(idxDef, node, 0)
	if !reflect.DeepEqual([]int32{0}, leadingPos) {
		t.Fatalf("unexpected leading positions, got=%v, want=%v", leadingPos, []int32{0})
	}
}

func makeEqFilterExpr(colPos int32) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_F{
			F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "="},
				Args: []*planpb.Expr{
					{
						Expr: &planpb.Expr_Col{
							Col: &planpb.ColRef{
								RelPos: 0,
								ColPos: colPos,
							},
						},
					},
					{
						Expr: &planpb.Expr_Lit{
							Lit: &planpb.Literal{
								Value: &planpb.Literal_I64Val{I64Val: 1},
							},
						},
					},
				},
			},
		},
	}
}
