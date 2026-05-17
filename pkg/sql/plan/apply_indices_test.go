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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexOnlyScanGuard_RandomRangesScenario(t *testing.T) {
	// Simulates sysbench random_ranges on a 10M-row table with secondary index on k.
	// Query: SELECT count(k) FROM sbtest1 WHERE k BETWEEN ? AND ? OR k BETWEEN ? AND ? ...
	// With 10 ranges, estimated outcnt ≈ 10000, selectivity ≈ 0.001.
	//
	// The old guard used InFilterCardLimitNonPK (10000) which rejects this case.
	// The new guard uses GetInFilterCardLimitOnPK which for a 10M table returns 1000000.
	tableCnt := float64(10_000_000)
	outcnt := float64(10_000)
	selectivity := 0.001

	// Old behavior (regression): would reject index-only scan
	oldThreshold := float64(InFilterCardLimitNonPK) // 10000
	oldReject := selectivity >= InFilterSelectivityLimit || outcnt >= oldThreshold
	assert.True(t, oldReject, "old guard should reject random_ranges (outcnt=10000 >= threshold=10000)")

	// New behavior (fix): uses PK card limit scaled to table size
	// GetInFilterCardLimitOnPK("", 10M) = min(10M*0.3, 1M) = 1M (capped at InFilterCardLimitPK)
	newThreshold := float64(GetInFilterCardLimitOnPK("", tableCnt))
	newReject := selectivity >= InFilterSelectivityLimit || outcnt >= newThreshold
	assert.False(t, newReject, "new guard should allow random_ranges (outcnt=10000 < threshold=%v)", newThreshold)
	assert.Equal(t, int32(1_000_000), GetInFilterCardLimitOnPK("", tableCnt))

	// The OOM scenario: truly non-selective query on 10M table (selectivity 0.5, outcnt 5M)
	oomOutcnt := float64(5_000_000)
	oomSelectivity := 0.5
	oomRejectOld := oomSelectivity >= InFilterSelectivityLimit || oomOutcnt >= oldThreshold
	oomRejectNew := oomSelectivity >= InFilterSelectivityLimit || oomOutcnt >= newThreshold
	assert.True(t, oomRejectOld, "old guard should reject non-selective scan")
	assert.True(t, oomRejectNew, "new guard should also reject non-selective scan (selectivity >= 0.3)")
}

func TestTryIndexOnlyScan_RandomRangesNotRejected(t *testing.T) {
	// End-to-end test: call tryIndexOnlyScan with a node that simulates
	// sysbench random_ranges (10M rows, outcnt=10000, selectivity=0.001).
	// Verify the guard does NOT reject the index-only scan.
	//
	// Strategy: if the guard rejects, tryIndexOnlyScan returns -1 cleanly.
	// If the guard passes, execution continues to ResolveIndexTableByRef which
	// returns nil (mock has no index table), causing a nil-pointer panic downstream.
	// We use recover() to detect this: panic = guard passed, no panic = guard rejected.
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)

	idxDef := &IndexDef{
		IndexName:      "idx_k",
		Parts:          []string{"k", catalog.FakePrimaryKeyColName},
		Unique:         false,
		IndexTableName: "idx_tbl_k",
		TableExist:     true,
	}

	kColPos := int32(1)
	bindTag := builder.genNewBindTag()

	makeNode := func(tableCnt, outcnt, selectivity float64) *planpb.Node {
		return &planpb.Node{
			BindingTags: []int32{bindTag},
			TableDef: &planpb.TableDef{
				Name: "sbtest1",
				Name2ColIndex: map[string]int32{
					"k":                           kColPos,
					catalog.FakePrimaryKeyColName: 0,
				},
				Cols: []*planpb.ColDef{
					{Name: catalog.FakePrimaryKeyColName, Typ: planpb.Type{Id: int32(types.T_uint64)}},
					{Name: "k", Typ: planpb.Type{Id: int32(types.T_int32)}},
				},
				Pkey: &planpb.PrimaryKeyDef{
					PkeyColName: catalog.FakePrimaryKeyColName,
				},
				Indexes: []*planpb.IndexDef{idxDef},
			},
			Stats: &planpb.Stats{
				TableCnt:    tableCnt,
				Outcnt:      outcnt,
				Selectivity: selectivity,
			},
			FilterList: []*planpb.Expr{
				{
					Expr: &planpb.Expr_F{
						F: &planpb.Function{
							Func: &ObjectRef{ObjName: "between"},
							Args: []*planpb.Expr{
								{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: bindTag, ColPos: kColPos}}},
								{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 100}}}},
								{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 200}}}},
							},
						},
					},
				},
			},
		}
	}

	colRefCnt := map[[2]int32]int{
		{bindTag, kColPos}: 1,
	}

	// Helper: returns true if guard passed (panic from nil deref after guard),
	// false if guard rejected (clean -1 return).
	guardPassed := func(node *planpb.Node) bool {
		passed := false
		func() {
			defer func() {
				if r := recover(); r != nil {
					passed = true
				}
			}()
			builder.tryIndexOnlyScan(idxDef, node, colRefCnt, nil, &Snapshot{})
		}()
		return passed
	}

	// random_ranges scenario: 10M rows, outcnt=10000, selectivity=0.001
	// With the fix (PK card limit = 1M), guard should PASS.
	assert.True(t, guardPassed(makeNode(10_000_000, 10_000, 0.001)),
		"random_ranges (10M rows, outcnt=10000) should pass the guard")

	// Same but with outcnt=50000 (still well below 1M threshold)
	assert.True(t, guardPassed(makeNode(10_000_000, 50_000, 0.005)),
		"moderate outcnt (50000) on 10M table should pass the guard")

	// High outcnt (2M on 10M table) → guard should REJECT
	assert.False(t, guardPassed(makeNode(10_000_000, 2_000_000, 0.2)),
		"high outcnt (2M) should be rejected by guard")

	// High selectivity (0.5) → guard should REJECT
	assert.False(t, guardPassed(makeNode(10_000_000, 5_000_000, 0.5)),
		"high selectivity (0.5) should be rejected by guard")

	// Verify threshold arithmetic
	assert.True(t, 10_000 >= InFilterCardLimitNonPK,
		"random_ranges outcnt (10000) >= old threshold (10000) → old code would reject")
	assert.True(t, float64(10_000) < float64(GetInFilterCardLimitOnPK("", 10_000_000)),
		"random_ranges outcnt (10000) < new threshold (1M) → new code allows")
}

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

func TestSuspendScanProtection_PreservesNewProtectionAlongsideOriginal(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	const scanID int32 = 89

	builder.protectedScans[scanID] = 3
	restore := builder.suspendScanProtection(scanID)
	builder.protectedScans[scanID] = 2

	restore()

	assert.Equal(t, 5, builder.protectedScans[scanID])
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

func TestCalculateFilteredPostModeOverFetchFactor_ActualValues(t *testing.T) {
	testCases := []struct {
		limit    uint64
		expected float64
	}{
		{
			limit:    3,
			expected: 5.0,
		},
		{
			limit:    10,
			expected: 5.0,
		},
		{
			limit:    49,
			expected: 5.0,
		},
		{
			limit:    50,
			expected: 2.0,
		},
		{
			limit:    99,
			expected: 2.0,
		},
		{
			limit:    100,
			expected: 1.5,
		},
		{
			limit:    199,
			expected: 1.5,
		},
		{
			limit:    200,
			expected: 1.3,
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			assert.Equal(t, tc.expected, calculateFilteredPostModeOverFetchFactor(tc.limit))
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
	require.NotNil(t, scanNode.IndexReaderParam)
	require.Len(t, scanNode.IndexReaderParam.OrderBy, 1)
	assert.Equal(t, uint64(20), scanNode.IndexReaderParam.Limit.GetLit().GetU64Val())
	indexParamCol := scanNode.IndexReaderParam.OrderBy[0].Expr.GetCol()
	require.NotNil(t, indexParamCol)
	assert.Equal(t, int32(100), indexParamCol.RelPos)
	assert.Equal(t, int32(0), indexParamCol.ColPos)
	assert.Equal(t, catalog.IndexTableIndexColName, indexParamCol.Name)
	assert.Equal(t, planpb.OrderBySpec_DESC, scanNode.IndexReaderParam.OrderBy[0].Flag)

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
	require.NotNil(t, scanNode.IndexReaderParam)
	require.Len(t, scanNode.IndexReaderParam.OrderBy, 1)
	assert.Equal(t, uint64(20), scanNode.IndexReaderParam.Limit.GetLit().GetU64Val())
	assert.Equal(t, planpb.OrderBySpec_OrderByFlag(0), scanNode.IndexReaderParam.OrderBy[0].Flag)
	assert.Equal(t, catalog.IndexTableIndexColName, scanNode.IndexReaderParam.OrderBy[0].Expr.GetCol().Name)
}

func TestHandleMessageFromTopToScanRewritesRegularIndexPKOrderToHiddenKey(t *testing.T) {
	builder, rootNodeID := makeTestRegularIndexMessageBuilder(t, 2, 1, planpb.OrderBySpec_DESC)

	builder.handleMessageFromTopToScan(rootNodeID)

	scanNode := builder.qry.Nodes[0]
	sortNode := builder.qry.Nodes[1]

	require.Len(t, sortNode.SendMsgList, 1)
	require.Len(t, scanNode.RecvMsgList, 1)
	require.Len(t, scanNode.OrderBy, 1)
	require.NotNil(t, scanNode.IndexReaderParam)
	require.Len(t, scanNode.IndexReaderParam.OrderBy, 1)
	assert.Equal(t, uint64(20), scanNode.IndexReaderParam.Limit.GetLit().GetU64Val())

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
	indexParamCol := scanNode.IndexReaderParam.OrderBy[0].Expr.GetCol()
	require.NotNil(t, indexParamCol)
	assert.Equal(t, int32(100), indexParamCol.RelPos)
	assert.Equal(t, int32(0), indexParamCol.ColPos)
	assert.Equal(t, catalog.IndexTableIndexColName, indexParamCol.Name)
}

func TestHandleMessageFromTopToScanKeepsPKOrderWhenPrefixIncomplete(t *testing.T) {
	builder, rootNodeID := makeTestRegularIndexMessageBuilder(t, 1, 1, planpb.OrderBySpec_DESC)

	builder.handleMessageFromTopToScan(rootNodeID)

	scanNode := builder.qry.Nodes[0]
	sortNode := builder.qry.Nodes[1]

	require.Len(t, sortNode.SendMsgList, 1)
	require.Len(t, scanNode.RecvMsgList, 1)
	require.Len(t, scanNode.OrderBy, 1)
	assert.Nil(t, scanNode.IndexReaderParam)

	sortOrderCol := sortNode.OrderBy[0].Expr.GetCol()
	require.NotNil(t, sortOrderCol)
	assert.Equal(t, int32(1), sortOrderCol.ColPos)
	assert.Equal(t, catalog.IndexTablePrimaryColName, sortOrderCol.Name)

	scanOrderCol := scanNode.OrderBy[0].Expr.GetCol()
	require.NotNil(t, scanOrderCol)
	assert.Equal(t, int32(1), scanOrderCol.ColPos)
	assert.Equal(t, catalog.IndexTablePrimaryColName, scanOrderCol.Name)
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
	assert.Nil(t, scanNode.IndexReaderParam)
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
	assert.Nil(t, scanNode.IndexReaderParam)
	require.Len(t, sortProjectNode.ProjectList, 1)
}

func TestHandleMessageFromTopToScanSkipsOrderedLimitForOffsetOrRank(t *testing.T) {
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
						Lit: &planpb.Literal{Value: &planpb.Literal_U64Val{U64Val: 3}},
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
			assert.Nil(t, scanNode.IndexReaderParam)
		})
	}
}

func TestApplyIndicesForProjectSkipsOrderedLimitForOffsetOrRank(t *testing.T) {
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
						Lit: &planpb.Literal{Value: &planpb.Literal_U64Val{U64Val: 3}},
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
			builder, rootNodeID := makeTestRegularIndexProjectBuilder(
				t,
				2,
				GetColExpr(planpb.Type{Id: int32(types.T_int64)}, 100, 1),
				planpb.OrderBySpec_DESC,
			)
			sortNode := builder.qry.Nodes[2]
			tc.setup(sortNode)

			_, err := builder.applyIndicesForProject(rootNodeID, builder.qry.Nodes[rootNodeID], map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
			require.NoError(t, err)

			scanNode := builder.qry.Nodes[0]
			assert.Empty(t, sortNode.SendMsgList)
			assert.Empty(t, scanNode.RecvMsgList)
			assert.Empty(t, scanNode.OrderBy)
			assert.Nil(t, scanNode.IndexReaderParam)
		})
	}
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

func TestGetIndexForNonEquiCond_DetectsPairedRangeOnIndexColumn(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindTag := builder.genNewBindTag()
	idxDef := &IndexDef{
		IndexName:      "idx_price",
		Parts:          []string{"price", catalog.FakePrimaryKeyColName},
		Unique:         false,
		IndexTableName: "__mo_index_secondary_idx_price",
	}

	node := &planpb.Node{
		BindingTags: []int32{bindTag},
		TableDef: &planpb.TableDef{
			Name2ColIndex: map[string]int32{
				catalog.FakePrimaryKeyColName: 0,
				"price":                       1,
			},
			Cols: []*planpb.ColDef{
				{Name: catalog.FakePrimaryKeyColName, Typ: planpb.Type{Id: int32(types.T_uint64)}},
				{Name: "price", Typ: planpb.Type{Id: int32(types.T_int64)}},
			},
			Pkey:    &planpb.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
			Indexes: []*planpb.IndexDef{idxDef},
		},
		FilterList: []*planpb.Expr{
			makeRangeFilterExpr(bindTag, 1, ">=", 99),
			makeRangeFilterExpr(bindTag, 1, "<=", 299),
		},
	}

	idxPos, filterIdx := builder.getIndexForNonEquiCond([]*planpb.IndexDef{idxDef}, node)
	require.Equal(t, 0, idxPos)
	require.ElementsMatch(t, []int32{0, 1}, filterIdx)
}

func TestReplaceRangePairCondition_UsesPrefixBetweenForSecondaryIndex(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindTag := builder.genNewBindTag()
	filters := []*planpb.Expr{
		makeRangeFilterExpr(bindTag, 1, ">=", 99),
		makeRangeFilterExpr(bindTag, 1, "<=", 299),
	}
	filters[0].Selectivity = 0.3
	filters[1].Selectivity = 0.4

	idxTableDef := &planpb.TableDef{
		Cols: []*planpb.ColDef{
			{
				Name: catalog.IndexTableIndexColName,
				Typ:  planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
			},
			{
				Name: catalog.IndexTablePrimaryColName,
				Typ:  planpb.Type{Id: int32(types.T_uint64)},
			},
		},
	}

	expr := builder.replaceRangePairCondition(filters, []int32{0, 1}, 42, idxTableDef, 2)
	require.NotNil(t, expr.GetF())
	require.Equal(t, "prefix_between", expr.GetF().Func.ObjName)
	require.InDelta(t, 0.12, expr.Selectivity, 1e-9)
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

func makeRangeFilterExpr(relPos, colPos int32, op string, val int64) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_F{
			F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: op},
				Args: []*planpb.Expr{
					{
						Expr: &planpb.Expr_Col{
							Col: &planpb.ColRef{
								RelPos: relPos,
								ColPos: colPos,
							},
						},
					},
					{
						Expr: &planpb.Expr_Lit{
							Lit: &planpb.Literal{
								Value: &planpb.Literal_I64Val{I64Val: val},
							},
						},
					},
				},
			},
		},
	}
}

func makeSpatialConstGeometryExpr() *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_F{
			F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "st_geomfromtext"},
				Args: []*planpb.Expr{
					{
						Expr: &planpb.Expr_Lit{
							Lit: &planpb.Literal{
								Value: &planpb.Literal_Sval{Sval: "POINT(1 1)"},
							},
						},
					},
				},
			},
		},
	}
}

func makeSpatialColExpr(colPos int32) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_Col{
			Col: &planpb.ColRef{
				RelPos: 0,
				ColPos: colPos,
			},
		},
	}
}

func makeSpatialDistanceExpr(left, right *planpb.Expr) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_F{
			F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "st_distance"},
				Args: []*planpb.Expr{left, right},
			},
		},
	}
}

func makeComparisonExpr(op string, left, right *planpb.Expr) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_F{
			F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: op},
				Args: []*planpb.Expr{left, right},
			},
		},
	}
}

func makeInt64LiteralExpr(v int64) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_Lit{
			Lit: &planpb.Literal{
				Value: &planpb.Literal_I64Val{I64Val: v},
			},
		},
	}
}

func TestCheckSpatialIndexFilterPredicate(t *testing.T) {
	filter := &planpb.Expr{
		Expr: &planpb.Expr_F{
			F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "st_intersects"},
				Args: []*planpb.Expr{
					makeSpatialColExpr(1),
					makeSpatialConstGeometryExpr(),
				},
			},
		},
	}

	col := checkSpatialIndexFilter(filter)
	require.NotNil(t, col)
	require.Equal(t, int32(1), col.ColPos)
}

func TestCheckSpatialIndexFilterDistanceComparison(t *testing.T) {
	filter := makeComparisonExpr(
		"<=",
		makeSpatialDistanceExpr(makeSpatialColExpr(2), makeSpatialConstGeometryExpr()),
		makeInt64LiteralExpr(0),
	)

	col := checkSpatialIndexFilter(filter)
	require.NotNil(t, col)
	require.Equal(t, int32(2), col.ColPos)
}

func TestCheckSpatialIndexFilterDistanceComparisonConstOnLeft(t *testing.T) {
	filter := makeComparisonExpr(
		">=",
		makeInt64LiteralExpr(0),
		makeSpatialDistanceExpr(makeSpatialColExpr(3), makeSpatialConstGeometryExpr()),
	)

	col := checkSpatialIndexFilter(filter)
	require.NotNil(t, col)
	require.Equal(t, int32(3), col.ColPos)
}

func TestCheckSpatialIndexFilterDistanceRejectsNonConstGeometryArg(t *testing.T) {
	filter := makeComparisonExpr(
		"<=",
		makeSpatialDistanceExpr(makeSpatialColExpr(1), makeSpatialColExpr(2)),
		makeInt64LiteralExpr(0),
	)

	require.Nil(t, checkSpatialIndexFilter(filter))
}

func TestCheckIndexFilter_RangeOps(t *testing.T) {
	colExpr := makeSpatialColExpr(5)
	constExpr := makeInt64LiteralExpr(10)

	tests := []struct {
		name       string
		op         string
		left       *planpb.Expr
		right      *planpb.Expr
		wantType   int
		wantColPos int32
	}{
		{"col >= const", ">=", colExpr, constExpr, NonEqualIndexCondition, 5},
		{"col <= const", "<=", colExpr, constExpr, NonEqualIndexCondition, 5},
		{"col > const", ">", colExpr, constExpr, NonEqualIndexCondition, 5},
		{"col < const", "<", colExpr, constExpr, NonEqualIndexCondition, 5},
		{"const >= col", ">=", constExpr, colExpr, NonEqualIndexCondition, 5},
		{"const <= col", "<=", constExpr, colExpr, NonEqualIndexCondition, 5},
		{"const > col", ">", constExpr, colExpr, NonEqualIndexCondition, 5},
		{"const < col", "<", constExpr, colExpr, NonEqualIndexCondition, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := makeComparisonExpr(tt.op, tt.left, tt.right)
			fn := filter.GetF()
			gotType, gotCol := checkIndexFilter(fn)
			assert.Equal(t, tt.wantType, gotType)
			require.NotNil(t, gotCol)
			assert.Equal(t, tt.wantColPos, gotCol.ColPos)
		})
	}
}

func TestCanonicalRangeOp(t *testing.T) {
	colExpr := makeSpatialColExpr(1)
	constExpr := makeInt64LiteralExpr(5)

	tests := []struct {
		name  string
		op    string
		left  *planpb.Expr
		right *planpb.Expr
		want  string
	}{
		{"col >= const → >=", ">=", colExpr, constExpr, ">="},
		{"col > const → >", ">", colExpr, constExpr, ">"},
		{"col <= const → <=", "<=", colExpr, constExpr, "<="},
		{"col < const → <", "<", colExpr, constExpr, "<"},
		{"const >= col → <=", ">=", constExpr, colExpr, "<="},
		{"const > col → <", ">", constExpr, colExpr, "<"},
		{"const <= col → >=", "<=", constExpr, colExpr, ">="},
		{"const < col → >", "<", constExpr, colExpr, ">"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := makeComparisonExpr(tt.op, tt.left, tt.right)
			fn := filter.GetF()
			got := canonicalRangeOp(fn)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRangeFilterConstValue(t *testing.T) {
	colExpr := makeSpatialColExpr(1)
	constExpr := makeInt64LiteralExpr(42)

	// col >= const: value is const (right side)
	filter1 := makeComparisonExpr(">=", colExpr, constExpr)
	val1 := rangeFilterConstValue(filter1.GetF())
	require.NotNil(t, val1)
	assert.Equal(t, int64(42), val1.GetLit().GetI64Val())

	// const < col: value is const (left side)
	filter2 := makeComparisonExpr("<", constExpr, colExpr)
	val2 := rangeFilterConstValue(filter2.GetF())
	require.NotNil(t, val2)
	assert.Equal(t, int64(42), val2.GetLit().GetI64Val())
}
