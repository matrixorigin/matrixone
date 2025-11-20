// Copyright 2025 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func makeQueryWithScan(tableType string, rowsize float64, blockNum int32) *plan.Query {
	n := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		TableDef: &plan.TableDef{TableType: tableType},
		Stats: &plan.Stats{
			Rowsize:  rowsize,
			BlockNum: blockNum,
		},
	}
	return &plan.Query{
		Nodes: []*plan.Node{n},
		Steps: []int32{0},
	}
}

func TestGetExecType_VectorIndex_WideRows_OneCN(t *testing.T) {
	// rowsize just above threshold, blockNum between oneCN and multiCN thresholds
	q := makeQueryWithScan(catalog.SystemSI_IVFFLAT_TblType_Entries, float64(RowSizeThreshold+1), LargeBlockThresholdForOneCN+1)
	got := GetExecType(q, false, false)
	if got != ExecTypeAP_ONECN {
		t.Fatalf("expected ExecTypeAP_ONECN, got %v", got)
	}
}

func TestGetExecType_VectorIndex_WideRows_MultiCN(t *testing.T) {
	q := makeQueryWithScan(catalog.Hnsw_TblType_Storage, float64(RowSizeThreshold+1), LargeBlockThresholdForMultiCN+1)
	got := GetExecType(q, false, false)
	if got != ExecTypeAP_MULTICN {
		t.Fatalf("expected ExecTypeAP_MULTICN, got %v", got)
	}
}

func TestGetExecType_NonVectorTable_NotForcedByRowsize(t *testing.T) {
	// Non-vector tables should not trigger rowsize shortcut; with small blockNum, expect TP
	q := makeQueryWithScan("normal_table", float64(RowSizeThreshold+10), LargeBlockThresholdForOneCN)
	got := GetExecType(q, false, false)
	if got != ExecTypeTP {
		t.Fatalf("expected ExecTypeTP for non-vector table, got %v", got)
	}
}

// TestCalcNodeDOP_DistinctAggregation tests that distinct aggregation nodes
// are correctly set to Dop=1 and ForceOneCN=true
func TestCalcNodeDOP_DistinctAggregation(t *testing.T) {
	// Create a plan with an AGG node containing COUNT(DISTINCT ...)
	// Use a variable to avoid constant overflow when combining COUNT with Distinct flag
	countVal := uint64(function.COUNT)
	distinctVal := uint64(function.Distinct)
	countWithDistinct := int64(countVal | distinctVal)
	p := &plan.Plan{
		Plan: &plan.Plan_Query{
			Query: &plan.Query{
				Nodes: []*plan.Node{
					// Child node (scan)
					{
						NodeId:   0,
						NodeType: plan.Node_TABLE_SCAN,
						Stats:    DefaultStats(),
					},
					// AGG node with COUNT(DISTINCT ...)
					{
						NodeId:   1,
						NodeType: plan.Node_AGG,
						Children: []int32{0},
						Stats:    DefaultStats(),
						AggList: []*plan.Expr{
							{
								Expr: &plan.Expr_F{
									F: &plan.Function{
										Func: &plan.ObjectRef{
											// COUNT with Distinct flag: use uint64 to avoid overflow, then convert to int64
											// Similar to having_binder.go:144, we need to convert to uint64 first
											Obj:     countWithDistinct,
											ObjName: "count",
										},
										Args: []*plan.Expr{
											{
												Typ: plan.Type{Id: int32(types.T_int64)},
												Expr: &plan.Expr_Col{
													Col: &plan.ColRef{ColPos: 0},
												},
											},
										},
									},
								},
							},
						},
					},
				},
				Steps: []int32{1},
			},
		},
		IsPrepare: false,
	}

	// Call CalcNodeDOP with multiple CPUs
	ncpu := int32(8)
	lencn := 2
	CalcNodeDOP(p, 1, ncpu, lencn)

	// Verify that the AGG node has Dop=1 and ForceOneCN=true
	aggNode := p.GetQuery().Nodes[1]
	require.NotNil(t, aggNode.Stats, "AGG node should have Stats")
	require.Equal(t, int32(1), aggNode.Stats.Dop, "Distinct aggregation should have Dop=1")
	require.True(t, aggNode.Stats.ForceOneCN, "Distinct aggregation should have ForceOneCN=true")

	// Verify that child node also has Dop=1 (recursively set)
	childNode := p.GetQuery().Nodes[0]
	require.NotNil(t, childNode.Stats, "Child node should have Stats")
	require.Equal(t, int32(1), childNode.Stats.Dop, "Child node should have Dop=1 (recursively set)")
}

// TestCalcNodeDOP_NonDistinctAggregation tests that non-distinct aggregation nodes
// are not affected by the distinct aggregation logic
func TestCalcNodeDOP_NonDistinctAggregation(t *testing.T) {
	// Create a plan with an AGG node containing COUNT (without DISTINCT)
	p := &plan.Plan{
		Plan: &plan.Plan_Query{
			Query: &plan.Query{
				Nodes: []*plan.Node{
					// Child node (scan)
					{
						NodeId:   0,
						NodeType: plan.Node_TABLE_SCAN,
						Stats:    DefaultStats(),
					},
					// AGG node with COUNT (no DISTINCT)
					{
						NodeId:   1,
						NodeType: plan.Node_AGG,
						Children: []int32{0},
						Stats:    DefaultStats(),
						AggList: []*plan.Expr{
							{
								Expr: &plan.Expr_F{
									F: &plan.Function{
										Func: &plan.ObjectRef{
											Obj:     int64(function.COUNT), // COUNT without Distinct flag
											ObjName: "count",
										},
										Args: []*plan.Expr{
											{
												Typ: plan.Type{Id: int32(types.T_int64)},
												Expr: &plan.Expr_Col{
													Col: &plan.ColRef{ColPos: 0},
												},
											},
										},
									},
								},
							},
						},
					},
				},
				Steps: []int32{1},
			},
		},
		IsPrepare: false,
	}

	// Call CalcNodeDOP with multiple CPUs
	ncpu := int32(8)
	lencn := 2
	CalcNodeDOP(p, 1, ncpu, lencn)

	// Verify that the AGG node does NOT have ForceOneCN=true
	aggNode := p.GetQuery().Nodes[1]
	require.NotNil(t, aggNode.Stats, "AGG node should have Stats")
	require.False(t, aggNode.Stats.ForceOneCN, "Non-distinct aggregation should NOT have ForceOneCN=true")
	// Dop should be calculated normally (not forced to 1)
	require.Greater(t, aggNode.Stats.Dop, int32(0), "Non-distinct aggregation should have Dop > 0")
}

// TestCalcNodeDOP_DistinctAggregationWithNilStats tests that distinct aggregation
// nodes with nil Stats are handled correctly
func TestCalcNodeDOP_DistinctAggregationWithNilStats(t *testing.T) {
	// Create a plan with an AGG node containing COUNT(DISTINCT ...) but no Stats
	// Use a variable to avoid constant overflow when combining COUNT with Distinct flag
	countVal := uint64(function.COUNT)
	distinctVal := uint64(function.Distinct)
	countWithDistinct := int64(countVal | distinctVal)
	p := &plan.Plan{
		Plan: &plan.Plan_Query{
			Query: &plan.Query{
				Nodes: []*plan.Node{
					// Child node (scan)
					{
						NodeId:   0,
						NodeType: plan.Node_TABLE_SCAN,
						Stats:    DefaultStats(),
					},
					// AGG node with COUNT(DISTINCT ...) but nil Stats
					{
						NodeId:   1,
						NodeType: plan.Node_AGG,
						Children: []int32{0},
						Stats:    nil, // nil Stats
						AggList: []*plan.Expr{
							{
								Expr: &plan.Expr_F{
									F: &plan.Function{
										Func: &plan.ObjectRef{
											// COUNT with Distinct flag: use uint64 to avoid overflow, then convert to int64
											// Similar to having_binder.go:144, we need to convert to uint64 first
											Obj:     countWithDistinct,
											ObjName: "count",
										},
										Args: []*plan.Expr{
											{
												Typ: plan.Type{Id: int32(types.T_int64)},
												Expr: &plan.Expr_Col{
													Col: &plan.ColRef{ColPos: 0},
												},
											},
										},
									},
								},
							},
						},
					},
				},
				Steps: []int32{1},
			},
		},
		IsPrepare: false,
	}

	// Call CalcNodeDOP
	ncpu := int32(8)
	lencn := 2
	CalcNodeDOP(p, 1, ncpu, lencn)

	// Verify that Stats was created and set correctly
	aggNode := p.GetQuery().Nodes[1]
	require.NotNil(t, aggNode.Stats, "Stats should be created for distinct aggregation")
	require.Equal(t, int32(1), aggNode.Stats.Dop, "Distinct aggregation should have Dop=1")
	require.True(t, aggNode.Stats.ForceOneCN, "Distinct aggregation should have ForceOneCN=true")
}
