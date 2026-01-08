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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	index2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func makeQueryWithScan(tableType string, rowsize float64, blockNum int32) *planpb.Query {
	n := &planpb.Node{
		NodeType: planpb.Node_TABLE_SCAN,
		TableDef: &planpb.TableDef{TableType: tableType},
		Stats: &planpb.Stats{
			Rowsize:  rowsize,
			BlockNum: blockNum,
		},
	}
	return &planpb.Query{
		Nodes: []*planpb.Node{n},
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

// TestUpdateStatsInfo_Decimal64_NegativeValues tests that negative decimal64 values
// are correctly converted to float64 for statistics
func TestUpdateStatsInfo_Decimal64_NegativeValues(t *testing.T) {
	// Test case: DECIMAL(10, 2) with negative values
	// Example: -123.45 and 456.78
	scale := int32(2)

	// Create negative decimal: -123.45
	negativeValue, err := types.Decimal64FromFloat64(-123.45, 10, scale)
	require.NoError(t, err)

	// Create positive decimal: 456.78
	positiveValue, err := types.Decimal64FromFloat64(456.78, 10, scale)
	require.NoError(t, err)

	// Create zonemap with negative min and positive max
	zm := index2.NewZM(types.T_decimal64, scale)
	minBuf := types.EncodeDecimal64(&negativeValue)
	maxBuf := types.EncodeDecimal64(&positiveValue)
	index2.UpdateZM(zm, minBuf)
	index2.UpdateZM(zm, maxBuf)

	// Create table definition with decimal64 column
	tableDef := &planpb.TableDef{
		Name: "test_table",
		Cols: []*planpb.ColDef{
			{
				Name: "balance",
				Typ: planpb.Type{
					Id:    int32(types.T_decimal64),
					Scale: scale,
					Width: 10,
				},
			},
			{
				Name: catalog.Row_ID,
			},
		},
	}

	// Create TableStatsInfo
	info := &TableStatsInfo{
		ColumnZMs:     []index2.ZM{zm},
		DataTypes:     []types.Type{types.New(types.T_decimal64, 10, scale)},
		ColumnNDVs:    []float64{2},
		NullCnts:      []int64{0},
		ColumnSize:    []int64{8},
		ShuffleRanges: []*pb.ShuffleRange{nil},
	}

	// Create StatsInfo
	statsInfo := &pb.StatsInfo{
		MinValMap:       make(map[string]float64),
		MaxValMap:       make(map[string]float64),
		NdvMap:          make(map[string]float64),
		DataTypeMap:     make(map[string]uint64),
		NullCntMap:      make(map[string]uint64),
		SizeMap:         make(map[string]uint64),
		ShuffleRangeMap: make(map[string]*pb.ShuffleRange),
	}

	// Call UpdateStatsInfo
	UpdateStatsInfo(info, tableDef, statsInfo)

	// Verify results
	minVal := statsInfo.MinValMap["balance"]
	maxVal := statsInfo.MaxValMap["balance"]

	// The key assertion: min should be less than max
	require.Less(t, minVal, maxVal, "Min value should be less than max value")

	// Verify approximate values (allowing for floating point precision)
	require.InDelta(t, -123.45, minVal, 0.01, "Min value should be approximately -123.45")
	require.InDelta(t, 456.78, maxVal, 0.01, "Max value should be approximately 456.78")

	// Before the fix, minVal would have been a huge positive number like 18446744073514074000
	// This check ensures that didn't happen
	require.Greater(t, minVal, -1000.0, "Min value should not be an extremely large number")
	require.Less(t, minVal, 0.0, "Min value should be negative")
}

// TestUpdateStatsInfo_Decimal128_NegativeValues tests that negative decimal128 values
// are correctly converted with proper scale
func TestUpdateStatsInfo_Decimal128_NegativeValues(t *testing.T) {
	// Test case: DECIMAL(20, 4) with negative values
	scale := int32(4)

	// Create negative decimal: -9876543210.1234
	negativeValue, err := types.Decimal128FromFloat64(-9876543210.1234, 20, scale)
	require.NoError(t, err)

	// Create positive decimal: 1234567890.5678
	positiveValue, err := types.Decimal128FromFloat64(1234567890.5678, 20, scale)
	require.NoError(t, err)

	// Create zonemap
	zm := index2.NewZM(types.T_decimal128, scale)
	minBuf := types.EncodeDecimal128(&negativeValue)
	maxBuf := types.EncodeDecimal128(&positiveValue)
	index2.UpdateZM(zm, minBuf)
	index2.UpdateZM(zm, maxBuf)

	// Create table definition
	tableDef := &planpb.TableDef{
		Name: "test_table",
		Cols: []*planpb.ColDef{
			{
				Name: "amount",
				Typ: planpb.Type{
					Id:    int32(types.T_decimal128),
					Scale: scale,
					Width: 20,
				},
			},
			{
				Name: catalog.Row_ID,
			},
		},
	}

	// Create TableStatsInfo
	info := &TableStatsInfo{
		ColumnZMs:     []index2.ZM{zm},
		DataTypes:     []types.Type{types.New(types.T_decimal128, 20, scale)},
		ColumnNDVs:    []float64{2},
		NullCnts:      []int64{0},
		ColumnSize:    []int64{16},
		ShuffleRanges: []*pb.ShuffleRange{nil},
	}

	// Create StatsInfo
	statsInfo := &pb.StatsInfo{
		MinValMap:       make(map[string]float64),
		MaxValMap:       make(map[string]float64),
		NdvMap:          make(map[string]float64),
		DataTypeMap:     make(map[string]uint64),
		NullCntMap:      make(map[string]uint64),
		SizeMap:         make(map[string]uint64),
		ShuffleRangeMap: make(map[string]*pb.ShuffleRange),
	}

	// Call UpdateStatsInfo
	UpdateStatsInfo(info, tableDef, statsInfo)

	// Verify results
	minVal := statsInfo.MinValMap["amount"]
	maxVal := statsInfo.MaxValMap["amount"]

	// The key assertion: min should be less than max
	require.Less(t, minVal, maxVal, "Min value should be less than max value")

	// Verify approximate values
	require.InDelta(t, -9876543210.1234, minVal, 0.01, "Min value should be approximately -9876543210.1234")
	require.InDelta(t, 1234567890.5678, maxVal, 0.01, "Max value should be approximately 1234567890.5678")

	// Ensure min is negative and within reasonable range
	require.Less(t, minVal, 0.0, "Min value should be negative")
}

// TestUpdateStatsInfo_Decimal_DifferentScales tests decimal conversion with various scales
func TestUpdateStatsInfo_Decimal_DifferentScales(t *testing.T) {
	testCases := []struct {
		name     string
		scale    int32
		minFloat float64
		maxFloat float64
	}{
		{"scale_0", 0, -100.0, 200.0},
		{"scale_2", 2, -99.99, 199.99},
		{"scale_4", 4, -1234.5678, 5678.1234},
		{"scale_6", 6, -0.123456, 0.987654},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create decimal values
			minDec, err := types.Decimal64FromFloat64(tc.minFloat, 18, tc.scale)
			require.NoError(t, err)

			maxDec, err := types.Decimal64FromFloat64(tc.maxFloat, 18, tc.scale)
			require.NoError(t, err)

			// Create zonemap
			zm := index2.NewZM(types.T_decimal64, tc.scale)
			minBuf := types.EncodeDecimal64(&minDec)
			maxBuf := types.EncodeDecimal64(&maxDec)
			index2.UpdateZM(zm, minBuf)
			index2.UpdateZM(zm, maxBuf)

			// Create table definition
			tableDef := &planpb.TableDef{
				Name: "test_table",
				Cols: []*planpb.ColDef{
					{
						Name: "value",
						Typ: planpb.Type{
							Id:    int32(types.T_decimal64),
							Scale: tc.scale,
							Width: 18,
						},
					},
					{
						Name: catalog.Row_ID,
					},
				},
			}

			// Create TableStatsInfo
			info := &TableStatsInfo{
				ColumnZMs:     []index2.ZM{zm},
				DataTypes:     []types.Type{types.New(types.T_decimal64, 18, tc.scale)},
				ColumnNDVs:    []float64{2},
				NullCnts:      []int64{0},
				ColumnSize:    []int64{8},
				ShuffleRanges: []*pb.ShuffleRange{nil},
			}

			// Create StatsInfo
			statsInfo := &pb.StatsInfo{
				MinValMap:       make(map[string]float64),
				MaxValMap:       make(map[string]float64),
				NdvMap:          make(map[string]float64),
				DataTypeMap:     make(map[string]uint64),
				NullCntMap:      make(map[string]uint64),
				SizeMap:         make(map[string]uint64),
				ShuffleRangeMap: make(map[string]*pb.ShuffleRange),
			}

			// Call UpdateStatsInfo
			UpdateStatsInfo(info, tableDef, statsInfo)

			// Verify results
			minVal := statsInfo.MinValMap["value"]
			maxVal := statsInfo.MaxValMap["value"]

			require.Less(t, minVal, maxVal, "Min value should be less than max value")
			require.InDelta(t, tc.minFloat, minVal, 0.01, "Min value mismatch")
			require.InDelta(t, tc.maxFloat, maxVal, 0.01, "Max value mismatch")
		})
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
	p := &planpb.Plan{
		Plan: &planpb.Plan_Query{
			Query: &planpb.Query{
				Nodes: []*planpb.Node{
					// Child node (scan)
					{
						NodeId:   0,
						NodeType: planpb.Node_TABLE_SCAN,
						Stats:    DefaultStats(),
					},
					// AGG node with COUNT(DISTINCT ...)
					{
						NodeId:   1,
						NodeType: planpb.Node_AGG,
						Children: []int32{0},
						Stats:    DefaultStats(),
						AggList: []*planpb.Expr{
							{
								Expr: &planpb.Expr_F{
									F: &planpb.Function{
										Func: &planpb.ObjectRef{
											// COUNT with Distinct flag: use uint64 to avoid overflow, then convert to int64
											// Similar to having_binder.go:144, we need to convert to uint64 first
											Obj:     countWithDistinct,
											ObjName: "count",
										},
										Args: []*planpb.Expr{
											{
												Typ: planpb.Type{Id: int32(types.T_int64)},
												Expr: &planpb.Expr_Col{
													Col: &planpb.ColRef{ColPos: 0},
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
	p := &planpb.Plan{
		Plan: &planpb.Plan_Query{
			Query: &planpb.Query{
				Nodes: []*planpb.Node{
					// Child node (scan)
					{
						NodeId:   0,
						NodeType: planpb.Node_TABLE_SCAN,
						Stats:    DefaultStats(),
					},
					// AGG node with COUNT (no DISTINCT)
					{
						NodeId:   1,
						NodeType: planpb.Node_AGG,
						Children: []int32{0},
						Stats:    DefaultStats(),
						AggList: []*planpb.Expr{
							{
								Expr: &planpb.Expr_F{
									F: &planpb.Function{
										Func: &planpb.ObjectRef{
											Obj:     int64(function.COUNT), // COUNT without Distinct flag
											ObjName: "count",
										},
										Args: []*planpb.Expr{
											{
												Typ: planpb.Type{Id: int32(types.T_int64)},
												Expr: &planpb.Expr_Col{
													Col: &planpb.ColRef{ColPos: 0},
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
	p := &planpb.Plan{
		Plan: &planpb.Plan_Query{
			Query: &planpb.Query{
				Nodes: []*planpb.Node{
					// Child node (scan)
					{
						NodeId:   0,
						NodeType: planpb.Node_TABLE_SCAN,
						Stats:    DefaultStats(),
					},
					// AGG node with COUNT(DISTINCT ...) but nil Stats
					{
						NodeId:   1,
						NodeType: planpb.Node_AGG,
						Children: []int32{0},
						Stats:    nil, // nil Stats
						AggList: []*planpb.Expr{
							{
								Expr: &planpb.Expr_F{
									F: &planpb.Function{
										Func: &planpb.ObjectRef{
											// COUNT with Distinct flag: use uint64 to avoid overflow, then convert to int64
											// Similar to having_binder.go:144, we need to convert to uint64 first
											Obj:     countWithDistinct,
											ObjName: "count",
										},
										Args: []*planpb.Expr{
											{
												Typ: planpb.Type{Id: int32(types.T_int64)},
												Expr: &planpb.Expr_Col{
													Col: &planpb.ColRef{ColPos: 0},
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

func TestGetExprNdv(t *testing.T) {
	ctx := context.Background()
	mockCtx := &MockCompilerContext{ctx: ctx}
	builder := NewQueryBuilder(plan.Query_SELECT, mockCtx, false, false)

	// Setup test table with stats
	builder.qry.Nodes = append(builder.qry.Nodes, &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		Stats: &plan.Stats{
			TableCnt: 1000,
		},
	})

	// Mock column stats
	colRef := &plan.ColRef{
		RelPos: 0,
		ColPos: 0,
		Name:   "test_col",
	}
	builder.tag2Table = make(map[int32]*TableDef)
	builder.tag2Table[0] = &TableDef{
		Name: "test_table",
		Cols: []*ColDef{
			{
				Name: "test_col",
				Typ:  plan.Type{Id: int32(types.T_int64)},
			},
		},
	}

	t.Run("year function", func(t *testing.T) {
		expr := &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "year"},
					Args: []*plan.Expr{
						{Expr: &plan.Expr_Col{Col: colRef}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		// year divides by 365, so result can be negative if col ndv is -1
		require.NotEqual(t, 0.0, ndv)
	})

	t.Run("substring function", func(t *testing.T) {
		expr := &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "substring"},
					Args: []*plan.Expr{
						{Expr: &plan.Expr_Col{Col: colRef}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		require.LessOrEqual(t, ndv, 25.0)
	})

	t.Run("mod with i64 literal", func(t *testing.T) {
		expr := &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "%"},
					Args: []*plan.Expr{
						{Expr: &plan.Expr_Col{Col: colRef}},
						{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 10}}}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		require.Equal(t, 10.0, ndv)
	})

	t.Run("mod with i32 literal", func(t *testing.T) {
		expr := &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "mod"},
					Args: []*plan.Expr{
						{Expr: &plan.Expr_Col{Col: colRef}},
						{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I32Val{I32Val: 5}}}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		require.Equal(t, 5.0, ndv)
	})

	t.Run("mod with i16 literal", func(t *testing.T) {
		expr := &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "%"},
					Args: []*plan.Expr{
						{Expr: &plan.Expr_Col{Col: colRef}},
						{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I16Val{I16Val: 3}}}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		require.Equal(t, 3.0, ndv)
	})

	t.Run("mod with i8 literal", func(t *testing.T) {
		expr := &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "%"},
					Args: []*plan.Expr{
						{Expr: &plan.Expr_Col{Col: colRef}},
						{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I8Val{I8Val: 2}}}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		require.Equal(t, 2.0, ndv)
	})

	t.Run("mod with u64 literal", func(t *testing.T) {
		expr := &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "%"},
					Args: []*plan.Expr{
						{Expr: &plan.Expr_Col{Col: colRef}},
						{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 100}}}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		require.Equal(t, 100.0, ndv)
	})

	t.Run("mod with u32 literal", func(t *testing.T) {
		expr := &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "%"},
					Args: []*plan.Expr{
						{Expr: &plan.Expr_Col{Col: colRef}},
						{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U32Val{U32Val: 50}}}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		require.Equal(t, 50.0, ndv)
	})

	t.Run("mod with u16 literal", func(t *testing.T) {
		expr := &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "%"},
					Args: []*plan.Expr{
						{Expr: &plan.Expr_Col{Col: colRef}},
						{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U16Val{U16Val: 20}}}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		require.Equal(t, 20.0, ndv)
	})

	t.Run("mod with u8 literal", func(t *testing.T) {
		expr := &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "%"},
					Args: []*plan.Expr{
						{Expr: &plan.Expr_Col{Col: colRef}},
						{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U8Val{U8Val: 7}}}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		require.Equal(t, 7.0, ndv)
	})

	t.Run("mod with negative literal", func(t *testing.T) {
		expr := &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "%"},
					Args: []*plan.Expr{
						{Expr: &plan.Expr_Col{Col: colRef}},
						{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: -5}}}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		// Negative modValue falls back to column NDV
		require.Equal(t, -1.0, ndv)
	})

	t.Run("mod with non-literal", func(t *testing.T) {
		expr := &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "%"},
					Args: []*plan.Expr{
						{Expr: &plan.Expr_Col{Col: colRef}},
						{Expr: &plan.Expr_Col{Col: colRef}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		// Non-literal falls back to column NDV
		require.Equal(t, -1.0, ndv)
	})

	t.Run("mod with single arg", func(t *testing.T) {
		expr := &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "%"},
					Args: []*plan.Expr{
						{Expr: &plan.Expr_Col{Col: colRef}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		// Single arg falls back to column NDV
		require.Equal(t, -1.0, ndv)
	})

	t.Run("default function", func(t *testing.T) {
		expr := &plan.Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "abs"},
					Args: []*plan.Expr{
						{Expr: &plan.Expr_Col{Col: colRef}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		// Default function returns column NDV
		require.Equal(t, -1.0, ndv)
	})

	t.Run("column reference", func(t *testing.T) {
		expr := &plan.Expr{
			Expr: &plan.Expr_Col{Col: colRef},
		}
		ndv := getExprNdv(expr, builder)
		require.True(t, ndv > 0 || ndv == -1)
	})

	t.Run("unsupported expr type", func(t *testing.T) {
		expr := &plan.Expr{
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 42}},
			},
		}
		ndv := getExprNdv(expr, builder)
		require.Equal(t, -1.0, ndv)
	})
}
