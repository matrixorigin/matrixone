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
	planfunction "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func countReachableNodeTypeFromRoot(query *planpb.Query, rootID int32, nodeType planpb.Node_NodeType) int {
	seen := make(map[int32]struct{})
	var visit func(int32) int
	visit = func(nodeID int32) int {
		if _, ok := seen[nodeID]; ok {
			return 0
		}
		seen[nodeID] = struct{}{}
		node := query.Nodes[nodeID]
		count := 0
		if node.NodeType == nodeType {
			count++
		}
		for _, childID := range node.Children {
			count += visit(childID)
		}
		return count
	}
	return visit(rootID)
}

type partialSumTestOptions struct {
	joinType         planpb.Node_JoinType
	dimensionKey     string
	aggregate        string
	fakePrimaryKey   bool
	compositePK      bool
	groupFactKey     bool
	groupFactStatus  bool
	filterFactStatus bool
	unknownFactNDV   bool
	factJoinNDV      float64
	distinct         bool
	joinRows         float64
}

func newPartialSumTestBuilder(t *testing.T, options partialSumTestOptions) (*QueryBuilder, int32) {
	t.Helper()
	mock := NewMockOptimizer(false)
	orders := DeepCopyTableDef(mock.ctxt.tables["orders"], true)
	customer := DeepCopyTableDef(mock.ctxt.tables["customer"], true)
	if options.fakePrimaryKey {
		customer.Pkey.PkeyColName = catalog.FakePrimaryKeyColName
	}
	if options.compositePK {
		customer.Pkey.Names = []string{"c_custkey", "c_nationkey"}
	}

	orderCustomerPos, ok := tableColumnPosition(orders, "o_custkey")
	require.True(t, ok)
	orderPricePos, ok := tableColumnPosition(orders, "o_totalprice")
	require.True(t, ok)
	orderStatusPos, ok := tableColumnPosition(orders, "o_orderstatus")
	require.True(t, ok)
	dimensionKeyPos, ok := tableColumnPosition(customer, options.dimensionKey)
	require.True(t, ok)
	segmentPos, ok := tableColumnPosition(customer, "c_mktsegment")
	require.True(t, ok)

	const (
		factTag      int32 = 1
		dimensionTag int32 = 2
		groupTag     int32 = 3
		aggTag       int32 = 4
	)
	factJoinExpr := GetColExpr(orders.Cols[orderCustomerPos].Typ, factTag, orderCustomerPos)
	dimensionJoinExpr := GetColExpr(customer.Cols[dimensionKeyPos].Typ, dimensionTag, dimensionKeyPos)
	joinCond := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool), NotNullable: true},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: "="},
			Args: []*planpb.Expr{factJoinExpr, dimensionJoinExpr},
		}},
	}
	aggExpr, err := BindFuncExprImplByPlanExpr(
		mock.CurrentContext().GetContext(),
		options.aggregate,
		[]*planpb.Expr{GetColExpr(orders.Cols[orderPricePos].Typ, factTag, orderPricePos)},
	)
	require.NoError(t, err)
	if options.distinct {
		aggExpr.GetF().Func.Obj = int64(uint64(aggExpr.GetF().Func.Obj) | uint64(planfunction.Distinct))
	}
	groupBy := []*planpb.Expr{GetColExpr(customer.Cols[segmentPos].Typ, dimensionTag, segmentPos)}
	if options.groupFactKey {
		groupBy = append(groupBy, DeepCopyExpr(factJoinExpr))
	}
	var factFilters []*planpb.Expr
	if options.groupFactStatus {
		statusExpr := GetColExpr(orders.Cols[orderStatusPos].Typ, factTag, orderStatusPos)
		groupBy = append(groupBy, statusExpr)
		if options.filterFactStatus {
			eqOpen, err := BindFuncExprImplByPlanExpr(mock.CurrentContext().GetContext(), "=", []*planpb.Expr{
				DeepCopyExpr(statusExpr), MakePlan2StringConstExprWithType("O"),
			})
			require.NoError(t, err)
			eqFinal, err := BindFuncExprImplByPlanExpr(mock.CurrentContext().GetContext(), "=", []*planpb.Expr{
				DeepCopyExpr(statusExpr), MakePlan2StringConstExprWithType("F"),
			})
			require.NoError(t, err)
			statusFilter, err := BindFuncExprImplByPlanExpr(mock.CurrentContext().GetContext(), "or", []*planpb.Expr{eqOpen, eqFinal})
			require.NoError(t, err)
			factFilters = []*planpb.Expr{statusFilter}
		}
	}

	joinRows := options.joinRows
	if joinRows == 0 {
		joinRows = 1_000_000
	}
	query := &planpb.Query{
		StmtType: planpb.Query_SELECT,
		Nodes: []*planpb.Node{
			{
				NodeId:      0,
				NodeType:    planpb.Node_TABLE_SCAN,
				TableDef:    orders,
				BindingTags: []int32{factTag},
				FilterList:  factFilters,
				Stats:       &planpb.Stats{Outcnt: 1_000_000, TableCnt: 1_000_000},
			},
			{
				NodeId:      1,
				NodeType:    planpb.Node_TABLE_SCAN,
				TableDef:    customer,
				BindingTags: []int32{dimensionTag},
				Stats:       &planpb.Stats{Outcnt: 100, TableCnt: 100},
			},
			{
				NodeId:   2,
				NodeType: planpb.Node_JOIN,
				JoinType: options.joinType,
				Children: []int32{0, 1},
				OnList:   []*planpb.Expr{joinCond},
				Stats:    &planpb.Stats{Outcnt: joinRows, TableCnt: 1_000_000},
			},
			{
				NodeId:      3,
				NodeType:    planpb.Node_AGG,
				Children:    []int32{2},
				GroupBy:     groupBy,
				AggList:     []*planpb.Expr{aggExpr},
				BindingTags: []int32{groupTag, aggTag},
				Stats:       &planpb.Stats{Outcnt: 5, TableCnt: 1_000_000},
			},
		},
		Steps: []int32{3},
	}

	builder := NewQueryBuilder(planpb.Query_SELECT, mock.CurrentContext(), false, false)
	builder.qry = query
	builder.ctxByNode = make([]*BindContext, len(query.Nodes))
	builder.nextBindTag = aggTag
	builder.tag2Table[factTag] = orders
	builder.tag2Table[dimensionTag] = customer
	builder.tag2NodeID[factTag] = 0
	builder.tag2NodeID[dimensionTag] = 1
	if !options.unknownFactNDV {
		// Deterministic fixture statistics: the mock catalog has table row
		// counts but deliberately no per-column NDVs.
		factJoinNDV := options.factJoinNDV
		if factJoinNDV == 0 {
			factJoinNDV = 100
		}
		builder.derivedColNdv[[2]int32{factTag, orderCustomerPos}] = factJoinNDV
	}
	return builder, 3
}

func TestPartialSumPushdownThroughUniqueDimensionJoin(t *testing.T) {
	tests := []struct {
		name            string
		options         partialSumTestOptions
		wantAggs        int
		wantPartialRows float64
	}{
		{
			name: "primary key inner join",
			options: partialSumTestOptions{
				joinType:     planpb.Node_INNER,
				dimensionKey: "c_custkey",
				aggregate:    "sum",
			},
			wantAggs:        2,
			wantPartialRows: 100,
		},
		{
			name: "fact grouping NDV survives partial aggregate",
			options: partialSumTestOptions{
				joinType:     planpb.Node_INNER,
				dimensionKey: "c_custkey",
				aggregate:    "sum",
				groupFactKey: true,
			},
			wantAggs:        2,
			wantPartialRows: 100,
		},
		{
			name: "finite filtered fact domain enables pushdown",
			options: partialSumTestOptions{
				joinType:         planpb.Node_INNER,
				dimensionKey:     "c_custkey",
				aggregate:        "sum",
				groupFactStatus:  true,
				filterFactStatus: true,
			},
			wantAggs:        2,
			wantPartialRows: 200,
		},
		{
			name: "unmatched fact keys are not hidden by dimension cardinality",
			options: partialSumTestOptions{
				joinType:     planpb.Node_INNER,
				dimensionKey: "c_custkey",
				aggregate:    "sum",
				factJoinNDV:  100_000,
				joinRows:     250,
			},
			wantAggs: 1,
		},
		{
			name: "unknown fact join NDV rejects pushdown",
			options: partialSumTestOptions{
				joinType:       planpb.Node_INNER,
				dimensionKey:   "c_custkey",
				aggregate:      "sum",
				unknownFactNDV: true,
			},
			wantAggs: 1,
		},
		{
			name: "unknown fact grouping NDV rejects pushdown",
			options: partialSumTestOptions{
				joinType:        planpb.Node_INNER,
				dimensionKey:    "c_custkey",
				aggregate:       "sum",
				groupFactStatus: true,
			},
			wantAggs: 1,
		},
		{
			name: "non unique dimension key",
			options: partialSumTestOptions{
				joinType:     planpb.Node_INNER,
				dimensionKey: "c_nationkey",
				aggregate:    "sum",
			},
			wantAggs: 1,
		},
		{
			name: "fake primary key",
			options: partialSumTestOptions{
				joinType:       planpb.Node_INNER,
				dimensionKey:   "c_custkey",
				aggregate:      "sum",
				fakePrimaryKey: true,
			},
			wantAggs: 1,
		},
		{
			name: "partial composite primary key",
			options: partialSumTestOptions{
				joinType:     planpb.Node_INNER,
				dimensionKey: "c_custkey",
				aggregate:    "sum",
				compositePK:  true,
			},
			wantAggs: 1,
		},
		{
			name: "outer join",
			options: partialSumTestOptions{
				joinType:     planpb.Node_LEFT,
				dimensionKey: "c_custkey",
				aggregate:    "sum",
			},
			wantAggs: 1,
		},
		{
			name: "non decomposable aggregate",
			options: partialSumTestOptions{
				joinType:     planpb.Node_INNER,
				dimensionKey: "c_custkey",
				aggregate:    "max",
			},
			wantAggs: 1,
		},
		{
			name: "distinct sum",
			options: partialSumTestOptions{
				joinType:     planpb.Node_INNER,
				dimensionKey: "c_custkey",
				aggregate:    "sum",
				distinct:     true,
			},
			wantAggs: 1,
		},
		{
			name: "insufficient reduction",
			options: partialSumTestOptions{
				joinType:     planpb.Node_INNER,
				dimensionKey: "c_custkey",
				aggregate:    "sum",
				joinRows:     150,
			},
			wantAggs: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder, rootID := newPartialSumTestBuilder(t, test.options)
			if test.options.filterFactStatus {
				groupCol := builder.qry.Nodes[rootID].GroupBy[1].GetCol()
				domain, known := finiteColumnDomain(builder.qry.Nodes[0].FilterList[0], groupCol.RelPos, groupCol.ColPos)
				require.True(t, known)
				require.Len(t, domain, 2)
			}
			rootID = builder.pushPartialSumsThroughUniqueDimensions(rootID)
			require.Equal(t, test.wantAggs, countReachableNodeTypeFromRoot(builder.qry, rootID, planpb.Node_AGG))

			if test.wantAggs == 2 {
				ReCalcNodeStats(rootID, builder, true, false, true)
				outerAgg := builder.qry.Nodes[rootID]
				require.Equal(t, planpb.Node_AGG, outerAgg.NodeType, "the final aggregate preserves duplicate dimension attributes")
				join := builder.qry.Nodes[outerAgg.Children[0]]
				partialAgg := builder.qry.Nodes[join.Children[0]]
				require.Equal(t, planpb.Node_AGG, partialAgg.NodeType)
				require.Equal(t, test.wantPartialRows, partialAgg.Stats.Outcnt)
				require.Equal(t, int32(2), outerAgg.GroupBy[0].GetCol().RelPos, "dimension grouping stays above the join")
				if test.options.groupFactKey {
					require.Equal(t, float64(100), outerAgg.Stats.Outcnt)
				}
			}
		})
	}
}
