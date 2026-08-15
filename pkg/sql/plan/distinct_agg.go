// Copyright 2023 Matrix Origin
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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// RequiresSingleStageDistinctAgg reports whether an aggregate node contains a
// DISTINCT state that MergeGroup cannot combine exactly. COUNT(DISTINCT ...)
// stores its canonical arguments in the aggregate state, so BatchMerge can
// deduplicate values that appeared in different local workers. Other DISTINCT
// aggregates keep the conservative single-worker contract until their merge
// behavior has equivalent end-to-end coverage.
func RequiresSingleStageDistinctAgg(node *plan.Node) bool {
	if node == nil {
		return false
	}
	for _, expr := range node.AggList {
		if expr == nil {
			continue
		}
		agg := expr.GetF()
		if agg == nil || agg.Func == nil || uint64(agg.Func.Obj)&function.Distinct == 0 {
			continue
		}
		baseID := int64(uint64(agg.Func.Obj) & function.DistinctMask)
		functionID, _ := function.DecodeOverloadID(baseID)
		if functionID != function.COUNT {
			return true
		}
	}
	return false
}

func (builder *QueryBuilder) optimizeDistinctAgg(nodeID int32) error {
	node := builder.qry.Nodes[nodeID]

	for _, childID := range node.Children {
		if err := builder.optimizeDistinctAgg(childID); err != nil {
			return err
		}
	}

	if node.NodeType == plan.Node_AGG {

		for _, flag := range node.GroupingFlag {
			if !flag {
				return nil
			}
		}

		if len(node.AggList) != 1 {
			return nil
		}

		aggFunc := node.AggList[0].GetF()
		if aggFunc == nil || aggFunc.Func == nil ||
			uint64(aggFunc.Func.Obj)&function.Distinct == 0 {
			return nil
		}
		baseID := int64(uint64(aggFunc.Func.Obj) & function.DistinctMask)
		functionID, _ := function.DecodeOverloadID(baseID)
		if functionID != function.COUNT && functionID != function.SUM {
			return nil
		}

		// Multi-arg COUNT(DISTINCT col1, col2, ...) cannot be optimized into a simple
		// GROUP BY because the distinct combination spans multiple columns.
		if len(aggFunc.Args) != 1 {
			return nil
		}

		// COUNT(DISTINCT (col1, col2)) — tuple syntax; normalization to multi-arg
		// form happens in HavingBinder.BindAggFunc. Just skip GROUP BY optimization.
		if aggFunc.Args[0].Typ.Id == int32(types.T_tuple) {
			return nil
		}

		oldGroupLen := len(node.GroupBy)
		oldGroupBy := node.GroupBy
		toCount := aggFunc.Args[0]
		toCountNeedsPadSpaceKey := hasPadSpaceStringProvenance(toCount)
		innerGroupBy := append(slices.Clone(oldGroupBy), toCount)
		var innerGroupByHashKey []int32
		hasPadSpaceGroupKey := hasPadSpacePhysicalGroupKey(node)
		if hasPadSpaceGroupKey {
			innerGroupByHashKey = slices.Clone(node.GroupByHashKey)
			if !toCountNeedsPadSpaceKey {
				innerGroupByHashKey = append(innerGroupByHashKey, int32(oldGroupLen))
			}
		} else {
			candidate := &plan.Node{NodeType: plan.Node_AGG, GroupBy: innerGroupBy}
			builder.determineGroupByHashKey(candidate)
			innerGroupByHashKey = candidate.GroupByHashKey
		}
		if toCountNeedsPadSpaceKey &&
			(hasPadSpaceGroupKey || hashKeyContains(innerGroupByHashKey, int32(oldGroupLen)) || len(innerGroupByHashKey) == 0) {
			physicalKeys, err := builder.buildPadSpacePhysicalKeyList(
				[]*plan.Expr{toCount}, []bool{true},
			)
			if err != nil {
				return err
			}
			physicalKeyPos := int32(len(innerGroupBy))
			if hasPadSpaceGroupKey {
				innerGroupByHashKey = append(innerGroupByHashKey, physicalKeyPos)
			} else if len(innerGroupByHashKey) == 0 {
				innerGroupByHashKey = make([]int32, oldGroupLen, oldGroupLen+1)
				for i := range oldGroupLen {
					innerGroupByHashKey[i] = int32(i)
				}
				innerGroupByHashKey = append(innerGroupByHashKey, physicalKeyPos)
			} else {
				for i, pos := range innerGroupByHashKey {
					if pos == int32(oldGroupLen) {
						innerGroupByHashKey[i] = physicalKeyPos
					}
				}
			}
			innerGroupBy = append(innerGroupBy, physicalKeys[0])
		}

		// The pre-deduplication path shuffles one logical DISTINCT column. Keep
		// it for byte-equality keys; PAD SPACE uses a separate physical key.
		useDistinctKeyPreAgg := functionID == function.COUNT &&
			!toCountNeedsPadSpaceKey &&
			toCount.GetCol() != nil &&
			isSupportedDistinctKeyShuffleType(toCount.Typ.Id) &&
			shouldUseDistinctKeyPreAggregation(node, builder)

		localGroupTag := builder.genNewBindTag()
		localAggregateTag := builder.genNewBindTag()
		localNodeID := builder.appendNode(&plan.Node{
			NodeType:       plan.Node_AGG,
			Children:       []int32{node.Children[0]},
			GroupBy:        innerGroupBy,
			GroupByHashKey: innerGroupByHashKey,
			BindingTags:    []int32{localGroupTag, localAggregateTag},
			SpillMem:       builder.aggSpillMem,
		}, builder.ctxByNode[node.Children[0]])
		localNode := builder.qry.Nodes[localNodeID]
		builder.determineGroupByHashKey(localNode)

		resultNodeID := localNodeID
		resultGroupTag := localGroupTag
		if useDistinctKeyPreAgg {
			builder.markDistinctKeyLocalPreAgg(localNode)

			finalGroupTag := builder.genNewBindTag()
			finalAggregateTag := builder.genNewBindTag()
			finalGroupBy := make([]*plan.Expr, len(innerGroupBy))
			for i, expr := range innerGroupBy {
				finalGroupBy[i] = distinctPairGroupCol(
					expr, localGroupTag, int32(i))
			}
			finalNodeID := builder.appendNode(&plan.Node{
				NodeType:    plan.Node_AGG,
				Children:    []int32{localNodeID},
				GroupBy:     finalGroupBy,
				BindingTags: []int32{finalGroupTag, finalAggregateTag},
				SpillMem:    builder.aggSpillMem,
			}, builder.ctxByNode[localNodeID])
			finalNode := builder.qry.Nodes[finalNodeID]
			builder.determineGroupByHashKey(finalNode)
			builder.markDistinctKeyShuffle(finalNode, int32(oldGroupLen))

			resultNodeID = finalNodeID
			resultGroupTag = finalGroupTag
		}

		node.Children[0] = resultNodeID
		node.GroupBy = make([]*plan.Expr, oldGroupLen)
		for i := range node.GroupBy {
			node.GroupBy[i] = distinctPairGroupCol(
				oldGroupBy[i], resultGroupTag, int32(i))
		}

		aggFunc.Func.Obj &= function.DistinctMask
		aggFunc.Args[0] = distinctPairGroupCol(
			toCount, resultGroupTag, int32(oldGroupLen))
	}
	return nil
}

// A PAD SPACE physical group key is a semantic equality key, not a functional
// dependency proof. A rewritten DISTINCT argument must therefore extend it.
func hasPadSpacePhysicalGroupKey(node *plan.Node) bool {
	for _, pos := range node.GroupByHashKey {
		if pos >= 0 && int(pos) < len(node.GroupBy) && isCastOverload(node.GroupBy[pos], 3) {
			return true
		}
	}
	return false
}

func hashKeyContains(hashKey []int32, target int32) bool {
	return slices.Contains(hashKey, target)
}

func distinctPairGroupCol(source *plan.Expr, tag, pos int32) *plan.Expr {
	return &plan.Expr{
		Typ:         source.Typ,
		Ndv:         source.Ndv,
		Selectivity: source.Selectivity,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: tag,
			ColPos: pos,
		}},
	}
}

func isSupportedDistinctKeyShuffleType(typ int32) bool {
	switch types.T(typ) {
	case types.T_int16, types.T_int32, types.T_int64,
		types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_char, types.T_varchar, types.T_text:
		return true
	default:
		return false
	}
}

func (builder *QueryBuilder) markDistinctKeyLocalPreAgg(node *plan.Node) {
	if builder.distinctKeyLocalPreAggs == nil {
		builder.distinctKeyLocalPreAggs = make(map[*plan.Node]struct{})
	}
	builder.distinctKeyLocalPreAggs[node] = struct{}{}
}

func (builder *QueryBuilder) markDistinctKeyShuffle(node *plan.Node, col int32) {
	if builder.distinctKeyShuffleCols == nil {
		builder.distinctKeyShuffleCols = make(map[*plan.Node]int32)
	}
	builder.distinctKeyShuffleCols[node] = col
}
