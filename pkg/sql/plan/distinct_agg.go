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
	"context"
	"math"
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
		if shuffleArg := tupleDistinctShuffleArgument(node, builder); shuffleArg >= 0 {
			return builder.optimizeTupleCountDistinct(node, shuffleArg)
		}
		if !canOptimizeDistinctAgg(node) {
			// HavingBinder leaves a single-argument COUNT/SUM/AVG DISTINCT value
			// unchanged because this rewrite normally gives it a separate physical
			// key. The rewrite is optional, though: for example, it is skipped
			// when this node has a sibling aggregate. In that case the generic
			// DISTINCT executor hashes the arguments directly, so establish the
			// comparison-only PAD SPACE key at this actual consumer boundary.
			return normalizeUnrewrittenDistinctAggArguments(builder.GetContext(), node)
		}

		aggFunc := node.AggList[0].GetF()
		baseID := int64(uint64(aggFunc.Func.Obj) & function.DistinctMask)
		functionID, _ := function.DecodeOverloadID(baseID)

		oldGroupLen := len(node.GroupBy)
		oldGroupBy := node.GroupBy
		distinctArg := aggFunc.Args[0]
		distinctArgNeedsPadSpaceKey := hasPadSpaceStringProvenance(distinctArg)
		innerGroupBy := append(slices.Clone(oldGroupBy), distinctArg)
		var innerGroupByHashKey []int32
		hasPadSpaceGroupKey := hasPadSpacePhysicalGroupKey(node)
		if hasPadSpaceGroupKey {
			innerGroupByHashKey = slices.Clone(node.GroupByHashKey)
			if !distinctArgNeedsPadSpaceKey {
				innerGroupByHashKey = append(innerGroupByHashKey, int32(oldGroupLen))
			}
		} else {
			candidate := &plan.Node{NodeType: plan.Node_AGG, GroupBy: innerGroupBy}
			builder.determineGroupByHashKey(candidate)
			innerGroupByHashKey = candidate.GroupByHashKey
		}
		if distinctArgNeedsPadSpaceKey &&
			(hasPadSpaceGroupKey || hashKeyContains(innerGroupByHashKey, int32(oldGroupLen)) || len(innerGroupByHashKey) == 0) {
			physicalKeys, err := builder.buildPadSpacePhysicalKeyList(
				[]*plan.Expr{distinctArg}, []bool{true},
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

		// The pre-deduplication path shuffles one DISTINCT equality key. PAD SPACE
		// has a separate physical key, so retain the local pre-dedup topology and
		// distribute that canonical key instead of the visible representation.
		distinctKeyShuffleCol := int32(oldGroupLen)
		if distinctArgNeedsPadSpaceKey {
			distinctKeyShuffleCol = int32(len(innerGroupBy) - 1)
		}
		useDistinctKeyPreAgg := functionID == function.COUNT &&
			distinctArg.GetCol() != nil &&
			isSupportedDistinctKeyShuffleType(distinctArg.Typ.Id) &&
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
				finalGroupBy[i] = builder.distinctPairGroupCol(
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
			builder.markDistinctKeyShuffle(finalNode, distinctKeyShuffleCol)

			resultNodeID = finalNodeID
			resultGroupTag = finalGroupTag
		}

		node.Children[0] = resultNodeID
		node.GroupBy = make([]*plan.Expr, oldGroupLen)
		for i := range node.GroupBy {
			node.GroupBy[i] = builder.distinctPairGroupCol(
				oldGroupBy[i], resultGroupTag, int32(i))
		}

		aggFunc.Func.Obj &= function.DistinctMask
		aggFunc.Args[0] = builder.distinctPairGroupCol(
			distinctArg, resultGroupTag, int32(oldGroupLen))
	}
	return nil
}

func canOptimizeDistinctAgg(node *plan.Node) bool {
	// Deduplicating (complete grouping keys, argument) is not sufficient for
	// ROLLUP: the same argument can occur in different detail groups but must
	// be counted only once in their common subtotal. Keep the DISTINCT bit so
	// the streaming operator owns independent distinct state at every prefix.
	if IsSortRollupOption(node.ExtraOptions) {
		return false
	}
	for _, flag := range node.GroupingFlag {
		if !flag {
			return false
		}
	}
	if len(node.AggList) != 1 {
		return false
	}
	aggFunc := node.AggList[0].GetF()
	if aggFunc == nil || aggFunc.Func == nil ||
		uint64(aggFunc.Func.Obj)&function.Distinct == 0 || len(aggFunc.Args) != 1 {
		return false
	}
	baseID := int64(uint64(aggFunc.Func.Obj) & function.DistinctMask)
	functionID, _ := function.DecodeOverloadID(baseID)
	if functionID != function.COUNT && functionID != function.SUM && functionID != function.AVG {
		return false
	}
	return aggFunc.Args[0].Typ.Id != int32(types.T_tuple)
}

func normalizeUnrewrittenDistinctAggArguments(ctx context.Context, node *plan.Node) error {
	for _, agg := range node.AggList {
		f := agg.GetF()
		if f == nil || f.Func == nil || uint64(f.Func.Obj)&function.Distinct == 0 {
			continue
		}
		baseID := int64(uint64(f.Func.Obj) & function.DistinctMask)
		functionID, _ := function.DecodeOverloadID(baseID)
		if functionID != function.COUNT && functionID != function.SUM && functionID != function.AVG {
			continue
		}
		for i := range f.Args {
			var err error
			f.Args[i], err = appendPadSpaceComparisonCastIfNeeded(ctx, f.Args[i])
			if err != nil {
				return err
			}
		}
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

// Only estimates resolved through the statistics owner may cross a new binding.
// A copied Expr.Ndv alone is not provenance for a synthesized column.
func (builder *QueryBuilder) distinctPairGroupCol(source *plan.Expr, tag, pos int32) *plan.Expr {
	ndv := getExprNdv(source, builder)
	if ndv > 0 && !math.IsInf(ndv, 0) && !math.IsNaN(ndv) {
		if builder.syntheticNDVCols == nil {
			builder.syntheticNDVCols = make(map[[2]int32]struct{})
		}
		builder.syntheticNDVCols[[2]int32{tag, pos}] = struct{}{}
	}
	return &plan.Expr{
		Typ:         source.Typ,
		Ndv:         ndv,
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

// tupleDistinctShuffleArgument admits only the equality domains already owned
// by the scalar exchange. The complete tuple remains the Group identity;
// marginal NDV chooses an exchange key, never a distinctness proof.
func tupleDistinctShuffleArgument(node *plan.Node, builder *QueryBuilder) int {
	if hasInactiveGroupingColumn(node.GroupingFlag) || len(node.AggList) != 1 ||
		len(node.Children) != 1 || node.Children[0] < 0 || int(node.Children[0]) >= len(builder.qry.Nodes) {
		return -1
	}
	f := node.AggList[0].GetF()
	if f == nil || f.Func == nil || len(f.Args) < 2 || uint64(f.Func.Obj)&function.Distinct == 0 {
		return -1
	}
	id, _ := function.DecodeOverloadID(int64(uint64(f.Func.Obj) & function.DistinctMask))
	if id != function.COUNT {
		return -1
	}
	best, highest := -1, float64(0)
	for i, arg := range f.Args {
		if arg == nil || !isSupportedDistinctKeyShuffleType(arg.Typ.Id) {
			return -1
		}
		source := arg
		if isCastOverload(source, 2) && len(source.GetF().Args) > 0 {
			source = source.GetF().Args[0]
		}
		if source.GetCol() == nil {
			return -1
		}
		// Bind-time NOT NULL is insufficient across outer joins or materialized
		// inputs. Reuse the existing input-path proof before admitting this plan.
		if !builder.exprEffectivelyNotNullableBeforeRemap(arg, node.Children[0]) {
			return -1
		}
		ndv := max(arg.Ndv, source.Ndv, getExprNdv(source, builder))
		if math.IsNaN(ndv) || math.IsInf(ndv, 0) {
			continue
		}
		ndv = estimateNDVAfterSelection(ndv, builder.qry.Nodes[node.Children[0]].Stats)
		if ndv > highest {
			best, highest = i, ndv
		}
	}
	if highest < shuffleDistinctGroupMinNDV || !shouldUseDistinctKeyPreAggregation(node, builder) {
		return -1
	}
	return best
}

func (builder *QueryBuilder) optimizeTupleCountDistinct(node *plan.Node, shuffleArg int) error {
	f := node.AggList[0].GetF()
	groupLen := len(node.GroupBy)
	oldGroups := node.GroupBy
	keys := slices.Clone(oldGroups)
	for _, arg := range f.Args {
		normalized, err := appendPadSpaceWindowKeyCastIfNeeded(builder.GetContext(), arg)
		if err != nil {
			return err
		}
		normalized = DeepCopyExpr(normalized)
		normalized.Ndv = max(arg.Ndv, getExprNdv(arg, builder))
		keys = append(keys, normalized)
	}
	// An old PK-minimized/physical string key does not determine new tuple axes.
	// Keep every canonical component in both pair stages, even after FD analysis.
	hashKeys := slices.Clone(node.GroupByHashKey)
	if len(hashKeys) == 0 {
		for i := range oldGroups {
			hashKeys = append(hashKeys, int32(i))
		}
	}
	for i := range f.Args {
		hashKeys = append(hashKeys, int32(groupLen+i))
	}
	// The wire uses an empty list for all columns; explicit keys must be a
	// strict subset. Do not apply another FD reduction to these tuple keys.
	if len(hashKeys) == len(keys) {
		hashKeys = nil
	}
	childID := node.Children[0]
	ctx := builder.ctxByNode[childID]
	localTag, localAggTag := builder.genNewBindTag(), builder.genNewBindTag()
	localID := builder.appendNode(&plan.Node{
		NodeType:       plan.Node_AGG,
		Children:       []int32{childID},
		GroupBy:        keys,
		GroupByHashKey: hashKeys,
		BindingTags:    []int32{localTag, localAggTag},
		SpillMem:       builder.aggSpillMem,
	}, ctx)
	builder.markDistinctKeyLocalPreAgg(builder.qry.Nodes[localID])
	finalKeys := make([]*plan.Expr, len(keys))
	for i, key := range keys {
		finalKeys[i] = builder.distinctPairGroupCol(key, localTag, int32(i))
	}
	finalTag, finalAggTag := builder.genNewBindTag(), builder.genNewBindTag()
	finalID := builder.appendNode(&plan.Node{
		NodeType:       plan.Node_AGG,
		Children:       []int32{localID},
		GroupBy:        finalKeys,
		GroupByHashKey: slices.Clone(hashKeys),
		BindingTags:    []int32{finalTag, finalAggTag},
		SpillMem:       builder.aggSpillMem,
	}, ctx)
	builder.markDistinctKeyShuffle(builder.qry.Nodes[finalID], int32(groupLen+shuffleArg))
	node.Children[0] = finalID
	node.GroupBy = make([]*plan.Expr, groupLen)
	for i, group := range oldGroups {
		node.GroupBy[i] = builder.distinctPairGroupCol(group, finalTag, int32(i))
	}
	f.Func.Obj &= function.DistinctMask
	f.Args = []*plan.Expr{builder.distinctPairGroupCol(keys[groupLen], finalTag, int32(groupLen))}
	return nil
}
