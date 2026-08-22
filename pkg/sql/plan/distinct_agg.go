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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func (builder *QueryBuilder) optimizeDistinctAgg(nodeID int32) error {
	node := builder.qry.Nodes[nodeID]

	for _, childID := range node.Children {
		if err := builder.optimizeDistinctAgg(childID); err != nil {
			return err
		}
	}

	if node.NodeType == plan.Node_AGG {
		if !canOptimizeDistinctAgg(node) {
			// HavingBinder leaves a single-argument COUNT/SUM DISTINCT value
			// unchanged because this rewrite normally gives it a separate physical
			// key. The rewrite is optional, though: for example, it is skipped
			// when this node has a sibling aggregate. In that case the generic
			// DISTINCT executor hashes the arguments directly, so establish the
			// comparison-only PAD SPACE key at this actual consumer boundary.
			return normalizeUnrewrittenDistinctAggArguments(builder.GetContext(), node)
		}

		aggFunc := node.AggList[0].GetF()

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
			// Re-run the functional-dependency proof with the DISTINCT argument
			// included. It may be determined by a grouped primary key, but a
			// value from another joined table still has to extend the hash key.
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

		newGroupTag := builder.genNewBindTag()
		newAggregateTag := builder.genNewBindTag()
		aggNodeID := builder.appendNode(&plan.Node{
			NodeType:       plan.Node_AGG,
			Children:       []int32{node.Children[0]},
			GroupBy:        innerGroupBy,
			GroupByHashKey: innerGroupByHashKey,
			BindingTags:    []int32{newGroupTag, newAggregateTag},
			SpillMem:       builder.aggSpillMem,
		}, builder.ctxByNode[node.Children[0]])
		builder.determineGroupByHashKey(builder.qry.Nodes[aggNodeID])

		node.Children[0] = aggNodeID
		node.GroupBy = make([]*plan.Expr, oldGroupLen)
		for i := range node.GroupBy {
			node.GroupBy[i] = &plan.Expr{
				Typ: oldGroupBy[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: newGroupTag,
						ColPos: int32(i),
					},
				},
			}
		}

		aggFunc.Func.Obj &= function.DistinctMask
		aggFunc.Args[0] = &plan.Expr{
			Typ: toCount.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: newGroupTag,
					ColPos: int32(oldGroupLen),
				},
			},
		}
	}
	return nil
}

func canOptimizeDistinctAgg(node *plan.Node) bool {
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
		uint64(aggFunc.Func.Obj)&function.Distinct == 0 ||
		(aggFunc.Func.ObjName != "count" && aggFunc.Func.ObjName != "sum") ||
		len(aggFunc.Args) != 1 {
		return false
	}
	return aggFunc.Args[0].Typ.Id != int32(types.T_tuple)
}

func normalizeUnrewrittenDistinctAggArguments(ctx context.Context, node *plan.Node) error {
	for _, agg := range node.AggList {
		f := agg.GetF()
		if f == nil || f.Func == nil ||
			uint64(f.Func.Obj)&function.Distinct == 0 ||
			(f.Func.ObjName != "count" && f.Func.ObjName != "sum") {
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
