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

import "github.com/matrixorigin/matrixone/pkg/pb/plan"

// pushdownUncorrelatedSingleJoinFilters moves a FILTER + uncorrelated SINGLE
// JOIN pair to the smallest join input that supplies the filter's outer
// columns.  The pair moves as one unit: SINGLE still checks the scalar
// subquery cardinality, and FILTER still applies the original SQL predicate.
//
// For example,
//
//	filter(A.x = S.x, single(inner(A, B), S))
//
// becomes
//
//	inner(filter(A.x = S.x, single(A, S)), B).
//
// Only deterministic pairs cross INNER joins, or the preserved left input of
// SEMI/ANTI joins.  Correlated/right SINGLE joins and LIMIT boundaries remain
// in place.
func (builder *QueryBuilder) pushdownUncorrelatedSingleJoinFilters(nodeID int32) int32 {
	node := builder.qry.Nodes[nodeID]
	for i, childID := range node.Children {
		node.Children[i] = builder.pushdownUncorrelatedSingleJoinFilters(childID)
	}

	if !builder.canPushdownSingleJoinFilterPair(nodeID) {
		return nodeID
	}
	return builder.pushdownSingleJoinFilterPair(nodeID)
}

func (builder *QueryBuilder) canPushdownSingleJoinFilterPair(filterID int32) bool {
	filter := builder.qry.Nodes[filterID]
	if filter.NodeType != plan.Node_FILTER || filter.IsEnd || filter.FilterIsBarrier ||
		filter.RollupFilter || filter.Limit != nil || filter.Offset != nil ||
		len(filter.Children) != 1 || len(filter.FilterList) == 0 {
		return false
	}
	for _, expr := range filter.FilterList {
		if ContainsVolatileFunction(expr) || hasCorrCol(expr) {
			return false
		}
	}

	single := builder.qry.Nodes[filter.Children[0]]
	if single.NodeType != plan.Node_JOIN || single.JoinType != plan.Node_SINGLE ||
		single.IsRightJoin || single.Limit != nil || single.Offset != nil ||
		len(single.Children) != 2 || len(single.OnList) != 0 {
		return false
	}
	if !builder.cteSubtreeIsDeterministic(single.Children[1], make(map[int32]bool)) {
		return false
	}

	scalarTags := tagsForSubtree(builder, single.Children[1])
	return exprListReferencesAnyTag(filter.FilterList, scalarTags)
}

func (builder *QueryBuilder) pushdownSingleJoinFilterPair(filterID int32) int32 {
	filter := builder.qry.Nodes[filterID]
	single := builder.qry.Nodes[filter.Children[0]]
	outerID := single.Children[0]
	outer := builder.qry.Nodes[outerID]

	if outer.NodeType != plan.Node_JOIN || outer.Limit != nil || outer.Offset != nil || len(outer.Children) != 2 {
		return filterID
	}

	scalarTags := tagsForSubtree(builder, single.Children[1])
	target := builder.singleJoinFilterTarget(outer, filter.FilterList, scalarTags)
	if target < 0 {
		return filterID
	}

	single.Children[0] = outer.Children[target]
	outer.Children[target] = builder.pushdownSingleJoinFilterPair(filterID)
	return outerID
}

func (builder *QueryBuilder) singleJoinFilterTarget(
	join *plan.Node,
	filters []*plan.Expr,
	scalarTags map[int32]bool,
) int {
	var candidates []int
	switch join.JoinType {
	case plan.Node_INNER:
		candidates = []int{0, 1}
	case plan.Node_SEMI, plan.Node_ANTI:
		candidates = []int{0}
	default:
		return -1
	}

	for _, childIdx := range candidates {
		allowed := make(map[int32]bool, len(scalarTags)+4)
		for tag := range scalarTags {
			allowed[tag] = true
		}
		childTags := tagsForSubtree(builder, join.Children[childIdx])
		for tag := range childTags {
			allowed[tag] = true
		}

		valid := true
		for _, filter := range filters {
			side := getJoinSideWithOuterScope(filter, allowed, nil, 0)
			if side&(JoinSideOuter|JoinSideCorrelated|JoinSideRight|JoinSideMark) != 0 {
				valid = false
				break
			}
		}
		if valid && exprListReferencesAnyTag(filters, childTags) {
			return childIdx
		}
	}
	return -1
}

func tagsForSubtree(builder *QueryBuilder, nodeID int32) map[int32]bool {
	tags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(nodeID) {
		tags[tag] = true
	}
	return tags
}

func exprListReferencesAnyTag(exprs []*plan.Expr, tags map[int32]bool) bool {
	for _, expr := range exprs {
		for tag := range tags {
			if containsTag(expr, tag) {
				return true
			}
		}
	}
	return false
}
