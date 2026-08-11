// Copyright 2026 Matrix Origin
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
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
)

const cteReuseEstimatedMaterializedBytesLimit = float64(32 * mpool.MB)

// reuseMultiReferenceCTEs converts profitable, full-drain references to one
// producer step and one SINK_SCAN per consumer. All uncertain shapes keep the
// historical inline behavior.
func (builder *QueryBuilder) reuseMultiReferenceCTEs(rootID int32) int32 {
	if builder.isForUpdate {
		return rootID
	}

	for _, cteRef := range builder.cteRefs {
		if !builder.canReuseCTE(cteRef, rootID) {
			continue
		}

		producer := cteRef.occurrences[0]
		sinkID := appendSinkNodeWithTag(builder, producer.ctx, producer.rootID, producer.rootTag)
		builder.qry.Nodes[sinkID].ExtraOptions = materialized.CTESinkOption
		sourceStep := builder.appendStep(sinkID)

		replacements := make(map[int32]int32, len(cteRef.occurrences))
		for _, occurrence := range cteRef.occurrences {
			replacements[occurrence.rootID] = builder.appendSharedCTEScan(cteRef, occurrence, sourceStep)
		}
		rootID = builder.replaceCTEOccurrences(rootID, replacements, make(map[int32]bool))
	}
	return rootID
}

func (builder *QueryBuilder) canReuseCTE(cteRef *CTERef, rootID int32) bool {
	if cteRef == nil || cteRef.isRecursive || len(cteRef.occurrences) < 2 ||
		cteRef.hasNestedRef || cteRef.hasNestedUse {
		return false
	}

	first := cteRef.occurrences[0]
	for _, occurrence := range cteRef.occurrences {
		if occurrence.isCorrelated || !sameCTEOutput(first, occurrence) ||
			!builder.cteSubtreeIsDeterministic(occurrence.rootID, make(map[int32]bool)) {
			return false
		}
	}

	if !builder.cteConsumersFullyDrain(rootID, cteRef.occurrences) {
		return false
	}

	ReCalcNodeStats(first.rootID, builder, true, false, true)
	stats := builder.qry.Nodes[first.rootID].Stats
	producerCost := builder.cteProducerCost(first.rootID, make(map[int32]bool))
	if stats == nil || stats.Cost <= 0 || producerCost <= 0 ||
		!finitePositive(stats.Cost) || !finitePositive(producerCost) ||
		!cteReuseFitsMemory(stats, first.types) {
		return false
	}

	refCount := float64(len(cteRef.occurrences))
	return cteReuseIsProfitable(producerCost, stats.Outcnt, refCount)
}

func cteReuseFitsMemory(stats *planpb.Stats, outputTypes []planpb.Type) bool {
	if stats == nil || !finitePositive(stats.Outcnt) || !finitePositive(stats.Rowsize) {
		return false
	}
	for i := range outputTypes {
		if !types.T(outputTypes[i].Id).IsFixedLen() {
			return false
		}
	}
	estimatedBytes := stats.Outcnt * stats.Rowsize
	return finitePositive(estimatedBytes) && estimatedBytes <= cteReuseEstimatedMaterializedBytesLimit
}

func finitePositive(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func cteReuseIsProfitable(producerCost, producerOutcnt, referenceCount float64) bool {
	if producerCost <= 0 || producerOutcnt <= 0 || referenceCount < 2 ||
		math.IsNaN(producerCost) || math.IsNaN(producerOutcnt) || math.IsNaN(referenceCount) ||
		math.IsInf(producerCost, 0) || math.IsInf(producerOutcnt, 0) || math.IsInf(referenceCount, 0) {
		return false
	}
	inlineCost := referenceCount * producerCost
	consumerCost := referenceCount * producerOutcnt
	sharedCost := producerCost + consumerCost
	if !finitePositive(inlineCost) || !finitePositive(consumerCost) || !finitePositive(sharedCost) {
		return false
	}
	return sharedCost < inlineCost
}

// Node Stats.Cost is not uniformly cumulative: lightweight PROJECT nodes can
// carry only their output cost while a descendant scan or aggregate carries
// the actual work. The maximum subtree cost is the conservative comparable
// producer cost used by the reuse decision.
func (builder *QueryBuilder) cteProducerCost(nodeID int32, seen map[int32]bool) float64 {
	if seen[nodeID] {
		return 0
	}
	seen[nodeID] = true
	node := builder.qry.Nodes[nodeID]
	cost := float64(0)
	if node.Stats != nil {
		cost = node.Stats.Cost
	}
	for _, childID := range node.Children {
		cost = math.Max(cost, builder.cteProducerCost(childID, seen))
	}
	return cost
}

func sameCTEOutput(left, right cteOccurrence) bool {
	if len(left.headings) != len(right.headings) || len(left.types) != len(right.types) {
		return false
	}
	for i := range left.headings {
		if left.headings[i] != right.headings[i] || !samePlanType(left.types[i], right.types[i]) {
			return false
		}
	}
	return true
}

func samePlanType(left, right planpb.Type) bool {
	return left.Id == right.Id && left.NotNullable == right.NotNullable &&
		left.AutoIncr == right.AutoIncr && left.Width == right.Width &&
		left.Scale == right.Scale && left.Table == right.Table &&
		left.Enumvalues == right.Enumvalues && left.Charset == right.Charset
}

func (builder *QueryBuilder) cteSubtreeIsDeterministic(nodeID int32, seen map[int32]bool) bool {
	if seen[nodeID] {
		return true
	}
	seen[nodeID] = true
	node := builder.qry.Nodes[nodeID]
	switch node.NodeType {
	case planpb.Node_FUNCTION_SCAN, planpb.Node_EXTERNAL_SCAN,
		planpb.Node_EXTERNAL_FUNCTION, planpb.Node_LOCK_OP, planpb.Node_INSERT,
		planpb.Node_DELETE, planpb.Node_MULTI_UPDATE, planpb.Node_POSTDML,
		planpb.Node_RECURSIVE_CTE, planpb.Node_RECURSIVE_SCAN, planpb.Node_SINK,
		planpb.Node_SINK_SCAN, planpb.Node_SAMPLE:
		return false
	}

	exprLists := [][]*planpb.Expr{
		node.ProjectList,
		node.OnList,
		node.FilterList,
		node.GroupBy,
		node.AggList,
		node.WinSpecList,
		node.TimeWindowPartitionBy,
		node.TblFuncExprList,
		node.BlockFilterList,
		node.FillVal,
		node.OnUpdateExprs,
	}
	for _, exprList := range exprLists {
		for _, expr := range exprList {
			if expr == nil || !exprCanRemoveProject(expr) {
				return false
			}
		}
	}
	for _, orderBy := range node.OrderBy {
		if orderBy == nil || orderBy.Expr == nil || !exprCanRemoveProject(orderBy.Expr) {
			return false
		}
	}
	for _, expr := range []*planpb.Expr{
		node.Limit,
		node.Offset,
		node.Interval,
		node.Sliding,
		node.Timestamp,
		node.WEnd,
	} {
		if expr != nil && !exprCanRemoveProject(expr) {
			return false
		}
	}
	for _, filterList := range [][]*planpb.RuntimeFilterSpec{
		node.RuntimeFilterProbeList,
		node.RuntimeFilterBuildList,
	} {
		for _, filter := range filterList {
			if filter == nil ||
				(filter.Expr == nil && filter.BuildExpr == nil) {
				return false
			}
			if filter.Expr != nil && filter.BuildExpr != nil &&
				!exprStructuralEqual(filter.Expr, filter.BuildExpr) {
				// During rollout, metadata-independent RAW_V1 can carry the
				// same expression in both fields. Only a divergent dual layout
				// is contradictory.
				return false
			}
			if filter.Expr != nil &&
				!exprCanRemoveProject(filter.Expr) {
				return false
			}
			if filter.BuildExpr != nil &&
				!exprCanRemoveProject(filter.BuildExpr) {
				return false
			}
		}
	}
	for _, childID := range node.Children {
		if !builder.cteSubtreeIsDeterministic(childID, seen) {
			return false
		}
	}
	return true
}

func (builder *QueryBuilder) cteConsumersFullyDrain(rootID int32, occurrences []cteOccurrence) bool {
	parents := make(map[int32][]int32)
	builder.collectCTEParents(rootID, parents, make(map[int32]bool))

	for _, occurrence := range occurrences {
		type consumerPath struct {
			nodeID  int32
			drained bool
		}
		queue := make([]consumerPath, 0, len(parents[occurrence.rootID]))
		for _, nodeID := range parents[occurrence.rootID] {
			queue = append(queue, consumerPath{nodeID: nodeID})
		}
		seen := make(map[consumerPath]bool)
		for len(queue) > 0 {
			path := queue[0]
			queue = queue[1:]
			if seen[path] {
				continue
			}
			seen[path] = true
			node := builder.qry.Nodes[path.nodeID]
			if node.Limit != nil || node.Offset != nil {
				return false
			}
			if node.NodeType == planpb.Node_AGG || node.NodeType == planpb.Node_SORT ||
				node.NodeType == planpb.Node_DISTINCT || node.NodeType == planpb.Node_WINDOW {
				path.drained = true
			}
			if !path.drained && (node.NodeType == planpb.Node_APPLY ||
				node.NodeType == planpb.Node_JOIN && (node.JoinType == planpb.Node_SEMI ||
					node.JoinType == planpb.Node_ANTI || node.JoinType == planpb.Node_SINGLE ||
					node.JoinType == planpb.Node_MARK)) {
				return false
			}
			for _, parentID := range parents[path.nodeID] {
				queue = append(queue, consumerPath{nodeID: parentID, drained: path.drained})
			}
		}
	}
	return true
}

func (builder *QueryBuilder) collectCTEParents(nodeID int32, parents map[int32][]int32, seen map[int32]bool) {
	if seen[nodeID] {
		return
	}
	seen[nodeID] = true
	for _, childID := range builder.qry.Nodes[nodeID].Children {
		parents[childID] = append(parents[childID], nodeID)
		builder.collectCTEParents(childID, parents, seen)
	}
}

func (builder *QueryBuilder) appendSharedCTEScan(cteRef *CTERef, occurrence cteOccurrence, sourceStep int32) int32 {
	projectList := make([]*planpb.Expr, len(occurrence.types))
	cols := make([]*planpb.ColDef, len(occurrence.types))
	for i := range occurrence.types {
		projectList[i] = &planpb.Expr{
			Typ: occurrence.types[i],
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
				RelPos: occurrence.rootTag,
				ColPos: int32(i),
			}},
		}
		cols[i] = &planpb.ColDef{Name: occurrence.headings[i], Typ: occurrence.types[i]}
	}
	node := &planpb.Node{
		NodeType:    planpb.Node_SINK_SCAN,
		SourceStep:  []int32{sourceStep},
		ProjectList: projectList,
		BindingTags: []int32{occurrence.rootTag},
		TableDef: &planpb.TableDef{
			Name: string(cteRef.ast.Name.Alias),
			Cols: cols,
		},
	}
	return builder.appendNode(node, occurrence.ctx)
}

func (builder *QueryBuilder) replaceCTEOccurrences(nodeID int32, replacements map[int32]int32, seen map[int32]bool) int32 {
	if replacement, ok := replacements[nodeID]; ok {
		return replacement
	}
	if seen[nodeID] {
		return nodeID
	}
	seen[nodeID] = true
	node := builder.qry.Nodes[nodeID]
	for i, childID := range node.Children {
		node.Children[i] = builder.replaceCTEOccurrences(childID, replacements, seen)
	}
	return nodeID
}
