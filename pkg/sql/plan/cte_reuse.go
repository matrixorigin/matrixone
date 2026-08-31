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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

const (
	cteReuseEstimatedMaterializedBytesLimit = float64(32 * mpool.MB)
	// Reuse may deliberately trade repeated base-table work for a bounded
	// materialized spill. The execution owner still admits every byte against
	// the statement/CN spill budget; this planner ceiling only prevents a bad
	// estimate from selecting an unbounded shared spool.
	cteReuseEstimatedSpillBytesLimit = float64(8 * mpool.GB)
	// Predicate-free spill reuse needs a wide estimated advantage because its
	// producer cannot be narrowed by the consumers. Requiring both three
	// consumers and this margin means that the choice still wins if the modeled
	// materialization cost is underestimated by almost 2x.
	cteReuseSpillCostSafetyFactor = float64(2)
)

// reuseMultiReferenceCTEs converts profitable, full-drain references to one
// producer step and one SINK_SCAN per consumer. All uncertain shapes keep the
// historical inline behavior.
func (builder *QueryBuilder) reuseMultiReferenceCTEs(rootID int32) int32 {
	if builder.isForUpdate {
		return rootID
	}

	for _, cteRef := range builder.cteRefs {
		producerRootID, hashBuildOccurrences, ok := builder.reusableCTEProducer(cteRef, rootID)
		if !ok {
			continue
		}

		producer := cteRef.occurrences[0]
		sinkID := appendSinkNodeWithTag(builder, producer.ctx, producerRootID, producer.rootTag)
		builder.qry.Nodes[sinkID].ExtraOptions = materialized.CTESinkOption
		sourceStep := builder.appendStep(sinkID)

		replacements := make(map[int32]int32, len(cteRef.occurrences))
		for _, occurrence := range cteRef.occurrences {
			replacements[occurrence.rootID] = builder.appendSharedCTEScan(
				cteRef, occurrence, sourceStep, hashBuildOccurrences[occurrence.rootID])
		}
		rootID = builder.replaceCTEOccurrences(rootID, replacements, make(map[int32]bool))
	}
	return rootID
}

func (builder *QueryBuilder) reusableCTEProducer(
	cteRef *CTERef,
	rootID int32,
) (int32, map[int32]bool, bool) {
	if cteRef == nil || cteRef.isRecursive || len(cteRef.occurrences) < 2 {
		return 0, nil, false
	}

	first := cteRef.occurrences[0]
	allOccurrencesAreCurrentRoleClosures := currentRoleClosureOutput(first.types)
	for _, occurrence := range cteRef.occurrences {
		if occurrence.isCorrelated || !sameCTEOutput(first, occurrence) ||
			!builder.cteSubtreeIsDeterministic(occurrence.rootID, make(map[int32]bool)) {
			return 0, nil, false
		}
		allOccurrencesAreCurrentRoleClosures = allOccurrencesAreCurrentRoleClosures &&
			builder.cteSubtreeIsCurrentRoleClosure(occurrence.rootID, make(map[int32]bool))
	}

	if cteRef.hasNestedUse && !allOccurrencesAreCurrentRoleClosures {
		return 0, nil, false
	}
	if allOccurrencesAreCurrentRoleClosures {
		// This exemption is deliberately limited to the one-column closure
		// primitive itself. mo_current_roles computes its complete fixed-width
		// batch before producing any row, so LIMIT and SEMI/ANTI consumers cannot
		// save its internal SQL work. A scan, join, filter, aggregate, variable-
		// width projection, or any other surrounding operation must use the
		// ordinary full-drain, memory, and profitability guards below.
		return first.rootID, nil, builder.cteOccurrencesReachable(rootID, cteRef.occurrences)
	}
	hashBuildOccurrences, fullyDrained := builder.cteConsumerDrainRequirements(rootID, cteRef.occurrences)
	if !fullyDrained {
		return 0, nil, false
	}

	producerRootID := first.rootID
	sharedPredicate, predicateAware := builder.cteSharedConsumerPredicate(rootID, cteRef.occurrences)
	discardProducerFilter := func() {
		if !predicateAware || producerRootID != int32(len(builder.qry.Nodes)-1) {
			return
		}
		builder.qry.Nodes = builder.qry.Nodes[:producerRootID]
		builder.ctxByNode = builder.ctxByNode[:producerRootID]
	}
	if predicateAware {
		producerRootID = builder.appendNode(&planpb.Node{
			NodeType:   planpb.Node_FILTER,
			Children:   []int32{producerRootID},
			FilterList: []*planpb.Expr{sharedPredicate},
		}, first.ctx)
	}

	ReCalcNodeStats(producerRootID, builder, true, false, true)
	stats := builder.qry.Nodes[producerRootID].Stats
	producerCost := builder.cteProducerCost(producerRootID, make(map[int32]bool))
	if stats == nil || stats.Cost <= 0 || producerCost <= 0 ||
		!finitePositive(stats.Cost) || !finitePositive(producerCost) {
		discardProducerFilter()
		return 0, nil, false
	}
	refCount := float64(len(cteRef.occurrences))
	spillEligible := predicateAware || len(hashBuildOccurrences) > 0 ||
		len(cteRef.occurrences) >= 3 && cteReuseIsProfitableWithSafetyFactor(
			producerCost, stats.Outcnt, refCount, cteReuseSpillCostSafetyFactor)
	storageStats := stats
	storageTypes := first.types
	if requiredTypes, narrowed := builder.cteStorageOutputTypes(rootID, cteRef.occurrences); narrowed {
		storageTypes = requiredTypes
		if rowSize, fixed := fixedOutputRowSize(requiredTypes); fixed && stats != nil {
			storageStats = &planpb.Stats{Outcnt: stats.Outcnt, Rowsize: rowSize}
		}
	}
	if !cteReuseFitsStorage(storageStats, storageTypes, spillEligible) {
		discardProducerFilter()
		return 0, nil, false
	}

	if !cteReuseIsProfitable(producerCost, stats.Outcnt, refCount) {
		discardProducerFilter()
		return 0, nil, false
	}
	return producerRootID, hashBuildOccurrences, true
}

// cteStorageOutputTypes mirrors the final SINK/SINK_SCAN column pruning when
// estimating a shared source. Walking upward from each occurrence excludes the
// producer subtree even when an aggregate's output tag is also referenced by
// its own HAVING expression. The union across consumers is therefore the
// materialized schema that final column pruning will retain.
func (builder *QueryBuilder) cteStorageOutputTypes(
	rootID int32,
	occurrences []cteOccurrence,
) ([]planpb.Type, bool) {
	if len(occurrences) == 0 || len(occurrences[0].types) == 0 {
		return nil, false
	}
	parents := make(map[int32][]int32)
	reachable := make(map[int32]bool)
	builder.collectCTEParents(rootID, parents, reachable)
	required := make([]bool, len(occurrences[0].types))
	requiredCount := 0
	for _, occurrence := range occurrences {
		if !reachable[occurrence.rootID] {
			return occurrences[0].types, false
		}
		colRefCnt := make(map[[2]int32]int)
		queue := append([]int32(nil), parents[occurrence.rootID]...)
		seen := make(map[int32]bool)
		for len(queue) > 0 {
			nodeID := queue[0]
			queue = queue[1:]
			if seen[nodeID] {
				continue
			}
			seen[nodeID] = true
			countCTEConsumerNodeColRefs(builder.qry.Nodes[nodeID], colRefCnt)
			queue = append(queue, parents[nodeID]...)
		}
		for colPos := range required {
			if !required[colPos] && colRefCnt[[2]int32{occurrence.rootTag, int32(colPos)}] > 0 {
				required[colPos] = true
				requiredCount++
			}
		}
	}
	if requiredCount == 0 || requiredCount == len(required) {
		return occurrences[0].types, false
	}
	outputTypes := make([]planpb.Type, 0, requiredCount)
	for colPos, keep := range required {
		if keep {
			outputTypes = append(outputTypes, occurrences[0].types[colPos])
		}
	}
	return outputTypes, true
}

func countCTEConsumerNodeColRefs(node *planpb.Node, colRefCnt map[[2]int32]int) {
	for _, exprList := range [][]*planpb.Expr{
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
	} {
		increaseRefCntForExprList(exprList, 1, colRefCnt)
	}
	for _, orderBy := range node.OrderBy {
		if orderBy != nil {
			increaseRefCnt(orderBy.Expr, 1, colRefCnt)
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
		if expr != nil {
			increaseRefCnt(expr, 1, colRefCnt)
		}
	}
}

func fixedOutputRowSize(outputTypes []planpb.Type) (float64, bool) {
	rowSize := 0
	for i := range outputTypes {
		if outputTypes[i].Id < 0 || outputTypes[i].Id > int32(^uint8(0)) {
			return 0, false
		}
		oid := types.T(outputTypes[i].Id)
		if !oid.IsFixedLen() || oid.TypeLen() <= 0 {
			return 0, false
		}
		rowSize += oid.TypeLen()
		if !outputTypes[i].NotNullable {
			// Conservatively cover the per-row share of a null bitmap.
			rowSize++
		}
	}
	return float64(rowSize), rowSize > 0
}

func cteReuseFitsStorage(stats *planpb.Stats, outputTypes []planpb.Type, spillEligible bool) bool {
	if stats == nil || !finitePositive(stats.Outcnt) || !finitePositive(stats.Rowsize) {
		return false
	}
	estimatedBytes := stats.Outcnt * stats.Rowsize
	if !finitePositive(estimatedBytes) {
		return false
	}

	fixedWidth := true
	for i := range outputTypes {
		if !types.T(outputTypes[i].Id).IsFixedLen() {
			fixedWidth = false
			break
		}
	}
	if fixedWidth && estimatedBytes <= cteReuseEstimatedMaterializedBytesLimit {
		return true
	}
	return spillEligible && estimatedBytes <= cteReuseEstimatedSpillBytesLimit
}

func finitePositive(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func cteReuseIsProfitable(producerCost, producerOutcnt, referenceCount float64) bool {
	return cteReuseIsProfitableWithSafetyFactor(producerCost, producerOutcnt, referenceCount, 1)
}

func cteReuseIsProfitableWithSafetyFactor(
	producerCost, producerOutcnt, referenceCount, safetyFactor float64,
) bool {
	if producerCost <= 0 || producerOutcnt <= 0 || referenceCount < 2 ||
		safetyFactor < 1 ||
		math.IsNaN(producerCost) || math.IsNaN(producerOutcnt) || math.IsNaN(referenceCount) ||
		math.IsNaN(safetyFactor) || math.IsInf(producerCost, 0) ||
		math.IsInf(producerOutcnt, 0) || math.IsInf(referenceCount, 0) || math.IsInf(safetyFactor, 0) {
		return false
	}
	inlineCost := referenceCount * producerCost
	// A shared spool pays once to write every producer row and once per
	// consumer to read it. Counting the write is especially important for a
	// predicate-aware producer that may cross the in-memory boundary and spill.
	materializationCost := (referenceCount + 1) * producerOutcnt
	sharedCost := producerCost + materializationCost
	if !finitePositive(inlineCost) || !finitePositive(materializationCost) || !finitePositive(sharedCost) {
		return false
	}
	// Divide instead of multiplying sharedCost so the safety check cannot
	// overflow after all individual terms have passed the finite guards.
	return sharedCost < inlineCost/safetyFactor
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

func statementStableFunctionScan(node *planpb.Node) bool {
	return node != nil && node.NodeType == planpb.Node_FUNCTION_SCAN &&
		node.TableDef != nil && node.TableDef.TblFunc != nil &&
		node.TableDef.TblFunc.Name == "mo_current_roles" &&
		len(node.TblFuncExprList) == 0 && len(node.Children) == 0
}

func currentRoleClosureOutput(outputTypes []planpb.Type) bool {
	return len(outputTypes) == 1 && outputTypes[0].Id == int32(types.T_int64)
}

func (builder *QueryBuilder) cteSubtreeIsCurrentRoleClosure(
	nodeID int32,
	seen map[int32]bool,
) bool {
	if seen[nodeID] {
		return false
	}
	seen[nodeID] = true
	node := builder.qry.Nodes[nodeID]
	if statementStableFunctionScan(node) {
		return len(node.FilterList) == 0 && node.Limit == nil && node.Offset == nil &&
			len(node.OrderBy) == 0 && len(node.RuntimeFilterProbeList) == 0 &&
			len(node.RuntimeFilterBuildList) == 0
	}
	return node.NodeType == planpb.Node_PROJECT && len(node.Children) == 1 &&
		len(node.ProjectList) == 1 && node.ProjectList[0].Typ.Id == int32(types.T_int64) &&
		len(node.FilterList) == 0 && node.Limit == nil && node.Offset == nil &&
		len(node.OrderBy) == 0 && len(node.RuntimeFilterProbeList) == 0 &&
		len(node.RuntimeFilterBuildList) == 0 &&
		builder.cteSubtreeIsCurrentRoleClosure(node.Children[0], seen)
}

func (builder *QueryBuilder) cteSubtreeIsDeterministic(nodeID int32, seen map[int32]bool) bool {
	// An earlier eligible CTE may already have replaced a nested producer with
	// a guarded materialized source. Follow that source and validate its real
	// producer so an outer CTE can still be shared instead of forcing a choice
	// between inner and outer reuse.
	return builder.subtreeIsDeterministic(nodeID, seen, true)
}

func (builder *QueryBuilder) subtreeIsDeterministic(nodeID int32, seen map[int32]bool, allowMaterializedSink bool) bool {
	if seen[nodeID] {
		return true
	}
	seen[nodeID] = true
	node := builder.qry.Nodes[nodeID]
	if node.NodeType == planpb.Node_SINK_SCAN && allowMaterializedSink {
		if len(node.SourceStep) != 1 || node.SourceStep[0] < 0 || int(node.SourceStep[0]) >= len(builder.qry.Steps) {
			return false
		}
		sinkID := builder.qry.Steps[node.SourceStep[0]]
		if sinkID < 0 || int(sinkID) >= len(builder.qry.Nodes) {
			return false
		}
		sink := builder.qry.Nodes[sinkID]
		return sink.NodeType == planpb.Node_SINK && len(sink.Children) == 1 &&
			sink.ExtraOptions == materialized.CTESinkOption &&
			builder.subtreeIsDeterministic(sink.Children[0], seen, true)
	}
	switch node.NodeType {
	case planpb.Node_FUNCTION_SCAN:
		if !statementStableFunctionScan(node) {
			return false
		}
	case planpb.Node_EXTERNAL_SCAN,
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
		if !builder.subtreeIsDeterministic(childID, seen, allowMaterializedSink) {
			return false
		}
	}
	return true
}

func (builder *QueryBuilder) cteOccurrencesReachable(rootID int32, occurrences []cteOccurrence) bool {
	reachable := make(map[int32]bool)
	builder.collectCTEParents(rootID, make(map[int32][]int32), reachable)
	for _, occurrence := range occurrences {
		if !reachable[occurrence.rootID] {
			return false
		}
	}
	return true
}

func (builder *QueryBuilder) cteConsumersFullyDrain(rootID int32, occurrences []cteOccurrence) bool {
	_, ok := builder.cteConsumerDrainRequirements(rootID, occurrences)
	return ok
}

// cteConsumerDrainRequirements proves that every reader consumes the complete
// materialized stream. A logical right input of an equality SEMI join is a
// full-drain boundary because hash build must consume the complete membership
// set. The returned occurrences must retain that physical build-side contract
// through later join costing.
func (builder *QueryBuilder) cteConsumerDrainRequirements(
	rootID int32,
	occurrences []cteOccurrence,
) (map[int32]bool, bool) {
	parents := make(map[int32][]int32)
	reachable := make(map[int32]bool)
	builder.collectCTEParents(rootID, parents, reachable)
	hashBuildOccurrences := make(map[int32]bool)

	for _, occurrence := range occurrences {
		// replaceCTEOccurrences rewrites only the tree below rootID. An
		// occurrence owned by a separately appended step cannot participate in
		// reuse: wrapping its subtree in a producer would leave the original
		// consumer in place and make later mutating planner passes visit the
		// shared subtree twice.
		if !reachable[occurrence.rootID] {
			return nil, false
		}
		type consumerPath struct {
			nodeID  int32
			childID int32
			drained bool
		}
		queue := make([]consumerPath, 0, len(parents[occurrence.rootID]))
		for _, nodeID := range parents[occurrence.rootID] {
			queue = append(queue, consumerPath{nodeID: nodeID, childID: occurrence.rootID})
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
			if node.NodeType == planpb.Node_AGG || node.NodeType == planpb.Node_SORT ||
				node.NodeType == planpb.Node_DISTINCT || node.NodeType == planpb.Node_WINDOW {
				path.drained = true
			}
			// LIMIT/OFFSET can stop a consumer early only before an operator that
			// must read its complete input. A Top-N SORT or an aggregate still
			// drains its CTE input even when its own output is limited.
			if !path.drained && (node.Limit != nil || node.Offset != nil) {
				return nil, false
			}
			if !path.drained && node.NodeType == planpb.Node_APPLY {
				return nil, false
			}
			if !path.drained && node.NodeType == planpb.Node_JOIN {
				switch node.JoinType {
				case planpb.Node_SEMI:
					if node.IsRightJoin || len(node.Children) != 2 ||
						path.childID != node.Children[1] || !builder.IsEquiJoin(node) {
						return nil, false
					}
					path.drained = true
					hashBuildOccurrences[occurrence.rootID] = true
				case planpb.Node_MARK:
					if node.IsRightJoin || len(node.Children) != 2 ||
						path.childID != node.Children[1] ||
						!builder.cteMarkJoinBecomesHashSemi(path.nodeID, parents) {
						return nil, false
					}
					path.drained = true
					hashBuildOccurrences[occurrence.rootID] = true
				case planpb.Node_ANTI, planpb.Node_SINGLE:
					return nil, false
				}
			}
			for _, parentID := range parents[path.nodeID] {
				queue = append(queue, consumerPath{
					nodeID: parentID, childID: path.nodeID, drained: path.drained,
				})
			}
		}
	}
	return hashBuildOccurrences, true
}

// cteMarkJoinBecomesHashSemi recognizes the binder shape for a positive
// uncorrelated IN/EXISTS membership predicate. Multiple WHERE membership
// predicates bind as a left-deep MARK chain below one FILTER, so the positive
// marker may be above other MARK joins. optimizeFilters deterministically
// pushes each marker predicate to its owner and turns the equality MARK into
// SEMI before build/probe costing. All other intervening shapes fail closed.
func (builder *QueryBuilder) cteMarkJoinBecomesHashSemi(
	nodeID int32,
	parents map[int32][]int32,
) bool {
	node := builder.qry.Nodes[nodeID]
	if node.NodeType != planpb.Node_JOIN || node.JoinType != planpb.Node_MARK ||
		len(node.Children) != 2 || len(node.BindingTags) != 1 {
		return false
	}

	markTag := node.BindingTags[0]
	childID := nodeID

markerSearch:
	for {
		if len(parents[childID]) != 1 {
			return false
		}
		parentID := parents[childID][0]
		parent := builder.qry.Nodes[parentID]
		if parent.Limit != nil || parent.Offset != nil {
			return false
		}
		switch parent.NodeType {
		case planpb.Node_FILTER:
			if parent.FilterIsBarrier {
				return false
			}
			positiveMarker := false
			for _, filter := range parent.FilterList {
				if isCTEPositiveMarkFilter(filter, markTag) {
					positiveMarker = true
					break
				}
			}
			if !positiveMarker {
				return false
			}
			break markerSearch
		case planpb.Node_JOIN:
			if parent.JoinType != planpb.Node_MARK || len(parent.Children) != 2 ||
				parent.Children[0] != childID {
				return false
			}
			childID = parentID
		default:
			return false
		}
	}

	leftTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(node.Children[0]) {
		leftTags[tag] = true
	}
	rightTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(node.Children[1]) {
		rightTags[tag] = true
	}
	for _, condition := range node.OnList {
		if fn := condition.GetF(); fn != nil && fn.Func != nil && len(fn.Args) == 1 {
			funcID, _ := function.DecodeOverloadID(fn.Func.GetObj())
			if funcID == function.ISTRUE {
				condition = fn.Args[0]
			}
		}
		if isEquiCond(condition, leftTags, rightTags) {
			return true
		}
	}
	return false
}

func isCTEPositiveMarkFilter(expr *planpb.Expr, markTag int32) bool {
	col := expr.GetCol()
	if col != nil && col.RelPos == markTag {
		return true
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) != 1 {
		return false
	}
	funcID, _ := function.DecodeOverloadID(fn.Func.GetObj())
	if funcID != function.ISTRUE {
		return false
	}
	col = fn.Args[0].GetCol()
	return col != nil && col.RelPos == markTag
}

// cteSharedConsumerPredicate returns a safe superset predicate for one shared
// producer. Every occurrence must have a deterministic predicate that depends
// only on that occurrence's output. The original consumer predicates remain in
// place, so SQL three-valued logic is preserved: if a row can pass consumer i,
// it necessarily passes P1 OR ... OR Pn at the producer.
func (builder *QueryBuilder) cteSharedConsumerPredicate(
	rootID int32,
	occurrences []cteOccurrence,
) (*planpb.Expr, bool) {
	parents := make(map[int32][]int32)
	reachable := make(map[int32]bool)
	builder.collectCTEParents(rootID, parents, reachable)

	localPredicates := make([][]*planpb.Expr, len(occurrences))
	commonColumns := make(map[int32]bool)
	for i, occurrence := range occurrences {
		if !reachable[occurrence.rootID] {
			return nil, false
		}
		localPredicates[i] = builder.cteOccurrenceLocalPredicates(occurrence, parents)
		constrainedColumns := make(map[int32]bool)
		for _, predicate := range localPredicates[i] {
			if colPos, ok := ctePredicateSingleOutputColumn(predicate, occurrence.rootTag); ok {
				constrainedColumns[colPos] = true
			}
		}
		if len(constrainedColumns) == 0 {
			return nil, false
		}
		if i == 0 {
			for colPos := range constrainedColumns {
				commonColumns[colPos] = true
			}
		} else {
			for colPos := range commonColumns {
				if !constrainedColumns[colPos] {
					delete(commonColumns, colPos)
				}
			}
		}
	}
	if len(commonColumns) == 0 {
		return nil, false
	}

	disjuncts := make([]*planpb.Expr, 0, len(occurrences))
	producerTag := occurrences[0].rootTag
	for i, occurrence := range occurrences {
		predicates := make([]*planpb.Expr, 0, len(localPredicates[i]))
		for _, predicate := range localPredicates[i] {
			colPos, ok := ctePredicateSingleOutputColumn(predicate, occurrence.rootTag)
			if !ok || !commonColumns[colPos] {
				continue
			}
			predicate = DeepCopyExpr(predicate)
			replaceColRefTag(predicate, occurrence.rootTag, producerTag)
			predicates = append(predicates, predicate)
		}
		conjunct, ok := builder.combineCTEPredicates("and", predicates)
		if !ok {
			return nil, false
		}
		disjuncts = append(disjuncts, conjunct)
	}

	return builder.combineCTEPredicates("or", disjuncts)
}

// ctePredicateSingleOutputColumn identifies routing predicates such as
// channel='web', year=2025 or key BETWEEN 1 AND 10. Requiring the same output
// column to be constrained by every consumer keeps optional predicates (for
// example a HAVING condition used by only half of the consumers) out of the
// shared producer, where they can otherwise block deeper pushdown.
func ctePredicateSingleOutputColumn(expr *planpb.Expr, tag int32) (int32, bool) {
	var colPos int32
	found := false
	var visit func(*planpb.Expr) bool
	visit = func(current *planpb.Expr) bool {
		if current == nil {
			return true
		}
		switch item := current.Expr.(type) {
		case *planpb.Expr_Col:
			if item.Col.RelPos != tag {
				return false
			}
			if !found {
				colPos = item.Col.ColPos
				found = true
				return true
			}
			return colPos == item.Col.ColPos
		case *planpb.Expr_F:
			for _, arg := range item.F.Args {
				if !visit(arg) {
					return false
				}
			}
		case *planpb.Expr_List:
			for _, value := range item.List.List {
				if !visit(value) {
					return false
				}
			}
		case *planpb.Expr_Corr, *planpb.Expr_Sub, *planpb.Expr_W:
			return false
		}
		return true
	}
	return colPos, visit(expr) && found
}

// cteOccurrenceLocalPredicates walks only through operators across which a
// selection on one input can safely move. It intentionally ignores predicates
// involving another CTE occurrence; those are join semantics, not producer
// bounds. CTE occurrences are trees before reuse, but the parent walk keeps a
// visited set to fail closed if a future binder introduces a DAG here.
func (builder *QueryBuilder) cteOccurrenceLocalPredicates(
	occurrence cteOccurrence,
	parents map[int32][]int32,
) []*planpb.Expr {
	tagSet := map[int32]bool{occurrence.rootTag: true}
	predicates := make([]*planpb.Expr, 0, 2)
	queue := append([]int32(nil), parents[occurrence.rootID]...)
	seen := make(map[int32]bool)
	for len(queue) > 0 {
		nodeID := queue[0]
		queue = queue[1:]
		if seen[nodeID] {
			return nil
		}
		seen[nodeID] = true
		node := builder.qry.Nodes[nodeID]

		var candidates []*planpb.Expr
		switch node.NodeType {
		case planpb.Node_FILTER:
			if node.FilterIsBarrier || node.Limit != nil || node.Offset != nil {
				continue
			}
			candidates = node.FilterList
		case planpb.Node_JOIN:
			if node.JoinType != planpb.Node_INNER || node.Limit != nil || node.Offset != nil {
				continue
			}
			candidates = append(candidates, node.OnList...)
			candidates = append(candidates, node.FilterList...)
		default:
			continue
		}

		for _, predicate := range candidates {
			if predicate == nil || !containsTag(predicate, occurrence.rootTag) ||
				!containsOnlyTags(predicate, tagSet) || !exprCanRemoveProject(predicate) {
				continue
			}
			predicates = append(predicates, predicate)
		}
		queue = append(queue, parents[nodeID]...)
	}
	return predicates
}

func (builder *QueryBuilder) combineCTEPredicates(
	name string,
	predicates []*planpb.Expr,
) (*planpb.Expr, bool) {
	if len(predicates) == 0 {
		return nil, false
	}
	combined := predicates[0]
	for i := 1; i < len(predicates); i++ {
		var err error
		combined, err = BindFuncExprImplByPlanExpr(
			builder.GetContext(), name, []*planpb.Expr{combined, predicates[i]},
		)
		if err != nil {
			return nil, false
		}
	}
	return combined, true
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

func (builder *QueryBuilder) appendSharedCTEScan(
	cteRef *CTERef,
	occurrence cteOccurrence,
	sourceStep int32,
	requiresHashBuild bool,
) int32 {
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
	if requiresHashBuild {
		node.ExtraOptions = materialized.CTEHashBuildScanOption
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
