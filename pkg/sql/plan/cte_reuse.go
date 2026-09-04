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
	if builder.isForUpdate || builder.sharedComputationDisabled() ||
		builder.sessionSelectLimitMayStopEarly {
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
		// primitive itself plus direct identity projections. mo_current_roles
		// computes its complete fixed-width batch before producing any row, so
		// LIMIT and SEMI/ANTI consumers cannot save its internal SQL work. A
		// computed projection, scan, join, filter, aggregate, or any other
		// surrounding operation must use the ordinary full-drain, memory, and
		// profitability guards below.
		return first.rootID, nil, builder.cteOccurrencesReachable(rootID, cteRef.occurrences)
	}
	hashBuildOccurrences, hasDrainWitness := builder.cteConsumerDrainRequirements(rootID, cteRef.occurrences)
	if !hasDrainWitness {
		return 0, nil, false
	}
	producerRootID := first.rootID
	sharedPredicate, predicateAware, rowDomainExact := builder.cteSharedConsumerPredicate(
		rootID, cteRef.occurrences)
	if !rowDomainExact && !builder.cteProducerEvaluationIsTotal(
		first.rootID, first.rootID, first.ctx, make(map[int32]bool),
	) {
		return 0, nil, false
	}
	if !builder.cteOutputDemandPreservesEvaluation(
		rootID, cteRef.occurrences, rowDomainExact,
	) {
		return 0, nil, false
	}
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
		if rowSize, fixed := fixedOutputRowSize(requiredTypes); fixed {
			storageStats = &planpb.Stats{Outcnt: stats.Outcnt, Rowsize: rowSize}
		}
	}
	if !cteReuseFitsStorage(storageStats, storageTypes, spillEligible) {
		discardProducerFilter()
		return 0, nil, false
	}
	estimatedMaterializedBytes, _, estimateKnown := cteEstimatedMaterializedBytes(
		storageStats, storageTypes,
	)
	if !estimateKnown {
		discardProducerFilter()
		return 0, nil, false
	}

	if !cteReuseIsProfitable(producerCost, stats.Outcnt, refCount) {
		discardProducerFilter()
		return 0, nil, false
	}
	if !builder.reserveSharedMaterialization(
		estimatedMaterializedBytes,
		storageStats.Outcnt,
		storageTypes,
	) {
		discardProducerFilter()
		return 0, nil, false
	}
	return producerRootID, hashBuildOccurrences, true
}

// cteProducerEvaluationIsTotal closes the evaluation-domain gap left when no
// exact union of consumer predicates can be pushed into the shared producer.
// Inlining may push an individual consumer predicate through projections and
// aggregates, avoiding any producer expression on excluded rows. Eager
// materialization cannot preserve that behavior unless every expression in
// the producer is proved total and side-effect free. This deliberately checks
// more than the final demanded columns: grouping keys, HAVING, join conditions
// and intermediate expressions can all have a smaller legacy row domain.
func (builder *QueryBuilder) cteProducerEvaluationIsTotal(
	rootID, nodeID int32,
	ctx *BindContext,
	seen map[int32]bool,
) bool {
	if nodeID < 0 || int(nodeID) >= len(builder.qry.Nodes) || seen[nodeID] {
		return nodeID >= 0 && int(nodeID) < len(builder.qry.Nodes)
	}
	seen[nodeID] = true
	node := builder.qry.Nodes[nodeID]
	if node == nil || !areTruncationSafePredicates(node.FilterList) ||
		!areTruncationSafePredicates(node.OnList) ||
		!areTruncationSafePredicates(node.BlockFilterList) {
		return false
	}
	if node.NodeType == planpb.Node_SINK_SCAN {
		// A previously admitted materialized producer is an optimization
		// boundary: outer predicates cannot be pushed back through its source.
		return len(node.SourceStep) == 1
	}
	for _, exprList := range [][]*planpb.Expr{
		node.ProjectList,
		node.GroupBy,
		node.WinSpecList,
		node.TimeWindowPartitionBy,
		node.TblFuncExprList,
		node.FillVal,
		node.OnUpdateExprs,
		node.PhysicalEqualityKeyList,
	} {
		for _, expr := range exprList {
			if !builder.cteExprIsTotal(rootID, expr, ctx, make(map[[2]int32]bool)) {
				return false
			}
		}
	}
	for _, agg := range node.AggList {
		if !builder.cteAggregateIsTotal(rootID, agg, ctx, make(map[[2]int32]bool)) {
			return false
		}
	}
	for _, orderBy := range node.OrderBy {
		if orderBy == nil ||
			!builder.cteExprIsTotal(rootID, orderBy.Expr, ctx, make(map[[2]int32]bool)) {
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
		node.GapFillStart,
		node.GapFillEnd,
	} {
		if expr != nil &&
			!builder.cteExprIsTotal(rootID, expr, ctx, make(map[[2]int32]bool)) {
			return false
		}
	}
	if node.RowsetData != nil {
		for _, col := range node.RowsetData.Cols {
			if col == nil {
				return false
			}
			for _, data := range col.Data {
				if data == nil ||
					!builder.cteExprIsTotal(rootID, data.Expr, ctx, make(map[[2]int32]bool)) {
					return false
				}
			}
		}
	}
	if node.IndexReaderParam != nil {
		for _, orderBy := range node.IndexReaderParam.OrderBy {
			if orderBy == nil || !builder.cteExprIsTotal(
				rootID, orderBy.Expr, ctx, make(map[[2]int32]bool),
			) {
				return false
			}
		}
		for _, expr := range []*planpb.Expr{
			node.IndexReaderParam.Limit,
			distRangeLowerBound(node.IndexReaderParam.DistRange),
			distRangeUpperBound(node.IndexReaderParam.DistRange),
		} {
			if expr != nil && !builder.cteExprIsTotal(rootID, expr, ctx, make(map[[2]int32]bool)) {
				return false
			}
		}
	}
	if node.VectorIndexScan != nil {
		for _, expr := range append([]*planpb.Expr{
			node.VectorIndexScan.QueryVector,
			node.VectorIndexScan.CandidateLimit,
			node.VectorIndexScan.FirstRoundLimit,
			distRangeLowerBound(node.VectorIndexScan.DistanceRange),
			distRangeUpperBound(node.VectorIndexScan.DistanceRange),
		}, node.VectorIndexScan.PreFilters...) {
			if expr != nil && !builder.cteExprIsTotal(rootID, expr, ctx, make(map[[2]int32]bool)) {
				return false
			}
		}
	}
	if node.DedupJoinCtx != nil {
		for _, expr := range node.DedupJoinCtx.UpdateColExprList {
			if !builder.cteExprIsTotal(rootID, expr, ctx, make(map[[2]int32]bool)) {
				return false
			}
		}
	}
	for _, specs := range [][]*planpb.RuntimeFilterSpec{
		node.RuntimeFilterProbeList,
		node.RuntimeFilterBuildList,
	} {
		for _, spec := range specs {
			if spec == nil ||
				spec.Expr != nil && !builder.cteExprIsTotal(
					rootID, spec.Expr, ctx, make(map[[2]int32]bool),
				) ||
				spec.BuildExpr != nil && !builder.cteExprIsTotal(
					rootID, spec.BuildExpr, ctx, make(map[[2]int32]bool),
				) {
				return false
			}
		}
	}
	for _, childID := range node.Children {
		if !builder.cteProducerEvaluationIsTotal(rootID, childID, ctx, seen) {
			return false
		}
	}
	return true
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
	requiredByOccurrence, ok := builder.cteRequiredOutputColumns(rootID, occurrences)
	if !ok {
		return occurrences[0].types, false
	}

	required := make([]bool, len(occurrences[0].types))
	requiredCount := 0
	for _, consumerRequired := range requiredByOccurrence {
		for colPos, keep := range consumerRequired {
			if keep && !required[colPos] {
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

func (builder *QueryBuilder) cteRequiredOutputColumns(
	rootID int32,
	occurrences []cteOccurrence,
) ([][]bool, bool) {
	if len(occurrences) == 0 || len(occurrences[0].types) == 0 {
		return nil, false
	}
	parents := make(map[int32][]int32)
	reachable := make(map[int32]bool)
	builder.collectCTEParents(rootID, parents, reachable)
	requiredByOccurrence := make([][]bool, 0, len(occurrences))
	for _, occurrence := range occurrences {
		if !reachable[occurrence.rootID] {
			return nil, false
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
		required := make([]bool, len(occurrence.types))
		for colPos := range required {
			required[colPos] = colRefCnt[[2]int32{occurrence.rootTag, int32(colPos)}] > 0
		}
		requiredByOccurrence = append(requiredByOccurrence, required)
	}
	return requiredByOccurrence, true
}

// cteOutputDemandPreservesEvaluation rejects sharing when the materialized
// producer expands either the consumer set or the row domain on which a
// fallible output is evaluated. Determinism is insufficient: a cast can be
// deterministic and still fail on rows excluded by one consumer's predicate.
func (builder *QueryBuilder) cteOutputDemandPreservesEvaluation(
	rootID int32,
	occurrences []cteOccurrence,
	rowDomainExact bool,
) bool {
	requiredByOccurrence, ok := builder.cteRequiredOutputColumns(rootID, occurrences)
	if !ok {
		return false
	}
	for colPos := range occurrences[0].types {
		requiredCount := 0
		for _, consumerRequired := range requiredByOccurrence {
			if consumerRequired[colPos] {
				requiredCount++
			}
		}
		if requiredCount > 0 &&
			(requiredCount < len(occurrences) || !rowDomainExact) &&
			!builder.cteOutputColumnIsTotal(
				occurrences[0],
				int32(colPos),
			) {
			return false
		}
	}
	return true
}

func (builder *QueryBuilder) cteOutputColumnIsTotal(occurrence cteOccurrence, colPos int32) bool {
	return builder.cteColumnIsTotal(
		occurrence.rootID,
		occurrence.rootTag,
		colPos,
		occurrence.ctx,
		make(map[[2]int32]bool),
	)
}

func (builder *QueryBuilder) cteColumnIsTotal(
	rootID, tag, colPos int32,
	ctx *BindContext,
	visiting map[[2]int32]bool,
) bool {
	ref := [2]int32{tag, colPos}
	if visiting[ref] {
		return false
	}
	visiting[ref] = true
	defer delete(visiting, ref)

	nodeID, tagPos, ok := builder.findCTEBindingOwner(rootID, tag, make(map[int32]bool))
	if !ok {
		// Aggregate binding tags are owned by the query-block context rather
		// than retained on every intermediate node. Resolve them back to the
		// bound expressions; an unrecognized tag is not a proof of totality.
		if ctx != nil && tag == ctx.groupTag && colPos >= 0 && int(colPos) < len(ctx.groups) {
			return builder.cteExprIsTotal(rootID, ctx.groups[colPos], ctx, visiting)
		}
		if ctx != nil && tag == ctx.aggregateTag && colPos >= 0 && int(colPos) < len(ctx.aggregates) {
			return builder.cteAggregateIsTotal(rootID, ctx.aggregates[colPos], ctx, visiting)
		}
		return false
	}
	node := builder.qry.Nodes[nodeID]
	if node.NodeType == planpb.Node_TABLE_SCAN {
		return node.TableDef != nil && colPos >= 0 && int(colPos) < len(node.TableDef.Cols)
	}
	if node.NodeType == planpb.Node_AGG && len(node.BindingTags) >= 2 {
		if tagPos == 0 && colPos >= 0 && int(colPos) < len(node.GroupBy) {
			return builder.cteExprIsTotal(rootID, node.GroupBy[colPos], ctx, visiting)
		}
		if tagPos == 1 && colPos >= 0 && int(colPos) < len(node.AggList) {
			return builder.cteAggregateIsTotal(rootID, node.AggList[colPos], ctx, visiting)
		}
		return false
	}
	if colPos < 0 || int(colPos) >= len(node.ProjectList) {
		return false
	}
	return builder.cteExprIsTotal(rootID, node.ProjectList[colPos], ctx, visiting)
}

func (builder *QueryBuilder) findCTEBindingOwner(
	nodeID, tag int32,
	seen map[int32]bool,
) (int32, int, bool) {
	if seen[nodeID] {
		return 0, 0, false
	}
	seen[nodeID] = true
	node := builder.qry.Nodes[nodeID]
	for tagPos, bindingTag := range node.BindingTags {
		if bindingTag == tag {
			return nodeID, tagPos, true
		}
	}
	for _, childID := range node.Children {
		if ownerID, tagPos, ok := builder.findCTEBindingOwner(childID, tag, seen); ok {
			return ownerID, tagPos, true
		}
	}
	return 0, 0, false
}

func (builder *QueryBuilder) cteExprIsTotal(
	rootID int32,
	expr *planpb.Expr,
	ctx *BindContext,
	visiting map[[2]int32]bool,
) bool {
	if expr == nil {
		return false
	}
	if col := expr.GetCol(); col != nil {
		return builder.cteColumnIsTotal(rootID, col.RelPos, col.ColPos, ctx, visiting)
	}
	if expr.GetLit() != nil || expr.GetP() != nil {
		return true
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != "cast" || len(fn.Args) != 2 ||
		fn.Args[1].GetT() == nil || !singleRowCastIsTotal(fn.Args[0].Typ, expr.Typ) {
		return false
	}
	overload, ok := function.GetFunctionByIdWithoutError(fn.Func.Obj)
	return ok && !overload.CannotFold() && !overload.IsRealTimeRelated() &&
		builder.cteExprIsTotal(rootID, fn.Args[0], ctx, visiting)
}

func (builder *QueryBuilder) cteAggregateIsTotal(
	rootID int32,
	expr *planpb.Expr,
	ctx *BindContext,
	visiting map[[2]int32]bool,
) bool {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	functionID, _ := function.DecodeOverloadID(fn.Func.Obj)
	switch functionID {
	case function.STARCOUNT, function.COUNT, function.ANY_VALUE:
		for _, arg := range fn.Args {
			if !builder.cteExprIsTotal(rootID, arg, ctx, visiting) {
				return false
			}
		}
		return true
	case function.MIN, function.MAX:
		return len(fn.Args) == 1 &&
			builder.cteExprIsTotal(rootID, fn.Args[0], ctx, visiting) &&
			comparisonEvaluationIsTotal(function.LESS_THAN, fn.Args[0].Typ, fn.Args[0].Typ)
	case function.SUM:
		if len(fn.Args) != 1 ||
			!builder.cteExprIsTotal(rootID, fn.Args[0], ctx, visiting) {
			return false
		}
		// Match the executor's data-error contract. Floating SUM permits
		// infinity, Decimal64 accumulates unchecked into Decimal128, and the
		// Decimal128 fast path checks overflow only above precision 28.
		switch types.T(fn.Args[0].Typ.Id) {
		case types.T_float32, types.T_float64, types.T_decimal64:
			return true
		case types.T_decimal128:
			return validDecimalPlanType(fn.Args[0].Typ) && fn.Args[0].Typ.Width <= 28
		default:
			return false
		}
	default:
		return false
	}
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
		node.PhysicalEqualityKeyList,
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
		node.GapFillStart,
		node.GapFillEnd,
	} {
		if expr != nil {
			increaseRefCnt(expr, 1, colRefCnt)
		}
	}
	for _, specs := range [][]*planpb.RuntimeFilterSpec{
		node.RuntimeFilterProbeList,
		node.RuntimeFilterBuildList,
	} {
		for _, spec := range specs {
			if spec != nil {
				increaseCTERefCnt(spec.Expr, colRefCnt)
				increaseCTERefCnt(spec.BuildExpr, colRefCnt)
			}
		}
	}
	if node.RowsetData != nil {
		for _, col := range node.RowsetData.Cols {
			if col == nil {
				continue
			}
			for _, data := range col.Data {
				if data != nil {
					increaseRefCnt(data.Expr, 1, colRefCnt)
				}
			}
		}
	}
	if node.IndexReaderParam != nil {
		increaseCTERefCnt(node.IndexReaderParam.Limit, colRefCnt)
		increaseCTERefCnt(distRangeLowerBound(node.IndexReaderParam.DistRange), colRefCnt)
		increaseCTERefCnt(distRangeUpperBound(node.IndexReaderParam.DistRange), colRefCnt)
		for _, orderBy := range node.IndexReaderParam.OrderBy {
			if orderBy != nil {
				increaseRefCnt(orderBy.Expr, 1, colRefCnt)
			}
		}
	}
	if node.VectorIndexScan != nil {
		increaseCTERefCnt(node.VectorIndexScan.QueryVector, colRefCnt)
		increaseCTERefCnt(node.VectorIndexScan.CandidateLimit, colRefCnt)
		increaseCTERefCnt(node.VectorIndexScan.FirstRoundLimit, colRefCnt)
		increaseCTERefCnt(distRangeLowerBound(node.VectorIndexScan.DistanceRange), colRefCnt)
		increaseCTERefCnt(distRangeUpperBound(node.VectorIndexScan.DistanceRange), colRefCnt)
		increaseRefCntForExprList(node.VectorIndexScan.PreFilters, 1, colRefCnt)
	}
	if node.DedupJoinCtx != nil {
		increaseRefCntForExprList(node.DedupJoinCtx.UpdateColExprList, 1, colRefCnt)
	}
	for _, target := range node.LockTargets {
		if target != nil {
			increaseCTERefCnt(target.LockRows, colRefCnt)
		}
	}
}

func increaseCTERefCnt(expr *planpb.Expr, colRefCnt map[[2]int32]int) {
	if expr != nil {
		increaseRefCnt(expr, 1, colRefCnt)
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
	estimatedBytes, fixedWidth, ok := cteEstimatedMaterializedBytes(stats, outputTypes)
	if !ok {
		return false
	}
	if fixedWidth && estimatedBytes <= cteReuseEstimatedMaterializedBytesLimit {
		return true
	}
	declaredRowSize, sizeKnown := materializedDeclaredRowSize(outputTypes)
	return spillEligible && sizeKnown &&
		declaredRowSize <= float64(materialized.MaxSpillBatchBytes)/2 &&
		estimatedBytes <= cteReuseEstimatedSpillBytesLimit
}

func cteEstimatedMaterializedBytes(
	stats *planpb.Stats,
	outputTypes []planpb.Type,
) (float64, bool, bool) {
	if stats == nil || !finitePositive(stats.Outcnt) || !finitePositive(stats.Rowsize) {
		return 0, false, false
	}
	declaredRowSize, sizeKnown := materializedDeclaredRowSize(outputTypes)
	if !sizeKnown {
		return 0, false, false
	}
	fixedWidth := true
	for i := range outputTypes {
		if !types.T(outputTypes[i].Id).IsFixedLen() {
			fixedWidth = false
			break
		}
	}
	estimatedRowSize := math.Max(stats.Rowsize, declaredRowSize)
	estimatedBytes := stats.Outcnt * estimatedRowSize
	return estimatedBytes, fixedWidth, finitePositive(estimatedBytes)
}

const (
	// A spill record contains an eight-byte record frame, the grouping codec's
	// payload frame, and the stable batch header. The transient parameter and
	// grouping-provenance trailers are charged at their largest one-row
	// representation because execution may legally emit one row per batch.
	sharedMaterializationSpillRecordFixedBytes    = 1 + 8 + 8 + 8 + 4*5 + 4 + 4 + 8 + 4 + 4
	sharedMaterializationSpillVectorFixedBytes    = 4 + 1 + types.TSize + 4*4 + 1 + 4
	sharedMaterializationSpillVectorMetadataBytes = 2 * (1 + 4 + 1)
	sharedMaterializationSpillNullableBytes       = 24 + 8
	sharedMaterializationSpillGroupingBytes       = 24 + 8
)

func estimatedSharedMaterializationSpillBytes(
	materializedBytes, estimatedRows float64,
	outputTypes []planpb.Type,
) (float64, bool) {
	if !finitePositive(materializedBytes) || !finitePositive(estimatedRows) || len(outputTypes) == 0 {
		return 0, false
	}
	recordOverhead := float64(sharedMaterializationSpillRecordFixedBytes)
	for i := range outputTypes {
		recordOverhead += float64(sharedMaterializationSpillVectorFixedBytes +
			sharedMaterializationSpillVectorMetadataBytes +
			sharedMaterializationSpillGroupingBytes)
		if !outputTypes[i].NotNullable {
			// A one-row bitmap occupies one word plus its wire framing.
			recordOverhead += float64(sharedMaterializationSpillNullableBytes)
		}
	}
	// Batch cardinality is an execution property, not a planner invariant. Use
	// the legal worst case of one spill record per estimated row so framing and
	// vector metadata cannot make an otherwise exact spill cap fail.
	spillBytes := materializedBytes + math.Ceil(estimatedRows)*recordOverhead
	return spillBytes, finitePositive(spillBytes)
}

func (builder *QueryBuilder) reserveSharedMaterialization(
	materializedBytes, estimatedRows float64,
	outputTypes []planpb.Type,
) bool {
	spillBytes, spillEstimateKnown := estimatedSharedMaterializationSpillBytes(
		materializedBytes, estimatedRows, outputTypes,
	)
	if !spillEstimateKnown {
		return false
	}
	memoryBytes := math.Min(materializedBytes, float64(materialized.MaxSourceRetainedBytes))
	memoryLimit := math.MaxFloat64
	// Charge every source against spill as well as retained memory. Even a
	// byte-small source spills after the bounded in-memory batch-count limit,
	// and batch count is not a hard planner proof from estimated rows.
	spillLimit := cteReuseEstimatedSpillBytesLimit
	if builder.compCtx != nil {
		if proc := builder.compCtx.GetProcess(); proc != nil && proc.Base != nil {
			if proc.Base.Lim.Size > 0 {
				memoryLimit = float64(proc.Base.Lim.Size)
			}
			if proc.Base.Lim.SpillSize > 0 {
				spillLimit = math.Min(spillLimit, float64(proc.Base.Lim.SpillSize))
			}
		}
	}
	if !finitePositive(memoryLimit) ||
		builder.sharedMaterializationMemoryBytes > memoryLimit-memoryBytes ||
		!finitePositive(spillLimit) ||
		builder.sharedMaterializationSpillBytes > spillLimit-spillBytes {
		return false
	}
	builder.sharedMaterializationMemoryBytes += memoryBytes
	builder.sharedMaterializationSpillBytes += spillBytes
	return true
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
	if node.NodeType != planpb.Node_PROJECT || len(node.Children) != 1 ||
		len(node.ProjectList) != 1 || node.ProjectList[0].Typ.Id != int32(types.T_int64) ||
		node.ProjectList[0].GetCol() == nil || node.ProjectList[0].GetCol().ColPos != 0 {
		return false
	}
	return len(node.FilterList) == 0 && node.Limit == nil && node.Offset == nil &&
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

func distRangeLowerBound(distRange *planpb.DistRange) *planpb.Expr {
	if distRange == nil {
		return nil
	}
	return distRange.LowerBound
}

func distRangeUpperBound(distRange *planpb.DistRange) *planpb.Expr {
	if distRange == nil {
		return nil
	}
	return distRange.UpperBound
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
		node.PhysicalEqualityKeyList,
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
		node.GapFillStart,
		node.GapFillEnd,
	} {
		if expr != nil && !exprCanRemoveProject(expr) {
			return false
		}
	}
	if node.RowsetData != nil {
		for _, col := range node.RowsetData.Cols {
			if col == nil {
				return false
			}
			for _, data := range col.Data {
				if data == nil || data.Expr == nil || !exprCanRemoveProject(data.Expr) {
					return false
				}
			}
		}
	}
	if node.IndexReaderParam != nil {
		for _, orderBy := range node.IndexReaderParam.OrderBy {
			if orderBy == nil || orderBy.Expr == nil || !exprCanRemoveProject(orderBy.Expr) {
				return false
			}
		}
		for _, expr := range []*planpb.Expr{
			node.IndexReaderParam.Limit,
			distRangeLowerBound(node.IndexReaderParam.DistRange),
			distRangeUpperBound(node.IndexReaderParam.DistRange),
		} {
			if expr != nil && !exprCanRemoveProject(expr) {
				return false
			}
		}
	}
	if node.VectorIndexScan != nil {
		for _, expr := range append([]*planpb.Expr{
			node.VectorIndexScan.QueryVector,
			node.VectorIndexScan.CandidateLimit,
			node.VectorIndexScan.FirstRoundLimit,
			distRangeLowerBound(node.VectorIndexScan.DistanceRange),
			distRangeUpperBound(node.VectorIndexScan.DistanceRange),
		}, node.VectorIndexScan.PreFilters...) {
			if expr != nil && !exprCanRemoveProject(expr) {
				return false
			}
		}
	}
	if node.DedupJoinCtx != nil {
		for _, expr := range node.DedupJoinCtx.UpdateColExprList {
			if expr == nil || !exprCanRemoveProject(expr) {
				return false
			}
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

func (builder *QueryBuilder) cteHasDrainWitness(rootID int32, occurrences []cteOccurrence) bool {
	_, ok := builder.cteConsumerDrainRequirements(rootID, occurrences)
	return ok
}

// cteConsumerDrainRequirements proves that at least one legacy occurrence must
// evaluate the complete producer. That witness makes eager materialization
// preserve the producer's error/evaluation domain; other readers may stop
// early because the bounded materialized source supports independent release.
// A witness behind a join must be on the exact logical build path of every join
// ancestor: an empty hash build can skip its entire probe subtree, even when
// that subtree contains an aggregate or sort. The returned witness occurrences
// must retain their physical build-side contract through later join costing.
func (builder *QueryBuilder) cteConsumerDrainRequirements(
	rootID int32,
	occurrences []cteOccurrence,
) (map[int32]bool, bool) {
	parents := make(map[int32][]int32)
	reachable := make(map[int32]bool)
	builder.collectCTEParents(rootID, parents, reachable)
	hashBuildOccurrences := make(map[int32]bool)
	requiredBuildChildByJoin := make(map[int32]int32)
	hasDrainWitness := false

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
			nodeID            int32
			childID           int32
			requiresHashBuild bool
		}
		queue := make([]consumerPath, 0, len(parents[occurrence.rootID]))
		for _, nodeID := range parents[occurrence.rootID] {
			queue = append(queue, consumerPath{nodeID: nodeID, childID: occurrence.rootID})
		}
		witnessWithoutHashBuild := len(queue) == 0
		witnessWithHashBuild := false
		seen := make(map[consumerPath]bool)
		for len(queue) > 0 {
			path := queue[0]
			queue = queue[1:]
			if seen[path] {
				continue
			}
			seen[path] = true
			node := builder.qry.Nodes[path.nodeID]
			// LIMIT can stop the subtree before it completes. Only a positive
			// literal limit on a proven blocking operator is a witness: LIMIT 0 is
			// compiled without its input steps, and a dynamic limit may be zero at
			// execution time. OFFSET alone does not shorten a fully consumed stream.
			if node.Limit != nil && !cteLimitPreservesFullInput(node) {
				continue
			}
			// APPLY may skip its right input when the left side is empty. Block
			// sampling can terminate successfully after a subset of its input.
			// Neither node can carry a complete-evaluation witness upward.
			if node.NodeType == planpb.Node_APPLY || node.NodeType == planpb.Node_SAMPLE {
				continue
			}
			if node.NodeType == planpb.Node_JOIN {
				switch node.JoinType {
				case planpb.Node_INNER:
					// A non-shuffle INNER hash join can stop before reading its
					// probe input when the build is empty. Default costing may put
					// either logical child on build, so reserve this exact child and
					// preserve it with the marker below. A fixed join-order hint
					// forbids that physical move and therefore accepts only the
					// already-right child.
					if len(node.Children) != 2 || !builder.IsEquiJoin(node) ||
						path.childID != node.Children[0] && path.childID != node.Children[1] {
						continue
					}
					if path.childID == node.Children[0] && builder.optimizerHints != nil &&
						builder.optimizerHints.joinOrdering != 0 {
						continue
					}
					// Existing runtime-filter dependencies and a direct function-scan
					// build both pin logical child 1. determineBuildAndProbeSide returns
					// before ordinary costing for these shapes, so logical child 0
					// cannot be promised as the later physical build.
					if path.childID == node.Children[0] &&
						(len(node.RuntimeFilterBuildList) != 0 ||
							builder.qry.Nodes[node.Children[1]].NodeType == planpb.Node_FUNCTION_SCAN) {
						continue
					}
					siblingID := node.Children[0]
					if path.childID == siblingID {
						siblingID = node.Children[1]
					}
					if builder.subtreeContainsCTEHashBuildScan(siblingID, make(map[int32]bool)) {
						continue
					}
					if requiredChild, exists := requiredBuildChildByJoin[path.nodeID]; exists && requiredChild != path.childID {
						continue
					}
					requiredBuildChildByJoin[path.nodeID] = path.childID
					path.requiresHashBuild = true
				case planpb.Node_LEFT:
					// LEFT hash/loop joins consume the complete logical right build
					// before probing. Preserve the non-right physical orientation;
					// the nullable/probe side can never establish this witness.
					if node.IsRightJoin || len(node.Children) != 2 ||
						path.childID != node.Children[1] ||
						builder.subtreeContainsCTEHashBuildScan(node.Children[0], make(map[int32]bool)) {
						continue
					}
					if requiredChild, exists := requiredBuildChildByJoin[path.nodeID]; exists && requiredChild != path.childID {
						continue
					}
					requiredBuildChildByJoin[path.nodeID] = path.childID
					path.requiresHashBuild = true
				case planpb.Node_SEMI:
					if node.IsRightJoin || len(node.Children) != 2 ||
						path.childID != node.Children[1] ||
						builder.subtreeContainsCTEHashBuildScan(node.Children[0], make(map[int32]bool)) ||
						!builder.IsEquiJoin(node) {
						continue
					}
					if requiredChild, exists := requiredBuildChildByJoin[path.nodeID]; exists && requiredChild != path.childID {
						continue
					}
					requiredBuildChildByJoin[path.nodeID] = path.childID
					path.requiresHashBuild = true
				case planpb.Node_MARK:
					if node.IsRightJoin || len(node.Children) != 2 ||
						path.childID != node.Children[1] ||
						builder.subtreeContainsCTEHashBuildScan(node.Children[0], make(map[int32]bool)) ||
						!builder.cteMarkJoinBecomesHashSemi(path.nodeID, parents) {
						continue
					}
					if requiredChild, exists := requiredBuildChildByJoin[path.nodeID]; exists && requiredChild != path.childID {
						continue
					}
					requiredBuildChildByJoin[path.nodeID] = path.childID
					path.requiresHashBuild = true
				default:
					// CROSS, outer, probe-sensitive and unknown join shapes do not
					// prove that this exact input is consumed completely.
					continue
				}
			}
			parentIDs := parents[path.nodeID]
			if len(parentIDs) == 0 {
				if path.requiresHashBuild {
					witnessWithHashBuild = true
				} else {
					witnessWithoutHashBuild = true
				}
				continue
			}
			for _, parentID := range parentIDs {
				queue = append(queue, consumerPath{
					nodeID: parentID, childID: path.nodeID,
					requiresHashBuild: path.requiresHashBuild,
				})
			}
		}
		if witnessWithoutHashBuild || witnessWithHashBuild {
			hasDrainWitness = true
			if !witnessWithoutHashBuild && witnessWithHashBuild {
				hashBuildOccurrences[occurrence.rootID] = true
			}
		}
	}
	return hashBuildOccurrences, hasDrainWitness
}

func cteLimitPreservesFullInput(node *planpb.Node) bool {
	if node == nil || node.Limit == nil {
		return true
	}
	// Hash aggregation and sort cannot produce even their first row before
	// consuming the complete input. DISTINCT and WINDOW are deliberately not
	// included: their implementations may emit before reaching end-of-input.
	if node.NodeType != planpb.Node_AGG && node.NodeType != planpb.Node_SORT {
		return false
	}
	literal := node.Limit.GetLit()
	if literal == nil || literal.Isnull {
		return false
	}
	value, ok := literal.Value.(*planpb.Literal_U64Val)
	return ok && value.U64Val > 0
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
// producer. The original consumer predicates remain in place, so SQL
// three-valued logic is preserved: if a row can pass consumer i, it necessarily
// passes P1 OR ... OR Pn at the producer. rowDomainExact additionally proves
// that no truncation-safe local predicate was omitted from that union.
func (builder *QueryBuilder) cteSharedConsumerPredicate(
	rootID int32,
	occurrences []cteOccurrence,
) (*planpb.Expr, bool, bool) {
	parents := make(map[int32][]int32)
	reachable := make(map[int32]bool)
	builder.collectCTEParents(rootID, parents, reachable)

	localPredicates := make([][]*planpb.Expr, len(occurrences))
	localDomainsComplete := make([]bool, len(occurrences))
	commonColumns := make(map[int32]bool)
	hasUnfilteredConsumer := false
	for i, occurrence := range occurrences {
		if !reachable[occurrence.rootID] {
			return nil, false, false
		}
		var ok bool
		localPredicates[i], localDomainsComplete[i], ok =
			builder.cteOccurrenceLocalPredicates(occurrence, parents)
		if !ok {
			return nil, false, false
		}
		if len(localPredicates[i]) == 0 {
			if localDomainsComplete[i] {
				hasUnfilteredConsumer = true
			}
			continue
		}
		constrainedColumns := make(map[int32]bool)
		for _, predicate := range localPredicates[i] {
			if colPos, ok := ctePredicateSingleOutputColumn(predicate, occurrence.rootTag); ok {
				constrainedColumns[colPos] = true
			}
		}
		if len(constrainedColumns) == 0 {
			return nil, false, false
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
	if hasUnfilteredConsumer {
		// One unfiltered consumer already requires the complete producer
		// domain; the union is therefore exactly the full domain.
		return nil, false, true
	}
	for i := range localPredicates {
		if len(localPredicates[i]) == 0 {
			// This consumer has only local predicates that cannot safely be
			// copied into the shared producer. The full domain is the only safe
			// producer bound, but it is not an exact evaluation domain.
			return nil, false, false
		}
	}
	if len(commonColumns) == 0 {
		return nil, false, false
	}

	disjuncts := make([]*planpb.Expr, 0, len(occurrences))
	producerTag := occurrences[0].rootTag
	rowDomainExact := true
	for _, complete := range localDomainsComplete {
		rowDomainExact = rowDomainExact && complete
	}
	for i, occurrence := range occurrences {
		predicates := make([]*planpb.Expr, 0, len(localPredicates[i]))
		for _, predicate := range localPredicates[i] {
			colPos, ok := ctePredicateSingleOutputColumn(predicate, occurrence.rootTag)
			if !ok || !commonColumns[colPos] {
				rowDomainExact = false
				continue
			}
			predicate = DeepCopyExpr(predicate)
			replaceColRefTag(predicate, occurrence.rootTag, producerTag)
			predicates = append(predicates, predicate)
		}
		conjunct, ok := builder.combineCTEPredicates("and", predicates)
		if !ok {
			return nil, false, false
		}
		disjuncts = append(disjuncts, conjunct)
	}

	sharedPredicate, ok := builder.combineCTEPredicates("or", disjuncts)
	return sharedPredicate, ok, ok && rowDomainExact
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
) ([]*planpb.Expr, bool, bool) {
	tagSet := map[int32]bool{occurrence.rootTag: true}
	predicates := make([]*planpb.Expr, 0, 2)
	domainComplete := true
	queue := append([]int32(nil), parents[occurrence.rootID]...)
	seen := make(map[int32]bool)
	for len(queue) > 0 {
		nodeID := queue[0]
		queue = queue[1:]
		if seen[nodeID] {
			return nil, false, false
		}
		seen[nodeID] = true
		node := builder.qry.Nodes[nodeID]
		if node.Limit != nil || node.Offset != nil {
			// A full-drain operator such as Top-N SORT can read every input row
			// while still delaying unneeded output expressions until after the
			// bound. Materializing before that bound expands their row domain.
			domainComplete = false
		}

		var candidates []*planpb.Expr
		switch node.NodeType {
		case planpb.Node_FILTER:
			if node.FilterIsBarrier || node.Limit != nil || node.Offset != nil {
				domainComplete = false
				continue
			}
			candidates = node.FilterList
		case planpb.Node_JOIN:
			// A join can remove rows from at least one input. Treat every join
			// shape as an inexact output-evaluation boundary; the INNER case
			// below is traversed only to collect safe producer bounds.
			domainComplete = false
			if node.JoinType != planpb.Node_INNER || node.Limit != nil || node.Offset != nil {
				continue
			}
			candidates = append(candidates, node.OnList...)
			candidates = append(candidates, node.FilterList...)
		case planpb.Node_AGG, planpb.Node_SORT:
			// These nodes establish the complete input evaluation domain before
			// an ancestor can reduce their output. A local LIMIT/OFFSET has already
			// made the proof inexact above.
			continue
		default:
			// Keep walking through projection and other binding boundaries. We do
			// not copy predicates across them without an explicit tag inversion,
			// but an outer FILTER/JOIN/LIMIT must still make the row-domain proof
			// inexact instead of disguising this occurrence as unfiltered.
			if len(node.FilterList) != 0 || node.NodeType == planpb.Node_APPLY ||
				node.NodeType == planpb.Node_SAMPLE {
				domainComplete = false
			}
			queue = append(queue, parents[nodeID]...)
			continue
		}

		for _, predicate := range candidates {
			if predicate == nil || !containsTag(predicate, occurrence.rootTag) {
				// A constant, parameter, or volatile predicate can reduce every
				// occurrence row even though it has no CTE column reference.
				domainComplete = false
				continue
			}
			if !containsOnlyTags(predicate, tagSet) {
				// A cross-relation predicate cannot be copied into the producer,
				// but it can still shrink the inline occurrence's evaluation domain.
				domainComplete = false
				continue
			}
			if !exprCanRemoveProject(predicate) {
				domainComplete = false
				continue
			}
			if !isTruncationSafePredicateExpr(predicate) {
				// The ordinary filter optimizer can move this deterministic local
				// predicate below the CTE output, but copying it into an OR across
				// consumers could introduce new evaluation failures. Omit it from
				// the producer bound and remember that the row domain is inexact.
				domainComplete = false
				continue
			}
			predicates = append(predicates, predicate)
		}
		queue = append(queue, parents[nodeID]...)
	}
	return predicates, domainComplete, true
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
