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

package group

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func stableFinalizationExpressions(exprs []*plan.Expr) bool {
	for _, expr := range exprs {
		if !stableFinalizationExpression(expr) {
			return false
		}
	}
	return true
}

func stableFinalizationExpression(expr *plan.Expr) bool {
	if expr == nil {
		return true
	}
	switch value := expr.Expr.(type) {
	case *plan.Expr_F:
		if value.F == nil || !stableFinalizationFunction(value.F.Func) {
			return false
		}
		return stableFinalizationExpressions(value.F.Args)
	case *plan.Expr_List:
		if value.List == nil {
			return false
		}
		return stableFinalizationExpressions(value.List.List)
	default:
		return true
	}
}

func stableFinalizationFunction(ref *plan.ObjectRef) bool {
	if ref == nil || ref.ObjName == "" {
		return false
	}
	overload, exists := function.GetFunctionByIdWithoutError(ref.Obj)
	return exists && !overload.CannotFold() && !overload.IsRealTimeRelated()
}

func supportedFinalizationAggregates(
	aggs []aggexec.AggFuncExecExpression,
) bool {
	for i := range aggs {
		agg := &aggs[i]
		if agg.IsDistinct() ||
			agg.GetConfigType() != plan.AggregateConfigType_AGG_CONFIG_NONE {
			return false
		}
		switch agg.GetAggID() {
		case aggexec.AggIdOfCountColumn, aggexec.AggIdOfCountStar,
			aggexec.AggIdOfSum, aggexec.AggIdOfMin, aggexec.AggIdOfMax:
		default:
			return false
		}
	}
	return true
}

func supportsConcreteFinalization(aggs []aggexec.GroupAggFuncExec) bool {
	for _, agg := range aggs {
		if agg == nil || !aggexec.SupportsChunkFinalization(agg) {
			return false
		}
	}
	return true
}

func (ctr *container) attachFinalConsumer(
	consumer colexec.FinalizedBatchConsumer,
) (colexec.FinalizedBatchConsumerToken, bool) {
	if consumer == nil || ctr.finalConsumer != nil || ctr.finalConsumerDisabled {
		return 0, false
	}
	ctr.nextFinalConsumerToken++
	if ctr.nextFinalConsumerToken == 0 {
		ctr.nextFinalConsumerToken++
	}
	ctr.finalConsumer = consumer
	ctr.finalConsumerToken = ctr.nextFinalConsumerToken
	return ctr.finalConsumerToken, true
}

func (ctr *container) detachFinalConsumer(
	token colexec.FinalizedBatchConsumerToken,
) {
	if token == 0 || token != ctr.finalConsumerToken {
		return
	}
	ctr.finalConsumer = nil
	ctr.finalConsumerToken = 0
}

func (group *Group) TryAttachFinalizedBatchConsumer(
	consumer colexec.FinalizedBatchConsumer,
) (colexec.FinalizedBatchConsumerToken, bool) {
	stats := group.OpAnalyzer.GetOpStats()
	switch {
	case !group.NeedEval:
		stats.AddExtraStat("GroupTopKFallbackPartial", 1)
		return 0, false
	case group.ctr.mtyp == H0:
		stats.AddExtraStat("GroupTopKFallbackScalar", 1)
		return 0, false
	case group.ctr.groupingAware || hasInactiveGroupingColumn(group.GroupingFlag):
		stats.AddExtraStat("GroupTopKFallbackGroupingSets", 1)
		return 0, false
	case !stableFinalizationExpressions(group.ProjectList):
		stats.AddExtraStat("GroupTopKFallbackVolatileProjection", 1)
		return 0, false
	case !supportedFinalizationAggregates(group.Aggs) ||
		len(group.ctr.aggList) != len(group.Aggs) ||
		!supportsConcreteFinalization(group.ctr.aggList):
		stats.AddExtraStat("GroupTopKFallbackUnsupportedAggregate", 1)
		return 0, false
	}
	token, attached := group.ctr.attachFinalConsumer(consumer)
	if attached {
		stats.AddExtraStat("GroupTopKFusionEligible", 1)
	}
	return token, attached
}

func (group *Group) DetachFinalizedBatchConsumer(
	token colexec.FinalizedBatchConsumerToken,
) {
	group.ctr.detachFinalConsumer(token)
}

func (mergeGroup *MergeGroup) TryAttachFinalizedBatchConsumer(
	consumer colexec.FinalizedBatchConsumer,
) (colexec.FinalizedBatchConsumerToken, bool) {
	// MergeGroup learns its hash-key shape and constructs concrete aggregate
	// executors only after decoding the first partial batch. Accept a provisional
	// attachment here and close the capability gate before the first admission.
	stats := mergeGroup.OpAnalyzer.GetOpStats()
	if !stableFinalizationExpressions(mergeGroup.ProjectList) {
		stats.AddExtraStat("GroupTopKFallbackVolatileProjection", 1)
		return 0, false
	}
	if !supportedFinalizationAggregates(mergeGroup.Aggs) {
		stats.AddExtraStat("GroupTopKFallbackUnsupportedAggregate", 1)
		return 0, false
	}
	token, attached := mergeGroup.ctr.attachFinalConsumer(consumer)
	if attached {
		stats.AddExtraStat("GroupTopKFusionEligible", 1)
	}
	return token, attached
}

func (mergeGroup *MergeGroup) DetachFinalizedBatchConsumer(
	token colexec.FinalizedBatchConsumerToken,
) {
	mergeGroup.ctr.detachFinalConsumer(token)
}

type finalProjection func(*batch.Batch) (*batch.Batch, error)

// drainFinalResults transfers one bounded aggregate chunk at a time into the
// attached consumer. It returns false only when the provisional capability
// gate closes before any batch has been admitted, so the caller can safely use
// the ordinary pull path.
func (ctr *container) drainFinalResults(
	proc *process.Process,
	opAnalyzer process.Analyzer,
	aggExprs []aggexec.AggFuncExecExpression,
	project finalProjection,
) (bool, error) {
	if ctr.finalConsumer == nil || ctr.finalConsumerDisabled {
		return false, nil
	}
	// A single resident output chunk already has the target O(B) peak. Calling
	// back into Top cannot release another aggregate chunk early, so it only
	// adds boundary overhead. Keep the ordinary pull path unless spill state
	// proves that another disjoint resident partition still has to be loaded.
	if !ctr.finalBatchAdmitted && len(ctr.groupByBatches) <= 1 &&
		ctr.currentSpillBkt == nil &&
		(ctr.spillBkts == nil || ctr.spillBkts.Len() == 0) {
		opAnalyzer.GetOpStats().AddExtraStat(
			"GroupTopKFallbackSingleChunk", 1)
		ctr.finalConsumerDisabled = true
		return false, nil
	}
	if ctr.mtyp == H0 || ctr.groupingAware ||
		len(ctr.aggList) != len(aggExprs) ||
		!supportsConcreteFinalization(ctr.aggList) {
		if ctr.finalBatchAdmitted {
			return true, moerr.NewInternalErrorNoCtx(
				"aggregate finalization capability changed after admission")
		}
		opAnalyzer.GetOpStats().AddExtraStat(
			"GroupTopKFallbackConcreteCapability", 1)
		ctr.finalConsumerDisabled = true
		return false, nil
	}
	opStats := opAnalyzer.GetOpStats()
	opStats.AddExtraStat("GroupTopKFusionUsed", 1)

	if err := ctr.releaseFinalRecoveryCapacity(); err != nil {
		return true, err
	}
	for {
		for ctr.currBatchIdx < len(ctr.groupByBatches) {
			if err, canceled := vm.CancelCheck(proc); canceled {
				return true, err
			}
			chunk := ctr.currBatchIdx
			source := ctr.groupByBatches[chunk]
			if source == nil || source.RowCount() == 0 {
				ctr.currBatchIdx++
				continue
			}

			for i, agg := range ctr.aggList {
				vec, err := aggexec.FinalizeChunk(proc.Ctx, agg, chunk)
				if err != nil {
					source.Clean(ctr.mp)
					ctr.groupByBatches[chunk] = nil
					return true, err
				}
				if !vec.HasPrepareParamKind() {
					vec.SetPrepareParamKind(ctr.prepareParamKind.Get(i))
				}
				source.Vecs = append(source.Vecs, vec)
			}
			ctr.currBatchIdx++

			projected, err := project(source)
			if err == nil {
				opStats.AddExtraStat("GroupTopKFinalizedChunks", 1)
				opStats.AddExtraStat(
					"GroupTopKFinalizedRows", int64(projected.RowCount()))
				opStats.AddExtraStat(
					"GroupTopKFinalizedBytes", int64(projected.Size()))
				opAnalyzer.Output(projected)
				err = ctr.finalConsumer.ConsumeFinalizedBatch(proc, projected)
			}
			// The source owns every group and aggregate vector. Projection
			// vectors remain owned by its expression executors.
			source.Clean(ctr.mp)
			ctr.groupByBatches[chunk] = nil
			if err != nil {
				return true, err
			}
			ctr.finalBatchAdmitted = true
		}

		// Every key/result chunk in this resident partition has been consumed.
		// Release the output owners and lookup index before loading another
		// partition; neither is needed by Top after the synchronous callback.
		ctr.freeGroupByBatches()
		ctr.hr.Free0()
		ctr.freeAggList()
		loaded, err := ctr.loadSpilledData(proc, opAnalyzer, aggExprs)
		if err != nil {
			return true, err
		}
		if !loaded {
			break
		}
		if len(ctr.aggList) != len(aggExprs) ||
			!supportsConcreteFinalization(ctr.aggList) {
			return true, moerr.NewInternalErrorNoCtx(
				"spill reload changed aggregate finalization capability")
		}
	}
	if err := ctr.releaseFinalRecoveryCapacity(); err != nil {
		return true, err
	}
	ctr.state = vm.End
	return true, nil
}
