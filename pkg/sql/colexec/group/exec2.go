// Copyright 2024 Matrix Origin
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
	"bytes"
	"context"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	// we use this size as preferred output batch size, which is typical
	// in MO.
	aggBatchSize = aggexec.AggBatchSize

	// we use this size as pre-allocated size for hash table.
	aggHtPreAllocSize = 1024

	// Aggregate spill keeps the historical fanout: aggregate state makes each
	// record relatively expensive, while partial aggregation usually keeps a
	// first-level bucket below the resident capacity. DISTINCT has no aggregate
	// state and can retain almost every input row, so use a wider first pass to
	// avoid rewriting every bucket at the next level. Its optional per-bucket
	// writer buffers remain bounded to at most 4 MiB in total.
	spillNumBuckets         = 32
	spillMaskBits           = 5 // log2(spillNumBuckets)
	spillDistinctNumBuckets = 64
	spillDistinctMaskBits   = 6 // log2(spillDistinctNumBuckets)
	spillMaxNumBuckets      = spillDistinctNumBuckets
	spillMaxPass            = 3
	spillIOBufSize          = 1024 * 1024 // 1 MiB read-ahead buffer for spill file reads
	spillWrBufSize          = 64 * 1024   // 64 KiB bounded buffer per open spill bucket
)

func (ctr *container) spillPartitionCount() int {
	if len(ctr.aggList) == 0 {
		return spillDistinctNumBuckets
	}
	return spillNumBuckets
}

func hasInactiveGroupingColumn(flags []bool) bool {
	for _, flag := range flags {
		if !flag {
			return true
		}
	}
	return false
}

// UsesGroupingAwareHash reports whether partial group keys use the extended
// hash grammar that distinguishes a rolled-up grouping sentinel from SQL NULL.
// MergeGroup must know this from the plan: an individual partial can contain
// only the fully active grouping set and therefore carry no sentinel bits even
// though later partials in the same stream do.
func (group *Group) UsesGroupingAwareHash() bool {
	return group != nil &&
		(group.DynamicGrouping || hasInactiveGroupingColumn(group.GroupingFlag))
}

func (group *Group) Prepare(proc *process.Process) (err error) {
	group.diagnosticsLogged = false
	group.ctr.state = vm.Build
	if group.ctr.mp != nil {
		group.ctr.free()
	}
	group.ctr.prepareParamKind.Reset(group.Aggs)
	group.ctr.aggExprs = group.Aggs
	group.ctr.prepareParamKindWireV1 = prepareParamKindWireV1Enabled(proc) &&
		hasPrepareParamKindPreservingAgg(group.Aggs)
	group.ctr.mp = mpool.MustNewNoLock("group_mpool")
	if group.ctr.allocationAccount != nil {
		if err = group.ctr.mp.BindAllocationAccount(
			group.ctr.allocationAccount,
		); err != nil {
			return err
		}
		group.ctr.budget, err = proc.GetExecutionResourceBudget()
		if err != nil {
			return err
		}
		if err = group.ctr.installRecoveryCapacity(); err != nil {
			return err
		}
	}
	group.ctr.legacyTextMinMax = useLegacyTextMinMaxForRemote(proc)
	group.ctr.legacyVarianceState = useLegacyVarianceStateForRemote(proc)

	// debug,
	// group.ctr.mp.EnableDetailRecording()

	if group.OpAnalyzer != nil {
		group.OpAnalyzer.Reset()
	}
	group.OpAnalyzer = process.NewAnalyzer(group.GetIdx(), group.IsFirst, group.IsLast, "group")

	// Ordered aggregate setup consumes the effective spill threshold. Set it
	// before preparing aggregate executors so the first execution behaves the
	// same as a reused prepared operator.
	group.ctr.setSpillMem(group.SpillMem)
	group.ctr.setGroupByHashKey(group.GroupByHashKey)
	if len(group.GroupByHashKey) > 0 &&
		(group.DynamicGrouping || hasInactiveGroupingColumn(group.GroupingFlag)) {
		return moerr.NewInternalErrorNoCtx("group-by hash key cannot be used with grouping sets")
	}
	if err = group.ctr.validateGroupByHashKey(len(group.GroupBy)); err != nil {
		return err
	}

	if err = group.prepareGroupAndAggArg(proc); err != nil {
		return err
	}

	if err = group.PrepareProjectionWithAllocation(
		proc, group.ctr.expressionAllocation); err != nil {
		return err
	}

	return nil
}

func (group *Group) prepareGroupAndAggArg(proc *process.Process) (err error) {
	if len(group.ctr.groupByEvaluate.Executor) == len(group.GroupBy) {
		group.ctr.groupByEvaluate.ResetForNextQuery()
	} else {
		// calculate the key width and key nullable, and hash table type.
		group.ctr.keyWidth, group.ctr.keyNullable = 0, false
		hashKeyCount := len(group.GroupBy)
		if len(group.GroupByHashKey) > 0 {
			hashKeyCount = len(group.GroupByHashKey)
		}
		for i := 0; i < hashKeyCount; i++ {
			exprIdx := i
			if len(group.GroupByHashKey) > 0 {
				exprIdx = int(group.GroupByHashKey[i])
			}
			group.ctr.keyNullable = group.ctr.keyNullable ||
				!group.GroupBy[exprIdx].Typ.NotNullable
		}
		for i := 0; i < hashKeyCount; i++ {
			exprIdx := i
			if len(group.GroupByHashKey) > 0 {
				exprIdx = int(group.GroupByHashKey[i])
			}
			expr := group.GroupBy[exprIdx]
			if expr.Typ.Id == int32(types.T_tuple) {
				return moerr.NewInternalErrorNoCtx("tuple is not supported as group by column")
			}
			width := GetKeyWidth(types.T(expr.Typ.Id), expr.Typ.Width, group.ctr.keyNullable)
			group.ctr.keyWidth += int32(width)
		}

		if group.ctr.keyWidth == 0 {
			group.ctr.mtyp = H0
		} else if group.ctr.keyWidth <= 8 {
			group.ctr.mtyp = H8
		} else {
			group.ctr.mtyp = HStr
		}

		group.ctr.groupingAware = false
		if group.DynamicGrouping {
			group.ctr.mtyp = HStr
			group.ctr.groupingAware = true
		}
		for _, flag := range group.GroupingFlag {
			if !flag {
				group.ctr.mtyp = HStr
				group.ctr.groupingAware = true
				break
			}
		}

		// create group by evaluate
		group.ctr.groupByEvaluate.Free()
		group.ctr.groupByEvaluate, err = colexec.MakeEvalVectorWithAllocation(
			proc,
			group.GroupBy,
			group.ctr.expressionAllocation,
		)
		if err != nil {
			return err
		}
	}

	if group.ctr.mtyp == H0 {
		// no group by, only one group, always create the dummy group by batch.
		if len(group.ctr.groupByBatches) == 0 {
			groupByBatch, err := group.ctr.createNewGroupByBatch(
				group.ctr.groupByEvaluate.Vec,
				1,
			)
			if err != nil {
				return err
			}
			group.ctr.groupByBatches = append(
				group.ctr.groupByBatches,
				groupByBatch,
			)
			group.ctr.groupByBatches[0].SetRowCount(1)
		}
	}

	needMakeAggArg := true
	if len(group.ctr.aggArgEvaluate) == len(group.Aggs) {
		needMakeAggArg = false
		for i := range group.ctr.aggArgEvaluate {
			if len(group.ctr.aggArgEvaluate[i].Vec) != len(group.Aggs[i].GetArgExpressions()) {
				needMakeAggArg = true
				break
			} else {
				group.ctr.aggArgEvaluate[i].ResetForNextQuery()
			}
		}
	}

	if needMakeAggArg {
		for i := range group.ctr.aggArgEvaluate {
			group.ctr.aggArgEvaluate[i].Free()
		}
		group.ctr.aggArgEvaluate = make([]colexec.ExprEvalVector, 0, len(group.Aggs))
		for _, ag := range group.Aggs {
			e, err := colexec.MakeEvalVectorWithAllocation(
				proc,
				ag.GetArgExpressions(),
				group.ctr.expressionAllocation,
			)
			if err != nil {
				return err
			}
			group.ctr.aggArgEvaluate = append(group.ctr.aggArgEvaluate, e)
		}
	}

	// have not generated aggList agg exec yet, lets do it.
	if len(group.Aggs) > 0 {
		if len(group.ctr.aggList) == len(group.Aggs) {
			for _, ag := range group.ctr.aggList {
				ag.Free()
				if group.ctr.mtyp == H0 {
					if err := ag.GroupGrow(1); err != nil {
						return err
					}
				}
			}
		} else {
			group.ctr.aggList, err = group.ctr.makeAggList(group.Aggs)
			if err != nil {
				return err
			}
		}
	}
	group.configureH0OrderedAggSpill(proc)

	return nil
}

func GetKeyWidth(id types.T, width0 int32, nullable bool) (width int) {
	if id.FixedLength() < 0 {
		width = 128
		if width0 > 0 {
			width = int(width0)
		}

		if id == types.T_array_float32 {
			width *= 4
		}
		if id == types.T_array_float64 {
			width *= 8
		}
		if id == types.T_array_bf16 || id == types.T_array_float16 {
			width *= 2
		}
		// T_array_int8 / T_array_uint8 are 1 byte/element -> width unchanged
		// (width0 already counts).
	} else {
		width = id.TypeLen()
	}

	if nullable {
		width++
	}
	return width
}

// main entry of the group operator.
func (group *Group) Call(proc *process.Process) (vm.CallResult, error) {
	var err error

	var isCancel bool
	if err, isCancel = vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	switch group.ctr.state {
	case vm.Build, vm.EvalReset:
		if group.ctr.state == vm.EvalReset {
			group.ctr.resetForSpill()
			group.ctr.state = vm.Build
		}

		// receive all data, loop till exhuasted.
		for !group.ctr.inputDone {
			var r vm.CallResult
			r, err = vm.ChildrenCall(group.GetChildren(0), proc, group.OpAnalyzer)
			if err != nil {
				return vm.CancelResult, err
			}

			// all handled, going to eval mode.
			//
			// XXX: Note that this test, r.Batch == nil is treated as ExecStop.
			// I am not sure this is correct, but our code depends on this.
			// Esp, some table function will produce ExecNext result with nil
			// batch as end of data.   Shuffle, on the otherhand may product
			// more batches after sending a ExecStop result.
			//
			// if r.Status == vm.ExecStop || r.Batch == nil {
			if r.Batch == nil {
				group.ctr.state = vm.Eval
				group.ctr.inputDone = true
			}

			// empty batch, skip.
			if r.Batch == nil || r.Batch.IsEmpty() {
				continue
			}

			if len(group.ctr.aggList) != len(group.Aggs) {
				group.ctr.aggList, err = group.ctr.makeAggList(group.Aggs)
				if err != nil {
					return vm.CancelResult, err
				}
			}

			// build one batch.
			var needSpill bool
			needSpill, err = group.buildOneBatch(proc, r.Batch)
			if err != nil {
				return vm.CancelResult, err
			}

			if needSpill {
				// we need to spill the data to disk.
				if group.NeedEval {
					if bytes, rows, err := group.ctr.spillDataToDisk(proc, group.OpAnalyzer, nil); err != nil {
						return vm.CancelResult, err
					} else {
						group.OpAnalyzer.Spill(bytes)
						group.OpAnalyzer.SpillRows(rows)
					}
					// continue the loop, to receive more data.
				} else {
					// break the loop, output the intermediate result.
					// set state to Eval, so that we can output ALL
					// the intermediate result.
					group.ctr.state = vm.Eval
					break
				}
			}
		}

		if group.ctr.inputDone {
			// EOF and cancellation can arrive in the same child call. Observe
			// cancellation before flushing or reloading spill state.
			if err, isCancel = vm.CancelCheck(proc); isCancel {
				return vm.CancelResult, err
			}
			if err = group.ensureRuntimeEmptyGroupingSet(); err != nil {
				return vm.CancelResult, err
			}
		}

		// spilling -- spill whatever left in memory, and load first spilled bucket.
		if group.ctr.isSpilling() {
			if group.ctr.distinctSpill != nil {
				if _, err = group.ctr.drainExactCountDistinct(
					proc, group.OpAnalyzer); err != nil {
					return vm.CancelResult, err
				}
			}
			if bytes, rows, err := group.ctr.spillDataToDisk(proc, group.OpAnalyzer, nil); err != nil {
				return vm.CancelResult, err
			} else {
				group.OpAnalyzer.Spill(bytes)
				group.OpAnalyzer.SpillRows(rows)
			}
			if group.ctr.distinctSpill != nil && group.ctr.mtyp != H0 {
				if err = group.ctr.prepareGroupedDistinctContributions(proc); err != nil {
					return vm.CancelResult, err
				}
			}
			if _, err = group.ctr.loadSpilledData(proc, group.OpAnalyzer, group.Aggs); err != nil {
				return vm.CancelResult, err
			}
		}
		if group.NeedEval && group.ctr.inputDone {
			if err = group.ctr.finalizeExactCountDistinct(
				proc, group.OpAnalyzer); err != nil {
				return vm.CancelResult, err
			}
		}

		return group.outputOneBatch(proc)

	case vm.Eval:
		return group.outputOneBatch(proc)

	case vm.End:
		return vm.CancelResult, nil
	}

	err = moerr.NewInternalError(proc.Ctx, "bug: unknown group state")
	return vm.CancelResult, err
}

// ensureRuntimeEmptyGroupingSet preserves the one-row identity of a legacy
// all-rolled grouping-set branch. It applies to both final and partial Group:
// partial empty states merge idempotently, while a single-stage Group emits
// the SQL result directly.
func (group *Group) ensureRuntimeEmptyGroupingSet() error {
	if group.DynamicGrouping || len(group.GroupBy) == 0 ||
		len(group.GroupingFlag) != len(group.GroupBy) ||
		len(group.ctr.groupByBatches) > 0 || group.ctr.isSpilling() {
		return nil
	}
	for _, active := range group.GroupingFlag {
		if active {
			return nil
		}
	}

	groupTypes := group.ctr.groupByEvaluate.Typ
	if len(groupTypes) != len(group.GroupBy) {
		return moerr.NewInternalErrorNoCtx(
			"invalid empty grouping-set group metadata")
	}
	output, err := group.ctr.newRuntimeEmptyGroupingSetBatch(groupTypes, nil)
	if err != nil {
		return err
	}
	if len(group.ctr.aggList) != len(group.Aggs) {
		group.ctr.aggList, err = group.ctr.makeAggList(group.Aggs)
		if err != nil {
			output.Clean(group.ctr.mp)
			return err
		}
	}
	for _, agg := range group.ctr.aggList {
		if err = agg.GroupGrow(1); err != nil {
			output.Clean(group.ctr.mp)
			return err
		}
	}
	group.ctr.groupByTypes = append(group.ctr.groupByTypes[:0], groupTypes...)
	group.ctr.groupByBatches = append(group.ctr.groupByBatches, output)
	return nil
}

func (group *Group) buildOneBatch(proc *process.Process, bat *batch.Batch) (bool, error) {
	var err error

	// without group by, there is only one group.
	if group.ctr.mtyp == H0 {
		if err = group.evaluateBuildInput(proc, bat); err != nil {
			return false, err
		}
		// COUNT(*) is row-count only.  Projection-pruned joins can represent a
		// very large number of matches with one zero-column batch, so consuming
		// it a UnitLimit-sized slice at a time would recreate the materialization
		// cost that the join avoided.
		if len(group.ctr.aggList) == 1 &&
			group.ctr.aggList[0].AggID() == aggexec.AggIdOfCountStar &&
			len(group.ctr.aggArgEvaluate) == 1 &&
			len(group.ctr.aggArgEvaluate[0].Vec) > 0 {
			if err = group.ctr.aggList[0].BulkFill(
				0, group.ctr.aggArgEvaluate[0].Vec,
			); err != nil {
				return false, err
			}
			group.OpAnalyzer.SetMemUsed(group.ctr.memUsed())
			return false, nil
		}
		// note that in prepare we already called GroupGrow(1) for each agg.
		var oneGroup [hashmap.UnitLimit]uint64
		for i := range oneGroup {
			oneGroup[i] = 1
		}
		for offset := 0; offset < bat.RowCount(); offset += hashmap.UnitLimit {
			n := min(hashmap.UnitLimit, bat.RowCount()-offset)
			groups := oneGroup[:n]
			for {
				for i, agg := range group.ctr.aggList {
					if err = agg.PreflightBatchFill(
						offset, groups, group.ctr.aggArgEvaluate[i].Vec); err != nil {
						break
					}
				}
				if err != nil {
					if retried, retryErr := group.retryBuildBatchAfterCapacity(
						proc, err); retried {
						err = nil
						continue
					} else {
						return false, retryErr
					}
				}
				for i, agg := range group.ctr.aggList {
					if err = agg.BatchFill(
						offset, groups, group.ctr.aggArgEvaluate[i].Vec); err != nil {
						return false, err
					}
				}
				break
			}
			shouldDrain, err := group.ctr.shouldDrainExactCountDistinct()
			if err != nil {
				return false, err
			}
			if shouldDrain {
				if _, err := group.ctr.drainExactCountDistinct(
					proc, group.OpAnalyzer); err != nil {
					return false, err
				}
			}
		}
		group.OpAnalyzer.SetMemUsed(group.ctr.memUsed())
		return false, nil
	} else {
		// here is a strange loop.   our hash table exposed something called
		// hashmap.UnitLimit -- which limits per iteration insert mini batch size.
		count := bat.RowCount()
		evaluated := false
		var hashBytesBefore int64
		if !group.ctr.hr.IsEmpty() {
			hashBytesBefore = group.ctr.hr.Hash.Size()
		}
		for i := 0; i < count; i += hashmap.UnitLimit {
			n := min(count-i, hashmap.UnitLimit)
			var preview groupInsertPreview
			var aggregateGroupScratch [hashmap.UnitLimit]uint64
			for {
				err = nil
				if !evaluated {
					if err = group.evaluateBuildInput(proc, bat); err != nil {
						if retried, retryErr := group.retryBuildBatchAfterCapacity(proc, err); retried {
							continue
						} else {
							return false, retryErr
						}
					}
					evaluated = true
				}
				if group.ctr.hr.IsEmpty() {
					err = group.ctr.buildHashTable(proc.Ctx, 0)
					if err == nil {
						hashBytesBefore = group.ctr.hr.Hash.Size()
					}
				}
				if err == nil {
					hashKeyVecs := group.ctr.hashKeyVectors(
						group.ctr.groupByEvaluate.Vec)
					err = group.ctr.hr.TxnItr.PreviewInsert(
						i, n, hashKeyVecs,
						group.ctr.hr.Hash.GroupCount(),
						&group.ctr.hr.insertPlan)
					if err == nil {
						preview.values = group.ctr.hr.insertPlan.Values()
						preview.inserted = group.ctr.hr.insertPlan.Inserted()
						preview.newGroups = int(group.ctr.hr.insertPlan.NewGroups())
					}
					// Reserve only for the groups the immutable preview proved this
					// unit will publish. Rejection is still before hash/key/aggregate
					// mutation, so the resident prefix can be spilled and this same
					// input offset retried without duplication.
					if err == nil &&
						!group.ctr.recoveryCapacityCovers(preview.newGroups) {
						err = group.ctr.ensureRecoveryCapacity(
							preview.newGroups,
							group.OpAnalyzer,
						)
					}
					if err == nil {
						err = group.ctr.hr.Hash.PreAlloc(
							group.ctr.hr.insertPlan.NewGroups())
					}
					if err == nil {
						err = group.ctr.preflightBuildChunk(
							group.ctr.groupByEvaluate.Vec, i, n,
							preview.inserted, preview.newGroups)
					}
					if err == nil {
						aggregateGroups := preview.values[:n]
						if group.DynamicGrouping {
							aggregateGroups, err = dynamicGroupingAggregateGroups(
								bat, i, aggregateGroups, aggregateGroupScratch[:n])
						}
						if err == nil {
							for j, agg := range group.ctr.aggList {
								if err = agg.PreflightBatchFill(
									i, aggregateGroups,
									group.ctr.aggArgEvaluate[j].Vec); err != nil {
									break
								}
							}
						}
					}
				}
				if err != nil {
					group.ctr.cancelGroupByPreflights()
				}
				if err == nil {
					vals, more, insertErr := group.ctr.commitGroupByChunk(
						group.ctr.groupByEvaluate.Vec, i, n, preview)
					if insertErr != nil {
						if !isGroupPrePublicationError(insertErr) {
							return false, insertErr
						}
						group.ctr.cancelGroupByPreflights()
						err = insertErr
					} else {
						if more > 0 {
							for _, agg := range group.ctr.aggList {
								if growErr := agg.GroupGrow(more); growErr != nil {
									return false, growErr
								}
							}
						}
						aggregateGroups := vals[:n]
						if group.DynamicGrouping {
							aggregateGroups, err = dynamicGroupingAggregateGroups(
								bat, i, aggregateGroups, aggregateGroupScratch[:n])
							if err != nil {
								return false, err
							}
						}
						for j, agg := range group.ctr.aggList {
							if err = agg.BatchFill(
								i, aggregateGroups, group.ctr.aggArgEvaluate[j].Vec); err != nil {
								return false, err
							}
						}
						break
					}
				}
				if retried, retryErr := group.retryBuildBatchAfterCapacity(proc, err); retried {
					evaluated = false
					continue
				} else {
					return false, retryErr
				}
			}
		} // end of mini batch for loop

		observeHashGrowth(group.OpAnalyzer.GetOpStats(), "GroupHashBuild", hashBytesBefore, group.ctr.hr.Hash.Size())
		// Prefer subdividing eligible exact DISTINCT keys before generic Group
		// spill. A hot group's key set can make progress only on this axis.
		shouldDrain, err := group.ctr.shouldDrainExactCountDistinct()
		if err != nil {
			return false, err
		}
		if shouldDrain {
			if _, err := group.ctr.drainExactCountDistinct(
				proc, group.OpAnalyzer); err != nil {
				return false, err
			}
		}
		// Compact group state may still require existing group-hash spill. Before
		// its first record is written, move every eligible exact key to the
		// independent spool so no pre-activation hot-group record can survive.
		needSpill := group.ctr.needSpill(group.OpAnalyzer)
		if needSpill && group.ctr.distinctSpill == nil {
			hasDistinct, err := group.ctr.hasExactCountDistinctArguments()
			if err != nil {
				return false, err
			}
			if hasDistinct {
				if _, err := group.ctr.drainExactCountDistinct(
					proc, group.OpAnalyzer); err != nil {
					return false, err
				}
				needSpill = group.ctr.needSpill(group.OpAnalyzer)
			}
		}
		return needSpill, nil
	}
}

func dynamicGroupingAggregateGroups(
	bat *batch.Batch,
	offset int,
	groups []uint64,
	scratch []uint64,
) ([]uint64, error) {
	markerPos := len(bat.Vecs) - 2
	if markerPos < 0 || len(scratch) < len(groups) {
		return nil, moerr.NewInvalidInputNoCtx("dynamic grouping input is missing its aggregate marker")
	}
	marker := bat.Vecs[markerPos]
	if marker == nil || marker.GetType().Oid != types.T_bool || offset < 0 || offset+len(groups) > marker.Length() {
		return nil, moerr.NewInvalidInputNoCtx("invalid dynamic grouping aggregate marker")
	}

	hasSynthetic := false
	for i := range groups {
		row := offset + i
		if marker.IsNull(uint64(row)) {
			return nil, moerr.NewInvalidInputNoCtx("dynamic grouping aggregate marker cannot be NULL")
		}
		if vector.GetFixedAtNoTypeCheck[bool](marker, row) {
			hasSynthetic = true
		}
	}
	if !hasSynthetic {
		return groups, nil
	}
	copy(scratch, groups)
	for i := range groups {
		if vector.GetFixedAtNoTypeCheck[bool](marker, offset+i) {
			scratch[i] = aggexec.GroupNotMatched
		}
	}
	return scratch[:len(groups)], nil
}

func (group *Group) evaluateBuildInput(
	proc *process.Process,
	bat *batch.Batch,
) error {
	if err := group.evaluateGroupByAndAggArgs(proc, bat); err != nil {
		return err
	}
	for i := range group.Aggs {
		if i >= len(group.ctr.aggArgEvaluate) ||
			len(group.ctr.aggArgEvaluate[i].Vec) == 0 {
			continue
		}
		arg := group.ctr.aggArgEvaluate[i].Vec[0]
		if arg.Length() > 0 && !arg.AllNull() {
			group.ctr.prepareParamKind.Observe(i, arg.GetPrepareParamKind())
		}
	}
	return nil
}

func (group *Group) retryBuildBatchAfterCapacity(
	proc *process.Process,
	cause error,
) (bool, error) {
	if group == nil || group.ctr.allocationAccount == nil ||
		!mpool.IsRetryableAllocationCapacity(cause) {
		return false, cause
	}
	if drained, err := group.ctr.drainExactCountDistinct(
		proc, group.OpAnalyzer); err != nil {
		return false, err
	} else if drained {
		return true, nil
	}
	if group.ctr.mtyp == H0 || group.ctr.hr.IsEmpty() ||
		group.ctr.hr.Hash.GroupCount() == 0 {
		return false, cause
	}
	before := group.ctr.hr.Hash.GroupCount()
	bytes, rows, err := group.ctr.spillDataToDisk(proc, group.OpAnalyzer, nil)
	if err != nil {
		return false, err
	}
	if rows <= 0 || uint64(rows) < before {
		return false, moerr.NewInternalErrorNoCtx(
			"group capacity recovery made no measurable spill progress")
	}
	group.OpAnalyzer.Spill(bytes)
	group.OpAnalyzer.SpillRows(rows)
	group.ctr.aggList, err = group.ctr.makeAggList(group.Aggs)
	if err != nil {
		return false, err
	}
	return true, nil
}

func observeHashGrowth(stats *process.OperatorStats, prefix string, before, after int64) {
	if stats == nil || after <= before {
		return
	}
	stats.AddExtraStat(prefix+"GrowthBatches", 1)
	stats.AddExtraStat(prefix+"GrowthBytes", after-before)
	stats.SetMaxExtraStat(prefix+"MaxBytes", after)
}

func (ctr *container) buildHashTable(ctx context.Context, preAllocated uint64) error {
	if preAllocated < aggHtPreAllocSize {
		preAllocated = aggHtPreAllocSize
	}
	// build hash table
	if err := ctr.hr.BuildHashTable(
		ctx, ctr.mp,
		false,
		ctr.mtyp == HStr,
		ctr.keyNullable,
		ctr.groupingAware,
		preAllocated,
		ctr.hashAllocation,
		ctr.hashIterator,
	); err != nil {
		return err
	}

	// pre-allocate groups for each agg.
	for _, ag := range ctr.aggList {
		if err := ag.PreAllocateGroups(aggHtPreAllocSize); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) preflightBuildChunk(
	vs []*vector.Vector,
	offset int,
	rows int,
	insertList []uint8,
	more int,
) error {
	if offset < 0 || rows < 0 || len(insertList) < rows ||
		more < 0 || more > rows {
		return mpool.ErrAllocationAccountInvalid
	}
	if rows == 0 {
		return nil
	}
	selected := 0
	for _, flag := range insertList[:rows] {
		selected += int(flag)
	}
	if selected != more {
		return mpool.ErrAllocationAccountInvariant
	}
	if more == 0 {
		return nil
	}
	if ctr.hr.Hash == nil {
		return mpool.ErrAllocationAccountInvariant
	}
	if len(ctr.groupByBatches) == 0 {
		groupByBatch, err := ctr.createNewGroupByBatch(vs, aggBatchSize)
		if err != nil {
			return err
		}
		ctr.groupByBatches = append(ctr.groupByBatches, groupByBatch)
	}
	current := ctr.groupByBatches[len(ctr.groupByBatches)-1]
	space := aggBatchSize - current.RowCount()
	if more > space && ctr.groupByStandby == nil {
		var err error
		ctr.groupByStandby, err = ctr.createNewGroupByBatch(vs, aggBatchSize)
		if err != nil {
			return err
		}
	}
	preAllocateVectors := func(
		destinations []*vector.Vector,
		targetRows int,
		flags []uint8,
	) error {
		for i, destination := range destinations {
			if i >= len(vs) || vs[i] == nil {
				return mpool.ErrAllocationAccountInvariant
			}
			if err := destination.PreExtendSelectedBatchValidated(
				vs[i], offset, rows, flags, targetRows, ctr.mp,
			); err != nil {
				return err
			}
		}
		return nil
	}
	cancelPreflight := cancelSelectedBatchPreflights
	var currentFlags, standbyFlags [hashmap.UnitLimit]uint8
	currentRows, standbyRows := 0, 0
	for row, flag := range insertList[:rows] {
		if flag == 0 {
			continue
		}
		if currentRows < space {
			currentFlags[row] = 1
			currentRows++
		} else {
			standbyFlags[row] = 1
			standbyRows++
		}
	}
	if currentRows > 0 {
		if err := preAllocateVectors(
			current.Vecs,
			current.RowCount()+currentRows,
			currentFlags[:rows],
		); err != nil {
			ctr.cancelGroupByPreflights()
			return err
		}
	} else {
		cancelPreflight(current.Vecs)
	}
	if standbyRows == 0 && ctr.groupByStandby != nil {
		cancelPreflight(ctr.groupByStandby.Vecs)
	}
	if standbyRows > 0 {
		if ctr.groupByStandby == nil {
			return mpool.ErrAllocationAccountInvariant
		}
		if err := preAllocateVectors(
			ctr.groupByStandby.Vecs,
			standbyRows,
			standbyFlags[:rows],
		); err != nil {
			ctr.cancelGroupByPreflights()
			return err
		}
	}
	for _, agg := range ctr.aggList {
		if err := agg.PreAllocateGroups(more); err != nil {
			ctr.cancelGroupByPreflights()
			return err
		}
	}
	return nil
}

func cancelSelectedBatchPreflights(vectors []*vector.Vector) {
	for _, destination := range vectors {
		destination.CancelSelectedBatchPreflight()
	}
}

func (ctr *container) cancelGroupByPreflights() {
	if len(ctr.groupByBatches) != 0 {
		cancelSelectedBatchPreflights(
			ctr.groupByBatches[len(ctr.groupByBatches)-1].Vecs)
	}
	if ctr.groupByStandby != nil {
		cancelSelectedBatchPreflights(ctr.groupByStandby.Vecs)
	}
}

func (ctr *container) boundedSpillReloadPreAlloc(bucketRows int64) uint64 {
	if bucketRows <= 0 || ctr.spillHashPreAllocSize == 0 {
		return 0
	}
	requested := min(uint64(bucketRows), ctr.spillHashPreAllocSize)
	// Values below 10K are the test-only group-count spill mode, not bytes.
	if ctr.spillMem < 10000 {
		return requested
	}

	used := ctr.memUsed()
	if used >= ctr.spillMem {
		return 0
	}
	available := uint64(ctr.spillMem - used)
	estimate := hashtable.EstimateInt64HashMapSize
	initial := hashtable.Int64HashMapInitialAllocationBytes()
	if ctr.mtyp == HStr {
		estimate = hashtable.EstimateStringHashMapSize
		initial = hashtable.StringHashMapInitialAllocationBytes()
	}
	required := func(cardinality uint64) uint64 {
		target := estimate(cardinality)
		if target > ^uint64(0)-initial {
			return ^uint64(0)
		}
		// PreAlloc builds the target cells before releasing the map's initial
		// cells, so the transient peak contains both allocations.
		return initial + target
	}
	if required(requested) <= available {
		return requested
	}

	// Find the largest cardinality whose hash-cell allocation fits below the
	// current spill threshold. buildHashTable still applies the historical 1024
	// minimum, so returning zero never makes the baseline allocation smaller.
	low, high := uint64(0), requested
	for low < high {
		mid := low + (high-low+1)/2
		if required(mid) <= available {
			low = mid
		} else {
			high = mid - 1
		}
	}
	return low
}

func (ctr *container) initGroupKeyTypesFromBatch(vs []*vector.Vector) {
	if len(vs) == 0 {
		return
	}

	if len(ctr.groupByTypes) == 0 {
		ctr.groupByTypes = make([]types.Type, len(vs))
		for i, vec := range vs {
			ctr.groupByTypes[i] = *vec.GetType()
		}
	}
}

func (ctr *container) createNewGroupByBatch(
	vs []*vector.Vector,
	size int,
) (*batch.Batch, error) {
	return ctr.createNewGroupByBatchWithAllocation(
		vs,
		size,
		ctr.groupByAllocation,
	)
}

func (ctr *container) createNewGroupByBatchWithAllocation(
	vs []*vector.Vector,
	size int,
	allocation *vector.AllocationAccountSelection,
) (*batch.Batch, error) {
	// initialize the groupByTypes.   this is again very bad design.
	// types should be resolved at plan time.
	if len(ctr.groupByTypes) == 0 {
		for _, vec := range vs {
			ctr.groupByTypes = append(ctr.groupByTypes, *vec.GetType())
		}
	}

	b := batch.NewOffHeapWithSize(len(ctr.groupByTypes))
	for i, typ := range ctr.groupByTypes {
		b.Vecs[i] = vector.NewOffHeapVecWithType(typ)
	}
	if err := b.SetAllocationAccount(allocation); err != nil {
		b.Clean(ctr.mp)
		return nil, err
	}
	if err := b.PreExtend(ctr.mp, size); err != nil {
		b.Clean(ctr.mp)
		return nil, err
	}
	b.SetRowCount(0)
	return b, nil
}

func (ctr *container) appendGroupByBatch(
	vs []*vector.Vector,
	offset int,
	insertList []uint8,
) (int, error) {
	return ctr.appendGroupByBatchWithStringSources(vs, offset, insertList, nil, 0)
}

func (ctr *container) appendGroupByBatchWithStringSources(
	vs []*vector.Vector,
	offset int,
	insertList []uint8,
	stringSources [][]types.StringSource,
	stringSourceOffset int,
) (int, error) {
	toIncrease, _ := countNonZeroAndFindKth(insertList, len(insertList)+1)
	if toIncrease == 0 {
		// A duplicate-only chunk must not create a fresh retained batch after
		// the hash preview has committed no new groups.
		return 0, nil
	}

	// first find the target batch.
	if len(ctr.groupByBatches) == 0 ||
		ctr.groupByBatches[len(ctr.groupByBatches)-1].RowCount() >= aggBatchSize {
		groupByBatch := ctr.groupByStandby
		ctr.groupByStandby = nil
		if groupByBatch == nil {
			var err error
			groupByBatch, err = ctr.createNewGroupByBatch(vs, aggBatchSize)
			if err != nil {
				return 0, err
			}
		}
		ctr.groupByBatches = append(ctr.groupByBatches, groupByBatch)
	}
	currBatch := ctr.groupByBatches[len(ctr.groupByBatches)-1]
	spaceLeft := aggBatchSize - currBatch.RowCount()

	thisTime := insertList
	addedRows := toIncrease
	kth := -1
	if toIncrease > spaceLeft {
		_, kth = countNonZeroAndFindKth(insertList, spaceLeft)
		thisTime = insertList[:kth+1]
		addedRows = spaceLeft
	}

	// there is enough space in the current batch to insert thisTime.
	for i, vec := range currBatch.Vecs {
		var err error
		if stringSources == nil {
			err = vec.UnionBatchPreflighted(
				vs[i], int64(offset), len(thisTime), thisTime, ctr.mp)
		} else if i >= len(stringSources) || stringSourceOffset < 0 ||
			stringSourceOffset+len(thisTime) > len(stringSources[i]) {
			return 0, mpool.ErrAllocationAccountInvariant
		} else {
			err = vec.UnionBatchPreflightedWithStringSourcesDeferredNormalization(
				vs[i], int64(offset), len(thisTime), thisTime,
				stringSources[i][stringSourceOffset:stringSourceOffset+len(thisTime)], ctr.mp)
		}
		if err != nil {
			return 0, err
		}
	}
	currBatch.AddRowCount(addedRows)

	if toIncrease > spaceLeft {
		// there is not enough space in the current batch to insert thisTime.
		// so we need to append the rest of the insertList to the next batch.
		_, err := ctr.appendGroupByBatchWithStringSources(
			vs, offset+kth+1, insertList[kth+1:], stringSources,
			stringSourceOffset+kth+1)
		if err != nil {
			return 0, err
		}
	}
	return toIncrease, nil
}

func (group *Group) outputOneBatch(proc *process.Process) (vm.CallResult, error) {
	// Build can switch directly to Eval and publish in the same Call. The
	// Call-entry check therefore does not cover cancellation that arrives while
	// the child batch is being built. Observe it at the output work-unit boundary
	// before advancing result ownership.
	if err, canceled := vm.CancelCheck(proc); canceled {
		return vm.CancelResult, err
	}

	if group.NeedEval {
		return group.ctr.outputOneBatchFinal(proc, group.OpAnalyzer, group.Aggs)
	} else {
		// The previous partial batch has returned to the caller. Only now may we
		// release its state and materialize the next exact-key leaf.
		if group.ctr.inputDone &&
			group.ctr.currBatchIdx >= len(group.ctr.groupByBatches) &&
			group.ctr.distinctSpill != nil {
			loaded, err := group.ctr.loadNextDistinctPartialLeaf(proc)
			if err != nil {
				return vm.CancelResult, err
			}
			if !loaded {
				group.ctr.state = vm.End
				return vm.CancelResult, nil
			}
		}
		// no need to eval, we are in streaming mode.  spill never happen
		// here.
		res, hasMore, err := group.getNextIntermediateResult(proc)
		if err != nil {
			return vm.CancelResult, err
		}
		if !hasMore {
			if group.ctr.inputDone {
				if group.ctr.distinctSpill != nil {
					// Keep Eval so the next Call can safely retire this returned
					// batch before loading another exact-key leaf.
					group.ctr.state = vm.Eval
				} else {
					group.ctr.state = vm.End
				}
			} else {
				// switch back to build to receive more data.
				// reset will set state to vm.Build, which will let us
				// process more by Call child.
				group.ctr.state = vm.EvalReset
			}
		}
		return res, nil
	}
}

func (group *Group) getNextIntermediateResult(proc *process.Process) (vm.CallResult, bool, error) {
	// the groupby batches are now in groupbybatches, partial agg result is in agglist.
	// now, we need to stream the partial results in the group by batch as aggs.
	if group.ctr.currBatchIdx >= len(group.ctr.groupByBatches) {
		// done.
		return vm.CancelResult, false, nil
	}
	curr := group.ctr.currBatchIdx
	group.ctr.currBatchIdx += 1
	hasMore := group.ctr.currBatchIdx < len(group.ctr.groupByBatches)

	batch := group.ctr.groupByBatches[curr]

	// XXX: Serialize chunk of aggList entries to batch.
	// This is also a pretty bad design, we would really like to
	// dump group state to a vector and put the vector into the batch.
	// But well,
	var (
		legacyBuffer bytes.Buffer
		accounted    *mpool.AccountedBuffer
		writer       io.Writer = &legacyBuffer
	)
	if group.ctr.allocationAccount != nil {
		var err error
		accounted, err = mpool.NewAccountedBuffer(
			proc.Mp(),
			group.ctr.allocationAccount,
			mpool.AllocationOwnerGroup,
			GroupAllocationSitePartialOutput,
		)
		if err != nil {
			return vm.CancelResult, false, err
		}
		defer accounted.Free()
		writer = accounted
	}
	if err := types.WriteInt32(writer, group.ctr.mtyp); err != nil {
		return vm.CancelResult, false, err
	}
	if err := writeSpillBool(writer, group.ctr.keyNullable); err != nil {
		return vm.CancelResult, false, err
	}
	nAggs := int32(len(group.ctr.aggList))
	if err := types.WriteInt32(writer, nAggs); err != nil {
		return vm.CancelResult, false, err
	}
	var prepareParamKindSources []prepareParamKindRowsSource
	if group.ctr.prepareParamKindWireV1 {
		prepareParamKindSources = make(
			[]prepareParamKindRowsSource, len(group.ctr.aggList))
	}
	for i, ag := range group.ctr.aggList {
		if vec := ag.PrepareParamKindVectorForChunk(curr); vec != nil &&
			vec.HasBinaryStringMetadata() && !binaryStringWireEnabled(proc) {
			return vm.CancelResult, false, moerr.NewInvalidStateNoCtx(
				"aggregate binary-string metadata requires MORPCVersion18")
		}
		if vec := ag.PrepareParamKindVectorForChunk(curr); vec != nil &&
			vec.HasExplicitTextStringMetadata() && !explicitTextWireEnabled(proc) {
			return vm.CancelResult, false, moerr.NewInvalidStateNoCtx(
				"aggregate explicit-text metadata requires MORPCVersion23")
		}
		if err := saveAggregateChunkForProtocol(
			ag, curr, writer, stringSourceWireEnabled(proc)); err != nil {
			return vm.CancelResult, false, err
		}
		if !group.ctr.prepareParamKindWireV1 ||
			!group.Aggs[i].PreservesFirstArgPrepareParamKind() {
			continue
		}
		var err error
		prepareParamKindSources[i], err = newPrepareParamKindRowsSource(
			ag.PrepareParamKindVectorForChunk(curr), nil)
		if err != nil {
			return vm.CancelResult, false, err
		}
	}
	if group.ctr.prepareParamKindWireV1 {
		if err := writePrepareParamKindTrailer(proc.Ctx, writer, group.Aggs,
			&group.ctr.prepareParamKind, prepareParamKindSources); err != nil {
			return vm.CancelResult, false, err
		}
	}
	if accounted != nil {
		if err := batch.SetAccountedExtraBuffer(accounted); err != nil {
			return vm.CancelResult, false, err
		}
	} else {
		batch.DropExtraBuffer()
		batch.ExtraBuf = legacyBuffer.Bytes()
	}

	res := vm.NewCallResult()
	res.Batch = batch
	return res, hasMore, nil
}
