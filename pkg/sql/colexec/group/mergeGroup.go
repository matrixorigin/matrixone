// Copyright 2025 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (mergeGroup *MergeGroup) Prepare(proc *process.Process) error {
	mergeGroup.ctr.state = vm.Build
	if mergeGroup.ctr.mp != nil {
		mergeGroup.ctr.free()
	}
	mergeGroup.ctr.prepareParamKind.Reset(mergeGroup.Aggs)
	mergeGroup.ctr.aggExprs = mergeGroup.Aggs
	mergeGroup.ctr.prepareParamKindWireV1 = prepareParamKindWireV1Enabled(proc) &&
		hasPrepareParamKindPreservingAgg(mergeGroup.Aggs)
	mergeGroup.ctr.mp = mpool.MustNewNoLock("merge_group_mpool")
	if mergeGroup.ctr.allocationAccount != nil {
		var err error
		if err = mergeGroup.ctr.mp.BindAllocationAccount(
			mergeGroup.ctr.allocationAccount,
		); err != nil {
			return err
		}
		mergeGroup.ctr.budget, err = proc.GetExecutionResourceBudget()
		if err != nil {
			return err
		}
		if err = mergeGroup.ctr.installRecoveryCapacity(); err != nil {
			return err
		}
	}
	mergeGroup.ctr.legacyTextMinMax = useLegacyTextMinMaxForRemote(proc)
	mergeGroup.ctr.legacyVarianceState = useLegacyVarianceStateForRemote(proc)
	mergeGroup.ctr.groupByTypes = nil
	mergeGroup.ctr.keyNullable = false
	mergeGroup.ctr.groupingAware = mergeGroup.GroupingAware
	mergeGroup.ctr.keyWidth = 0
	mergeGroup.ctr.mtyp = 0
	mergeGroup.ctr.setGroupByHashKey(mergeGroup.GroupByHashKey)
	if mergeGroup.EmptyGroupingSet || len(mergeGroup.EmptyGroupingSetIDs) > 0 {
		if !mergeGroup.GroupingAware || len(mergeGroup.GroupByTypes) == 0 ||
			(mergeGroup.EmptyGroupingSet && len(mergeGroup.EmptyGroupingSetIDs) > 0) {
			return moerr.NewInternalErrorNoCtx(
				"invalid empty grouping-set merge metadata")
		}
		if len(mergeGroup.EmptyGroupingSetIDs) > 0 &&
			(len(mergeGroup.GroupByTypes) < 2 ||
				mergeGroup.GroupByTypes[len(mergeGroup.GroupByTypes)-1].Oid != types.T_int64) {
			return moerr.NewInternalErrorNoCtx(
				"invalid empty grouping-set merge metadata")
		}
		previous := int64(-1)
		for _, setID := range mergeGroup.EmptyGroupingSetIDs {
			if setID <= previous {
				return moerr.NewInternalErrorNoCtx(
					"empty grouping-set ids must be strictly increasing")
			}
			previous = setID
		}
	}

	if mergeGroup.OpAnalyzer != nil {
		mergeGroup.OpAnalyzer.Reset()
	}
	mergeGroup.OpAnalyzer = process.NewAnalyzer(mergeGroup.GetIdx(), mergeGroup.IsFirst, mergeGroup.IsLast, "merge_group")

	err := mergeGroup.PrepareProjectionWithAllocation(
		proc, mergeGroup.ctr.expressionAllocation)
	if err != nil {
		return err
	}

	mergeGroup.ctr.setSpillMem(mergeGroup.SpillMem)

	return nil
}

func (mergeGroup *MergeGroup) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	switch mergeGroup.ctr.state {
	case vm.Build:
		// receive data and merge.
		for !mergeGroup.ctr.inputDone {
			r, err := vm.ChildrenCall(mergeGroup.GetChildren(0), proc, mergeGroup.OpAnalyzer)
			if err != nil {
				return vm.CancelResult, err
			}

			// all handled, going to eval mode.
			//
			// XXX: Note that this test, r.Batch == nil is treated as ExecStop.
			// if r.Status == vm.ExecStop || r.Batch == nil {
			if r.Batch == nil {
				mergeGroup.ctr.state = vm.Eval
				mergeGroup.ctr.inputDone = true
			}

			// empty batch, skip.
			if r.Batch == nil || r.Batch.IsEmpty() {
				continue
			}

			needSpill, err := mergeGroup.buildOneBatch(proc, r.Batch)
			if err != nil {
				return vm.CancelResult, err
			}

			if needSpill {
				if bytes, rows, err := mergeGroup.ctr.spillDataToDisk(proc, mergeGroup.OpAnalyzer, nil); err != nil {
					return vm.CancelResult, err
				} else {
					mergeGroup.OpAnalyzer.Spill(bytes)
					mergeGroup.OpAnalyzer.SpillRows(rows)
				}
			}
		}

		if mergeGroup.ctr.inputDone {
			// EOF and cancellation can arrive in the same child call. Observe
			// cancellation before final merge and spill materialization.
			if err, isCancel := vm.CancelCheck(proc); isCancel {
				return vm.CancelResult, err
			}
			if err := mergeGroup.ensureRuntimeEmptyGroupingSets(); err != nil {
				return vm.CancelResult, err
			}
		}

		// has partial results, merge them.
		if mergeGroup.PartialResults != nil {
			for i, ag := range mergeGroup.ctr.aggList {
				if len(mergeGroup.PartialResults) > i && mergeGroup.PartialResults[i] != nil {
					if err := ag.SetExtraInformation(mergeGroup.PartialResults[i], 0); err != nil {
						return vm.CancelResult, err
					}
				}
			}
		}

		if mergeGroup.ctr.isSpilling() {
			if mergeGroup.ctr.distinctSpill != nil {
				if _, err := mergeGroup.ctr.drainExactCountDistinct(
					proc, mergeGroup.OpAnalyzer); err != nil {
					return vm.CancelResult, err
				}
			}
			if bytes, rows, err := mergeGroup.ctr.spillDataToDisk(proc, mergeGroup.OpAnalyzer, nil); err != nil {
				return vm.CancelResult, err
			} else {
				mergeGroup.OpAnalyzer.Spill(bytes)
				mergeGroup.OpAnalyzer.SpillRows(rows)
			}
			if mergeGroup.ctr.distinctSpill != nil && mergeGroup.ctr.mtyp != H0 {
				if err := mergeGroup.ctr.prepareGroupedDistinctContributions(proc); err != nil {
					return vm.CancelResult, err
				}
			}
			if _, err := mergeGroup.ctr.loadSpilledData(proc, mergeGroup.OpAnalyzer, mergeGroup.Aggs); err != nil {
				return vm.CancelResult, err
			}
		}
		if mergeGroup.ctr.inputDone {
			if err := mergeGroup.ctr.finalizeExactCountDistinct(
				proc, mergeGroup.OpAnalyzer); err != nil {
				return vm.CancelResult, err
			}
		}

		// output the final result.
		return mergeGroup.ctr.outputOneBatchFinal(proc, mergeGroup.OpAnalyzer, mergeGroup.Aggs)

	case vm.Eval:
		return mergeGroup.ctr.outputOneBatchFinal(proc, mergeGroup.OpAnalyzer, mergeGroup.Aggs)
	case vm.End:
		return vm.CancelResult, nil
	}
	return vm.CancelResult, moerr.NewInternalError(proc.Ctx, "bug: unknown merge group state")
}

// ensureRuntimeEmptyGroupingSets makes the final merge boundary the owner of
// SQL's empty-input grouping-set semantics. Local Group or projection
// operators normally emit a key-only partial for an all-rolled grouping set,
// but a distributed scan can produce no partial pipeline at all. In that
// topology the final merge must still publish one empty aggregate state for
// every declared empty set.
func (mergeGroup *MergeGroup) ensureRuntimeEmptyGroupingSets() error {
	ctr := &mergeGroup.ctr
	if (!mergeGroup.EmptyGroupingSet && len(mergeGroup.EmptyGroupingSetIDs) == 0) ||
		ctr.mergePartialMetadataSet || len(ctr.groupByBatches) > 0 || ctr.isSpilling() {
		return nil
	}

	ctr.groupByTypes = append(ctr.groupByTypes[:0], mergeGroup.GroupByTypes...)
	setIDs := mergeGroup.EmptyGroupingSetIDs
	rows := len(setIDs)
	if mergeGroup.EmptyGroupingSet {
		setIDs = nil
		rows = 1
	}
	output, err := ctr.newRuntimeEmptyGroupingSetBatch(
		mergeGroup.GroupByTypes, setIDs)
	if err != nil {
		return err
	}

	ctr.mtyp = HStr
	ctr.groupingAware = true
	aggs, err := ctr.makeAggList(mergeGroup.Aggs)
	if err != nil {
		output.Clean(ctr.mp)
		return err
	}
	for _, agg := range aggs {
		if err = agg.GroupGrow(rows); err != nil {
			freeAggList(aggs)
			output.Clean(ctr.mp)
			return err
		}
	}
	ctr.aggList = aggs
	ctr.groupByBatches = append(ctr.groupByBatches, output)
	return nil
}

func (mergeGroup *MergeGroup) buildOneBatch(proc *process.Process, bat *batch.Batch) (bool, error) {
	if bat == nil || bat.RowCount() < 0 || bat.RowCount() > aggBatchSize {
		return false, moerr.NewInvalidInputNoCtx(
			"invalid merge-group partial row count")
	}
	for i, vec := range bat.Vecs {
		if vec == nil || vec.Length() != bat.RowCount() {
			return false, moerr.NewInvalidInputNoCtxf(
				"invalid merge-group partial column %d length", i)
		}
	}
	if err := mergeGroup.ctr.validateGroupByHashKey(len(bat.Vecs)); err != nil {
		return false, err
	}
	defer mergeGroup.ctr.freeSpillAggList()

	for {
		var err error
		if len(bat.Vecs) != 0 {
			err = mergeGroup.ctr.ensureRecoveryCapacity(
				bat.RowCount(),
				mergeGroup.OpAnalyzer,
			)
		}
		if err == nil {
			err = mergeGroup.prepareBuildBatch(proc, bat)
		}
		if err == nil {
			break
		}
		// A failed decode/preflight can leave recovery-class aggregate staging
		// behind. Return it before spilling the resident prefix so recovery uses
		// the full floor reserved for forward progress.
		mergeGroup.ctr.freeSpillAggList()
		if retried, retryErr := mergeGroup.retryBuildBatchAfterCapacity(proc, err); !retried {
			return false, retryErr
		}
	}

	// merge intermediate results with only Aggregation.
	if len(bat.Vecs) == 0 {
		groups := []uint64{1}
		for i, agg := range mergeGroup.ctr.aggList {
			if err := agg.PreflightBatchMerge(
				mergeGroup.ctr.spillAggList[i], 0, groups); err != nil {
				return false, err
			}
		}
		for i, agg := range mergeGroup.ctr.aggList {
			if err := agg.BatchMerge(
				mergeGroup.ctr.spillAggList[i], 0, groups); err != nil {
				return false, err
			}
		}
	} else {
		rowCount := bat.RowCount()
		hashKeyVecs := mergeGroup.ctr.hashKeyVectors(bat.Vecs)
		for i := 0; i < rowCount; i += hashmap.UnitLimit {
			n := min(rowCount-i, hashmap.UnitLimit)
			var preview groupInsertPreview
			for {
				var err error
				if mergeGroup.ctr.hr.IsEmpty() {
					err = mergeGroup.ctr.buildHashTable(proc.Ctx, 0)
				}
				if err == nil {
					err = mergeGroup.ctr.hr.TxnItr.PreviewInsert(
						i, n, hashKeyVecs,
						mergeGroup.ctr.hr.Hash.GroupCount(),
						&mergeGroup.ctr.hr.insertPlan)
					if err == nil {
						preview.values = mergeGroup.ctr.hr.insertPlan.Values()
						preview.inserted = mergeGroup.ctr.hr.insertPlan.Inserted()
						preview.newGroups = int(mergeGroup.ctr.hr.insertPlan.NewGroups())
					}
					if err == nil &&
						!mergeGroup.ctr.recoveryCapacityCovers(preview.newGroups) {
						err = mergeGroup.ctr.ensureRecoveryCapacity(
							preview.newGroups,
							mergeGroup.OpAnalyzer,
						)
					}
					if err == nil {
						err = mergeGroup.ctr.hr.Hash.PreAlloc(
							mergeGroup.ctr.hr.insertPlan.NewGroups())
					}
					if err == nil {
						err = mergeGroup.ctr.preflightBuildChunk(
							bat.Vecs, i, n, preview.inserted, preview.newGroups)
					}
					if err == nil {
						for j, agg := range mergeGroup.ctr.aggList {
							if err = agg.PreflightBatchMerge(
								mergeGroup.ctr.spillAggList[j], i,
								preview.values); err != nil {
								break
							}
						}
					}
				}
				if err != nil {
					mergeGroup.ctr.cancelGroupByPreflights()
				}
				if err == nil {
					mergeGroup.ctr.sanityCheck()
					vals, more, insertErr := mergeGroup.ctr.commitGroupByChunk(
						bat.Vecs, i, n, preview)
					if insertErr != nil {
						if !isGroupPrePublicationError(insertErr) {
							return false, insertErr
						}
						mergeGroup.ctr.cancelGroupByPreflights()
						err = insertErr
					} else {
						if more > 0 {
							for _, agg := range mergeGroup.ctr.aggList {
								if growErr := agg.GroupGrow(more); growErr != nil {
									return false, growErr
								}
							}
						}
						for j, agg := range mergeGroup.ctr.aggList {
							if err = agg.BatchMerge(
								mergeGroup.ctr.spillAggList[j], i, vals[:n]); err != nil {
								return false, err
							}
						}
						break
					}
				}

				// The decoded partial borrows recovery capacity. Release it before
				// spilling the resident prefix, then decode the immutable input again
				// and resume at the same not-yet-aggregated hash work unit.
				mergeGroup.ctr.freeSpillAggList()
				if retried, retryErr := mergeGroup.retryBuildBatchAfterCapacity(
					proc, err); !retried {
					return false, retryErr
				}
				if err = mergeGroup.prepareBuildBatch(proc, bat); err != nil {
					return false, err
				}
			}

			mergeGroup.ctr.sanityCheck()
		}
	}

	shouldDrain, err := mergeGroup.ctr.shouldDrainExactCountDistinct()
	if err != nil {
		return false, err
	}
	if shouldDrain {
		if _, err := mergeGroup.ctr.drainExactCountDistinct(
			proc, mergeGroup.OpAnalyzer); err != nil {
			return false, err
		}
	}
	needSpill := mergeGroup.ctr.needSpill(mergeGroup.OpAnalyzer)
	if needSpill && mergeGroup.ctr.distinctSpill == nil {
		hasDistinct, err := mergeGroup.ctr.hasExactCountDistinctArguments()
		if err != nil {
			return false, err
		}
		if hasDistinct {
			if _, err := mergeGroup.ctr.drainExactCountDistinct(
				proc, mergeGroup.OpAnalyzer); err != nil {
				return false, err
			}
			needSpill = mergeGroup.ctr.needSpill(mergeGroup.OpAnalyzer)
		}
	}
	return needSpill, nil
}

// prepareBuildBatch decodes one immutable partial. The caller subsequently
// preflights exact hash, key, and aggregate capacity before publishing groups,
// so any retryable rejection can replay the partial without duplicate state.
func (mergeGroup *MergeGroup) prepareBuildBatch(
	proc *process.Process,
	bat *batch.Batch,
) error {
	ctr := &mergeGroup.ctr
	if len(bat.ExtraBuf) == 0 {
		return moerr.NewInvalidInputNoCtx("merge-group partial metadata is missing")
	}
	ctr.freeSpillAggList()
	var err error
	ctr.spillAggList, err = ctr.makeSpillAggList(mergeGroup.Aggs)
	if err != nil {
		return err
	}

	{
		reader := bytes.NewReader(bat.ExtraBuf)
		incomingType, err := types.ReadInt32(reader)
		if err != nil {
			return err
		}
		incomingNullable, err := types.ReadBool(reader)
		if err != nil {
			return err
		}
		if incomingType < H0 || incomingType > HStr {
			return moerr.NewInvalidInputNoCtxf(
				"invalid merge-group hash type %d", incomingType)
		}
		if incomingType == H0 && len(bat.Vecs) != 0 ||
			incomingType != H0 && len(bat.Vecs) == 0 {
			return moerr.NewInvalidInputNoCtx(
				"merge-group hash type does not match group columns")
		}
		if incomingType == H0 && bat.RowCount() != 1 {
			return moerr.NewInvalidInputNoCtx(
				"merge-group H0 partial must contain exactly one row")
		}
		incomingHashVectors := ctr.hashKeyVectors(bat.Vecs)
		incomingGroupingAware := incomingType == HStr &&
			mergeGroupHashKeyHasGrouping(incomingHashVectors)
		if ctr.mergePartialMetadataSet &&
			(ctr.mtyp != incomingType || ctr.keyNullable != incomingNullable ||
				(!ctr.groupingAware && incomingGroupingAware)) {
			return moerr.NewInvalidInputNoCtx(
				"inconsistent merge-group partial metadata")
		}
		if err := validateMergeGroupHashMetadata(
			incomingType,
			incomingNullable,
			incomingHashVectors,
		); err != nil {
			return err
		}
		ctr.mtyp = incomingType
		ctr.keyNullable = incomingNullable
		// The plan-level declaration stays true even when this particular partial
		// contains only the fully active grouping set and no sentinel bits. For
		// compatibility with an undeclared single grouping partial, the first
		// partial may still promote the hash grammar before the table is built.
		ctr.groupingAware = ctr.groupingAware || incomingGroupingAware
		ctr.mergePartialMetadataSet = true

		if ctr.mtyp == H0 && len(ctr.groupByBatches) == 0 {
			gb, err := ctr.createNewGroupByBatch(bat.Vecs, 1)
			if err != nil {
				return err
			}
			gb.SetRowCount(1)
			ctr.groupByBatches = append(ctr.groupByBatches, gb)
		}

		nAggs, err := types.ReadInt32(reader)
		if err != nil {
			return err
		}
		if len(ctr.aggList) != len(mergeGroup.Aggs) {
			ctr.aggList, err = ctr.makeAggList(mergeGroup.Aggs)
			if err != nil {
				return err
			}
			ctr.configureOrderedAggSpill(proc, mergeGroup.OpAnalyzer, ctr.aggList)
		}
		if int(nAggs) != len(ctr.spillAggList) {
			return moerr.NewInternalError(
				proc.Ctx, "nAggs != len(mergeGroup.ctr.spillAggList)")
		}
		for i := int32(0); i < nAggs; i++ {
			if err := ctr.spillAggList[i].UnmarshalFromReader(reader, ctr.mp); err != nil {
				return err
			}
			if vec := ctr.spillAggList[i].PrepareParamKindVectorForChunk(0); vec != nil &&
				vec.HasBinaryStringMetadata() && !binaryStringWireEnabled(proc) {
				return moerr.NewInvalidStateNoCtx(
					"aggregate binary-string metadata requires MORPCVersion18")
			}
			if vec := ctr.spillAggList[i].PrepareParamKindVectorForChunk(0); vec != nil &&
				vec.HasExplicitTextStringMetadata() && !explicitTextWireEnabled(proc) {
				return moerr.NewInvalidStateNoCtx(
					"aggregate explicit-text metadata requires MORPCVersion23")
			}
			if vec := ctr.spillAggList[i].PrepareParamKindVectorForChunk(0); vec != nil &&
				vec.HasStringSourceMetadata() && !stringSourceWireEnabled(proc) {
				return moerr.NewInvalidStateNoCtx(
					"aggregate string source metadata requires MORPCVersion37")
			}
			if err := validateDecodedAggregateGroupCount(
				ctr.spillAggList[i], bat.RowCount()); err != nil {
				return err
			}
		}
		if !ctr.prepareParamKindWireV1 && reader.Len() > 0 {
			return moerr.NewInvalidStateNoCtx(
				"prepared parameter aggregate trailer requires MORPCVersion12")
		}
		if ctr.prepareParamKindWireV1 && reader.Len() > 0 {
			if err := mergeGroup.restorePartialPrepareParamKinds(
				proc, reader, nAggs); err != nil {
				return err
			}
		}
	}

	if ctr.mtyp == H0 || len(bat.Vecs) == 0 {
		return nil
	}
	if err := validateMergeGroupColumnTypes(ctr.groupByTypes, bat.Vecs); err != nil {
		return err
	}
	if ctr.hr.IsEmpty() {
		ctr.initGroupKeyTypesFromBatch(bat.Vecs)
		if err := ctr.buildHashTable(proc.Ctx, 0); err != nil {
			return err
		}
	}
	return nil
}

// validateMergeGroupHashMetadata keeps the serialized hash mode honest before
// an IntHashMap sees the partial. Its key encoder uses fixed eight-byte slots
// and trusts both the global NULL marker mode and the combined physical width.
func validateMergeGroupHashMetadata(
	mtyp int32,
	nullable bool,
	vectors []*vector.Vector,
) error {
	keyWidth := 0
	for i, vec := range vectors {
		if vec == nil {
			return moerr.NewInvalidInputNoCtxf(
				"nil merge-group hash key column %d", i)
		}
		if mtyp == H8 && vec.HasGrouping() {
			return moerr.NewInvalidInputNoCtxf(
				"merge-group H8 hash metadata cannot encode grouping key column %d", i)
		}
		if !nullable && mergeGroupHashKeyHasNull(vec) {
			return moerr.NewInvalidInputNoCtxf(
				"merge-group hash metadata marks keys non-nullable but hash key column %d contains null", i)
		}
		if mtyp != H8 {
			continue
		}
		typ := vec.GetType()
		width := GetKeyWidth(typ.Oid, typ.Width, nullable)
		if width < 0 || width > 8-keyWidth {
			return moerr.NewInvalidInputNoCtx(
				"merge-group H8 hash key width exceeds 8 bytes")
		}
		keyWidth += width
	}
	return nil
}

func mergeGroupHashKeyHasNull(vec *vector.Vector) bool {
	rows := uint64(vec.Length())
	if vec.IsConstNull() {
		return vec.GetGrouping().GetBitmap().CountRange(0, rows) < vec.Length()
	}
	return vec.GetNulls().GetBitmap().AnySetNotIn(
		vec.GetGrouping().GetBitmap(), 0, rows,
	)
}

func mergeGroupHashKeyHasGrouping(vectors []*vector.Vector) bool {
	for _, vec := range vectors {
		if vec != nil && vec.GetGrouping().GetBitmap().CountRange(
			0, uint64(vec.Length()),
		) > 0 {
			return true
		}
	}
	return false
}

func validateMergeGroupColumnTypes(
	expected []types.Type,
	vectors []*vector.Vector,
) error {
	for i, vec := range vectors {
		if vec == nil {
			return moerr.NewInvalidInputNoCtxf(
				"nil merge-group column %d", i)
		}
	}
	if len(expected) == 0 {
		return nil
	}
	if len(expected) != len(vectors) {
		return moerr.NewInvalidInputNoCtx(
			"inconsistent merge-group column count")
	}
	for i, vec := range vectors {
		incoming := *vec.GetType()
		want := expected[i]
		incoming.SetNotNull(false)
		want.SetNotNull(false)
		if incoming != want {
			return moerr.NewInvalidInputNoCtxf(
				"inconsistent merge-group column type at %d", i)
		}
	}
	return nil
}

func (mergeGroup *MergeGroup) restorePartialPrepareParamKinds(
	proc *process.Process,
	reader *bytes.Reader,
	nAggs int32,
) error {
	ctr := &mergeGroup.ctr
	targets := make([]prepareParamKindRowsTarget, len(ctr.spillAggList))
	for i, ag := range ctr.spillAggList {
		targets[i] = prepareParamKindChunkTarget(ag, 0)
	}
	summaries, err := readPrepareParamKindTrailer(
		proc.Ctx,
		reader,
		nAggs,
		&ctr.prepareParamKind,
		targets,
		ctr.mp,
		binaryStringWireEnabled(proc),
		explicitTextWireEnabled(proc),
	)
	if err != nil {
		return err
	}
	if reader.Len() != 0 {
		return moerr.NewInternalErrorNoCtx(
			"unexpected aggregate prepared parameter trailer bytes")
	}
	for i, agg := range ctr.spillAggList {
		if i >= len(mergeGroup.Aggs) ||
			!mergeGroup.Aggs[i].PreservesFirstArgPrepareParamKind() {
			continue
		}
		if !summaries[i].rows {
			if summaries[i].seen {
				agg.SetPrepareParamKind(summaries[i].kind)
			}
		}
	}
	return nil
}

func (mergeGroup *MergeGroup) retryBuildBatchAfterCapacity(
	proc *process.Process,
	cause error,
) (bool, error) {
	if mergeGroup == nil || mergeGroup.ctr.allocationAccount == nil ||
		!mpool.IsRetryableAllocationCapacity(cause) {
		return false, cause
	}
	if drained, err := mergeGroup.ctr.drainExactCountDistinct(
		proc, mergeGroup.OpAnalyzer); err != nil {
		return false, err
	} else if drained {
		return true, nil
	}
	if mergeGroup.ctr.mtyp == H0 || mergeGroup.ctr.hr.IsEmpty() ||
		mergeGroup.ctr.hr.Hash.GroupCount() == 0 {
		return false, cause
	}
	before := mergeGroup.ctr.hr.Hash.GroupCount()
	bytes, rows, err := mergeGroup.ctr.spillDataToDisk(
		proc,
		mergeGroup.OpAnalyzer,
		nil,
	)
	if err != nil {
		return false, err
	}
	if rows <= 0 || uint64(rows) < before {
		return false, moerr.NewInternalErrorNoCtx(
			"merge group capacity recovery made no measurable spill progress")
	}
	mergeGroup.OpAnalyzer.Spill(bytes)
	mergeGroup.OpAnalyzer.SpillRows(rows)
	return true, nil
}
