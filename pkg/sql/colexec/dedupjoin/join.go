// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package dedupjoin

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillutil"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// receiveWorkerMsg blocks until the mailbox yields a complete worker status or
// the context is canceled. Channel closure and legacy nil messages are invalid:
// this protocol requires exactly one explicit status from every non-merger.
func receiveWorkerMsg(ctx context.Context, mailbox *WorkerJoinMailbox) (*WorkerJoinMsg, error) {
	if mailbox == nil {
		return nil, moerr.NewInternalErrorNoCtx("dedup join worker mailbox is not initialized")
	}
	if err := context.Cause(ctx); err != nil {
		// Prefer an already-published terminal status. It may carry the
		// worker's original error, which is more useful than a generic parent
		// cancellation. Never wait for one after cancellation.
		select {
		case msg, ok := <-mailbox.ch:
			if ok && msg != nil {
				return msg, nil
			}
		default:
		}
		return nil, err
	}
	roundDone, stopped := mailbox.receiveState()
	if stopped {
		return nil, moerr.NewInternalErrorNoCtx(
			"dedup join worker mailbox is stopped before all workers finalized",
		)
	}
	select {
	case <-ctx.Done():
		select {
		case msg, ok := <-mailbox.ch:
			if ok && msg != nil {
				return msg, nil
			}
		default:
		}
		return nil, context.Cause(ctx)
	case <-roundDone:
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		return nil, moerr.NewInternalErrorNoCtx(
			"dedup join worker mailbox stopped before all workers finalized",
		)
	case msg, ok := <-mailbox.ch:
		if !ok {
			if err := context.Cause(ctx); err != nil {
				return nil, err
			}
			return nil, moerr.NewInternalErrorNoCtx(
				"dedup join worker channel closed before all workers finalized",
			)
		}
		if msg == nil {
			if err := context.Cause(ctx); err != nil {
				return nil, err
			}
			return nil, moerr.NewInternalErrorNoCtx(
				"dedup join worker returned an empty finalize status",
			)
		}
		return msg, nil
	}
}

// mergeCaptured folds a non-merger worker's captured state into the merger's.
// For each bucket set in msg.captured that the merger has not yet captured,
// the merger copies the per-column values from the worker's capturedVecs into
// its own and marks the bucket. First-wins semantics across workers: the
// merger retains whichever worker's values arrive first.
func (ctr *container) mergeCaptured(ap *DedupJoin, msg *WorkerJoinMsg, proc *process.Process) error {
	if ctr.capturedVecs == nil || msg.capturedVecs == nil {
		return nil
	}
	itr := msg.captured.Iterator()
	for itr.HasNext() {
		bucket := itr.Next()
		if ctr.captured.Contains(bucket) {
			continue
		}
		for cIdx := range ctr.capturedVecs {
			if err := ctr.capturedVecs[cIdx].Copy(
				msg.capturedVecs[cIdx],
				int64(bucket), int64(bucket),
				proc.Mp(),
			); err != nil {
				return err
			}
		}
		ctr.captured.Add(bucket)
	}
	return nil
}

const opName = "dedup_join"

func (dedupJoin *DedupJoin) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": dedup join ")
}
func (dedupJoin *DedupJoin) OpType() vm.OpType {
	return vm.DedupJoin
}
func (dedupJoin *DedupJoin) Prepare(proc *process.Process) (err error) {
	if dedupJoin.allocationAccount == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if dedupJoin.ctr.resultBatchByteLimit <= 0 {
		dedupJoin.ctr.resultBatchByteLimit = defaultDedupJoinResultBatchBytes
	}
	if len(dedupJoin.UpdateColIdxList) != len(dedupJoin.UpdateColExprList) {
		return moerr.NewInternalError(proc.Ctx, "dedup join update column/expression count mismatch")
	}
	for _, pos := range dedupJoin.UpdateColIdxList {
		if pos < 0 || int(pos) >= len(dedupJoin.LeftTypes) {
			return moerr.NewInternalError(proc.Ctx, "dedup join update column out of range")
		}
	}
	// The ordered assignment list may target one column repeatedly. Derive the
	// materialization set once per execution instead of de-duplicating it for
	// every logical action in a potentially large duplicate group.
	dedupJoin.ctr.stableCols = dedupJoin.ctr.stableCols[:0]
	seenUpdateCols := make([]bool, len(dedupJoin.LeftTypes))
	for _, pos := range dedupJoin.UpdateColIdxList {
		if !seenUpdateCols[pos] {
			seenUpdateCols[pos] = true
			dedupJoin.ctr.stableCols = append(dedupJoin.ctr.stableCols, pos)
		}
	}
	metadataPositions := make(map[int32]string, 2+len(dedupJoin.ForeignKeyChecks))
	validateMetadataPosition := func(pos int32, oid types.T, name string) error {
		if pos < 0 || int(pos) >= len(dedupJoin.Result) {
			return moerr.NewInternalErrorf(proc.Ctx, "dedup join %s result column out of range", name)
		}
		if prior, exists := metadataPositions[pos]; exists {
			return moerr.NewInternalErrorf(proc.Ctx,
				"dedup join metadata result column is shared by %s and %s", prior, name)
		}
		metadataPositions[pos] = name
		rp := dedupJoin.Result[pos]
		var typ types.Type
		if rp.Rel == 0 {
			if rp.Pos < 0 || int(rp.Pos) >= len(dedupJoin.LeftTypes) {
				return moerr.NewInternalErrorf(proc.Ctx, "dedup join %s source column out of range", name)
			}
			typ = dedupJoin.LeftTypes[rp.Pos]
		} else {
			if rp.Pos < 0 || int(rp.Pos) >= len(dedupJoin.RightTypes) {
				return moerr.NewInternalErrorf(proc.Ctx, "dedup join %s source column out of range", name)
			}
			typ = dedupJoin.RightTypes[rp.Pos]
		}
		if typ.Oid != oid {
			return moerr.NewInternalErrorf(proc.Ctx,
				"dedup join %s result column has type %s, expected %s", name, typ.Oid, oid)
		}
		return nil
	}
	if dedupJoin.HasODKUAffectedRows {
		if err := validateMetadataPosition(dedupJoin.AffectedRowsResultPos, types.T_uint64, "affected-rows"); err != nil {
			return err
		}
		if err := validateMetadataPosition(dedupJoin.PhysicalChangedResultPos, types.T_bool, "physical-change"); err != nil {
			return err
		}
	}
	if dedupJoin.EmitActionRows {
		if err := validateMetadataPosition(dedupJoin.ActionFinalResultPos, types.T_bool, "action-final"); err != nil {
			return err
		}
	}
	for i, check := range dedupJoin.ForeignKeyChecks {
		if err := validateMetadataPosition(
			check.EligibilityResultPos, types.T_bool, fmt.Sprintf("constraint eligibility %d", i)); err != nil {
			return err
		}
		if len(check.ColIdxList) == 0 {
			return moerr.NewInternalError(proc.Ctx, "dedup join FK check has no columns")
		}
		for _, pos := range check.ColIdxList {
			if pos < 0 || int(pos) >= len(dedupJoin.LeftTypes) {
				return moerr.NewInternalError(proc.Ctx, "dedup join FK column out of range")
			}
		}
	}
	for _, pos := range dedupJoin.UpdateCheckColIdxList {
		if pos < 0 || int(pos) >= len(dedupJoin.LeftTypes) {
			return moerr.NewInternalError(proc.Ctx, "dedup join ODKU check column out of range")
		}
	}
	if dedupJoin.OpAnalyzer == nil {
		dedupJoin.OpAnalyzer = process.NewAnalyzer(dedupJoin.GetIdx(), dedupJoin.IsFirst, dedupJoin.IsLast, "dedup join")
	} else {
		dedupJoin.OpAnalyzer.Reset()
	}
	dedupJoin.ctr.spillThreshold = colexec.ResolveSpillThreshold(dedupJoin.SpillThreshold)
	newEvalVectors := len(dedupJoin.ctr.vecs) == 0
	newUpdateExecs := len(dedupJoin.ctr.exprExecs) == 0 && len(dedupJoin.UpdateColExprList) > 0
	var evalExecs, updateExecs []colexec.ExpressionExecutor
	if newEvalVectors {
		evalExecs, err = hashbuild.NewExpressionExecutors(
			proc,
			dedupJoin.Conditions[0],
			dedupJoin.allocationAccount,
		)
		if err != nil {
			return err
		}
	}
	if newUpdateExecs {
		updateExecs, err = hashbuild.NewExpressionExecutors(
			proc,
			dedupJoin.UpdateColExprList,
			dedupJoin.allocationAccount,
		)
		if err != nil {
			for _, exec := range evalExecs {
				exec.Free()
			}
			return err
		}
	}
	if newEvalVectors {
		evecs := make([]evalVector, len(evalExecs))
		for i := range evalExecs {
			evecs[i].executor = evalExecs[i]
		}
		dedupJoin.ctr.vecs = make([]*vector.Vector, len(evalExecs))
		dedupJoin.ctr.evecs = evecs
	}
	if newUpdateExecs {
		dedupJoin.ctr.exprExecs = updateExecs
	}
	return err
}
func (dedupJoin *DedupJoin) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := dedupJoin.OpAnalyzer
	ctr := &dedupJoin.ctr
	result := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {
		case Build:
			err = dedupJoin.build(analyzer, proc)
			if err != nil {
				return result, hashbuild.TerminalBudgetError(proc.Ctx, err)
			}
			if ctr.mp == nil && !dedupJoin.IsShuffle && ctr.spillEngine == nil {
				ctr.state = End
			} else {
				ctr.state = Probe
			}
		case Probe:
			var bat *batch.Batch
			if dedupJoin.EmitActionRows && ctr.probeBat != nil {
				bat = ctr.probeBat
				// Spill-read mode: read probe batches from engine.
			} else if ctr.spillEngine != nil && ctr.spillEngine.IsProbing() {
				var readErr error
				bat, readErr = ctr.spillEngine.NextProbeBatch(proc)
				if readErr != nil {
					return result, hashbuild.TerminalBudgetError(proc.Ctx, readErr)
				}
				if bat == nil {
					ctr.spillEngine.FinishBucket()
					ctr.state = Finalize
					ctr.cleanBuf(proc)
					continue
				}
			} else if ctr.spillEngine != nil {
				ctr.state = Finalize
				ctr.cleanBuf(proc)
				continue
			} else {
				result, err = vm.ChildrenCall(dedupJoin.GetChildren(0), proc, analyzer)
				if err != nil {
					return result, hashbuild.TerminalBudgetError(proc.Ctx, err)
				}
				bat = result.Batch
				if bat == nil {
					ctr.state = Finalize
					ctr.cleanBuf(proc)
					continue
				}
				if bat.IsEmpty() {
					continue
				}
			}
			if ctr.batchRowCount == 0 {
				continue
			}
			if err := ctr.probe(bat, dedupJoin, proc, analyzer, &result); err != nil {
				return result, hashbuild.TerminalBudgetError(proc.Ctx, err)
			}
			return result, nil
		case Finalize:
			if dedupJoin.ctr.buf == nil {
				dedupJoin.ctr.lastPos = 0
				err := ctr.finalize(dedupJoin, proc)
				if err != nil {
					return result, hashbuild.TerminalBudgetError(proc.Ctx, err)
				}
				if ctr.state == End {
					continue
				}
			}
			if dedupJoin.ctr.lastPos >= len(dedupJoin.ctr.buf) {
				if dedupJoin.EmitActionRows && !ctr.finalizeDone {
					ctr.cleanBuf(proc)
					ctr.lastPos = 0
					continue
				}
				if ctr.spillEngine != nil {
					ctr.cleanBuf(proc)
					// Clear previous bucket state before advancing.
					ctr.cleanBucketState(proc)
					var allocationErr error
					ok, bktErr := ctr.spillEngine.AdvanceToNextBucket(proc, analyzer,
						func(jm *message.JoinMap, res spillutil.BucketResult) {
							if res == spillutil.BucketReady {
								ctr.mp = jm
								ctr.batches = jm.GetBatches()
								ctr.batchRowCount = jm.GetRowCount()
								rows := ctr.batchRowCount
								if dedupJoin.OnDuplicateAction == plan.Node_UPDATE {
									rows = int64(jm.GetGroupCount())
								}
								ctr.matched, allocationErr = colexec.NewAccountedBitmap(
									rows, proc.Mp(), dedupJoin.allocationAccount,
									mpool.AllocationOwnerHashBuild,
									dedupJoinAllocationSiteMatched,
								)
							}
						})
					if bktErr != nil {
						return result, hashbuild.TerminalBudgetError(proc.Ctx, bktErr)
					}
					if allocationErr != nil {
						return result, allocationErr
					}
					if ok && ctr.mp != nil {
						// BucketReady: init capture buffers for REPLACE spill path.
						if ctr.batchRowCount > 0 && len(dedupJoin.OldColCapturePlaceholderIdxList) > 0 {
							if err := ctr.initCaptureBuffers(dedupJoin, proc); err != nil {
								return result, hashbuild.TerminalBudgetError(proc.Ctx, err)
							}
						}
						ctr.state = Probe
						continue
					}
					if ok {
						ctr.state = Probe
						continue
					}
				}
				ctr.state = End
				continue
			}

			result.Batch = dedupJoin.ctr.buf[dedupJoin.ctr.lastPos]
			dedupJoin.ctr.lastPos++
			result.Status = vm.ExecHasMore
			return result, nil
		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (dedupJoin *DedupJoin) build(analyzer process.Analyzer, proc *process.Process) (err error) {
	ctr := &dedupJoin.ctr
	ctr.mp, err = process.MeasureWait(analyzer, resource.WaitOther, func() (*message.JoinMap, error) {
		return message.ReceiveJoinMap(dedupJoin.JoinMapTag, dedupJoin.IsShuffle, dedupJoin.ShuffleIdx, proc.GetMessageBoard(), proc.Ctx)
	})
	if err != nil {
		return
	}
	if ctr.mp != nil {
		ctr.maxAllocSize = max(ctr.maxAllocSize, ctr.mp.Size())
		if ctr.mp.IsSpilled() {
			payload, budget, takeErr := spillutil.TakeSpillBuildPayload(proc, ctr.mp)
			if takeErr != nil {
				return takeErr
			}
			if dedupJoin.allocationAccount == nil {
				_ = payload.Close()
				ctr.mp.Free()
				ctr.mp = nil
				return mpool.ErrAllocationAccountInvalid
			}
			engine, engineErr := spillutil.NewSpillEngine(spillutil.SpillEngineConfig{
				BuildKeyExprs:             dedupJoin.Conditions[1],
				ProbeKeyExprs:             dedupJoin.Conditions[0],
				SpillThreshold:            ctr.spillThreshold,
				NeedsBuildForEmptyProbe:   true,
				NeedAllocateSels:          dedupJoin.OnDuplicateAction == plan.Node_UPDATE,
				NeedBatches:               true,
				IsDedup:                   true,
				OnDuplicateAction:         dedupJoin.OnDuplicateAction,
				DedupBuildKeepLast:        dedupJoin.DedupBuildKeepLast,
				DedupColName:              dedupJoin.DedupColName,
				DedupColTypes:             dedupJoin.DedupColTypes,
				DelColIdx:                 dedupJoin.DelColIdx,
				DedupDeleteMarkerColIdx:   dedupJoin.DedupDeleteMarkerColIdx,
				DedupDeleteKeepColIdxList: dedupJoin.DedupDeleteKeepColIdxList,
				Budget:                    budget,
			}, dedupJoin.allocationAccount, mpool.AllocationOwnerHashBuild)
			if engineErr != nil {
				_ = payload.Close()
				ctr.mp.Free()
				ctr.mp = nil
				ctr.cleanEvalVectors()
				return engineErr
			}
			engine.InitFromSpilledFiles(payload.Files)
			ctr.spillEngine = engine
			if err := engine.ScatterProbeTable(proc,
				func() (*batch.Batch, error) {
					input, err := vm.ChildrenCall(dedupJoin.GetChildren(0), proc, analyzer)
					return input.Batch, err
				},
				analyzer,
				func(bat *batch.Batch) ([]*vector.Vector, error) {
					if err := ctr.evalJoinCondition(bat, proc); err != nil {
						return nil, err
					}
					return ctr.vecs, nil
				},
			); err != nil {
				ctr.mp.Free()
				ctr.mp = nil
				engine.Cleanup(proc)
				ctr.spillEngine = nil
				return err
			}
			ctr.mp.Free()
			ctr.mp = nil
			return
		}
	}
	if ctr.mp == nil {
		return
	}
	ctr.batches = ctr.mp.GetBatches()
	ctr.batchRowCount = ctr.mp.GetRowCount()
	if ctr.batchRowCount > 0 {
		rows := ctr.batchRowCount
		if dedupJoin.OnDuplicateAction == plan.Node_UPDATE {
			rows = int64(ctr.mp.GetGroupCount())
		}
		ctr.matched, err = colexec.NewAccountedBitmap(
			rows,
			proc.Mp(),
			dedupJoin.allocationAccount,
			mpool.AllocationOwnerHashBuild,
			dedupJoinAllocationSiteMatched,
		)
		if err != nil {
			return err
		}
	}
	if ctr.batchRowCount > 0 && len(dedupJoin.OldColCapturePlaceholderIdxList) > 0 {
		if err = ctr.initCaptureBuffers(dedupJoin, proc); err != nil {
			return err
		}
	}
	return
}

// initCaptureBuffers allocates per-capture-entry vectors pre-filled with NULL
// (one slot per build bucket) and pre-computes the Result→capture mapping.
// Only invoked when OldColCapturePlaceholderIdxList is non-empty, i.e. the
// REPLACE INTO merged main-table scan path.
func (ctr *container) initCaptureBuffers(ap *DedupJoin, proc *process.Process) error {
	if !ctr.mp.HashOnUnique() {
		// REPLACE INTO only issues capture when deduplicating on a unique key,
		// in which case every build row produces its own bucket. The non-unique
		// code path has a different bucket→row mapping and is intentionally not
		// supported here.
		return moerr.NewInternalError(proc.Ctx, "dedup join old-col capture requires hashOnUnique build")
	}
	n := len(ap.OldColCapturePlaceholderIdxList)
	ctr.capturedVecs = make([]*vector.Vector, n)
	for i, probePos := range ap.OldColCaptureProbeIdxList {
		typ := ap.LeftTypes[probePos]
		vec, err := vector.NewOffHeapVecWithTypeAndAllocation(
			typ,
			ap.stateAllocation,
		)
		if err != nil {
			return err
		}
		if err := vector.AppendMultiFixed(vec, 0, true, int(ctr.batchRowCount), proc.Mp()); err != nil {
			vec.Free(proc.Mp())
			ctr.capturedVecs[i] = nil
			return err
		}
		ctr.capturedVecs[i] = vec
	}
	var err error
	ctr.captured, err = colexec.NewAccountedBitmap(
		ctr.batchRowCount,
		proc.Mp(),
		ap.allocationAccount,
		mpool.AllocationOwnerHashBuild,
		dedupJoinAllocationSiteCaptured,
	)
	if err != nil {
		ctr.cleanCaptured(proc)
		return err
	}
	ctr.captureResultIdx = make([]int32, len(ap.Result))
	for j := range ctr.captureResultIdx {
		ctr.captureResultIdx[j] = -1
	}
	for j, rp := range ap.Result {
		if rp.Rel != 1 {
			continue
		}
		for k, placeholderPos := range ap.OldColCapturePlaceholderIdxList {
			if rp.Pos == placeholderPos {
				ctr.captureResultIdx[j] = int32(k)
				break
			}
		}
	}
	return nil
}

func (ctr *container) appendBuildSelectionRow(
	ap *DedupJoin,
	dst *batch.Batch,
	sel int32,
	proc *process.Process,
) error {
	idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
	for j, rp := range ap.Result {
		if rp.Rel == 1 {
			if err := dst.Vecs[j].UnionOne(
				ctr.batches[idx1].Vecs[rp.Pos], int64(idx2), proc.Mp()); err != nil {
				return err
			}
		} else if err := dst.Vecs[j].UnionNull(proc.Mp()); err != nil {
			return err
		}
	}
	dst.AddRowCount(1)
	return nil
}

// finalizeODKUActionRows emits at most one bounded batch. finalizeGroup and
// finalizeActionIdx are the durable cursor; result rows themselves are released
// on the next Call and are never retained for the whole build bucket.
func (ctr *container) finalizeODKUActionRows(
	ap *DedupJoin,
	proc *process.Process,
) error {
	if ctr.mp == nil || ctr.matched == nil {
		ctr.finalizeDone = true
		return nil
	}
	if err := ap.resetRBat(); err != nil {
		return err
	}

	zeroSels := ctr.mp.GetSels(0)
	for ctr.finalizeZeroIdx < len(zeroSels) {
		if err := ctr.appendBuildSelectionRow(
			ap, ctr.rbat, zeroSels[ctr.finalizeZeroIdx], proc); err != nil {
			return err
		}
		ctr.finalizeZeroIdx++
		if ctr.actionResultBatchFull() {
			ctr.buf = []*batch.Batch{ctr.rbat}
			ctr.lastPos = 0
			return nil
		}
	}

	groupCount := uint64(ctr.matched.Len())
	for ctr.finalizeGroup < groupCount {
		if ctr.matched.Contains(ctr.finalizeGroup) {
			ctr.finalizeGroup++
			continue
		}
		if ctr.mp.HashOnUnique() {
			if err := ctr.appendBuildSelectionRow(
				ap, ctr.rbat, int32(ctr.finalizeGroup), proc); err != nil {
				return err
			}
			ctr.finalizeGroup++
			if ctr.actionResultBatchFull() {
				ctr.buf = []*batch.Batch{ctr.rbat}
				ctr.lastPos = 0
				return nil
			}
			continue
		}
		sels := ctr.mp.GetSels(ctr.finalizeGroup + 1)
		if len(sels) == 0 {
			return moerr.NewInternalError(proc.Ctx, "ODKU finalize group has no actions")
		}
		if len(sels) == 1 {
			if err := ctr.appendBuildSelectionRow(ap, ctr.rbat, sels[0], proc); err != nil {
				return err
			}
			ctr.finalizeGroup++
			if ctr.actionResultBatchFull() {
				ctr.buf = []*batch.Batch{ctr.rbat}
				ctr.lastPos = 0
				return nil
			}
			continue
		}

		if !ctr.finalizeActionActive {
			idx1, idx2 := sels[0]/colexec.DefaultBatchSize, sels[0]%colexec.DefaultBatchSize
			if ctr.joinBat1 == nil {
				ctr.joinBat1, ctr.cfs1 = colexec.NewJoinBatch(ctr.batches[0], proc.Mp())
			}
			if ctr.joinBat2 == nil {
				ctr.joinBat2, ctr.cfs2 = colexec.NewJoinBatch(ctr.batches[0], proc.Mp())
			}
			if err := colexec.SetJoinBatchValues(
				ctr.joinBat1, ctr.batches[idx1], int64(idx2), 1, ctr.cfs1); err != nil {
				return err
			}
			ctr.finalizeCurrentVecs = snapshotVectors(
				ctr.finalizeCurrentVecs, ctr.joinBat1, ap.UpdateColIdxList)
			ctr.finalizeActionIdx = 1
			ctr.finalizeLogicalAffect = 1
			ctr.finalizeActionActive = true
			if err := ctr.appendFinalizeActionRow(
				ap, ctr.rbat, 0, false, false,
				ctr.allForeignKeysEligible(ap.ForeignKeyChecks), proc); err != nil {
				return err
			}
			if ctr.actionResultBatchFull() {
				ctr.buf = []*batch.Batch{ctr.rbat}
				ctr.lastPos = 0
				return nil
			}
		}

		yield := false
		err := ctr.withRestoredJoinBat1Vectors(ap.UpdateColIdxList, func() error {
			restoreUpdateVectors(ctr.joinBat1, ap.UpdateColIdxList, ctr.finalizeCurrentVecs)
			for ctr.finalizeActionIdx < len(sels) {
				sel := sels[ctr.finalizeActionIdx]
				idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
				if err := colexec.SetJoinBatchValues(
					ctr.joinBat2, ctr.batches[idx1], int64(idx2), 1, ctr.cfs2); err != nil {
					return err
				}
				ctr.snapshotForeignKeys(ap.ForeignKeyChecks)
				changed, err := ctr.applyUpdateExpressions(
					proc, ap.UpdateColIdxList, ap.UpdateCheckColIdxList)
				if err != nil {
					return err
				}
				ctr.finalizeCurrentVecs = snapshotVectors(
					ctr.finalizeCurrentVecs, ctr.joinBat1, ap.UpdateColIdxList)
				ctr.finalizeLogicalAffect += odkuAffectedRows(changed, ap.CountFoundRows)
				isFinal := ctr.finalizeActionIdx == len(sels)-1
				affectedRows := uint64(0)
				if isFinal {
					affectedRows = ctr.finalizeLogicalAffect
				}
				if err := ctr.appendFinalizeActionRow(
					ap, ctr.rbat, affectedRows, isFinal, isFinal,
					ctr.finalizeInsertConstraintEligibility(ap.ForeignKeyChecks, isFinal), proc); err != nil {
					return err
				}
				ctr.finalizeActionIdx++
				if ctr.actionResultBatchFull() {
					yield = true
					break
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		if ctr.finalizeActionIdx == len(sels) {
			ctr.finalizeActionActive = false
			ctr.finalizeCurrentVecs = nil
			ctr.finalizeGroup++
		}
		if yield {
			ctr.buf = []*batch.Batch{ctr.rbat}
			ctr.lastPos = 0
			return nil
		}
	}

	ctr.finalizeDone = true
	if ctr.rbat.RowCount() > 0 {
		ctr.buf = []*batch.Batch{ctr.rbat}
		ctr.lastPos = 0
	}
	return nil
}

func (ctr *container) finalize(ap *DedupJoin, proc *process.Process) error {
	var err error
	if ap.EmitActionRows && ctr.finalizePrepared {
		return ctr.finalizeODKUActionRows(ap, proc)
	}
	if ap.needsFinalizeMerge() {
		if !ap.IsMerger {
			if ap.Mailbox == nil {
				return moerr.NewInternalErrorNoCtx("dedup join worker mailbox is not initialized")
			}
			msg := &WorkerJoinMsg{matched: ctr.matched}
			if len(ap.OldColCapturePlaceholderIdxList) > 0 {
				msg.captured = ctr.captured
				msg.capturedVecs = ctr.capturedVecs
			}
			if err := context.Cause(proc.Ctx); err != nil {
				return err
			}
			sent, stopped, roundDone := ap.Mailbox.trySend(msg)
			if stopped {
				// The merger already terminated this generation. Ownership
				// remains local and Free will release the capture vectors.
				ctr.state = End
				return nil
			}
			if !sent {
				return moerr.NewInternalErrorNoCtx(
					"dedup join worker mailbox is unexpectedly full",
				)
			}
			// Ownership transfers only after trySend succeeds. Before that
			// point Reset/Free still owns and releases these vectors.
			ctr.captured = nil
			ctr.capturedVecs = nil
			ctr.matched = nil
			// Publication, not acknowledgement, is the worker's single status
			// for this round. Mark it before waiting so concurrent cancellation
			// cannot make Reset enqueue a duplicate abort status.
			ctr.roundStatusPublished = true
			select {
			case <-roundDone:
				// completeRound closes this acknowledgement and installs the
				// next round under the same mailbox lock, before any later
				// trySend can enter. From this point Reset must publish an
				// abort for that next round: the merger may advance before this
				// worker resumes execution.
				ctr.roundStatusPublished = false
			case <-proc.Ctx.Done():
				return context.Cause(proc.Ctx)
			}
			ctr.finalizeDone = true
			return nil
		}

		for cnt := 1; cnt < int(ap.NumCPU); cnt++ {
			msg, err := receiveWorkerMsg(proc.Ctx, ap.Mailbox)
			if err != nil {
				freeWorkerJoinMsg(msg, proc)
				ap.Mailbox.stopAndDrain(proc)
				return err
			}

			if msg.aborted {
				freeWorkerJoinMsg(msg, proc)
				ap.Mailbox.stopAndDrain(proc)
				if msg.err != nil {
					return msg.err
				}
				if err := context.Cause(proc.Ctx); err != nil {
					return err
				}
				// A normal upper-operator early stop is not a query error, but
				// no partial unmatched-build output may escape.
				ctr.state = End
				return nil
			}
			if ctr.matched != nil && msg.matched != nil {
				ctr.matched.Or(msg.matched)
			}
			var mergeErr error
			if len(ap.OldColCapturePlaceholderIdxList) > 0 && msg.captured != nil {
				mergeErr = ctr.mergeCaptured(ap, msg, proc)
			}
			freeWorkerJoinMsg(msg, proc)
			if mergeErr != nil {
				ap.Mailbox.stopAndDrain(proc)
				return mergeErr
			}
		}
		if err := context.Cause(proc.Ctx); err != nil {
			ap.Mailbox.stopAndDrain(proc)
			return err
		}
		// Do not release a fast worker into the next spill bucket until every
		// worker's status for this bucket has been collected.
		ap.Mailbox.completeRound()
	}
	if ap.EmitActionRows {
		// Probe and build batches can have different schemas. The probe replay
		// cache is no longer needed once finalize begins; rebuild it lazily from
		// the build schema instead of reusing incompatible setters.
		if ctr.joinBat1 != nil {
			ctr.joinBat1.Clean(proc.Mp())
			ctr.joinBat1 = nil
			ctr.cfs1 = nil
		}
		if ctr.joinBat2 != nil {
			ctr.joinBat2.Clean(proc.Mp())
			ctr.joinBat2 = nil
			ctr.cfs2 = nil
		}
		ctr.finalizePrepared = true
		return ctr.finalizeODKUActionRows(ap, proc)
	}

	if ctr.matched == nil {
		return nil
	}
	if ap.OnDuplicateAction != plan.Node_UPDATE || ctr.mp.HashOnUnique() {
		if ctr.matched.Count() == 0 {
			// constructDedupJoin copies node.ProjectList into ap.Result without
			// dedup, and the REPLACE planner can alias multiple projections onto
			// the same build column, so a non-capture build position may be
			// referenced more than once. Ownership transfer (steal the build
			// vector and nil it out) is only safe when a position is referenced
			// exactly once; duplicates must copy, otherwise the second reference
			// reads a nil vector. Count references first.
			buildPosRefCount := make(map[int32]int, len(ap.Result))
			for j, rp := range ap.Result {
				if rp.Rel != 1 {
					continue
				}
				if len(ctr.captureResultIdx) > 0 && ctr.captureResultIdx[j] >= 0 {
					continue
				}
				buildPosRefCount[rp.Pos]++
			}
			ap.ctr.buf = make([]*batch.Batch, len(ctr.batches))
			for i := range ap.ctr.buf {
				ap.ctr.buf[i] = batch.NewOffHeapWithSize(len(ap.Result))
				bat := ctr.batches[i]
				ap.ctr.buf[i].Attrs = bat.Attrs
				batSize := bat.RowCount()
				// Flat-index offset of this build batch in capturedVecs space.
				// hashOnUnique guarantees a 1:1 bucket↔flat-row mapping.
				capOffset := int64(i) * int64(colexec.DefaultBatchSize)
				for j, rp := range ap.Result {
					if rp.Rel == 1 {
						if len(ctr.captureResultIdx) > 0 && ctr.captureResultIdx[j] >= 0 {
							// Capture column: when matched==0, no probe hit any
							// bucket, so capturedVecs are still all-NULL. Emit a
							// pre-filled NULL vector directly instead of copying.
							cIdx := ctr.captureResultIdx[j]
							if ctr.captured != nil && ctr.captured.Count() > 0 {
								typ := ap.RightTypes[rp.Pos]
								ap.ctr.buf[i].Vecs[j], err = ap.newResultVector(typ)
								if err != nil {
									return err
								}
								if err := ap.ctr.buf[i].Vecs[j].UnionBatch(ctr.capturedVecs[cIdx], capOffset, batSize, nil, proc.Mp()); err != nil {
									return err
								}
							} else {
								ap.ctr.buf[i].Vecs[j], err = ap.newResultVector(ap.RightTypes[rp.Pos])
								if err != nil {
									return err
								}
								if err := vector.AppendMultiFixed(ap.ctr.buf[i].Vecs[j], 0, true, batSize, proc.Mp()); err != nil {
									return err
								}
							}
						} else if buildPosRefCount[rp.Pos] == 1 {
							// Non-capture build column referenced exactly once:
							// transfer ownership from the build batch to avoid a
							// full copy.
							ap.ctr.buf[i].Vecs[j] = bat.Vecs[rp.Pos]
							bat.Vecs[rp.Pos] = nil
						} else {
							// Non-capture build column referenced more than once
							// (aliased projections). Copy so every reference gets
							// its own valid vector; ownership transfer here would
							// leave later references reading a nil vector.
							typ := ap.RightTypes[rp.Pos]
							ap.ctr.buf[i].Vecs[j], err = ap.newResultVector(typ)
							if err != nil {
								return err
							}
							if err := vector.GetUnionAllFunction(typ, proc.Mp())(ap.ctr.buf[i].Vecs[j], bat.Vecs[rp.Pos]); err != nil {
								return err
							}
						}
					} else {
						ap.ctr.buf[i].Vecs[j], err = ap.newResultVector(ap.LeftTypes[rp.Pos])
						if err != nil {
							return err
						}
						if err := vector.AppendMultiFixed(ap.ctr.buf[i].Vecs[j], 0, true, batSize, proc.Mp()); err != nil {
							return err
						}
					}
				}
				ap.ctr.buf[i].SetRowCount(batSize)
			}
			return nil
		}
		count := int(ctr.batchRowCount) - ctr.matched.Count()
		if count == 0 {
			return nil
		}
		ctr.matched.Negate()
		sels, err := mpool.MakeSliceAccounted[int32](
			count,
			proc.Mp(),
			ap.allocationAccount,
			mpool.AllocationOwnerHashBuild,
			dedupJoinAllocationSiteFinalizeSelections,
		)
		if err != nil {
			return err
		}
		sels = sels[:0]
		defer mpool.FreeSlice(proc.Mp(), sels)
		itr := ctr.matched.Iterator()
		for itr.HasNext() {
			r := itr.Next()
			sels = append(sels, int32(r))
		}
		batCnt := (count-1)/colexec.DefaultBatchSize + 1
		ap.ctr.buf = make([]*batch.Batch, batCnt)
		for i := range ap.ctr.buf {
			var newSels []int32
			if i+1 < batCnt {
				newSels = sels[i*colexec.DefaultBatchSize : (i+1)*colexec.DefaultBatchSize]
			} else {
				newSels = sels[i*colexec.DefaultBatchSize:]
			}
			ap.ctr.buf[i] = batch.NewOffHeapWithSize(len(ap.Result))
			for j, rp := range ap.Result {
				if rp.Rel == 1 {
					ap.ctr.buf[i].Vecs[j], err = ap.newResultVector(ap.RightTypes[rp.Pos])
					if err != nil {
						return err
					}
					if err := unionSelsByBatch(ap.ctr.buf[i].Vecs[j], ctr.batches, rp.Pos, newSels, proc); err != nil {
						return err
					}
				} else {
					ap.ctr.buf[i].Vecs[j], err = ap.newResultVector(ap.LeftTypes[rp.Pos])
					if err != nil {
						return err
					}
					if err := vector.AppendMultiFixed(ap.ctr.buf[i].Vecs[j], 0, true, len(newSels), proc.Mp()); err != nil {
						return err
					}
				}
			}
			ap.ctr.buf[i].SetRowCount(len(newSels))
		}
	} else {
		sels := ctr.mp.GetSels(0)
		count := int(ctr.mp.GetGroupCount()) - ctr.matched.Count() + len(sels)
		if ap.EmitActionRows {
			count = len(sels)
			for group := uint64(0); group < uint64(ctr.matched.Len()); group++ {
				if !ctr.matched.Contains(group) {
					count += len(ctr.mp.GetSels(group + 1))
				}
			}
		}
		if count == 0 {
			return nil
		}
		batCnt := (count-1)/colexec.DefaultBatchSize + 1
		ap.ctr.buf = make([]*batch.Batch, batCnt)
		fillCnt := 0
		batIdx, rowIdx := 0, 0
		for fillCnt < len(sels) {
			batSize := colexec.DefaultBatchSize
			if fillCnt+batSize > len(sels) {
				batSize = len(sels) - fillCnt
			}
			ap.ctr.buf[batIdx] = batch.NewOffHeapWithSize(len(ap.Result))
			for i, rp := range ap.Result {
				if rp.Rel == 1 {
					ap.ctr.buf[batIdx].Vecs[i], err = ap.newResultVector(ap.RightTypes[rp.Pos])
					if err != nil {
						return err
					}
					for _, sel := range sels[fillCnt : fillCnt+batSize] {
						idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
						if err := ap.ctr.buf[batIdx].Vecs[i].UnionOne(ctr.batches[idx1].Vecs[rp.Pos], int64(idx2), proc.Mp()); err != nil {
							return err
						}
					}
				} else {
					ap.ctr.buf[batIdx].Vecs[i], err = ap.newResultVector(ap.LeftTypes[rp.Pos])
					if err != nil {
						return err
					}
					if err := vector.AppendMultiFixed(ap.ctr.buf[batIdx].Vecs[i], 0, true, batSize, proc.Mp()); err != nil {
						return err
					}
				}
			}
			ap.ctr.buf[batIdx].SetRowCount(batSize)
			fillCnt += batSize
			batIdx++
			rowIdx = batSize % colexec.DefaultBatchSize
		}
		if ctr.joinBat1 != nil {
			ctr.joinBat1.Clean(proc.GetMPool())
		}
		ctr.joinBat1, ctr.cfs1 = colexec.NewJoinBatch(ctr.batches[0], proc.Mp())
		bitmapLen := uint64(ctr.matched.Len())
		for i := uint64(0); i < bitmapLen; i++ {
			if ctr.matched.Contains(i) {
				continue
			}
			if rowIdx == 0 {
				ap.ctr.buf[batIdx] = batch.NewOffHeapWithSize(len(ap.Result))
				for i, rp := range ap.Result {
					if rp.Rel == 1 {
						ap.ctr.buf[batIdx].Vecs[i], err = ap.newResultVector(ap.RightTypes[rp.Pos])
						if err != nil {
							return err
						}
					} else {
						ap.ctr.buf[batIdx].Vecs[i], err = ap.newResultVector(ap.LeftTypes[rp.Pos])
						if err != nil {
							return err
						}
					}
				}
			}
			sels = ctr.mp.GetSels(i + 1)
			idx1, idx2 := sels[0]/colexec.DefaultBatchSize, sels[0]%colexec.DefaultBatchSize
			if ap.EmitActionRows && len(sels) > 1 {
				if err := colexec.SetJoinBatchValues(ctr.joinBat1, ctr.batches[idx1], int64(idx2), 1, ctr.cfs1); err != nil {
					return err
				}
				if ctr.joinBat2 == nil {
					ctr.joinBat2, ctr.cfs2 = colexec.NewJoinBatch(ctr.batches[0], proc.Mp())
				}
				err = ctr.withRestoredJoinBat1Vectors(ap.UpdateColIdxList, func() error {
					if err := ctr.appendFinalizeActionRow(
						ap, ap.ctr.buf[batIdx], 0, false, false,
						ctr.allForeignKeysEligible(ap.ForeignKeyChecks), proc); err != nil {
						return err
					}
					rowIdx++
					if rowIdx == colexec.DefaultBatchSize {
						batIdx++
						rowIdx = 0
					}
					logicalAffectedRows := uint64(1)
					for actionIdx, sel := range sels[1:] {
						if rowIdx == 0 {
							ap.ctr.buf[batIdx] = batch.NewOffHeapWithSize(len(ap.Result))
							for j, rp := range ap.Result {
								if rp.Rel == 1 {
									ap.ctr.buf[batIdx].Vecs[j], err = ap.newResultVector(ap.RightTypes[rp.Pos])
								} else {
									ap.ctr.buf[batIdx].Vecs[j], err = ap.newResultVector(ap.LeftTypes[rp.Pos])
								}
								if err != nil {
									return err
								}
							}
						}
						idx1, idx2 = sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
						if err := colexec.SetJoinBatchValues(ctr.joinBat2, ctr.batches[idx1], int64(idx2), 1, ctr.cfs2); err != nil {
							return err
						}
						ctr.snapshotForeignKeys(ap.ForeignKeyChecks)
						changed, err := ctr.applyUpdateExpressions(
							proc, ap.UpdateColIdxList, ap.UpdateCheckColIdxList)
						if err != nil {
							return err
						}
						logicalAffectedRows += odkuAffectedRows(changed, ap.CountFoundRows)
						isFinal := actionIdx == len(sels[1:])-1
						affectedRows := uint64(0)
						if isFinal {
							affectedRows = logicalAffectedRows
						}
						if err := ctr.appendFinalizeActionRow(
							ap, ap.ctr.buf[batIdx], affectedRows, isFinal, isFinal,
							ctr.finalizeInsertConstraintEligibility(ap.ForeignKeyChecks, isFinal), proc); err != nil {
							return err
						}
						rowIdx++
						if rowIdx == colexec.DefaultBatchSize {
							batIdx++
							rowIdx = 0
						}
					}
					return nil
				})
				if err != nil {
					return err
				}
				continue
			}
			if len(sels) == 1 {
				for j, rp := range ap.Result {
					if rp.Rel == 1 {
						if err := ap.ctr.buf[batIdx].Vecs[j].UnionOne(ctr.batches[idx1].Vecs[rp.Pos], int64(idx2), proc.Mp()); err != nil {
							return err
						}
					} else {
						if err := ap.ctr.buf[batIdx].Vecs[j].UnionNull(proc.Mp()); err != nil {
							return err
						}
					}
				}
			} else {
				var logicalAffectedRows uint64 = 1 // the first row is an INSERT
				err := colexec.SetJoinBatchValues(ctr.joinBat1, ctr.batches[idx1], int64(idx2), 1, ctr.cfs1)
				if err != nil {
					return err
				}
				if ctr.joinBat2 == nil {
					ctr.joinBat2, ctr.cfs2 = colexec.NewJoinBatch(ctr.batches[0], proc.Mp())
				}
				err = ctr.withRestoredJoinBat1Vectors(ap.UpdateColIdxList, func() error {
					for _, sel := range sels[1:] {
						idx1, idx2 = sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
						if err := colexec.SetJoinBatchValues(ctr.joinBat2, ctr.batches[idx1], int64(idx2), 1, ctr.cfs2); err != nil {
							return err
						}
						changed, err := ctr.applyUpdateExpressions(
							proc, ap.UpdateColIdxList, ap.UpdateCheckColIdxList)
						if err != nil {
							return err
						}
						logicalAffectedRows += odkuAffectedRows(changed, ap.CountFoundRows)
					}
					for j, rp := range ap.Result {
						if handled, err := appendODKUMetadata(
							ap.ctr.buf[batIdx].Vecs[j], ap.HasODKUAffectedRows, int32(j),
							ap.AffectedRowsResultPos, ap.PhysicalChangedResultPos,
							logicalAffectedRows, true, proc.Mp()); handled || err != nil {
							if err != nil {
								return err
							}
							continue
						}
						if rp.Rel == 1 {
							if err := ap.ctr.buf[batIdx].Vecs[j].UnionOne(ctr.joinBat1.Vecs[rp.Pos], 0, proc.Mp()); err != nil {
								return err
							}
						} else {
							if err := ap.ctr.buf[batIdx].Vecs[j].UnionNull(proc.Mp()); err != nil {
								return err
							}
						}
					}
					return nil
				})
				if err != nil {
					return err
				}
			}
			ap.ctr.buf[batIdx].AddRowCount(1)
			rowIdx++
			if rowIdx == colexec.DefaultBatchSize {
				batIdx++
				rowIdx = 0
			}
		}
	}
	return nil
}

// withRestoredJoinBat1Vectors temporarily permits UPDATE expressions to
// replace joinBat1 vector pointers. Expression executors retain ownership of
// their result vectors, so every return path must restore the join batch's
// original vectors before Reset or Free cleans either owner.
func (ctr *container) withRestoredJoinBat1Vectors(updateCols []int32, fn func() error) (err error) {
	if len(updateCols) == 0 {
		return fn()
	}
	if len(ctr.savedVecs) != len(updateCols) {
		ctr.savedVecs = make([]*vector.Vector, len(updateCols))
	}
	for i, pos := range updateCols {
		ctr.savedVecs[i] = ctr.joinBat1.Vecs[pos]
	}
	defer func() {
		for i, pos := range updateCols {
			ctr.joinBat1.Vecs[pos] = ctr.savedVecs[i]
		}
	}()
	return fn()
}

// applyUpdateExpressions implements SQL's left-to-right assignment semantics.
// Installing each result immediately lets the next expression observe the
// current row image. Expression executors retain ownership of their vectors;
// withRestoredJoinBat1Vectors restores the join batch on every exit path.
func (ctr *container) applyUpdateExpressions(
	proc *process.Process,
	updateCols, checkCols []int32,
) (bool, error) {
	// Callers seed joinBat1 from SetJoinBatchValues; after the first action this
	// method leaves it materialized in stableUpdateVecs. Thus the current image
	// is stable here and needs no pre-Eval copy.
	ctr.actionBeforeVecs = snapshotVectors(ctr.actionBeforeVecs, ctr.joinBat1, checkCols)
	actionImageBefore := ctr.actionBeforeVecs
	if !slices.Equal(updateCols, checkCols) {
		ctr.actionImageBeforeVecs = snapshotVectors(ctr.actionImageBeforeVecs, ctr.joinBat1, updateCols)
		actionImageBefore = ctr.actionImageBeforeVecs
	}
	for i, exprExec := range ctr.exprExecs {
		vec, err := exprExec.Eval(proc, []*batch.Batch{ctr.joinBat1, ctr.joinBat2}, nil)
		if err != nil {
			return false, err
		}
		ctr.joinBat1.Vecs[updateCols[i]] = vec
	}
	// A column expression may return a vector owned by joinBat2 (VALUES(...)),
	// and a function executor may reuse its result on the next Eval. Materialize
	// the completed row image before the caller advances joinBat2 to the next
	// logical input row; otherwise that advance can silently rewrite both the
	// current value and the before/after decision.
	changed := snapshotChanged(ctr.actionBeforeVecs, ctr.joinBat1, checkCols)
	if !changed {
		for i, pos := range updateCols {
			ctr.joinBat1.Vecs[pos] = actionImageBefore[i]
		}
		return false, nil
	}
	if err := ctr.stabilizeUpdateVectors(proc); err != nil {
		return false, err
	}
	return changed, nil
}

func (ctr *container) stabilizeUpdateVectors(proc *process.Process) error {
	// The probe and finalize phases can expose different join-batch widths. Keep
	// the existing per-column pools when the batch grows (and harmlessly retain
	// any trailing pools when it shrinks); replacing the outer slice would lose
	// ownership of already allocated vectors and leak them until process exit.
	if missing := len(ctr.joinBat1.Vecs) - len(ctr.stableUpdateVecs); missing > 0 {
		ctr.stableUpdateVecs = append(ctr.stableUpdateVecs, make([][]*vector.Vector, missing)...)
	}
	ctr.stableSources = ctr.stableSources[:0]
	for _, pos := range ctr.stableCols {
		ctr.stableSources = append(ctr.stableSources, ctr.joinBat1.Vecs[pos])
	}
	ctr.stableDests = ctr.stableDests[:0]
	for i, pos := range ctr.stableCols {
		var dst *vector.Vector
		for _, candidate := range ctr.stableUpdateVecs[pos] {
			inUse := false
			for _, source := range ctr.stableSources {
				if candidate == source {
					inUse = true
					break
				}
			}
			if !inUse {
				dst = candidate
				break
			}
		}
		if dst == nil {
			dst = vector.NewVec(*ctr.joinBat1.Vecs[pos].GetType())
			ctr.stableUpdateVecs[pos] = append(ctr.stableUpdateVecs[pos], dst)
		}
		dst.CleanOnlyData()
		if err := dst.UnionOne(ctr.stableSources[i], 0, proc.Mp()); err != nil {
			return err
		}
		ctr.stableDests = append(ctr.stableDests, dst)
	}
	for i, pos := range ctr.stableCols {
		ctr.joinBat1.Vecs[pos] = ctr.stableDests[i]
	}
	return nil
}

func snapshotVectors(dst []*vector.Vector, bat *batch.Batch, cols []int32) []*vector.Vector {
	if len(dst) != len(cols) {
		dst = make([]*vector.Vector, len(cols))
	}
	for i, pos := range cols {
		dst[i] = bat.Vecs[pos]
	}
	return dst
}

func snapshotChanged(before []*vector.Vector, after *batch.Batch, cols []int32) bool {
	for i, pos := range cols {
		left, right := before[i], after.Vecs[pos]
		leftNull, rightNull := left.IsNull(0), right.IsNull(0)
		if leftNull != rightNull || (!leftNull && !odkuValuesEqual(left, right)) {
			return true
		}
	}
	return false
}

func odkuPhysicalChanged(
	anyActionChanged bool,
	before []*vector.Vector,
	after *batch.Batch,
	cols []int32,
) bool {
	return anyActionChanged && snapshotChanged(before, after, cols)
}

func odkuValuesEqual(left, right *vector.Vector) bool {
	if left == nil || right == nil {
		return left == right
	}
	if left.GetType().Oid != right.GetType().Oid {
		return false
	}

	switch left.GetType().Oid {
	case types.T_char:
		// CHAR uses PAD SPACE comparison semantics. Keep this aligned with the
		// SQL equality operators: trailing spaces do not turn an assignment into
		// a logical change, fire implicit ON UPDATE columns, or cause a write.
		return bytes.Equal(
			bytes.TrimRight(left.GetBytesAt(0), " "),
			bytes.TrimRight(right.GetBytesAt(0), " "),
		)
	case types.T_float32:
		leftNormalizer := types.NewFloat32ScaleNormalizer(left.GetType().Scale)
		rightNormalizer := types.NewFloat32ScaleNormalizer(right.GetType().Scale)
		return odkuFloat32Equal(
			leftNormalizer.Normalize(vector.GetFixedAtNoTypeCheck[float32](left, 0)),
			rightNormalizer.Normalize(vector.GetFixedAtNoTypeCheck[float32](right, 0)),
		)
	case types.T_float64:
		return odkuFloat64Equal(
			vector.GetFixedAtNoTypeCheck[float64](left, 0),
			vector.GetFixedAtNoTypeCheck[float64](right, 0),
		)
	case types.T_bool:
		return odkuFixedValuesEqual[bool](left, right)
	case types.T_bit, types.T_uint64:
		return odkuFixedValuesEqual[uint64](left, right)
	case types.T_int8:
		return odkuFixedValuesEqual[int8](left, right)
	case types.T_int16:
		return odkuFixedValuesEqual[int16](left, right)
	case types.T_int32:
		return odkuFixedValuesEqual[int32](left, right)
	case types.T_int64, types.T_interval:
		return odkuFixedValuesEqual[int64](left, right)
	case types.T_uint8:
		return odkuFixedValuesEqual[uint8](left, right)
	case types.T_uint16:
		return odkuFixedValuesEqual[uint16](left, right)
	case types.T_uint32:
		return odkuFixedValuesEqual[uint32](left, right)
	case types.T_decimal64:
		return vector.GetFixedAtNoTypeCheck[types.Decimal64](left, 0).Compare(
			vector.GetFixedAtNoTypeCheck[types.Decimal64](right, 0)) == 0
	case types.T_decimal128:
		return vector.GetFixedAtNoTypeCheck[types.Decimal128](left, 0).Compare(
			vector.GetFixedAtNoTypeCheck[types.Decimal128](right, 0)) == 0
	case types.T_decimal256:
		return vector.GetFixedAtNoTypeCheck[types.Decimal256](left, 0).Compare(
			vector.GetFixedAtNoTypeCheck[types.Decimal256](right, 0)) == 0
	case types.T_date:
		return odkuFixedValuesEqual[types.Date](left, right)
	case types.T_time:
		return odkuFixedValuesEqual[types.Time](left, right)
	case types.T_datetime:
		return odkuFixedValuesEqual[types.Datetime](left, right)
	case types.T_timestamp:
		return odkuFixedValuesEqual[types.Timestamp](left, right)
	case types.T_year:
		return odkuFixedValuesEqual[types.MoYear](left, right)
	case types.T_enum:
		return odkuFixedValuesEqual[types.Enum](left, right)
	case types.T_uuid:
		return odkuFixedValuesEqual[types.Uuid](left, right)
	case types.T_TS:
		return odkuFixedValuesEqual[types.TS](left, right)
	case types.T_Rowid:
		return odkuFixedValuesEqual[types.Rowid](left, right)
	case types.T_Blockid:
		return odkuFixedValuesEqual[types.Blockid](left, right)
	case types.T_Objectid:
		return odkuFixedValuesEqual[types.Objectid](left, right)
	case types.T_array_float32:
		l, r := types.BytesToArray[float32](left.GetBytesAt(0)), types.BytesToArray[float32](right.GetBytesAt(0))
		return odkuFloat32ArrayEqual(l, r)
	case types.T_array_float64:
		l, r := types.BytesToArray[float64](left.GetBytesAt(0)), types.BytesToArray[float64](right.GetBytesAt(0))
		return odkuFloat64ArrayEqual(l, r)
	case types.T_array_bf16:
		return odkuNarrowFloatArrayEqual(
			types.BytesToArray[types.BF16](left.GetBytesAt(0)),
			types.BytesToArray[types.BF16](right.GetBytesAt(0)),
		)
	case types.T_array_float16:
		return odkuNarrowFloatArrayEqual(
			types.BytesToArray[types.Float16](left.GetBytesAt(0)),
			types.BytesToArray[types.Float16](right.GetBytesAt(0)),
		)
	case types.T_array_int8:
		return types.ArrayElementCompare[int8](
			types.BytesToArray[int8](left.GetBytesAt(0)),
			types.BytesToArray[int8](right.GetBytesAt(0)),
		) == 0
	case types.T_array_uint8:
		return types.ArrayElementCompare[uint8](
			types.BytesToArray[uint8](left.GetBytesAt(0)),
			types.BytesToArray[uint8](right.GetBytesAt(0)),
		) == 0
	case types.T_json:
		return bytejson.CompareByteJson(
			types.DecodeJson(left.GetBytesAt(0)),
			types.DecodeJson(right.GetBytesAt(0)),
		) == 0
	case types.T_varchar, types.T_blob, types.T_text, types.T_binary,
		types.T_varbinary, types.T_datalink, types.T_geometry, types.T_geometry32:
		return bytes.Equal(left.GetBytesAt(0), right.GetBytesAt(0))
	default:
		// ODKU target columns cannot use pseudo/internal tuple types. Fail closed
		// instead of falling back through interface{}: GetAny forces hot-path heap
		// escapes and can silently give an unsupported type accidental semantics.
		return false
	}
}

func odkuFixedValuesEqual[T comparable](left, right *vector.Vector) bool {
	return vector.GetFixedAtNoTypeCheck[T](left, 0) ==
		vector.GetFixedAtNoTypeCheck[T](right, 0)
}

func odkuFloat32Equal(left, right float32) bool {
	return left == right || (math.IsNaN(float64(left)) && math.IsNaN(float64(right)))
}

func odkuFloat64Equal(left, right float64) bool {
	return left == right || (math.IsNaN(left) && math.IsNaN(right))
}

func odkuFloat32ArrayEqual(left, right []float32) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if !odkuFloat32Equal(left[i], right[i]) {
			return false
		}
	}
	return true
}

func odkuFloat64ArrayEqual(left, right []float64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if !odkuFloat64Equal(left[i], right[i]) {
			return false
		}
	}
	return true
}

func odkuNarrowFloatArrayEqual[T interface{ ToFloat32() float32 }](left, right []T) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if !odkuFloat32Equal(left[i].ToFloat32(), right[i].ToFloat32()) {
			return false
		}
	}
	return true
}

func odkuAffectedRows(changed, countFoundRows bool) uint64 {
	if changed {
		return 2
	}
	if countFoundRows {
		return 1
	}
	return 0
}

func appendODKUMetadata(
	vec *vector.Vector,
	enabled bool,
	resultPos, affectedRowsPos, physicalChangedPos int32,
	affectedRows uint64,
	physicalChanged bool,
	mp *mpool.MPool,
) (bool, error) {
	if !enabled {
		return false, nil
	}
	switch resultPos {
	case affectedRowsPos:
		return true, vector.AppendFixed(vec, affectedRows, false, mp)
	case physicalChangedPos:
		return true, vector.AppendFixed(vec, physicalChanged, false, mp)
	default:
		return false, nil
	}
}

func (ctr *container) snapshotForeignKeys(checks []ODKUForeignKeyCheck) {
	if len(ctr.foreignKeyBeforeVecs) != len(checks) {
		ctr.foreignKeyBeforeVecs = make([][]*vector.Vector, len(checks))
	}
	for i := range checks {
		ctr.foreignKeyBeforeVecs[i] = snapshotVectors(
			ctr.foreignKeyBeforeVecs[i], ctr.joinBat1, checks[i].ColIdxList)
	}
}

func (ctr *container) foreignKeyChanges(checks []ODKUForeignKeyCheck) []bool {
	if len(ctr.foreignKeyEligibility) != len(checks) {
		ctr.foreignKeyEligibility = make([]bool, len(checks))
	}
	for i := range checks {
		ctr.foreignKeyEligibility[i] = snapshotChanged(
			ctr.foreignKeyBeforeVecs[i], ctr.joinBat1, checks[i].ColIdxList)
	}
	return ctr.foreignKeyEligibility
}

func (ctr *container) allForeignKeysEligible(checks []ODKUForeignKeyCheck) []bool {
	if len(ctr.foreignKeyEligibility) != len(checks) {
		ctr.foreignKeyEligibility = make([]bool, len(checks))
	}
	for i := range ctr.foreignKeyEligibility {
		ctr.foreignKeyEligibility[i] = true
	}
	return ctr.foreignKeyEligibility
}

// finalizeInsertConstraintEligibility keeps two independent facts distinct:
// whether this logical action changed a constraint tuple, and whether the
// finalized group originated from an INSERT in this statement. The final row
// of a new group must remain eligible for every final-image constraint even
// when its last UPDATE action changed an unrelated column.
func (ctr *container) finalizeInsertConstraintEligibility(
	checks []ODKUForeignKeyCheck,
	isFinal bool,
) []bool {
	if isFinal {
		return ctr.allForeignKeysEligible(checks)
	}
	return ctr.foreignKeyChanges(checks)
}

func appendODKUActionMetadata(
	vec *vector.Vector,
	ap *DedupJoin,
	resultPos int32,
	affectedRows uint64,
	physicalChanged, actionFinal bool,
	fkEligibility []bool,
	mp *mpool.MPool,
) (bool, error) {
	if handled, err := appendODKUMetadata(
		vec, ap.HasODKUAffectedRows, resultPos,
		ap.AffectedRowsResultPos, ap.PhysicalChangedResultPos,
		affectedRows, physicalChanged, mp,
	); handled || err != nil {
		return handled, err
	}
	if ap.EmitActionRows && resultPos == ap.ActionFinalResultPos {
		return true, vector.AppendFixed(vec, actionFinal, false, mp)
	}
	for i, check := range ap.ForeignKeyChecks {
		if resultPos == check.EligibilityResultPos {
			return true, vector.AppendFixed(vec, fkEligibility[i], false, mp)
		}
	}
	return false, nil
}

func (ctr *container) appendProbeActionRow(
	ap *DedupJoin,
	dst *batch.Batch,
	probe *batch.Batch,
	probeRow int64,
	affectedRows uint64,
	physicalChanged, actionFinal bool,
	fkEligibility []bool,
	proc *process.Process,
) error {
	for j, rp := range ap.Result {
		if handled, err := appendODKUActionMetadata(
			dst.Vecs[j], ap, int32(j), affectedRows, physicalChanged,
			actionFinal, fkEligibility, proc.Mp(),
		); handled || err != nil {
			if err != nil {
				return err
			}
			continue
		}
		if rp.Rel == 1 {
			var srcVec *vector.Vector
			if int(rp.Pos) >= len(ctr.joinBat1.Vecs) || ctr.joinBat1.Vecs[rp.Pos].GetType().Oid == types.T_Rowid {
				srcVec = ctr.joinBat2.Vecs[rp.Pos]
			} else {
				srcVec = ctr.joinBat1.Vecs[rp.Pos]
			}
			if err := dst.Vecs[j].UnionOne(srcVec, 0, proc.Mp()); err != nil {
				return err
			}
		} else if err := dst.Vecs[j].UnionOne(probe.Vecs[rp.Pos], probeRow, proc.Mp()); err != nil {
			return err
		}
	}
	dst.AddRowCount(1)
	return nil
}

func (ctr *container) appendFinalizeActionRow(
	ap *DedupJoin,
	dst *batch.Batch,
	affectedRows uint64,
	physicalChanged, actionFinal bool,
	fkEligibility []bool,
	proc *process.Process,
) error {
	for j, rp := range ap.Result {
		if handled, err := appendODKUActionMetadata(
			dst.Vecs[j], ap, int32(j), affectedRows, physicalChanged,
			actionFinal, fkEligibility, proc.Mp(),
		); handled || err != nil {
			if err != nil {
				return err
			}
			continue
		}
		if rp.Rel == 1 {
			if err := dst.Vecs[j].UnionOne(ctr.joinBat1.Vecs[rp.Pos], 0, proc.Mp()); err != nil {
				return err
			}
		} else if err := dst.Vecs[j].UnionNull(proc.Mp()); err != nil {
			return err
		}
	}
	dst.AddRowCount(1)
	return nil
}

func (ctr *container) actionResultBatchFull() bool {
	// The byte threshold is intentionally checked after materialization: an
	// update expression's varlen result is not known beforehand. Therefore the
	// budget is soft and can be crossed only by the final admitted row.
	return ctr.rbat.RowCount() >= colexec.DefaultBatchSize ||
		ctr.rbat.RowCount() > 0 && ctr.rbat.Size() >= ctr.resultBatchByteLimit
}

func restoreUpdateVectors(bat *batch.Batch, cols []int32, values []*vector.Vector) {
	for i, pos := range cols {
		bat.Vecs[pos] = values[i]
	}
}

func (ctr *container) actionSelection(group uint64, action int) int32 {
	if ctr.mp.HashOnUnique() {
		return int32(group - 1)
	}
	return ctr.mp.GetSels(group)[action]
}

func (ctr *container) actionCount(group uint64) int {
	if ctr.mp.HashOnUnique() {
		return 1
	}
	return len(ctr.mp.GetSels(group))
}

// probeODKUActionRows replays a hot-key group across as many Call invocations
// as needed. The child batch is borrowed until probeRow reaches its end; no
// subsequent child Call can invalidate it while probeActionActive is true.
func (ctr *container) probeODKUActionRows(
	bat *batch.Batch,
	ap *DedupJoin,
	proc *process.Process,
	result *vm.CallResult,
) error {
	if err := ap.resetRBat(); err != nil {
		return err
	}
	if ctr.probeBat == nil {
		if err := ctr.evalJoinCondition(bat, proc); err != nil {
			return err
		}
		ctr.probeBat = bat
		ctr.probeRow = 0
	}
	if ctr.joinBat1 == nil {
		ctr.joinBat1, ctr.cfs1 = colexec.NewJoinBatch(bat, proc.Mp())
	}
	if ctr.joinBat2 == nil && ctr.batchRowCount > 0 {
		ctr.joinBat2, ctr.cfs2 = colexec.NewJoinBatch(ctr.batches[0], proc.Mp())
	}
	if ctr.cachedItr == nil {
		ctr.cachedItr = ctr.mp.NewIterator()
	}

	for ctr.probeRow < bat.RowCount() {
		start := ctr.probeRow
		n := min(hashmap.UnitLimit, bat.RowCount()-start)
		vals, zvals, err := ctr.cachedItr.Find(start, n, ctr.vecs)
		if err != nil {
			return err
		}
		for ctr.probeRow < start+n {
			idx := ctr.probeRow - start
			if zvals[idx] == 0 || vals[idx] == 0 {
				ctr.probeRow++
				continue
			}
			group := vals[idx]
			if !ctr.probeActionActive {
				if err := colexec.SetJoinBatchValues(
					ctr.joinBat1, bat, int64(ctr.probeRow), 1, ctr.cfs1); err != nil {
					return err
				}
				ctr.groupBeforeVecs = snapshotVectors(
					ctr.groupBeforeVecs, ctr.joinBat1, ap.UpdateColIdxList)
				ctr.probeCurrentVecs = snapshotVectors(
					ctr.probeCurrentVecs, ctr.joinBat1, ap.UpdateColIdxList)
				ctr.probeGroup = group
				ctr.probeActionIdx = 0
				ctr.probeLogicalAffected = 0
				ctr.probeAnyChanged = false
				ctr.probeActionActive = true
			} else if ctr.probeGroup != group {
				return moerr.NewInternalError(proc.Ctx, "ODKU action replay group changed while suspended")
			}

			yield := false
			err = ctr.withRestoredJoinBat1Vectors(ap.UpdateColIdxList, func() error {
				restoreUpdateVectors(ctr.joinBat1, ap.UpdateColIdxList, ctr.probeCurrentVecs)
				actionCount := ctr.actionCount(group)
				for ctr.probeActionIdx < actionCount {
					sel := ctr.actionSelection(group, ctr.probeActionIdx)
					idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
					if err := colexec.SetJoinBatchValues(
						ctr.joinBat2, ctr.batches[idx1], int64(idx2), 1, ctr.cfs2); err != nil {
						return err
					}
					ctr.snapshotForeignKeys(ap.ForeignKeyChecks)
					changed, err := ctr.applyUpdateExpressions(
						proc, ap.UpdateColIdxList, ap.UpdateCheckColIdxList)
					if err != nil {
						return err
					}
					ctr.probeCurrentVecs = snapshotVectors(
						ctr.probeCurrentVecs, ctr.joinBat1, ap.UpdateColIdxList)
					ctr.probeLogicalAffected += odkuAffectedRows(changed, ap.CountFoundRows)
					ctr.probeAnyChanged = ctr.probeAnyChanged || changed
					isFinal := ctr.probeActionIdx == actionCount-1
					physicalChanged := isFinal && odkuPhysicalChanged(
						ctr.probeAnyChanged, ctr.groupBeforeVecs, ctr.joinBat1, ap.UpdateColIdxList)
					affectedRows := uint64(0)
					if isFinal {
						affectedRows = ctr.probeLogicalAffected
					}
					if err := ctr.appendProbeActionRow(
						ap, ctr.rbat, bat, int64(ctr.probeRow), affectedRows,
						physicalChanged, isFinal, ctr.foreignKeyChanges(ap.ForeignKeyChecks), proc); err != nil {
						return err
					}
					ctr.probeActionIdx++
					if ctr.actionResultBatchFull() {
						yield = true
						break
					}
				}
				return nil
			})
			if err != nil {
				return err
			}
			if ctr.probeActionIdx == ctr.actionCount(group) {
				ctr.matched.Add(group - 1)
				ctr.probeActionActive = false
				ctr.probeCurrentVecs = nil
				ctr.probeRow++
			}
			if yield {
				result.Batch = ctr.rbat
				return nil
			}
		}
	}
	ctr.probeBat = nil
	ctr.probeRow = 0
	result.Batch = ctr.rbat
	return nil
}

func (ctr *container) probe(bat *batch.Batch, ap *DedupJoin, proc *process.Process, analyzer process.Analyzer, result *vm.CallResult) error {
	if ap.EmitActionRows {
		return ctr.probeODKUActionRows(bat, ap, proc, result)
	}
	if err := ap.resetRBat(); err != nil {
		return err
	}
	err := ctr.evalJoinCondition(bat, proc)
	if err != nil {
		return err
	}
	if ap.OnDuplicateAction == plan.Node_UPDATE {
		if ctr.joinBat1 == nil {
			ctr.joinBat1, ctr.cfs1 = colexec.NewJoinBatch(bat, proc.Mp())
		}
		if ctr.joinBat2 == nil && ctr.batchRowCount > 0 {
			ctr.joinBat2, ctr.cfs2 = colexec.NewJoinBatch(ctr.batches[0], proc.Mp())
		}
	}
	count := bat.RowCount()
	if ctr.cachedItr == nil {
		ctr.cachedItr = ctr.mp.NewIterator()
	}
	itr := ctr.cachedItr
	isPessimistic := proc.GetTxnOperator().Txn().IsPessimistic()
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		vals, zvals, err := itr.Find(i, n, ctr.vecs)
		if err != nil {
			return err
		}
		for k := 0; k < n; k++ {
			if zvals[k] == 0 || vals[k] == 0 {
				continue
			}
			switch ap.OnDuplicateAction {
			case plan.Node_FAIL:
				if ctr.mp.IsDeleted(vals[k] - 1) {
					continue
				}
				// REPLACE INTO merged-scan path: on bucket hit, capture the
				// probe-side old-column values into per-bucket buffers instead
				// of raising DuplicateEntry. The captured values are emitted
				// alongside the build row in finalize().
				if len(ap.OldColCapturePlaceholderIdxList) > 0 {
					bucket := uint64(vals[k] - 1)
					if !ctr.captured.Contains(bucket) {
						for cIdx, probePos := range ap.OldColCaptureProbeIdxList {
							if err := ctr.capturedVecs[cIdx].Copy(bat.Vecs[probePos], int64(bucket), int64(i+k), proc.Mp()); err != nil {
								return err
							}
						}
						ctr.captured.Add(bucket)
					}
					continue
				}
				// do nothing for txn.mode = Optimistic
				if !isPessimistic {
					continue
				}
				var rowStr string
				if len(ap.DedupColTypes) == 1 {
					if ap.DedupColName == catalog.IndexTableIndexColName {
						if ctr.vecs[0].GetType().Oid == types.T_varchar {
							t, _, schema, err := types.DecodeTuple(ctr.vecs[0].GetBytesAt(i + k))
							if err == nil && len(schema) > 1 {
								rowStr = t.ErrString(make([]int32, len(schema)))
							}
						}
					}
					if len(rowStr) == 0 {
						rowStr, err = colexec.FormatDedupKey(ctr.vecs[0], i+k, ap.DedupColTypes)
						if err != nil {
							return err
						}
					}
				} else {
					rowStr, err = colexec.FormatDedupKey(ctr.vecs[0], i+k, ap.DedupColTypes)
					if err != nil {
						return err
					}
				}
				return moerr.NewDuplicateEntry(proc.Ctx, rowStr, ap.DedupColName)
			case plan.Node_IGNORE:
				// The build side marks the old key of every UPDATE target as
				// deleted.  A match to that key is the row updating itself, not a
				// conflicting row to be ignored.
				if ctr.mp.IsDeleted(vals[k] - 1) {
					continue
				}
				if sels := ctr.mp.GetSels(vals[k]); len(sels) > 0 {
					for _, sel := range sels {
						ctr.matched.Add(uint64(sel))
					}
				} else {
					// Compact unique maps omit GroupSels; in that representation
					// group g still maps directly to build row g-1.
					ctr.matched.Add(vals[k] - 1)
				}

			case plan.Node_UPDATE:
				err := colexec.SetJoinBatchValues(ctr.joinBat1, bat, int64(i+k), 1, ctr.cfs1)
				if err != nil {
					return err
				}
				var logicalAffectedRows uint64
				err = ctr.withRestoredJoinBat1Vectors(ap.UpdateColIdxList, func() error {
					ctr.groupBeforeVecs = snapshotVectors(
						ctr.groupBeforeVecs, ctr.joinBat1, ap.UpdateColIdxList)
					anyActionChanged := false
					var actionSels []int32
					var uniqueActionSel [1]int32
					if ctr.mp.HashOnUnique() {
						uniqueActionSel[0] = int32(vals[k] - 1)
						actionSels = uniqueActionSel[:]
					} else {
						actionSels = ctr.mp.GetSels(vals[k])
					}
					for actionIdx, sel := range actionSels {
						idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
						if err := colexec.SetJoinBatchValues(ctr.joinBat2, ctr.batches[idx1], int64(idx2), 1, ctr.cfs2); err != nil {
							return err
						}
						if len(ap.ForeignKeyChecks) > 0 {
							ctr.snapshotForeignKeys(ap.ForeignKeyChecks)
						}
						changed, err := ctr.applyUpdateExpressions(
							proc, ap.UpdateColIdxList, ap.UpdateCheckColIdxList)
						if err != nil {
							return err
						}
						logicalAffectedRows += odkuAffectedRows(changed, ap.CountFoundRows)
						anyActionChanged = anyActionChanged || changed
						isFinal := actionIdx == len(actionSels)-1
						if ap.EmitActionRows || isFinal {
							physicalChanged := isFinal && odkuPhysicalChanged(
								anyActionChanged, ctr.groupBeforeVecs, ctr.joinBat1, ap.UpdateColIdxList)
							affectedRows := uint64(0)
							if isFinal {
								affectedRows = logicalAffectedRows
							}
							var fkEligibility []bool
							if len(ap.ForeignKeyChecks) > 0 {
								fkEligibility = ctr.foreignKeyChanges(ap.ForeignKeyChecks)
							}
							if err := ctr.appendProbeActionRow(
								ap, ctr.rbat, bat, int64(i+k), affectedRows,
								physicalChanged, isFinal, fkEligibility, proc); err != nil {
								return err
							}
						}
					}
					return nil
				})
				if err != nil {
					return err
				}
				ctr.matched.Add(vals[k] - 1)
			}
		}
	}
	result.Batch = ctr.rbat
	ap.ctr.lastPos = 0
	return nil
}
func (ctr *container) evalJoinCondition(bat *batch.Batch, proc *process.Process) error {
	for i := range ctr.evecs {
		vec, err := ctr.evecs[i].executor.Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			return err
		}
		ctr.vecs[i] = vec
		ctr.evecs[i].vec = vec
	}
	return nil
}

func unionSelsByBatch(dst *vector.Vector, batches []*batch.Batch, colPos int32, sels []int32, proc *process.Process) error {
	if len(sels) <= 16 {
		for _, sel := range sels {
			idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
			if err := dst.UnionOne(batches[idx1].Vecs[colPos], int64(idx2), proc.Mp()); err != nil {
				return err
			}
		}
		return nil
	}
	offsets := make([]int64, 0, len(sels))
	prevIdx := int32(-1)
	for _, sel := range sels {
		idx1 := sel / colexec.DefaultBatchSize
		idx2 := int64(sel % colexec.DefaultBatchSize)
		if idx1 != prevIdx {
			if prevIdx >= 0 && len(offsets) > 0 {
				if err := dst.Union(batches[prevIdx].Vecs[colPos], offsets, proc.Mp()); err != nil {
					return err
				}
				offsets = offsets[:0]
			}
			prevIdx = idx1
		}
		offsets = append(offsets, idx2)
	}
	if len(offsets) > 0 {
		if err := dst.Union(batches[prevIdx].Vecs[colPos], offsets, proc.Mp()); err != nil {
			return err
		}
	}
	return nil
}
func (dedupJoin *DedupJoin) newResultVector(typ types.Type) (*vector.Vector, error) {
	return vector.NewOffHeapVecWithTypeAndAllocation(typ, dedupJoin.resultAllocation)
}

func (dedupJoin *DedupJoin) resetRBat() error {
	ctr := &dedupJoin.ctr
	if ctr.rbat != nil {
		ctr.rbat.CleanOnlyData()
	} else {
		ctr.rbat = batch.NewOffHeapWithSize(len(dedupJoin.Result))
		for i, rp := range dedupJoin.Result {
			if rp.Rel == 0 {
				ctr.rbat.Vecs[i] = vector.NewOffHeapVecWithType(dedupJoin.LeftTypes[rp.Pos])
			} else {
				ctr.rbat.Vecs[i] = vector.NewOffHeapVecWithType(dedupJoin.RightTypes[rp.Pos])
			}
		}
		if err := ctr.rbat.SetAllocationAccount(dedupJoin.resultAllocation); err != nil {
			ctr.rbat.Clean(nil)
			ctr.rbat = nil
			return err
		}
	}
	return nil
}
