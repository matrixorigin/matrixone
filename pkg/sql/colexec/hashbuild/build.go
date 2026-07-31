// Copyright 2021 Matrix Origin
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

package hashbuild

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/runtimefilter"
	planfunction "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "hash_build"

func (hashBuild *HashBuild) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": hash build ")
}

func (hashBuild *HashBuild) OpType() vm.OpType {
	return vm.HashBuild
}

func (hashBuild *HashBuild) Prepare(proc *process.Process) (err error) {
	// A HashBuild can be reused after Reset.  The terminal gate belongs to the
	// new execution generation; the old MessageBoard is reset by the pipeline
	// before this generation's consumers are started.
	hashBuild.ctr.terminalMu.Lock()
	atomic.StoreUint32(&hashBuild.ctr.terminalPublished, 0)
	hashBuild.ctr.runtimeFilterDone = false
	hashBuild.ctr.diagnosticsLogged = false
	// spillFS is borrowed from the execution Process. Never carry that
	// generation-scoped service into a reused operator: the next Process may
	// resolve a different LOCAL service (or the old one may already be closed).
	hashBuild.ctr.spillFS = nil
	hashBuild.ctr.terminalMu.Unlock()

	if hashBuild.OpAnalyzer == nil {
		hashBuild.OpAnalyzer = process.NewAnalyzer(hashBuild.GetIdx(), hashBuild.IsFirst, hashBuild.IsLast, "hash build")
	} else {
		hashBuild.OpAnalyzer.Reset()
	}

	hashBuild.ctr.setSpillThreshold(hashBuild.SpillThreshold)
	hashBuild.ctr.spillUUID = fmt.Sprintf("hb_%d", hashBuildSpillSequence.Add(1))

	budget, err := proc.GetHashBuildBudget()
	if err != nil {
		return err
	}
	hashBuild.ctr.hashmapBuilder.setBudget(budget)
	if hashBuild.IsShuffle && hashBuild.RuntimeFilterSpec == nil {
		return moerr.NewInternalError(proc.Ctx, "shuffle hash build must have runtime filter")
	}
	if !hashBuild.NeedHashMap {
		return nil
	}

	hashBuild.ctr.hashmapBuilder.IsDedup = hashBuild.IsDedup
	hashBuild.ctr.hashmapBuilder.DedupBuildKeepLast = hashBuild.DedupBuildKeepLast
	hashBuild.ctr.hashmapBuilder.OnDuplicateAction = hashBuild.OnDuplicateAction
	hashBuild.ctr.hashmapBuilder.DedupColName = hashBuild.DedupColName
	hashBuild.ctr.hashmapBuilder.DedupColTypes = hashBuild.DedupColTypes
	hashBuild.ctr.hashmapBuilder.TrackNullKeys = hashBuild.TrackNullKeys

	return hashBuild.ctr.hashmapBuilder.Prepare(
		hashBuild.Conditions,
		hashBuild.DelColIdx,
		hashBuild.DedupDeleteMarkerColIdx,
		hashBuild.DedupDeleteKeepColIdxList,
		proc,
	)
}

func (hashBuild *HashBuild) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := hashBuild.OpAnalyzer
	result := vm.NewCallResult()
	ctr := &hashBuild.ctr
	for {
		switch ctr.state {
		case BuildHashMap:
			if err := hashBuild.build(proc, analyzer); err != nil {
				hashBuild.finalizeBuildFailure(proc, err)
				return result, err
			}

			ctr.state = HandleRuntimeFilter

		case HandleRuntimeFilter:
			if err := hashBuild.handleRuntimeFilter(proc); err != nil {
				hashBuild.finalizeBuildFailure(proc, err)
				return result, err
			}

			ctr.state = SendJoinMap

		case SendJoinMap:
			ctr.terminalMu.Lock()
			if hashBuild.JoinMapTag <= 0 {
				ctr.terminalMu.Unlock()
				err := moerr.NewInternalError(proc.Ctx, "wrong joinmap message tag!")
				hashBuild.finalizeBuildFailure(proc, err)
				return result, err
			}
			if atomic.LoadUint32(&ctr.terminalPublished) != 0 {
				ctr.terminalMu.Unlock()
				return result, moerr.NewQueryInterrupted(proc.Ctx)
			}

			var jm *message.JoinMap
			spillMode := len(ctr.spilledFds) > 0
			var spillPayloadErr error

			if ctr.hashmapBuilder.InputBatchRowCount > 0 {
				if spillMode {
					// In spill mode: send empty JoinMap with spill fds, no batches
					jm = message.NewJoinMap(message.GroupSels{}, nil, nil, nil, nil, proc.Mp())
				} else {
					// Normal mode: send hashmap and batches
					jm = ctr.hashmapBuilder.GetJoinMap(proc.Mp())
					jm.SetPushedRuntimeFilterIn(ctr.runtimeFilterIn)
				}
				jm.SetRowCount(int64(ctr.hashmapBuilder.InputBatchRowCount))
				jm.SetHasNullKey(ctr.hashmapBuilder.HasNullKey)
				jm.IncRef(hashBuild.JoinMapRefCnt)
				if spillMode {
					payload := message.SpillBuildPayload{LegacyFds: ctr.spilledFds}
					if ctr.spillBundle != nil {
						payload = message.SpillBuildPayload{
							Files:     ctr.spillBundle.accountedFiles(),
							BudgetRef: ctr.hashmapBuilder.budget,
						}
					}
					spillPayloadErr = jm.SetSpillBuildPayload(payload)
					if spillPayloadErr == nil {
						ctr.spilledFds = nil // ownership transferred
						ctr.spillBundle = nil
					}
				}
			}

			if spillPayloadErr != nil {
				jm.FreeMemory()
				ctr.terminalMu.Unlock()
				err := moerr.NewInternalError(proc.Ctx, spillPayloadErr.Error())
				hashBuild.finalizeBuildFailure(proc, err)
				return result, err
			}

			if !hashBuild.publishJoinMap(proc, jm) {
				// Reset/Free may have won the terminal gate concurrently during
				// cancellation.  Keep the producer side successful only if this
				// publication won; consumers must never see two terminal values.
				if jm != nil {
					jm.FreeMemory()
				}
				ctr.terminalMu.Unlock()
				return result, moerr.NewQueryInterrupted(proc.Ctx)
			}

			ctr.state = SendSucceed
			ctr.terminalMu.Unlock()

		case SendSucceed:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

// finalizeBuildFailure publishes every producer-side dependency before Call
// returns. Consumers may already be blocked in ReceiveJoinMap/RuntimeFilter;
// deferring publication until Reset could deadlock a pipeline scheduler that
// waits for those consumers before cleanup.
func (hashBuild *HashBuild) finalizeBuildFailure(proc *process.Process, err error) {
	hashBuild.ctr.terminalMu.Lock()
	defer hashBuild.ctr.terminalMu.Unlock()
	hashBuild.publishBuildError(proc, err)
	if !hashBuild.ctr.runtimeFilterDone {
		message.FinalizeRuntimeFilterOnBuildError(hashBuild.RuntimeFilterSpec, proc.GetMessageBoard())
		hashBuild.ctr.runtimeFilterDone = hashBuild.RuntimeFilterSpec != nil
	}
}

func (hashBuild *HashBuild) build(proc *process.Process, analyzer process.Analyzer) error {
	ctr := &hashBuild.ctr
	spillMode := false
	var spillFiles []*os.File
	bundleTransferred := false

	defer func() {
		observeHashBuildBudget(analyzer, ctr.hashmapBuilder.budget)
		for _, f := range spillFiles {
			if f != nil {
				f.Close()
			}
		}
		if !bundleTransferred && ctr.spillBundle != nil {
			ctr.spillBundle.release()
			ctr.spillBundle = nil
		}
		ctr.freeSpillExprExecs()
		// Build-key executors are producer scratch. No consumer reads them after
		// build() returns, so release their retained vectors and expression lease
		// here instead of holding both until pipeline Reset.
		ctr.hashmapBuilder.FreeTemporaryVectors(proc)
		ctr.hashmapBuilder.FreeExecutors()
		ctr.dropSpillScratchBuffers()
		ctr.releaseSpillScratchReservation()
	}()

	startSpill := func() error {
		if spillMode {
			return nil
		}
		if err := checkHashBuildCanceled(proc); err != nil {
			return err
		}
		// The current spill protocol moves one physical build payload into one
		// SpillEngine. Broadcast JoinMaps are ref-counted shared objects and
		// require a separate bucket/task exchange before they can spill safely.
		// Keep that unsupported topology fail-fast at the producer boundary.
		if hashBuild.JoinMapRefCnt != 1 {
			return moerr.NewInternalErrorf(
				proc.Ctx,
				"hash build spill requires exactly one consumer, got %d",
				hashBuild.JoinMapRefCnt,
			)
		}
		execs, err := ctr.initSpillExprExecs(proc, hashBuild.Conditions)
		if err != nil {
			return err
		}
		if spillFiles == nil {
			spillFiles = make([]*os.File, spillNumBuckets)
		}
		spillMode = true
		analyzer.GetOpStats().AddExtraStat("HashBuildSpillStarts", 1)
		// Drain retained copies oldest-first.  Each successful partition is
		// followed immediately by reservation and mpool release, so the source
		// batch and one partition scratch are the only simultaneous peaks.
		for len(ctr.hashmapBuilder.Batches.Buf) > 0 {
			if err := checkHashBuildCanceled(proc); err != nil {
				return err
			}
			bat := ctr.hashmapBuilder.Batches.Buf[0]
			if bat == nil {
				if err := ctr.hashmapBuilder.CleanCopiedBatchAt(0, proc); err != nil {
					return err
				}
				continue
			}
			if err := ctr.spillBatchBounded(proc, bat, spillFiles, execs, analyzer, true); err != nil {
				return err
			}
			if err := ctr.hashmapBuilder.CleanCopiedBatchAt(0, proc); err != nil {
				return err
			}
		}
		v2.HashBuildSpillDepthCounter.WithLabelValues("spill", "1").Inc()
		return nil
	}

	for {
		if err := checkHashBuildCanceled(proc); err != nil {
			return err
		}
		result, err := vm.ChildrenCall(hashBuild.GetChildren(0), proc, analyzer)
		if err != nil {
			return err
		}
		// A child can finish a Call after the pipeline was canceled. Do not copy
		// or spill the batch it returned after that cancellation.
		if err := checkHashBuildCanceled(proc); err != nil {
			return err
		}
		if result.Batch == nil {
			break
		}
		if result.Batch.IsEmpty() {
			continue
		}

		analyzer.Alloc(int64(result.Batch.Size()))
		// Durable row accounting is advanced exactly once on ingress.  In
		// particular, a rejected retained-copy admission below must not add the
		// same upstream batch a second time when it is spilled directly.
		ctr.hashmapBuilder.InputBatchRowCount += result.Batch.RowCount()
		if hashBuild.IsShuffle {
			// First prove that the current upstream batch can always be spilled
			// directly. This uses its actual materialization semantics and never
			// projects a hypothetical retained batch.
			if err := ctr.ensureDirectSpillScratchReservation(result.Batch, analyzer); err != nil {
				// Existing retained batches were admitted with a future-drain
				// proof. Drain them under that lease, then retry the direct proof
				// after their source reservations have been released.
				if spillMode || !errors.Is(err, process.ErrHashBuildBudgetAdmission) || len(ctr.hashmapBuilder.Batches.Buf) == 0 {
					return err
				}
				if err := startSpill(); err != nil {
					return err
				}
				if err := ctr.ensureDirectSpillScratchReservation(result.Batch, analyzer); err != nil {
					return err
				}
			}
			if !spillMode {
				// A batch may become retained only after its future spill scratch
				// is admitted. If that proof does not fit, do not copy it: switch
				// to the already-proven direct-spill path.
				if err := ctr.ensureRetainedSpillScratchReservation(result.Batch, analyzer); err != nil {
					if !errors.Is(err, process.ErrHashBuildBudgetAdmission) {
						return err
					}
					if err := startSpill(); err != nil {
						return err
					}
				}
			}
		}

		// If in spill mode, spill this batch directly to open files.
		if spillMode {
			err := ctr.spillBatchBounded(proc, result.Batch, spillFiles, ctr.spillExprExecs, analyzer, false)
			if err != nil {
				return err
			}
			continue
		}

		// Store original batch
		err = ctr.hashmapBuilder.copyBuildBatch(result.Batch, proc)
		if err != nil {
			if hashBuild.IsShuffle && errors.Is(err, process.ErrHashBuildBudgetAdmission) {
				// The source batch is still owned by the upstream operator.  Do
				// not retry CopyIntoBatches (or increment row count again); enter
				// spill recovery and write this batch directly.
				if err := startSpill(); err != nil {
					return err
				}
				if err := ctr.spillBatchBounded(proc, result.Batch, spillFiles, ctr.spillExprExecs, analyzer, false); err != nil {
					return err
				}
				continue
			}
			return err
		}

		// Check if we should enter spill mode based on batch memory size
		if hashBuild.shouldSpillBatches() {
			if err := startSpill(); err != nil {
				return err
			}
		}
	}

	// If we never entered spill mode, build the hashmap
	if !spillMode && hashBuild.NeedHashMap {
		if err := checkHashBuildCanceled(proc); err != nil {
			return err
		}
		needUniqueVec := false
		ctr.hashmapBuilder.uniqueKeySlots = nil
		if !hashBuild.IsShuffle && hashBuild.RuntimeFilterSpec != nil {
			// Membership-filter consumers own a separate typed-key contract.
			// Ordinary exact filters collect unique keys only when the plan
			// advertises every producer-side closure required by their payload
			// consumers. Serialized tuple filters additionally validate every
			// declared component slot before retaining the aligned unique-key
			// vectors needed to evaluate serial/serial_full.
			if hashBuild.RuntimeFilterSpec.UseMembershipFilter {
				needUniqueVec = hashBuild.RuntimeFilterSpec.Expr != nil &&
					hashBuild.RuntimeFilterSpec.Expr.GetF() == nil
			} else {
				encoding, ok := hashBuild.declaredRuntimeFilterEncoding(proc)
				needUniqueVec = ok &&
					encoding != keycodec.ExactRuntimeFilterUnsupported
			}
		}
		if needUniqueVec {
			var ok bool
			ctr.hashmapBuilder.uniqueKeySlots, ok =
				runtimeFilterCollectionSlotMask(
					hashBuild.RuntimeFilterSpec,
					len(hashBuild.Conditions),
				)
			if !ok {
				needUniqueVec = false
				ctr.hashmapBuilder.uniqueKeySlots = nil
			}
		}

		err := ctr.hashmapBuilder.BuildHashmap(hashBuild.HashOnPK, hashBuild.NeedAllocateSels, needUniqueVec, proc)
		collectionFallback, _ :=
			ctr.hashmapBuilder.runtimeFilterFallbackState()
		rebuildSafe := ctr.hashmapBuilder.RetainedBatchRecoverySafe()
		if err != nil && needUniqueVec &&
			!collectionFallback && rebuildSafe &&
			errors.Is(err, process.ErrHashBuildBudgetAdmission) {
			// Unique-key retention exists only for the optional runtime filter.
			// A mandatory map allocation can still lose admission while the
			// optional owner is live. Rebuild only while HashmapBuilder proves
			// that no destructive Dedup batch rewrite has started.
			ctr.hashmapBuilder.FreeHashMapOnly(proc)
			collectionFallback = true
			err = ctr.hashmapBuilder.BuildHashmap(
				hashBuild.HashOnPK,
				hashBuild.NeedAllocateSels,
				false,
				proc,
			)
			rebuildSafe = ctr.hashmapBuilder.RetainedBatchRecoverySafe()
		}
		if collectionFallback && analyzer != nil {
			analyzer.GetOpStats().AddExtraStat(
				"HashBuildRuntimeFilterCollectionFallbacks", 1)
		}
		if err != nil {
			if !hashBuild.IsShuffle || !errors.Is(err, process.ErrHashBuildBudgetAdmission) {
				return err
			}
			if !rebuildSafe {
				// Dedup may already have compacted Batches or be between
				// shrinking survivors and appending delete-only rows. Neither
				// replay nor spill can reconstruct the original input at this
				// point; return a controlled admission error instead of
				// publishing a semantically incomplete spill payload.
				return err
			}
			// Preserve the copied batches, discard only partial map state, and use
			// the pre-admitted emergency scratch lease to recover through spill.
			ctr.hashmapBuilder.FreeHashMapOnly(proc)
			if err := startSpill(); err != nil {
				return err
			}
		}
	}

	// spillBatchBounded flushes each selected bucket immediately; no persistent
	// 32-bucket vectors remain here. Flush serialized records accumulated across
	// source batches before rewinding every file and publishing the
	// complete set, including a spill entered after hard map-budget rejection.
	if spillMode {
		if err := checkHashBuildCanceled(proc); err != nil {
			return err
		}
		if err := ctr.flushSpillBuffers(proc, spillFiles, analyzer); err != nil {
			return err
		}
		for _, f := range spillFiles {
			if f != nil {
				if _, err := f.Seek(0, io.SeekStart); err != nil {
					return err
				}
			}
		}
		ctr.spilledFds = spillFiles
		spillFiles = nil
		bundleTransferred = true
	}

	if !hashBuild.NeedBatches {
		ctr.hashmapBuilder.cleanBatches(proc)
	}

	analyzer.Alloc(ctr.hashmapBuilder.GetSize())
	return nil
}

func observeHashBuildBudget(analyzer process.Analyzer, budget *process.HashBuildBudgetGeneration) {
	if analyzer == nil || budget == nil {
		return
	}
	snapshot := budget.Snapshot()
	stats := analyzer.GetOpStats()
	// These are query-CN generation snapshots, not operator-local sums. Keep
	// maxima so repeated sampling by one operator cannot double count them.
	stats.SetMaxExtraStat("QueryHashBudgetCapBytes", hashBuildStatInt64(snapshot.Cap))
	stats.SetMaxExtraStat("QueryHashBudgetPeakBytes", hashBuildStatInt64(snapshot.PeakUsed))
	stats.SetMaxExtraStat("QueryHashBudgetRejects", hashBuildStatInt64(snapshot.RejectCount))
	stats.SetMaxExtraStat("QueryHashBudgetReserves", hashBuildStatInt64(snapshot.ReserveCount))
	stats.SetMaxExtraStat("QueryHashBudgetReconciles", hashBuildStatInt64(snapshot.ReconcileCount))
	stats.SetMaxExtraStat("QuerySpillDiskUsedBytes", hashBuildStatInt64(snapshot.SpillDiskUsed))
	stats.SetMaxExtraStat("QuerySpillFDUsed", hashBuildStatInt64(snapshot.SpillFDUsed))
}

func hashBuildStatInt64(value uint64) int64 {
	if value > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(value)
}

// calculateBloomFilterProbability calculates the false positive rate for bloom filter
// based on row count. Reference fuzzyfilter experience, choose different false positive rates
// based on row count to balance memory usage and filtering accuracy.
func calculateBloomFilterProbability(rowCount int) float64 {
	switch {
	case rowCount < 10_0001:
		return 0.00001
	case rowCount < 100_0001:
		return 0.000003
	case rowCount < 1000_0001:
		return 0.000001
	case rowCount < 1_0000_0001:
		return 0.0000005
	case rowCount < 10_0000_0001:
		return 0.0000002
	default:
		return 0.0000001
	}
}

func planExprType(expr *plan.Expr) (types.Type, bool) {
	if expr == nil {
		return types.Type{}, false
	}
	return types.New(
		types.T(expr.Typ.Id),
		expr.Typ.Width,
		expr.Typ.Scale,
	), true
}

func runtimeFilterComponentSlots(spec *plan.RuntimeFilterSpec) ([]int, bool) {
	buildExpr := runtimefilter.BuildKeyExpr(spec)
	if buildExpr == nil || buildExpr.GetF() == nil {
		return nil, false
	}
	args := buildExpr.GetF().Args
	if len(args) == 0 {
		return nil, false
	}
	slots := make([]int, len(args))
	for i, arg := range args {
		if arg == nil || arg.GetCol() == nil || arg.GetCol().ColPos < 0 {
			return nil, false
		}
		slots[i] = int(arg.GetCol().ColPos)
	}
	return slots, true
}

func runtimeFilterCollectionSlotMask(
	spec *plan.RuntimeFilterSpec,
	conditionCount int,
) ([]bool, bool) {
	if spec == nil || conditionCount <= 0 {
		return nil, false
	}
	mask := make([]bool, conditionCount)
	if spec.UseMembershipFilter {
		if spec.Expr == nil || spec.Expr.GetF() != nil {
			return nil, false
		}
		// The established membership payload is the first join condition.
		mask[0] = true
		return mask, true
	}
	buildExpr := runtimefilter.BuildKeyExpr(spec)
	if buildExpr == nil {
		return nil, false
	}
	if col := buildExpr.GetCol(); col != nil {
		slot := int(col.ColPos)
		if slot < 0 || slot >= conditionCount {
			return nil, false
		}
		mask[slot] = true
		return mask, true
	}
	slots, ok := runtimeFilterComponentSlots(spec)
	if !ok {
		return nil, false
	}
	for _, slot := range slots {
		if slot < 0 || slot >= conditionCount {
			return nil, false
		}
		mask[slot] = true
	}
	return mask, true
}

// declaredRuntimeFilterEncoding validates the plan contract against the
// HashBuild condition slots which will materialize it. It is used before map
// construction so an invalid/stale plan cannot retain unique-key vectors just
// to publish PASS.
func (hashBuild *HashBuild) declaredRuntimeFilterEncoding(
	proc *process.Process,
) (keycodec.ExactRuntimeFilterEncoding, bool) {
	spec := hashBuild.RuntimeFilterSpec
	buildExpr := runtimefilter.BuildKeyExpr(spec)
	if buildExpr == nil {
		return keycodec.ExactRuntimeFilterUnsupported, false
	}
	if buildExpr.GetCol() != nil {
		slot := int(buildExpr.GetCol().ColPos)
		if slot < 0 || slot >= len(hashBuild.Conditions) {
			return keycodec.ExactRuntimeFilterUnsupported, false
		}
		payloadType, ok := planExprType(hashBuild.Conditions[slot])
		if !ok {
			return keycodec.ExactRuntimeFilterUnsupported, false
		}
		return runtimefilter.ExactKeyEncoding(
			spec, payloadType, proc.GetService()), true
	}

	slots, ok := runtimeFilterComponentSlots(spec)
	if !ok {
		return keycodec.ExactRuntimeFilterUnsupported, false
	}
	componentTypes := make([]types.Type, len(slots))
	for i, slot := range slots {
		if slot >= len(hashBuild.Conditions) {
			return keycodec.ExactRuntimeFilterUnsupported, false
		}
		componentTypes[i], ok = planExprType(hashBuild.Conditions[slot])
		if !ok {
			return keycodec.ExactRuntimeFilterUnsupported, false
		}
	}
	payloadType, ok := planExprType(buildExpr)
	if !ok {
		return keycodec.ExactRuntimeFilterUnsupported, false
	}
	return runtimefilter.ExactKeyEncodingWithComponents(
		spec, payloadType, componentTypes, proc.GetService()), true
}

// materializedRuntimeFilterComponents resolves the tuple arguments against the
// actual unique-key vectors. It also proves that every referenced slot is
// present and row-aligned before expression evaluation.
func materializedRuntimeFilterComponents(
	spec *plan.RuntimeFilterSpec,
	keys []*vector.Vector,
) ([]types.Type, int, bool) {
	slots, ok := runtimeFilterComponentSlots(spec)
	if !ok {
		return nil, 0, false
	}
	componentTypes := make([]types.Type, len(slots))
	rowCount := -1
	for i, slot := range slots {
		if slot >= len(keys) || keys[slot] == nil {
			return nil, 0, false
		}
		componentTypes[i] = *keys[slot].GetType()
		if rowCount == -1 {
			rowCount = keys[slot].Length()
		} else if keys[slot].Length() != rowCount {
			return nil, 0, false
		}
	}
	return componentTypes, rowCount, rowCount >= 0
}

func (hashBuild *HashBuild) handleRuntimeFilter(
	proc *process.Process,
) (retErr error) {
	ctr := &hashBuild.ctr
	if hashBuild.IsShuffle {
		//only support runtime filter pass for now in shuffle join
		var runtimeFilter message.RuntimeFilterMessage
		runtimeFilter.Tag = hashBuild.RuntimeFilterSpec.Tag
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		hashBuild.sendRuntimeFilter(runtimeFilter, hashBuild.RuntimeFilterSpec, proc)
		return nil
	}

	if hashBuild.RuntimeFilterSpec == nil {
		return nil
	}

	var runtimeFilter message.RuntimeFilterMessage
	runtimeFilter.Tag = hashBuild.RuntimeFilterSpec.Tag

	spec := hashBuild.RuntimeFilterSpec
	// Unique keys are source state for an optional message, never transferred
	// with the message payload. Release them on every terminal path, including
	// malformed cached plans and contradictory empty/missing states.
	defer func() {
		if err := ctr.hashmapBuilder.releaseOptionalRuntimeFilterKeys(
			proc,
		); retErr == nil && err != nil {
			retErr = err
		}
		ctr.hashmapBuilder.uniqueKeySlots = nil
	}()

	// send the unique join keys (doc_id membership pushdown) when requested
	if spec.UseMembershipFilter {
		// currently only support single-column key for this runtime filter;
		// composite key still uses original IN / PASS logic
		if spec.Expr != nil && spec.Expr.GetF() != nil {
			runtimeFilter.Typ = message.RuntimeFilter_PASS
			hashBuild.sendRuntimeFilter(runtimeFilter, spec, proc)
			return nil
		}

		if ctr.hashmapBuilder.InputBatchRowCount == 0 {
			runtimeFilter.Typ = message.RuntimeFilter_DROP
			hashBuild.sendRuntimeFilter(runtimeFilter, spec, proc)
			return nil
		}

		if len(ctr.hashmapBuilder.UniqueJoinKeys) == 0 ||
			ctr.hashmapBuilder.UniqueJoinKeys[0] == nil {
			// A non-empty build with missing payload state is not evidence that
			// the membership set is empty. Fail open just like ordinary exact
			// runtime filters.
			runtimeFilter.Typ = message.RuntimeFilter_PASS
			hashBuild.sendRuntimeFilter(runtimeFilter, spec, proc)
			return nil
		}

		keyVec := ctr.hashmapBuilder.UniqueJoinKeys[0]
		if keyVec.Length() == 0 {
			runtimeFilter.Typ = message.RuntimeFilter_DROP
			hashBuild.sendRuntimeFilter(runtimeFilter, spec, proc)
			return nil
		}
		rowCount := keyVec.Length()

		// Always send the unique join keys; the consumer (ivfflat / fulltext
		// search) decides whether to use them as an exact pk IN filter or to
		// build a membership filter, based on its own threshold.
		runtimeFilter.Typ = message.RuntimeFilter_UNIQUEJOINKEYS

		data, release, err := ctr.hashmapBuilder.marshalRuntimeFilterVector(keyVec)
		if err != nil {
			if hashBuild.fallbackRuntimeFilterOnBudgetAdmission(err, &runtimeFilter, spec, proc) {
				return nil
			}
			return err
		}
		runtimeFilter.Card = int32(rowCount)
		runtimeFilter.Data = data
		runtimeFilter.SetMemoryRelease(release)
		hashBuild.sendRuntimeFilter(runtimeFilter, spec, proc)
		return nil
	}

	buildExpr := runtimefilter.BuildKeyExpr(spec)
	if buildExpr == nil {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		hashBuild.sendRuntimeFilter(runtimeFilter, spec, proc)
		return nil
	}

	declaredEncoding, declared := hashBuild.declaredRuntimeFilterEncoding(proc)
	if !declared || declaredEncoding == keycodec.ExactRuntimeFilterUnsupported {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		hashBuild.sendRuntimeFilter(runtimeFilter, spec, proc)
		return nil
	}
	if ctr.hashmapBuilder.InputBatchRowCount == 0 {
		runtimeFilter.Typ = message.RuntimeFilter_DROP
		hashBuild.sendRuntimeFilter(runtimeFilter, spec, proc)
		return nil
	}

	if buildExpr.GetF() != nil {
		return hashBuild.handleSerializedRuntimeFilter(
			proc, &runtimeFilter, spec)
	}

	keySlot := int(buildExpr.GetCol().ColPos)
	if keySlot >= len(ctr.hashmapBuilder.UniqueJoinKeys) ||
		ctr.hashmapBuilder.UniqueJoinKeys[keySlot] == nil {
		// Missing payload state cannot prove that the probe is empty. Runtime
		// filters are optional, so fail open instead of silently discarding rows.
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		hashBuild.sendRuntimeFilter(runtimeFilter, spec, proc)
		return nil
	}

	keyVec := ctr.hashmapBuilder.UniqueJoinKeys[keySlot]
	keyType := keyVec.GetType()
	encoding := runtimefilter.ExactKeyEncoding(spec, *keyType, proc.GetService())
	if encoding == keycodec.ExactRuntimeFilterUnsupported {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		hashBuild.sendRuntimeFilter(runtimeFilter, spec, proc)
		return nil
	}
	if keyVec.Length() == 0 {
		// A non-empty build may still have no joinable keys (for example, all
		// keys are NULL), but only a validated payload contract makes that empty
		// vector trustworthy evidence for DROP.
		runtimeFilter.Typ = message.RuntimeFilter_DROP
		hashBuild.sendRuntimeFilter(runtimeFilter, spec, proc)
		return nil
	}

	hashmapCount := ctr.hashmapBuilder.GetGroupCount()
	inFilterCardLimit := spec.UpperLimit
	if hashmapCount > uint64(inFilterCardLimit) {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		hashBuild.sendRuntimeFilter(runtimeFilter, spec, proc)
		return nil
	}

	if encoding == keycodec.ExactRuntimeFilterFloatZeroClosed {
		if err := runtimefilter.CloseFloatSignedZero(
			keyVec,
			proc.Mp(),
			func() (func(), error) {
				overlap, err := ctr.hashmapBuilder.reserveUniqueAppendOverlap(keyVec, 1, 0)
				if err != nil || overlap == nil {
					return nil, err
				}
				return func() {
					overlap.Release()
				}, nil
			},
		); err != nil {
			if hashBuild.fallbackRuntimeFilterOnBudgetAdmission(err, &runtimeFilter, spec, proc) {
				return nil
			}
			return err
		}
	}
	rowCount := keyVec.Length()
	if rowCount > int(inFilterCardLimit) {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		hashBuild.sendRuntimeFilter(runtimeFilter, spec, proc)
		return nil
	}
	keyVec.GetNulls().Reset()
	keyVec.InplaceSort()
	data, release, err := ctr.hashmapBuilder.marshalRuntimeFilterVector(keyVec)
	if err != nil {
		if hashBuild.fallbackRuntimeFilterOnBudgetAdmission(err, &runtimeFilter, spec, proc) {
			return nil
		}
		return err
	}

	runtimeFilter.Typ = message.RuntimeFilter_IN
	runtimeFilter.Card = int32(rowCount)
	runtimeFilter.Data = data
	runtimeFilter.SetMemoryRelease(release)
	hashBuild.sendRuntimeFilter(runtimeFilter, spec, proc)
	ctr.runtimeFilterIn = true
	return nil
}

func (hashBuild *HashBuild) handleSerializedRuntimeFilter(
	proc *process.Process,
	runtimeFilter *message.RuntimeFilterMessage,
	spec *plan.RuntimeFilterSpec,
) error {
	ctr := &hashBuild.ctr
	componentTypes, rowCount, ok := materializedRuntimeFilterComponents(
		spec, ctr.hashmapBuilder.UniqueJoinKeys)
	if !ok {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		hashBuild.sendRuntimeFilter(*runtimeFilter, spec, proc)
		return nil
	}
	declaredPayloadType, ok := planExprType(
		runtimefilter.BuildKeyExpr(spec))
	if !ok || runtimefilter.ExactKeyEncodingWithComponents(
		spec,
		declaredPayloadType,
		componentTypes,
		proc.GetService(),
	) != keycodec.ExactRuntimeFilterRaw {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		hashBuild.sendRuntimeFilter(*runtimeFilter, spec, proc)
		return nil
	}
	if rowCount == 0 {
		// The complete component triangle is valid, so an empty aligned
		// unique-key set is trustworthy evidence that no probe key can match.
		runtimeFilter.Typ = message.RuntimeFilter_DROP
		hashBuild.sendRuntimeFilter(*runtimeFilter, spec, proc)
		return nil
	}
	if ctr.hashmapBuilder.GetGroupCount() > uint64(spec.UpperLimit) ||
		rowCount > int(spec.UpperLimit) {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		hashBuild.sendRuntimeFilter(*runtimeFilter, spec, proc)
		return nil
	}

	data, release, outputRows, usable, err :=
		hashBuild.materializeSerializedRuntimeFilter(
			proc, spec, componentTypes, rowCount)
	if err != nil {
		if hashBuild.fallbackRuntimeFilterOnBudgetAdmission(
			err, runtimeFilter, spec, proc,
		) {
			return nil
		}
		return err
	}
	if !usable {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		hashBuild.sendRuntimeFilter(*runtimeFilter, spec, proc)
		return nil
	}
	if outputRows == 0 {
		runtimeFilter.Typ = message.RuntimeFilter_DROP
		hashBuild.sendRuntimeFilter(*runtimeFilter, spec, proc)
		return nil
	}
	if outputRows > int(spec.UpperLimit) {
		if release != nil {
			release()
		}
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		hashBuild.sendRuntimeFilter(*runtimeFilter, spec, proc)
		return nil
	}

	runtimeFilter.Typ = message.RuntimeFilter_IN
	runtimeFilter.Card = int32(outputRows)
	runtimeFilter.Data = data
	runtimeFilter.SetMemoryRelease(release)
	hashBuild.sendRuntimeFilter(*runtimeFilter, spec, proc)
	ctr.runtimeFilterIn = true
	return nil
}

// materializeSerializedRuntimeFilter evaluates one proven serial/serial_full
// contract under the same query-wide HashBuild budget as the map and unique
// component vectors. It reuses the production component encoders, but
// precomputes a tight output-area bound from the actual unique values. The
// generic expression estimator must not be used here: a serial result is typed
// VARCHAR(max), which would reserve 64 KiB per tiny integer tuple and turn a
// useful index filter into PASS.
func (hashBuild *HashBuild) materializeSerializedRuntimeFilter(
	proc *process.Process,
	spec *plan.RuntimeFilterSpec,
	componentTypes []types.Type,
	rowCount int,
) (
	data []byte,
	release func(),
	outputRows int,
	usable bool,
	err error,
) {
	keys := hashBuild.ctr.hashmapBuilder.UniqueJoinKeys
	slots, ok := runtimeFilterComponentSlots(spec)
	if !ok || len(slots) != len(componentTypes) {
		return nil, nil, 0, false, nil
	}
	full := spec.KeyEncoding ==
		plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_SERIAL_FULL_V1

	encoders := make([]planfunction.SerialValueEncoder, len(slots))
	for i, slot := range slots {
		encoders[i], err =
			planfunction.NewSerialValueEncoder(keys[slot])
		if err != nil {
			return nil, nil, 0, false, err
		}
	}

	areaBound, maxRowBound, err := serializedRuntimeFilterBounds(
		proc, keys, slots, rowCount, full)
	if err != nil {
		return nil, nil, 0, false, err
	}
	peak, err := serializedRuntimeFilterAllocationPeak(
		rowCount, areaBound, maxRowBound)
	if err != nil {
		return nil, nil, 0, false, err
	}

	var reservation *process.HashBuildReservation
	if budget := hashBuild.ctr.hashmapBuilder.budget; budget != nil {
		reservation, err = budget.Reserve(peak)
		if err != nil {
			return nil, nil, 0, false, err
		}
		defer reservation.Release()
	}

	payloadType, ok := planExprType(
		runtimefilter.BuildKeyExpr(spec))
	if !ok || areaBound > uint64(math.MaxInt) {
		return nil, nil, 0, false, nil
	}
	payload := vector.NewOffHeapVecWithType(payloadType)
	defer payload.Free(proc.Mp())
	if err = payload.PreExtendWithArea(
		rowCount, int(areaBound), proc.Mp(),
	); err != nil {
		return nil, nil, 0, false, err
	}

	packerSize := maxRowBound
	if packerSize == 0 {
		packerSize = 1
	}
	packer := types.NewPackerWithSize(packerSize)
	defer packer.Close()

	for row := 0; row < rowCount; row++ {
		if row&8191 == 0 {
			if err = checkHashBuildCanceled(proc); err != nil {
				return nil, nil, 0, false, err
			}
		}
		packer.Reset()
		rowIsNull := false
		for i, slot := range slots {
			component := keys[slot]
			if component.IsNull(uint64(row)) {
				if !full {
					rowIsNull = true
					break
				}
				packer.EncodeNull()
				continue
			}
			encoders[i](component, row, packer)
		}
		if rowIsNull {
			// serial is NULL if any component is NULL. NULL build keys never
			// match SQL equality, so omit them rather than turning a reset null
			// bitmap into an empty byte-string key.
			continue
		}
		if err = vector.AppendBytes(
			payload, packer.GetBuf(), false, proc.Mp(),
		); err != nil {
			return nil, nil, 0, false, err
		}
	}

	if runtimefilter.ExactKeyEncodingWithComponents(
		spec,
		*payload.GetType(),
		componentTypes,
		proc.GetService(),
	) != keycodec.ExactRuntimeFilterRaw {
		return nil, nil, 0, false, nil
	}
	usable = true
	outputRows = payload.Length()
	if outputRows == 0 {
		return nil, nil, 0, true, nil
	}
	payload.InplaceSort()
	data, release, err =
		hashBuild.ctr.hashmapBuilder.marshalRuntimeFilterVector(payload)
	if err != nil {
		if release != nil {
			release()
		}
		return nil, nil, 0, false, err
	}
	return data, release, outputRows, true, nil
}

func serializedRuntimeFilterBounds(
	proc *process.Process,
	keys []*vector.Vector,
	slots []int,
	rowCount int,
	full bool,
) (areaBytes uint64, maxRowBytes uint64, err error) {
	for row := 0; row < rowCount; row++ {
		if row&8191 == 0 {
			if err = checkHashBuildCanceled(proc); err != nil {
				return 0, 0, err
			}
		}
		var rowBytes uint64
		rowIsNull := false
		for _, slot := range slots {
			component := keys[slot]
			var valueBytes uint64
			if component.IsNull(uint64(row)) {
				if !full {
					rowIsNull = true
					break
				}
				valueBytes = 1
			} else {
				valueBytes, err =
					planfunction.SerialEncodedValueSizeBound(component, row)
				if err != nil {
					return 0, 0, err
				}
			}
			if rowBytes > math.MaxUint64-valueBytes {
				return 0, 0, process.ErrHashBuildBudgetInvalid
			}
			rowBytes += valueBytes
		}
		if rowIsNull {
			continue
		}
		if rowBytes > maxRowBytes {
			maxRowBytes = rowBytes
		}
		if rowBytes > types.VarlenaInlineSize {
			if areaBytes > math.MaxUint64-rowBytes {
				return 0, 0, process.ErrHashBuildBudgetInvalid
			}
			areaBytes += rowBytes
		}
	}
	return areaBytes, maxRowBytes, nil
}

func serializedRuntimeFilterAllocationPeak(
	rowCount int,
	areaBytes uint64,
	maxRowBytes uint64,
) (uint64, error) {
	if rowCount < 0 ||
		uint64(rowCount) > math.MaxUint64/types.VarlenaSize ||
		areaBytes > math.MaxInt64 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	packerRequest := maxRowBytes
	if packerRequest == 0 {
		packerRequest = 1
	}
	packerCapacity, ok := types.PackerAllocationSize(packerRequest)
	if !ok {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	dataBytes := uint64(rowCount) * types.VarlenaSize
	if dataBytes > math.MaxInt64 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	dataCapacity, ok := mpool.GrowCapacity(0, int64(dataBytes))
	if !ok || dataCapacity < 0 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	areaCapacity, ok := mpool.GrowCapacity(0, int64(areaBytes))
	if !ok || areaCapacity < 0 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	// The output vector is pre-extended, so it has no allocate-copy-free
	// growth overlap. Account the packer's actual size class rather than its
	// requested slice: rounding can approach another full request.
	peak := uint64(dataCapacity)
	for _, part := range []uint64{
		uint64(areaCapacity),
		packerCapacity,
		(uint64(rowCount) + 7) / 8,
	} {
		if peak > math.MaxUint64-part {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		peak += part
	}
	return peak, nil
}

// Runtime filters are optional probe-side optimizations. If serializing one
// cannot be admitted under the query/CN hash-build budget, PASS preserves
// correctness and avoids turning a successful hash build into a query error.
// Lifecycle and accounting errors remain fatal.
func (hashBuild *HashBuild) fallbackRuntimeFilterOnBudgetAdmission(
	err error,
	runtimeFilter *message.RuntimeFilterMessage,
	spec *plan.RuntimeFilterSpec,
	proc *process.Process,
) bool {
	var budgetErr *process.HashBuildBudgetError
	if !errors.As(err, &budgetErr) || budgetErr.Kind != process.HashBuildBudgetErrorAdmission {
		return false
	}

	if hashBuild.OpAnalyzer != nil {
		stats := hashBuild.OpAnalyzer.GetOpStats()
		stats.AddExtraStat("HashBuildRuntimeFilterBudgetFallbacks", 1)
		stats.SetMaxExtraStat("HashBuildRuntimeFilterBudgetFallbackRequestedBytes", hashBuildStatInt64(budgetErr.Requested))
		stats.SetMaxExtraStat("HashBuildRuntimeFilterBudgetFallbackUsedBytes", hashBuildStatInt64(budgetErr.Used))
		stats.SetMaxExtraStat("HashBuildRuntimeFilterBudgetFallbackCapBytes", hashBuildStatInt64(budgetErr.Cap))
	}
	*runtimeFilter = message.RuntimeFilterMessage{
		Tag: spec.Tag,
		Typ: message.RuntimeFilter_PASS,
	}
	hashBuild.sendRuntimeFilter(*runtimeFilter, spec, proc)
	return true
}

func (hashBuild *HashBuild) sendRuntimeFilter(rt message.RuntimeFilterMessage, spec *plan.RuntimeFilterSpec, proc *process.Process) {
	message.SendRuntimeFilter(rt, spec, proc.GetMessageBoard())
	if spec != nil {
		hashBuild.ctr.runtimeFilterDone = true
	}
}
