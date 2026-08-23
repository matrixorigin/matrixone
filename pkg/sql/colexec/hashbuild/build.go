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
	"slices"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
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

	budget, err := proc.GetExecutionResourceBudget()
	if err != nil {
		return TerminalBudgetError(proc.Ctx, err)
	}
	hashBuild.ctr.hashmapBuilder.setBudget(budget)
	if hashBuild.ctr.hashmapBuilder.mapAllocationAccount == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if hashBuild.IsShuffle && hashBuild.RuntimeFilterSpec == nil {
		return moerr.NewInternalError(proc.Ctx, "shuffle hash build must have runtime filter")
	}
	if !hashBuild.NeedHashMap {
		return nil
	}
	if hashBuild.IsShuffle {
		if err = hashBuild.installRecoveryCapacity(budget); err != nil {
			return TerminalBudgetError(proc.Ctx, err)
		}
	}

	hashBuild.ctr.hashmapBuilder.IsDedup = hashBuild.IsDedup
	hashBuild.ctr.hashmapBuilder.DedupBuildKeepLast = hashBuild.DedupBuildKeepLast
	hashBuild.ctr.hashmapBuilder.OnDuplicateAction = hashBuild.OnDuplicateAction
	hashBuild.ctr.hashmapBuilder.DedupColName = hashBuild.DedupColName
	hashBuild.ctr.hashmapBuilder.DedupColTypes = hashBuild.DedupColTypes
	hashBuild.ctr.hashmapBuilder.TrackNullKeys = hashBuild.TrackNullKeys

	err = hashBuild.ctr.hashmapBuilder.Prepare(
		hashBuild.Conditions,
		hashBuild.DelColIdx,
		hashBuild.DedupDeleteMarkerColIdx,
		hashBuild.DedupDeleteKeepColIdxList,
		proc,
	)
	return TerminalBudgetError(proc.Ctx, err)
}

func (hashBuild *HashBuild) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := hashBuild.OpAnalyzer
	result := vm.NewCallResult()
	ctr := &hashBuild.ctr
	for {
		switch ctr.state {
		case BuildHashMap:
			if err := hashBuild.build(proc, analyzer); err != nil {
				err = TerminalBudgetError(proc.Ctx, err)
				hashBuild.finalizeBuildFailure(proc, err)
				return result, err
			}

			ctr.state = HandleRuntimeFilter

		case HandleRuntimeFilter:
			if err := hashBuild.handleRuntimeFilter(proc); err != nil {
				err = TerminalBudgetError(proc.Ctx, err)
				hashBuild.finalizeBuildFailure(proc, err)
				return result, err
			}

			ctr.state = SendJoinMap

		case SendJoinMap:
			if err := hashBuild.sendJoinMap(proc); err != nil {
				hashBuild.finalizeBuildFailure(proc, err)
				return result, err
			}
			ctr.state = SendSucceed

		case SendSucceed:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

// sendJoinMap serializes terminal publication with Reset and Free. The defer
// is part of the lifecycle contract: allocation, spill-payload, and message
// hooks may panic, and cleanup must never deadlock trying to reacquire this
// mutex while recovering the active statement.
func (hashBuild *HashBuild) sendJoinMap(proc *process.Process) error {
	ctr := &hashBuild.ctr
	ctr.terminalMu.Lock()
	defer ctr.terminalMu.Unlock()

	if hashBuild.JoinMapTag <= 0 {
		return moerr.NewInternalError(proc.Ctx, "wrong joinmap message tag!")
	}
	if hashBuild.JoinMapRefCnt <= 0 {
		return moerr.NewInternalErrorf(
			proc.Ctx,
			"invalid join map reference count: %d",
			hashBuild.JoinMapRefCnt,
		)
	}
	if atomic.LoadUint32(&ctr.terminalPublished) != 0 {
		return moerr.NewQueryInterrupted(proc.Ctx)
	}

	var jm *message.JoinMap
	joinMapOwned := false
	defer func() {
		if joinMapOwned && jm != nil {
			jm.FreeMemory()
		}
	}()
	spillMode := len(ctr.spilledFds) > 0
	if ctr.hashmapBuilder.InputBatchRowCount > 0 {
		if spillMode {
			jm = message.NewJoinMap(
				message.GroupSels{}, nil, nil, nil, nil, proc.Mp(),
			)
		} else {
			jm = ctr.hashmapBuilder.GetJoinMap(proc.Mp())
			if jm == nil {
				return process.ErrExecutionResourceInvalid
			}
			joinMapOwned = true
			jm.SetPushedRuntimeFilterIn(ctr.runtimeFilterIn)
		}
		if spillMode {
			joinMapOwned = true
		}
		jm.SetRowCount(int64(ctr.hashmapBuilder.InputBatchRowCount))
		jm.SetHasNullKey(ctr.hashmapBuilder.HasNullKey)
		jm.IncRef(hashBuild.JoinMapRefCnt)
		if spillMode {
			if ctr.spillBundle == nil || ctr.hashmapBuilder.budget == nil {
				return process.ErrExecutionResourceInvalid
			}
			payload := message.SpillBuildPayload{
				Files:     ctr.spillBundle.accountedFiles(),
				BudgetRef: ctr.hashmapBuilder.budget,
			}
			if err := jm.SetSpillBuildPayload(payload); err != nil {
				return moerr.NewInternalError(proc.Ctx, err.Error())
			}
			ctr.spilledFds = nil
			ctr.spillBundle = nil
		}
	}

	if !hashBuild.publishJoinMap(proc, jm) {
		return moerr.NewQueryInterrupted(proc.Ctx)
	}
	joinMapOwned = false
	return nil
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

func (hashBuild *HashBuild) build(
	proc *process.Process,
	analyzer process.Analyzer,
) (retErr error) {
	ctr := &hashBuild.ctr
	ctr.spillConditions = hashBuild.Conditions
	spillMode := false
	var spillFiles []*os.File
	bundleTransferred := false

	ensureRecovery := func(projection recoveryBatchProjection) error {
		if !hashBuild.IsShuffle {
			return nil
		}
		return ctr.ensureRecoveryCapacity(
			proc,
			hashBuild.Conditions,
			projection,
			analyzer,
		)
	}

	defer func() {
		observeExecutionResourceBudget(analyzer, ctr.hashmapBuilder.budget)
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
		ctr.spillConditions = nil
		// Build-key executors are producer scratch. No consumer reads them after
		// build() returns, so release their retained vectors here instead of
		// holding them until pipeline Reset.
		ctr.hashmapBuilder.FreeTemporaryVectors(proc)
		ctr.hashmapBuilder.FreeExecutors()
		ctr.dropSpillScratchBuffers()
		if err := hashBuild.releaseRecoveryCapacity(
			ctr.hashmapBuilder.mapAllocationAccount,
			true,
		); err != nil {
			retErr = errors.Join(retErr, err)
		}
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
		if spillFiles == nil {
			spillFiles = make([]*os.File, spillNumBuckets)
		}
		spillMode = true
		analyzer.GetOpStats().AddExtraStat("HashBuildSpillStarts", 1)
		// Drain retained copies oldest-first.  Each successful partition is
		// followed immediately by reservation and mpool release, so the source
		// batch and one partition scratch are the only simultaneous peaks.
		execs := ctr.spillExprExecs
		if len(ctr.hashmapBuilder.Batches.Buf) > 0 {
			var err error
			execs, err = ctr.initSpillExprExecs(proc, hashBuild.Conditions)
			if err != nil {
				return err
			}
		}
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
			if err := ctr.spillBatchWithPressure(proc, bat, spillFiles, execs, analyzer, true); err != nil {
				return err
			}
			if err := ctr.hashmapBuilder.CleanCopiedBatchAt(0, proc); err != nil {
				return err
			}
		}
		// No retained state remains after the drain. Drop every mandatory
		// recovery-class borrower before returning the conservative floor, then
		// let direct sources use ordinary allocation-led admission.
		ctr.dropMandatorySpillRecoveryScratch()
		if err := hashBuild.releaseRecoveryCapacity(
			ctr.hashmapBuilder.mapAllocationAccount,
			true,
		); err != nil {
			return err
		}
		v2.HashBuildSpillDepthCounter.WithLabelValues("spill", "1").Inc()
		return nil
	}
	spillDirect := func(bat *batch.Batch) error {
		if err := startSpill(); err != nil {
			return err
		}
		// Recovery headroom proves that already-retained batches can be drained;
		// an upstream-owned direct source cannot strand retained state. Admit its
		// scratch at the physical allocation sites instead of applying the
		// conservative retained-batch projection as a query-fatal gate.
		return ctr.spillBatchWithPressure(
			proc, bat, spillFiles, ctr.spillExprExecs, analyzer, false)
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

		inputBatchSize := int64(result.Batch.Size())
		analyzer.Alloc(inputBatchSize)
		// Durable row accounting is advanced exactly once on ingress.  In
		// particular, a rejected retained-copy admission below must not add the
		// same upstream batch a second time when it is spilled directly.
		ctr.hashmapBuilder.InputBatchRowCount += result.Batch.RowCount()
		// If in spill mode, spill this batch directly to open files.
		if spillMode {
			if err := spillDirect(result.Batch); err != nil {
				return err
			}
			continue
		}
		// Decide on the same threshold before retaining the crossing batch. The
		// input size was already computed for analyzer accounting, so this keeps
		// speculative spill sizing and reservation off the resident hot path while
		// preserving enough budget headroom to drain the batches already retained.
		if hashBuild.shouldSpillBeforeRetain(inputBatchSize) {
			if err := spillDirect(result.Batch); err != nil {
				return err
			}
			continue
		}

		// Store original batch
		retainedMemBefore := ctr.hashmapBuilder.Batches.MemSize
		var projection recoveryBatchProjection
		if hashBuild.IsShuffle {
			projection, err = ctr.hashmapBuilder.projectRetainedRecovery(result.Batch)
			if err == nil {
				err = ensureRecovery(projection)
			}
		}
		if err == nil {
			err = ctr.hashmapBuilder.copyBuildBatch(result.Batch, proc)
		}
		if err != nil {
			if hashBuild.IsShuffle && IsRetryableMemoryCapacity(err) {
				// The source batch is still owned by the upstream operator.  Do
				// not retry CopyIntoBatches (or increment row count again); enter
				// spill recovery and write this batch directly.
				if err := spillDirect(result.Batch); err != nil {
					return err
				}
				continue
			}
			return err
		}
		if hashBuild.IsShuffle {
			ctr.hashmapBuilder.retainedSpillTailSelected =
				projection.nextTailSelected
		}

		// Representation expansion (including const sources) or completion of a
		// partial retained batch can increase MemSize by more than the source's
		// logical Size. Keep a cold post-copy fallback for that exceptional
		// under-prediction; ordinary full non-const batches pay only the pre-copy
		// threshold check above.
		if ctr.hashmapBuilder.Batches.MemSize-retainedMemBefore > inputBatchSize &&
			hashBuild.shouldSpillBatches() {
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
			runtimefilter.ClassifyOptionalFallback(err) ==
				runtimefilter.OptionalFallbackBudgetAdmission {
			// Unique-key retention exists only for the optional runtime filter.
			// A mandatory map allocation can still lose admission while the
			// optional owner is live. Rebuild only while HashmapBuilder proves
			// that no destructive Dedup batch rewrite has started.
			ctr.hashmapBuilder.FreeHashMapOnly(proc)
			err = ctr.hashmapBuilder.BuildHashmap(
				hashBuild.HashOnPK,
				hashBuild.NeedAllocateSels,
				false,
				proc,
			)
			// Count a collection fallback only after the mandatory rebuild
			// succeeds. A failed retry is a fatal build, not a downgrade.
			collectionFallback = err == nil
			rebuildSafe = ctr.hashmapBuilder.RetainedBatchRecoverySafe()
		}
		if collectionFallback && analyzer != nil {
			analyzer.GetOpStats().AddExtraStat(
				"HashBuildRuntimeFilterCollectionFallbacks", 1)
		}
		if err != nil {
			if !hashBuild.IsShuffle || !IsRetryableMemoryCapacity(err) {
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
			// Preserve the copied batches and discard only partial map state.
			// Scratch is admitted lazily while draining; failure remains a
			// controlled resource error rather than an allocation past the cap.
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

func observeExecutionResourceBudget(analyzer process.Analyzer, budget *process.ExecutionResourceGeneration) {
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

func serializedRuntimeFilterSourceCol(expr *plan.Expr) *plan.ColRef {
	if expr == nil {
		return nil
	}
	if col := expr.GetCol(); col != nil {
		return col
	}
	fn := expr.GetF()
	if fn == nil || len(fn.Args) == 0 || fn.Args[0] == nil {
		return nil
	}
	return fn.Args[0].GetCol()
}

func serializedRuntimeFilterEvalArg(expr *plan.Expr) *plan.Expr {
	col := serializedRuntimeFilterSourceCol(expr)
	if expr == nil || col == nil {
		return nil
	}
	source := &plan.Expr{
		Typ: colExprType(expr),
		Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: 0,
			ColPos: col.ColPos,
			Name:   col.Name,
		}},
	}
	if expr.GetCol() != nil {
		source.Typ = expr.Typ
		return source
	}
	fn := expr.GetF()
	return &plan.Expr{
		Typ: expr.Typ,
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: fn.Func,
			Args: []*plan.Expr{source, fn.Args[1]},
		}},
	}
}

func colExprType(expr *plan.Expr) plan.Type {
	if expr.GetCol() != nil {
		return expr.Typ
	}
	return expr.GetF().Args[0].Typ
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
		slot, _, ok := runtimefilter.TupleComponentSlot(arg)
		if !ok {
			return nil, false
		}
		slots[i] = slot
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
		return runtimefilter.ExactKeyEncoding(spec, payloadType), true
	}

	slots, ok := runtimeFilterComponentSlots(spec)
	if !ok {
		return keycodec.ExactRuntimeFilterUnsupported, false
	}
	componentTypes := make([]types.Type, len(slots))
	args := buildExpr.GetF().Args
	for i, slot := range slots {
		if slot >= len(hashBuild.Conditions) {
			return keycodec.ExactRuntimeFilterUnsupported, false
		}
		conditionType, valid := planExprType(hashBuild.Conditions[slot])
		_, sourceType, validSlot := runtimefilter.TupleComponentSlot(args[i])
		if !valid || !validSlot || conditionType != sourceType {
			return keycodec.ExactRuntimeFilterUnsupported, false
		}
		componentTypes[i], ok = planExprType(args[i])
		if !ok {
			return keycodec.ExactRuntimeFilterUnsupported, false
		}
	}
	payloadType, ok := planExprType(buildExpr)
	if !ok {
		return keycodec.ExactRuntimeFilterUnsupported, false
	}
	return runtimefilter.ExactKeyEncodingWithComponents(
		spec, payloadType, componentTypes), true
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
	buildExpr := runtimefilter.BuildKeyExpr(spec)
	args := buildExpr.GetF().Args
	rowCount := -1
	for i, slot := range slots {
		if slot >= len(keys) || keys[slot] == nil {
			return nil, 0, false
		}
		_, sourceType, validSlot := runtimefilter.TupleComponentSlot(args[i])
		if !validSlot || *keys[slot].GetType() != sourceType {
			return nil, 0, false
		}
		var valid bool
		componentTypes[i], valid = planExprType(args[i])
		if !valid {
			return nil, 0, false
		}
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
	// A spilled build has no resident unique-key vector. Treating that absence
	// as an empty build would publish DROP and incorrectly discard every probe
	// row, so spill always disables this optional optimization.
	if len(ctr.spilledFds) > 0 {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		hashBuild.sendRuntimeFilter(runtimeFilter, spec, proc)
		return nil
	}

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
		if keyVec.GetGrouping().GetBitmap().CountRange(
			0, uint64(keyVec.Length()),
		) > 0 {
			runtimeFilter.Typ = message.RuntimeFilter_PASS
			hashBuild.sendRuntimeFilter(runtimeFilter, spec, proc)
			return nil
		}

		// Always send the unique join keys; the consumer (ivfflat / fulltext
		// search) decides whether to use them as an exact pk IN filter or to
		// build a membership filter, based on its own threshold.
		runtimeFilter.Typ = message.RuntimeFilter_UNIQUEJOINKEYS

		data, release, err := ctr.hashmapBuilder.marshalRuntimeFilterVector(keyVec, proc.Mp())
		if err != nil {
			if hashBuild.fallbackOptionalRuntimeFilter(err, &runtimeFilter, spec, proc) {
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
	encoding := runtimefilter.ExactKeyEncoding(spec, *keyType)
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
		); err != nil {
			if hashBuild.fallbackOptionalRuntimeFilter(err, &runtimeFilter, spec, proc) {
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
	data, release, err := ctr.hashmapBuilder.marshalRuntimeFilterVector(
		keyVec,
		proc.Mp(),
	)
	if err != nil {
		if hashBuild.fallbackOptionalRuntimeFilter(err, &runtimeFilter, spec, proc) {
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
		if moerr.IsMoErrCode(err, moerr.ErrOutOfRange) {
			// A comparison key outside the narrower probe domain cannot match any
			// probe row. The optional tuple encoder may reject that narrowing;
			// publish PASS rather than changing the join's error semantics.
			runtimeFilter.Typ = message.RuntimeFilter_PASS
			hashBuild.sendRuntimeFilter(*runtimeFilter, spec, proc)
			return nil
		}
		if hashBuild.fallbackOptionalRuntimeFilter(
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
// contract under the same physical allocation account as the map and unique
// component vectors. It reuses the production component encoders and
// precomputes a tight output-area bound from the actual unique values. The
// account observes the actual vector growth rather than an estimated duplicate
// reservation.
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

	components := keys
	componentSlots := append([]int(nil), slots...)
	buildArgs := runtimefilter.BuildKeyExpr(spec).GetF().Args
	var executors []colexec.ExpressionExecutor
	if slices.ContainsFunc(buildArgs, func(arg *plan.Expr) bool { return arg.GetF() != nil }) {
		evalArgs := make([]*plan.Expr, len(buildArgs))
		for i, arg := range buildArgs {
			evalArgs[i] = serializedRuntimeFilterEvalArg(arg)
			if evalArgs[i] == nil {
				return nil, nil, 0, false, nil
			}
		}
		executors, err = NewExpressionExecutors(
			proc, evalArgs, hashBuild.ctr.hashmapBuilder.mapAllocationAccount)
		if err != nil {
			return nil, nil, 0, false, err
		}
		defer func() {
			for _, executor := range executors {
				executor.Free()
			}
		}()
		keyBatch := batch.NewWithSize(len(keys))
		keyBatch.Vecs = keys
		keyBatch.SetRowCount(rowCount)
		components = make([]*vector.Vector, len(executors))
		componentSlots = make([]int, len(executors))
		for i, executor := range executors {
			components[i], err = executor.Eval(proc, []*batch.Batch{keyBatch}, nil)
			if err != nil {
				return nil, nil, 0, false, err
			}
			componentSlots[i] = i
		}
	}

	encoders := make([]planfunction.SerialValueEncoder, len(componentSlots))
	for i, slot := range componentSlots {
		encoders[i], err = planfunction.NewSerialValueEncoder(components[slot])
		if err != nil {
			return nil, nil, 0, false, err
		}
	}

	areaBound, maxRowBound, err := serializedRuntimeFilterBounds(
		proc, components, componentSlots, rowCount, full)
	if err != nil {
		return nil, nil, 0, false, err
	}

	payloadType, ok := planExprType(
		runtimefilter.BuildKeyExpr(spec))
	if !ok || areaBound > uint64(math.MaxInt) ||
		maxRowBound > uint64(math.MaxInt) ||
		hashBuild.ctr.hashmapBuilder.uniqueKeyAllocation == nil {
		return nil, nil, 0, false, nil
	}
	payload, err := vector.NewOffHeapVecWithTypeAndAllocation(
		payloadType,
		hashBuild.ctr.hashmapBuilder.uniqueKeyAllocation,
	)
	if err != nil {
		return nil, nil, 0, false, err
	}
	defer payload.Free(proc.Mp())
	if err = payload.PreExtendWithArea(
		rowCount, int(areaBound), proc.Mp(),
	); err != nil {
		return nil, nil, 0, false,
			runtimefilter.MarkOptionalAllocationError(err)
	}

	packerSize := maxRowBound
	if packerSize == 0 {
		packerSize = 1
	}
	scratch, err := mpool.NewAccountedBuffer(
		proc.Mp(),
		hashBuild.ctr.hashmapBuilder.mapAllocationAccount,
		mpool.AllocationOwnerHashBuild,
		HashBuildAllocationSiteRuntimeFilterScratch,
	)
	if err != nil {
		return nil, nil, 0, false, err
	}
	defer scratch.Free()
	if err = scratch.Resize(int(packerSize)); err != nil {
		if mpool.IsRetryableAllocationCapacity(err) {
			err = runtimefilter.MarkOptionalAllocationError(err)
		}
		return nil, nil, 0, false, err
	}
	packer := types.NewPackerWithFixedBuffer(scratch.Bytes())

	for row := 0; row < rowCount; row++ {
		if row&8191 == 0 {
			if err = checkHashBuildCanceled(proc); err != nil {
				return nil, nil, 0, false, err
			}
		}
		packer.Reset()
		rowIsNull := false
		for i, slot := range componentSlots {
			component := components[slot]
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
		if err = packer.Err(); err != nil {
			return nil, nil, 0, false, errors.Join(
				mpool.ErrAllocationAccountInvariant,
				err,
			)
		}
		if rowIsNull {
			// serial is NULL if any component is NULL. NULL build keys never
			// match SQL equality, so omit them rather than turning a reset null
			// bitmap into an empty byte-string key.
			continue
		}
		// Bounds plus PreExtendWithArea reserved the complete data and area
		// capacities. An append error contradicts that oracle and must remain
		// an unmarked fatal invariant error.
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
		hashBuild.ctr.hashmapBuilder.marshalRuntimeFilterVector(
			payload,
			proc.Mp(),
		)
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
				return 0, 0, process.ErrExecutionResourceInvalid
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
				return 0, 0, process.ErrExecutionResourceInvalid
			}
			areaBytes += rowBytes
		}
	}
	return areaBytes, maxRowBytes, nil
}

// Runtime filters are optional probe-side optimizations. Fail open only for a
// query/CN admission rejection or an allocation error marked at an exact
// optional payload/vector boundary. Cancellation, contract violations, and
// budget lifecycle/accounting errors remain fatal.
func (hashBuild *HashBuild) fallbackOptionalRuntimeFilter(
	err error,
	runtimeFilter *message.RuntimeFilterMessage,
	spec *plan.RuntimeFilterSpec,
	proc *process.Process,
) bool {
	kind := runtimefilter.ClassifyOptionalFallback(err)
	if kind == runtimefilter.OptionalFallbackNone {
		return false
	}

	if hashBuild.OpAnalyzer != nil {
		stats := hashBuild.OpAnalyzer.GetOpStats()
		if kind == runtimefilter.OptionalFallbackBudgetAdmission {
			var budgetErr *process.ExecutionResourceError
			if !errors.As(err, &budgetErr) {
				return false
			}
			stats.AddExtraStat("HashBuildRuntimeFilterBudgetFallbacks", 1)
			stats.SetMaxExtraStat("HashBuildRuntimeFilterBudgetFallbackRequestedBytes", hashBuildStatInt64(budgetErr.Requested))
			stats.SetMaxExtraStat("HashBuildRuntimeFilterBudgetFallbackUsedBytes", hashBuildStatInt64(budgetErr.Used))
			stats.SetMaxExtraStat("HashBuildRuntimeFilterBudgetFallbackCapBytes", hashBuildStatInt64(budgetErr.Cap))
		} else {
			stats.AddExtraStat(
				"HashBuildRuntimeFilterAllocationFallbacks", 1)
			if account := hashBuild.ctr.hashmapBuilder.mapAllocationAccount; account != nil {
				snapshot := account.Snapshot()
				stats.SetMaxExtraStat(
					"HashBuildRuntimeFilterBudgetFallbackUsedBytes",
					hashBuildStatInt64(snapshot.Used),
				)
				stats.SetMaxExtraStat(
					"HashBuildRuntimeFilterBudgetFallbackCapBytes",
					hashBuildStatInt64(snapshot.Limit),
				)
			}
		}
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
