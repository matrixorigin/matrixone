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
	"os"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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

			if ctr.hashmapBuilder.InputBatchRowCount > 0 {
				if spillMode {
					// In spill mode: send empty JoinMap with spill fds, no batches
					jm = message.NewJoinMap(message.GroupSels{}, nil, nil, nil, nil, proc.Mp())
					jm.Spilled = true
					if ctr.spillBundle != nil {
						jm.SetSpillBuildFiles(ctr.spillBundle.accountedFiles())
						jm.SetSpillBudget(ctr.hashmapBuilder.budget)
					} else {
						// Compatibility for tests and old callers that construct a
						// container with raw descriptors only.
						jm.SpillBuildFds = ctr.spilledFds
					}
					ctr.spilledFds = nil // ownership transferred
					ctr.spillBundle = nil
				} else {
					// Normal mode: send hashmap and batches
					jm = ctr.hashmapBuilder.GetJoinMap(proc.Mp())
					jm.SetPushedRuntimeFilterIn(ctr.runtimeFilterIn)
				}
				jm.SetRowCount(int64(ctr.hashmapBuilder.InputBatchRowCount))
				jm.SetHasNullKey(ctr.hashmapBuilder.HasNullKey)
				jm.IncRef(hashBuild.JoinMapRefCnt)
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
		ctr.dropSpillScratchBuffers()
		ctr.releaseSpillScratchReservation()
	}()

	startSpill := func() error {
		if spillMode {
			return nil
		}
		execs, err := ctr.initSpillExprExecs(proc, hashBuild.Conditions)
		if err != nil {
			return err
		}
		if spillFiles == nil {
			spillFiles = make([]*os.File, spillNumBuckets)
		}
		spillMode = true
		// Drain retained copies oldest-first.  Each successful partition is
		// followed immediately by reservation and mpool release, so the source
		// batch and one partition scratch are the only simultaneous peaks.
		for len(ctr.hashmapBuilder.Batches.Buf) > 0 {
			bat := ctr.hashmapBuilder.Batches.Buf[0]
			if bat == nil {
				if err := ctr.hashmapBuilder.CleanCopiedBatchAt(0, proc); err != nil {
					return err
				}
				continue
			}
			if err := ctr.spillBatchBounded(proc, bat, spillFiles, execs, analyzer); err != nil {
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
		result, err := vm.ChildrenCall(hashBuild.GetChildren(0), proc, analyzer)
		if err != nil {
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
			if err := ctr.ensureSpillScratchReservation(result.Batch); err != nil {
				// A larger ingress batch may require growing the emergency lease.
				// If retained copies are consuming the missing headroom, drain them
				// under the already-admitted lease, then retry for the current batch.
				if spillMode || !errors.Is(err, process.ErrHashBuildBudgetAdmission) || len(ctr.hashmapBuilder.Batches.Buf) == 0 {
					return err
				}
				if err := startSpill(); err != nil {
					return err
				}
				if err := ctr.ensureSpillScratchReservation(result.Batch); err != nil {
					return err
				}
			}
		}

		// If in spill mode, spill this batch directly to open files.
		if spillMode {
			err := ctr.spillBatchBounded(proc, result.Batch, spillFiles, ctr.spillExprExecs, analyzer)
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
				if err := ctr.spillBatchBounded(proc, result.Batch, spillFiles, ctr.spillExprExecs, analyzer); err != nil {
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
		needUniqueVec := true
		if hashBuild.IsShuffle || hashBuild.RuntimeFilterSpec == nil || hashBuild.RuntimeFilterSpec.Expr == nil {
			needUniqueVec = false
		}

		err := ctr.hashmapBuilder.BuildHashmap(hashBuild.HashOnPK, hashBuild.NeedAllocateSels, needUniqueVec, proc)
		if err != nil {
			if !hashBuild.IsShuffle || !errors.Is(err, process.ErrHashBuildBudgetAdmission) {
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
		if err := ctr.flushSpillBuffers(spillFiles, analyzer); err != nil {
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

func (hashBuild *HashBuild) handleRuntimeFilter(proc *process.Process) error {
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

	// send the unique join keys (doc_id membership pushdown) when requested
	if spec.UseMembershipFilter {
		// currently only support single-column key for this runtime filter;
		// composite key still uses original IN / PASS logic
		if spec.Expr != nil && spec.Expr.GetF() != nil {
			runtimeFilter.Typ = message.RuntimeFilter_PASS
			hashBuild.sendRuntimeFilter(runtimeFilter, spec, proc)
			return nil
		}

		// No data, directly DROP
		if ctr.hashmapBuilder.InputBatchRowCount == 0 ||
			len(ctr.hashmapBuilder.UniqueJoinKeys) == 0 ||
			ctr.hashmapBuilder.UniqueJoinKeys[0].Length() == 0 {
			runtimeFilter.Typ = message.RuntimeFilter_DROP
			hashBuild.sendRuntimeFilter(runtimeFilter, spec, proc)
			return nil
		}

		keyVec := ctr.hashmapBuilder.UniqueJoinKeys[0]
		rowCount := keyVec.Length()
		defer func() {
			for i := range ctr.hashmapBuilder.UniqueJoinKeys {
				ctr.hashmapBuilder.UniqueJoinKeys[i].Free(proc.Mp())
			}
			ctr.hashmapBuilder.UniqueJoinKeys = nil
		}()

		// Always send the unique join keys; the consumer (ivfflat / fulltext
		// search) decides whether to use them as an exact pk IN filter or to
		// build a membership filter, based on its own threshold.
		runtimeFilter.Typ = message.RuntimeFilter_UNIQUEJOINKEYS

		data, release, err := ctr.hashmapBuilder.marshalRuntimeFilterVector(keyVec)
		if err != nil {
			return err
		}
		runtimeFilter.Card = int32(rowCount)
		runtimeFilter.Data = data
		runtimeFilter.SetMemoryRelease(release)
		hashBuild.sendRuntimeFilter(runtimeFilter, spec, proc)
		return nil
	}

	if spec.Expr == nil {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		hashBuild.sendRuntimeFilter(runtimeFilter, spec, proc)
		return nil
	} else if ctr.hashmapBuilder.InputBatchRowCount == 0 || len(ctr.hashmapBuilder.UniqueJoinKeys) == 0 || ctr.hashmapBuilder.UniqueJoinKeys[0].Length() == 0 {
		runtimeFilter.Typ = message.RuntimeFilter_DROP
		hashBuild.sendRuntimeFilter(runtimeFilter, spec, proc)
		return nil
	}

	hashmapCount := ctr.hashmapBuilder.GetGroupCount()
	inFilterCardLimit := spec.UpperLimit

	defer func() {
		for i := range ctr.hashmapBuilder.UniqueJoinKeys {
			ctr.hashmapBuilder.UniqueJoinKeys[i].Free(proc.Mp())
		}
		ctr.hashmapBuilder.UniqueJoinKeys = nil
	}()

	if hashmapCount > uint64(inFilterCardLimit) {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		hashBuild.sendRuntimeFilter(runtimeFilter, spec, proc)
		return nil
	} else {
		if spec.Expr.GetF() != nil {
			// Composite runtime-filter expression evaluation has no sound peak
			// estimator yet. PASS preserves query correctness without allocating
			// unaccounted expression intermediates.
			runtimeFilter.Typ = message.RuntimeFilter_PASS
			hashBuild.sendRuntimeFilter(runtimeFilter, spec, proc)
			return nil
		}
		rowCount := ctr.hashmapBuilder.UniqueJoinKeys[0].Length()

		ctr.hashmapBuilder.UniqueJoinKeys[0].GetNulls().Reset()
		ctr.hashmapBuilder.UniqueJoinKeys[0].InplaceSort()
		data, release, err := ctr.hashmapBuilder.marshalRuntimeFilterVector(ctr.hashmapBuilder.UniqueJoinKeys[0])

		if err != nil {
			return err
		}

		runtimeFilter.Typ = message.RuntimeFilter_IN
		runtimeFilter.Card = int32(rowCount)
		runtimeFilter.Data = data
		runtimeFilter.SetMemoryRelease(release)
		hashBuild.sendRuntimeFilter(runtimeFilter, spec, proc)
		ctr.runtimeFilterIn = true
	}
	return nil
}

func (hashBuild *HashBuild) sendRuntimeFilter(rt message.RuntimeFilterMessage, spec *plan.RuntimeFilterSpec, proc *process.Process) {
	message.SendRuntimeFilter(rt, spec, proc.GetMessageBoard())
	if spec != nil {
		hashBuild.ctr.runtimeFilterDone = true
	}
}
