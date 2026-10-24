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
	"os"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
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
	hashBuild.ctr.spillAllowed = hashBuild.CanSpill && hashBuild.NeedHashMap && (hashBuild.IsShuffle || len(hashBuild.SpillExpectedScopes) > 0)
	if !hashBuild.IsShuffle && hashBuild.ctr.spillAllowed {
		group, err := proc.GetMessageBoard().BroadcastSpillGroup(hashBuild.JoinMapTag, hashBuild.SpillExpectedScopes)
		if err != nil {
			return err
		}
		hashBuild.SpillGroup, hashBuild.SpillEpoch = group, group.Epoch()
	}
	if hashBuild.OpAnalyzer == nil {
		hashBuild.OpAnalyzer = process.NewAnalyzer(hashBuild.GetIdx(), hashBuild.IsFirst, hashBuild.IsLast, "hash build")
	} else {
		hashBuild.OpAnalyzer.Reset()
	}

	hashBuild.ctr.setSpillThreshold(hashBuild.SpillThreshold)
	hashBuild.initMemoryBudget(proc)

	if !hashBuild.NeedHashMap {
		return nil
	}

	if hashBuild.IsShuffle && hashBuild.RuntimeFilterSpec == nil {
		return moerr.NewInternalError(proc.Ctx, "shuffle hash build must have runtime filter")
	}

	hashBuild.ctr.hashmapBuilder.IsDedup = hashBuild.IsDedup
	hashBuild.ctr.hashmapBuilder.DedupBuildKeepLast = hashBuild.DedupBuildKeepLast
	hashBuild.ctr.hashmapBuilder.OnDuplicateAction = hashBuild.OnDuplicateAction
	hashBuild.ctr.hashmapBuilder.DedupColName = hashBuild.DedupColName
	hashBuild.ctr.hashmapBuilder.DedupColTypes = hashBuild.DedupColTypes

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
				return result, err
			}

			ctr.state = HandleRuntimeFilter

		case HandleRuntimeFilter:
			if err := hashBuild.handleRuntimeFilter(proc); err != nil {
				return result, err
			}

			ctr.state = SendJoinMap

		case SendJoinMap:
			if hashBuild.JoinMapTag <= 0 {
				return result, moerr.NewInternalError(proc.Ctx, "wrong joinmap message tag!")
			}

			var jm *message.JoinMap
			spillMode := len(ctr.spilledFds) > 0

			if ctr.hashmapBuilder.InputBatchRowCount > 0 {
				if spillMode {
					// In spill mode: send empty JoinMap with spill fds, no batches
					jm = message.NewJoinMap(message.GroupSels{}, nil, nil, nil, nil, proc.Mp())
					jm.Spilled = true
					if hashBuild.SpillGroup != nil {
						files := ctr.spilledFds
						ctr.spilledFds = nil
						if err := hashBuild.SpillGroup.PublishBuild(files); err != nil {
							return result, err
						}
					} else {
						jm.SpillBuildFds = ctr.spilledFds
						ctr.spilledFds = nil
					}
				} else {
					// Normal mode: send hashmap and batches
					jm = ctr.hashmapBuilder.GetJoinMap(proc.Mp())
					reserved, throttler := ctr.memoryReserved, ctr.memoryThrottler
					ctr.memoryReserved = 0
					if throttler != nil && reserved > 0 {
						jm.SetMemoryRelease(func() { throttler.Release(reserved) })
					}
					jm.SetPushedRuntimeFilterIn(ctr.runtimeFilterIn)
				}
				jm.SetRowCount(int64(ctr.hashmapBuilder.InputBatchRowCount))
				if hashBuild.SpillGroup != nil {
					jm.IncRef(1)
					if !spillMode {
						if err := hashBuild.SpillGroup.PublishMemory(jm); err != nil {
							return result, err
						}
					}
				} else {
					jm.IncRef(hashBuild.JoinMapRefCnt)
				}
			}

			if jm == nil && hashBuild.SpillGroup != nil {
				if err := hashBuild.SpillGroup.PublishMemory(nil); err != nil {
					return result, err
				}
			}
			message.SendMessage(message.JoinMapMsg{
				JoinMapPtr: jm,
				IsShuffle:  hashBuild.IsShuffle,
				ShuffleIdx: hashBuild.ShuffleIdx,
				Tag:        hashBuild.JoinMapTag,
				Spilled:    spillMode,
			}, proc.GetMessageBoard())

			ctr.state = SendSucceed

		case SendSucceed:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (hashBuild *HashBuild) build(proc *process.Process, analyzer process.Analyzer) error {
	ctr := &hashBuild.ctr
	spillMode := false
	var spillFiles []*os.File
	var spillBuffers []*batch.Batch

	defer func() {
		for _, f := range spillFiles {
			if f != nil {
				f.Close()
			}
		}
		for _, buf := range spillBuffers {
			if buf != nil {
				buf.Clean(proc.Mp())
			}
		}
		ctr.freeSpillExprExecs()
	}()

	beginSpill := func(stage, reason string, ask int64) error {
		spillMode = true
		if _, err := ctr.initSpillExprExecs(proc, hashBuild.Conditions); err != nil {
			return err
		}
		uid, err := uuid.NewV7()
		if err != nil {
			return err
		}
		ctr.spillUUID = uid.String()
		available := int64(-1)
		if ctr.memoryThrottler != nil {
			available = ctr.memoryThrottler.Available()
		}
		logutil.Infof("entering spill mode, uid: %s, stage: %s, reason: %s, ask: %d, threshold: %d, input_rows: %d, retained: %d, reserved: %d, has_throttler: %t, available: %d",
			ctr.spillUUID, stage, reason, ask, ctr.spillThreshold, ctr.hashmapBuilder.InputBatchRowCount,
			ctr.hashmapBuilder.Batches.RetainedBytes(), ctr.memoryReserved, ctr.memoryThrottler != nil, available)
		spillFiles = make([]*os.File, spillNumBuckets)
		spillBuffers = ctr.acquireSpillBuffers(proc)
		for _, bat := range ctr.hashmapBuilder.Batches.Buf {
			if err := ctr.appendBuildBatchToSpillFiles(proc, bat, spillFiles, spillBuffers, ctr.spillExprExecs, analyzer); err != nil {
				return err
			}
		}
		ctr.hashmapBuilder.Batches.Clean(proc.Mp())
		ctr.releaseMemory(ctr.memoryReserved)
		return nil
	}
	// Ordinary broadcast builds have no consumer for copy forecasts or retained
	// allocation accounting. Shuffle/import spill budgets remain active even
	// when the execution has no import-memory view.
	accountBatches := ctr.spillAllowed && hashBuild.NeedHashMap
	// The operator threshold describes logical build data. Ordinary queries can
	// use it after copying because their actual allocation peak is still admitted
	// before copying. Imports and executions without that CN budget retain the
	// conservative capacity threshold.
	logicalThreshold := accountBatches && proc.Base.ImportMemory == nil && ctr.memoryThrottler != nil
	if accountBatches {
		ctr.hashmapBuilder.Batches.TrackAllocation()
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
		ctr.hashmapBuilder.InputBatchRowCount += result.Batch.RowCount()
		if !spillMode && accountBatches {
			growth := ctr.hashmapBuilder.Batches.AppendAllocation(result.Batch)
			if spill, reason := hashBuild.shouldSpillBeforeCopy(result.Batch, growth, !logicalThreshold); spill {
				if err := beginSpill("before-copy", reason, growth); err != nil {
					return err
				}
			}
		}
		if spillMode {
			if err := ctr.appendBuildBatchToSpillFiles(proc, result.Batch, spillFiles, spillBuffers, ctr.spillExprExecs, analyzer); err != nil {
				return err
			}
		} else {
			if err := ctr.hashmapBuilder.Batches.CopyIntoBatches(result.Batch, proc); err != nil {
				return err
			}
			ctr.reconcileBatches()
			if logicalThreshold && ctr.spillThreshold > 100000 && ctr.hashmapBuilder.CollectedBatchBytes() > ctr.spillThreshold {
				// beginSpill writes all collected batches, including this one.
				if err := beginSpill("after-copy", "logical-byte-threshold", 0); err != nil {
					return err
				}
			}
		}
	}
	needUniqueVec := !hashBuild.IsShuffle && hashBuild.RuntimeFilterSpec != nil && hashBuild.RuntimeFilterSpec.Expr != nil
	if !spillMode && ctr.spillAllowed && hashBuild.NeedHashMap && ctr.hashmapBuilder.InputBatchRowCount > 0 {
		bound, ok := ctr.hashmapBuilder.BuildMemoryBound(hashBuild.Conditions, hashBuild.NeedAllocateSels, needUniqueVec)
		// The import controller admits only key expressions with a capacity
		// contract. Ordinary joins retain their existing expression behavior;
		// an unknown import bound alone is not evidence of memory pressure.
		if !ok && proc.Base.ImportMemory != nil || ok && !ctr.acquireMemory(bound) {
			stage, reason := "build-hashmap", "reservation-denied"
			if !ok {
				stage, reason = "unsupported-controlled-bound", "unsupported-bound"
			}
			if err := beginSpill(stage, reason, bound); err != nil {
				return err
			}
		}
	}

	// Flush remaining buffered data
	if spillMode {
		for i, buf := range spillBuffers {
			if buf != nil && buf.RowCount() > 0 {
				file, err := ctr.ensureSpillFile(proc, spillFiles, i)
				if err != nil {
					return err
				}
				if _, err := ctr.flushBucketBuffer(proc, buf, file, analyzer); err != nil {
					return err
				}
			}
		}

		// Transfer ownership to container; nil local slice so defer won't close
		ctr.spilledFds = spillFiles
		spillFiles = nil
	}

	// If we never entered spill mode, build the hashmap
	if !spillMode && hashBuild.NeedHashMap {
		err := ctr.hashmapBuilder.BuildHashmap(hashBuild.HashOnPK, hashBuild.NeedAllocateSels, needUniqueVec, proc)
		if err != nil {
			return err
		}
	}

	if !hashBuild.NeedBatches {
		ctr.hashmapBuilder.Batches.Clean(proc.Mp())
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
	if len(ctr.spilledFds) > 0 || hashBuild.SpillGroup != nil && hashBuild.SpillGroup.IsSpilled() {
		if hashBuild.RuntimeFilterSpec != nil {
			message.SendRuntimeFilter(message.RuntimeFilterMessage{Tag: hashBuild.RuntimeFilterSpec.Tag, Typ: message.RuntimeFilter_PASS}, hashBuild.RuntimeFilterSpec, proc.GetMessageBoard())
		}
		return nil
	}
	if hashBuild.IsShuffle {
		//only support runtime filter pass for now in shuffle join
		var runtimeFilter message.RuntimeFilterMessage
		runtimeFilter.Tag = hashBuild.RuntimeFilterSpec.Tag
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		message.SendRuntimeFilter(runtimeFilter, hashBuild.RuntimeFilterSpec, proc.GetMessageBoard())
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
			message.SendRuntimeFilter(runtimeFilter, spec, proc.GetMessageBoard())
			return nil
		}

		// No data, directly DROP
		if ctr.hashmapBuilder.InputBatchRowCount == 0 ||
			len(ctr.hashmapBuilder.UniqueJoinKeys) == 0 ||
			ctr.hashmapBuilder.UniqueJoinKeys[0].Length() == 0 {
			runtimeFilter.Typ = message.RuntimeFilter_DROP
			message.SendRuntimeFilter(runtimeFilter, spec, proc.GetMessageBoard())
			return nil
		}

		keyVec := ctr.hashmapBuilder.UniqueJoinKeys[0]
		rowCount := keyVec.Length()

		// Always send the unique join keys; the consumer (ivfflat / fulltext
		// search) decides whether to use them as an exact pk IN filter or to
		// build a membership filter, based on its own threshold.
		runtimeFilter.Typ = message.RuntimeFilter_UNIQUEJOINKEYS

		data, err := keyVec.MarshalBinary()
		if err != nil {
			return err
		}
		runtimeFilter.Card = int32(rowCount)
		runtimeFilter.Data = data
		message.SendRuntimeFilter(runtimeFilter, spec, proc.GetMessageBoard())
		return nil
	}

	if spec.Expr == nil {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		message.SendRuntimeFilter(runtimeFilter, spec, proc.GetMessageBoard())
		return nil
	} else if ctr.hashmapBuilder.InputBatchRowCount == 0 || len(ctr.hashmapBuilder.UniqueJoinKeys) == 0 || ctr.hashmapBuilder.UniqueJoinKeys[0].Length() == 0 {
		runtimeFilter.Typ = message.RuntimeFilter_DROP
		message.SendRuntimeFilter(runtimeFilter, spec, proc.GetMessageBoard())
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
		message.SendRuntimeFilter(runtimeFilter, spec, proc.GetMessageBoard())
		return nil
	} else {
		rowCount := ctr.hashmapBuilder.UniqueJoinKeys[0].Length()

		var data []byte
		var err error
		// Composite primary key
		if spec.Expr.GetF() != nil {
			bat := batch.NewOffHeapWithSize(len(ctr.hashmapBuilder.UniqueJoinKeys))
			bat.SetRowCount(rowCount)
			copy(bat.Vecs, ctr.hashmapBuilder.UniqueJoinKeys)

			vec, free, erg := colexec.GetReadonlyResultFromExpression(proc, spec.Expr, []*batch.Batch{bat})
			if erg != nil {
				return erg
			}
			// InplaceSort reorders data but NOT the null bitmap.
			// NULLs are irrelevant for IN-filter: clear bitmap before sort.
			vec.GetNulls().Reset()
			vec.InplaceSort()
			data, err = vec.MarshalBinary()
			free()
		} else {
			ctr.hashmapBuilder.UniqueJoinKeys[0].GetNulls().Reset()
			ctr.hashmapBuilder.UniqueJoinKeys[0].InplaceSort()
			data, err = ctr.hashmapBuilder.UniqueJoinKeys[0].MarshalBinary()
		}

		if err != nil {
			return err
		}

		runtimeFilter.Typ = message.RuntimeFilter_IN
		runtimeFilter.Card = int32(rowCount)
		runtimeFilter.Data = data
		message.SendRuntimeFilter(runtimeFilter, spec, proc.GetMessageBoard())
		ctr.runtimeFilterIn = true
	}
	return nil
}
