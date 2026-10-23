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

package hashjoin

import (
	"bytes"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "hash_join"

func (hashJoin *HashJoin) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	switch hashJoin.JoinType {
	case plan.Node_INNER:
		buf.WriteString(": inner join ")
	case plan.Node_LEFT:
		buf.WriteString(": left join ")
	case plan.Node_RIGHT:
		buf.WriteString(": right join ")
	case plan.Node_SEMI:
		if hashJoin.IsRightJoin {
			buf.WriteString(": right semi join ")
		} else {
			buf.WriteString(": semi join ")
		}
	case plan.Node_ANTI:
		if hashJoin.IsRightJoin {
			buf.WriteString(": right anti join ")
		} else {
			buf.WriteString(": anti join ")
		}
	case plan.Node_SINGLE:
		buf.WriteString(": single join ")
	case plan.Node_OUTER:
		buf.WriteString(": full outer join ")
	}
}

func (hashJoin *HashJoin) OpType() vm.OpType {
	return vm.HashJoin
}

func (hashJoin *HashJoin) Prepare(proc *process.Process) (err error) {
	if err := hashJoin.ArmSpillParticipant(proc); err != nil {
		return err
	}
	if hashJoin.OpAnalyzer == nil {
		hashJoin.OpAnalyzer = process.NewAnalyzer(hashJoin.GetIdx(), hashJoin.IsFirst, hashJoin.IsLast, opName)
	} else {
		hashJoin.OpAnalyzer.Reset()
	}

	ctr := &hashJoin.ctr
	// Only a complete execution import contract consumes an output byte bound.
	// Generic shuffle spill still has independent build and file IO budgets.
	ctr.outputTarget, ctr.outputRowWidth = 0, 0
	if proc.Base.ImportMemory != nil {
		if proc.Base.ImportMemory.UnitBytes <= 0 {
			return moerr.NewInternalErrorf(proc.Ctx, "invalid join output memory unit %d", proc.Base.ImportMemory.UnitBytes)
		}
		ctr.outputTarget = proc.Base.ImportMemory.UnitBytes
		var fixed bool
		ctr.outputRowWidth, fixed = hashJoin.fixedOutputRowWidth()
		if !fixed {
			ctr.outputRowWidth = 0
		}
		for i := range ctr.joinInputLeases {
			if ctr.joinInputLeases[i] == nil {
				ctr.joinInputLeases[i], err = proc.Base.ImportMemory.AcquireJoinInput(proc.Ctx)
				if err != nil {
					return err
				}
			}
		}
		if ctr.outputLease == nil {
			ctr.outputLease, err = proc.Base.ImportMemory.AcquireOutput(proc.Ctx)
			if err != nil {
				return err
			}
		}
	}
	ctr.setSpillThreshold(hashJoin.SpillThreshold)

	if hashJoin.NonEqCond != nil && len(ctr.joinBats) == 0 {
		ctr.joinBats = make([]*batch.Batch, 2)
	}

	if len(ctr.eqCondVecs) == 0 {
		ctr.eqCondVecs = make([]*vector.Vector, len(hashJoin.EqConds[0]))
		ctr.eqCondExecs = make([]colexec.ExpressionExecutor, len(hashJoin.EqConds[0]))
		ctr.eqCondExecs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, hashJoin.EqConds[0])
		if err != nil {
			return err
		}

		if hashJoin.NonEqCond != nil {
			ctr.nonEqCondExec, err = colexec.NewExpressionExecutor(proc, hashJoin.NonEqCond)
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (hashJoin *HashJoin) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := hashJoin.OpAnalyzer

	ctr := &hashJoin.ctr
	input := vm.NewCallResult()
	result := vm.NewCallResult()
	var err error

	for {
		if ctr.spillWork != nil && ctr.spillWork.Ctx.Err() != nil {
			return result, ctr.spillWork.Ctx.Err()
		}
		if ctr.skew != nil {
			output, done, callErr := hashJoin.callSkew(proc)
			if callErr != nil || !done {
				return output, callErr
			}
			continue
		}
		switch ctr.state {
		case Build:
			err = hashJoin.build(analyzer, proc)
			if err != nil {
				return result, err
			}

			if ctr.spillGeneration && !ctr.spillConsumer {
				ctr.state = End
				continue
			}
			if ctr.mp == nil && len(ctr.spillQueue) == 0 && !hashJoin.EmitUnmatchedProbe() {
				// TODO: early terminate the probe side for shuffle join
				if !hashJoin.IsShuffle {
					ctr.state = End
					continue
				}
			}

			if hashJoin.CanSkipProbe && ctr.mp != nil && ctr.mp.PushedRuntimeFilterIn() && hashJoin.NonEqCond == nil {
				ctr.skipProbe = true
			}

			ctr.state = Probe

		case Probe:
			if ctr.leftBat == nil {
				input, err = hashJoin.getInputBatch(proc, analyzer)
				if err != nil {
					return result, err
				}
				bat := input.Batch
				if ctr.skew != nil {
					continue
				}

				if bat == nil {
					if hashJoin.EmitUnmatchedBuild() {
						ctr.state = SyncBitmap
					} else {
						ctr.state = End
					}

					continue
				}

				if bat.Last() {
					result.Batch = input.Batch
					return result, nil
				}

				if bat.IsEmpty() {
					continue
				}

				if ctr.mp == nil && !hashJoin.EmitUnmatchedProbe() {
					continue
				}

				ctr.leftBat = bat
				ctr.lastIdx = 0
			}

			hashJoin.resetResultBat()
			for i, rp := range hashJoin.ResultCols {
				if rp.Rel == 0 {
					ctr.resBat.Vecs[i].SetSorted(ctr.leftBat.Vecs[rp.Pos].GetSorted())
				}
			}

			if ctr.skipProbe {
				if err := ctr.emptyProbe(hashJoin, proc, &result); err != nil {
					return result, err
				}
				return result, nil
			}

			startRow := ctr.lastIdx

			if ctr.mp == nil {
				err = ctr.emptyProbe(hashJoin, proc, &result)
			} else {
				err = ctr.probe(hashJoin, proc, &result)
			}
			if err != nil {
				return result, err
			}

			if hashJoin.IsRightSemi() || hashJoin.IsRightAnti() {
				continue
			}

			if ctr.lastIdx == startRow && ctr.leftBat != nil &&
				(result.Batch == nil || result.Batch.IsEmpty()) {
				return result, moerr.NewInternalErrorNoCtx("hash join hanging")
			}

			return result, nil

		case SyncBitmap:
			err := ctr.syncBitmap(hashJoin, proc)
			if err != nil {
				return result, err
			}

			if ctr.rightRowsMatched == nil || (hashJoin.NumCPU > 1 && !hashJoin.IsMerger && !ctr.spillGeneration) {
				ctr.state = End
			} else {
				ctr.state = Finalize
			}

			continue

		case Finalize:
			err := ctr.finalize(hashJoin, proc, &result)
			if err != nil {
				return result, err
			}

			if result.Batch == nil {
				ctr.state = End

				// For spilled join, clean up current bucket and move to next
				if len(ctr.spillQueue) > 0 || ctr.probeBucketActive {
					ctr.rightRowsMatched = nil
					ctr.cleanHashMap()

					if len(ctr.spillQueue) > 0 || ctr.probeBucketActive {
						ctr.state = Probe
					}
				}
				continue
			}

			return result, nil

		default:
			if ctr.spillWork != nil {
				ctr.spillWork.Close()
				ctr.spillWork = nil
			}
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (hashJoin *HashJoin) build(analyzer process.Analyzer, proc *process.Process) (err error) {
	ctr := &hashJoin.ctr
	start := time.Now()
	defer analyzer.WaitStop(start)

	ctr.mp, err = message.ReceiveJoinMap(hashJoin.JoinMapTag, hashJoin.IsShuffle, hashJoin.ShuffleIdx, proc.GetMessageBoard(), proc.Ctx)
	if err != nil {
		return err
	}

	if ctr.mp != nil && hashJoin.SpillGroup != nil && !ctr.mp.IsSpilled() {
		if !hashJoin.SpillGroup.AcquireMemory(hashJoin.SpillEpoch, ctr.mp) {
			ctr.mp = nil
			return moerr.NewInternalError(proc.Ctx, "broadcast generation aborted")
		}
	}
	if ctr.mp != nil {
		ctr.maxAllocSize = max(ctr.maxAllocSize, ctr.mp.Size())

		// Handle spilled build side
		if ctr.mp.IsSpilled() {
			ctr.spillGeneration = true
			if hashJoin.SpillGroup != nil {
				return hashJoin.buildBroadcastSpill(proc, analyzer)
			}
			ctr.spillConsumer = true
			spilledBuildFds := ctr.mp.TakeSpillBuildFds()

			// Register build fds in spillQueue immediately so cleanupSpillFiles
			// can close them even if we return early (e.g. context cancelled).
			// Generate unique baseNames for sub-bucket naming during re-spill.
			uid, err := uuid.NewV7()
			if err != nil {
				return err
			}
			uidStr := uid.String()
			ctr.spillQueue = make([]spillBucket, len(spilledBuildFds))
			for i, fd := range spilledBuildFds {
				ctr.spillQueue[i] = spillBucket{
					buildFd:  fd,
					baseName: fmt.Sprintf("%s_%d", uidStr, i),
					depth:    1,
				}
			}

			// Create writers for probe side (files created lazily on first write).
			// Disable writers for empty-build buckets so scatter discards those
			// probe rows immediately, matching the re-spill validBuckets pattern.
			spillWriters, err := createRootProbeSpillBucketFiles()
			if err != nil {
				return err
			}
			needsProbeForEmpty := hashJoin.EmitUnmatchedProbe()
			if !needsProbeForEmpty {
				for i := range spilledBuildFds {
					if spilledBuildFds[i] == nil {
						spillWriters[i].name = ""
					}
				}
			}
			spillBuffers := make([]*batch.Batch, spillNumBuckets)

			defer func() {
				for i := range spillWriters {
					spillWriters[i].close()
				}
				for _, buf := range spillBuffers {
					if buf != nil {
						buf.Clean(proc.Mp())
					}
				}
			}()

			// Spill each probe batch as it arrives
			for {
				input, err := vm.ChildrenCall(hashJoin.GetChildren(0), proc, analyzer)
				if err != nil {
					return err
				}
				if input.Batch == nil {
					break
				}
				if !input.Batch.IsEmpty() {
					if err := ctr.appendProbeBatchToSpillFiles(proc, input.Batch, spillWriters, spillBuffers, analyzer, 0); err != nil {
						return err
					}
				}
			}

			// Flush remaining buffered data
			for i, buf := range spillBuffers {
				if _, err := ctr.flushBucketBuffer(proc, buf, &spillWriters[i], analyzer); err != nil {
					return err
				}
			}

			// Transfer probe fd ownership into spillQueue.
			// handOffFd seeks fd to 0; returns nil if probe bucket was empty.
			for i := range ctr.spillQueue {
				ctr.spillQueue[i].probeFd = spillWriters[i].handOffFd()
			}

			ctr.mp.Free()
			ctr.mp = nil
			return err
		}
	}

	ctr.rightBats = ctr.mp.GetBatches()
	ctr.rightRowCnt = ctr.mp.GetRowCount()

	if hashJoin.EmitUnmatchedBuild() {
		if ctr.rightRowCnt > 0 {
			ctr.rightRowsMatched = &bitmap.Bitmap{}
			ctr.rightRowsMatched.InitWithSize(ctr.rightRowCnt)
		}
	}

	return
}

func (hashJoin *HashJoin) getInputBatch(proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	// For unspilled join, simply call children.
	// In spill mode, spillQueue can become empty while we are still reading the
	// currently loaded bucket via probeBucketReader.
	if hashJoin.ctr.spillGeneration && len(hashJoin.ctr.spillQueue) == 0 && !hashJoin.ctr.probeBucketActive {
		return vm.NewCallResult(), nil
	}
	if len(hashJoin.ctr.spillQueue) == 0 && !hashJoin.ctr.probeBucketActive {
		return vm.ChildrenCall(hashJoin.GetChildren(0), proc, analyzer)
	}

	// For spilled join, load bucket and return probe batches
	return hashJoin.getSpilledInputBatch(proc, analyzer)
}

func (ctr *container) probe(hashJoin *HashJoin, proc *process.Process, result *vm.CallResult) error {
	err := ctr.evalJoinCondition(ctr.leftBat, proc)
	if err != nil {
		return err
	}

	if hashJoin.NonEqCond != nil {
		if ctr.joinBats[0] == nil {
			ctr.joinBats[0], ctr.cfs1 = colexec.NewJoinBatch(ctr.leftBat, proc.Mp())
		}
		if ctr.joinBats[1] == nil && ctr.rightRowCnt > 0 {
			ctr.joinBats[1], ctr.cfs2 = colexec.NewJoinBatch(ctr.rightBats[0], proc.Mp())
		}
	}

	if ctr.itr == nil {
		ctr.itr = ctr.mp.NewIterator()
	}
	leftRowCnt := ctr.leftBat.RowCount()
	resRowCnt := 0

	for {
		switch ctr.probeState {
		case psNextBatch:
			if ctr.lastIdx < leftRowCnt {
				hashBatch := min(leftRowCnt-ctr.lastIdx, hashmap.UnitLimit)
				ctr.vs, ctr.zvs = ctr.itr.Find(ctr.lastIdx, hashBatch, ctr.eqCondVecs)
				ctr.vsIdx = 0
				ctr.probeState = psBatchRow
			} else {
				ctr.resBat.AddRowCount(resRowCnt)
				result.Batch = ctr.resBat
				ctr.lastIdx = 0
				ctr.leftBat = nil
				ctr.probeState = psNextBatch
				return nil
			}

		case psBatchRow:
			z, v := ctr.zvs[ctr.vsIdx], ctr.vs[ctr.vsIdx]
			row := int64(ctr.lastIdx)
			idx := int64(v) - 1
			idx1 := idx / colexec.DefaultBatchSize
			idx2 := idx % colexec.DefaultBatchSize

			ctr.lastIdx++
			ctr.vsIdx++

			if z == 0 || v == 0 {
				if hashJoin.EmitUnmatchedProbe() {
					if err := ctr.appendOneNotMatch(hashJoin, proc, row); err != nil {
						return err
					}
					resRowCnt++
				}

				if ctr.vsIdx >= len(ctr.vs) {
					ctr.probeState = psNextBatch
				}

				if resRowCnt >= colexec.DefaultBatchSize || ctr.outputFull() {
					ctr.resBat.AddRowCount(resRowCnt)
					result.Batch = ctr.resBat
					return nil
				}

				continue
			}

			if hashJoin.HashOnPK || ctr.mp.HashOnUnique() {
				if hashJoin.NonEqCond == nil {
					if !hashJoin.IsRightSemi() && !hashJoin.IsAnti() {
						err = ctr.appendOneMatch(hashJoin, proc, row, idx1, idx2)
						if err != nil {
							return err
						}

						resRowCnt++
					}

					if hashJoin.IsRightJoin {
						if hashJoin.IsSingle() && ctr.rightRowsMatched.Contains(uint64(idx)) {
							return moerr.NewInternalError(proc.Ctx, "scalar subquery returns more than 1 row")
						}

						ctr.rightRowsMatched.Add(uint64(idx))
					}
				} else {
					ok, err := ctr.evalNonEqCondition(ctr.leftBat, row, proc, idx1, idx2)
					if err != nil {
						return err
					}

					if ok {
						if !hashJoin.IsRightSemi() && !hashJoin.IsAnti() {
							err = ctr.appendOneMatch(hashJoin, proc, row, idx1, idx2)
							if err != nil {
								return err
							}

							resRowCnt++
						}

						if hashJoin.IsRightJoin {
							if hashJoin.IsSingle() && ctr.rightRowsMatched.Contains(uint64(idx)) {
								return moerr.NewInternalError(proc.Ctx, "scalar subquery returns more than 1 row")
							}

							ctr.rightRowsMatched.Add(uint64(idx))
						}
					} else if hashJoin.EmitUnmatchedProbe() {
						err = ctr.appendOneNotMatch(hashJoin, proc, row)
						if err != nil {
							return err
						}
						resRowCnt++
					}
				}
			} else {
				ctr.sels = ctr.mp.GetSels(uint64(idx))
				ctr.leftRowMatched = false

				if hashJoin.NonEqCond == nil {
					if hashJoin.IsLeftSingle() {
						if len(ctr.sels) > 1 {
							return moerr.NewInternalError(proc.Ctx, "scalar subquery returns more than 1 row")
						}
					} else if hashJoin.IsLeftSemi() {
						if err := ctr.appendOneNotMatch(hashJoin, proc, row); err != nil {
							return err
						}
						resRowCnt++
						ctr.sels = nil
					} else if hashJoin.IsLeftAnti() {
						ctr.sels = nil
					}
				}
			}

			if len(ctr.sels) > 0 {
				ctr.probeState = psSelsForOneRow
			} else if ctr.vsIdx < len(ctr.vs) {
				ctr.probeState = psBatchRow
			} else {
				ctr.probeState = psNextBatch
			}

			if resRowCnt >= colexec.DefaultBatchSize || ctr.outputFull() {
				ctr.resBat.AddRowCount(resRowCnt)
				result.Batch = ctr.resBat
				return nil
			}

		case psSelsForOneRow:
			row := int64(ctr.lastIdx - 1)
			processCount := min(len(ctr.sels), colexec.DefaultBatchSize-resRowCnt)
			if hashJoin.NonEqCond == nil && ctr.outputTarget > 0 && !hashJoin.IsRightSemi() && !hashJoin.IsAnti() {
				var bytes int64
				processCount, bytes = ctr.boundedMatchCount(hashJoin, row, processCount)
				ctr.outputBytes += bytes
			}
			sels := ctr.sels[:processCount]
			if hashJoin.NonEqCond == nil {
				ctr.sels = ctr.sels[processCount:]
				if hashJoin.IsRightJoin {
					for _, sel := range sels {
						if hashJoin.IsSingle() && ctr.rightRowsMatched.Contains(uint64(sel)) {
							return moerr.NewInternalError(proc.Ctx, "scalar subquery returns more than 1 row")
						}

						ctr.rightRowsMatched.Add(uint64(sel))
					}
				}

				if !hashJoin.IsRightSemi() && !hashJoin.IsAnti() {
					for j, rp := range hashJoin.ResultCols {
						if rp.Rel == 0 {
							err = ctr.resBat.Vecs[j].UnionMulti(ctr.leftBat.Vecs[rp.Pos], row, processCount, proc.Mp())
							if err != nil {
								return err
							}
						} else {
							for _, sel := range sels {
								idx1 := sel / colexec.DefaultBatchSize
								idx2 := sel % colexec.DefaultBatchSize
								err = ctr.resBat.Vecs[j].UnionOne(ctr.rightBats[idx1].Vecs[rp.Pos], int64(idx2), proc.Mp())
								if err != nil {
									return err
								}
							}
						}
					}
				}

				resRowCnt += processCount
			} else {
				for _, sel := range sels {
					// Retire each candidate only when evaluated. A controlled
					// varlen output can stop at its first crossing accepted row.
					ctr.sels = ctr.sels[1:]
					idx1 := int64(sel / colexec.DefaultBatchSize)
					idx2 := int64(sel % colexec.DefaultBatchSize)
					ok, err := ctr.evalNonEqCondition(ctr.leftBat, int64(row), proc, idx1, idx2)
					if err != nil {
						return err
					}

					if ok {
						if hashJoin.IsRightJoin {
							if hashJoin.IsSingle() && ctr.rightRowsMatched.Contains(uint64(sel)) {
								return moerr.NewInternalError(proc.Ctx, "scalar subquery returns more than 1 row")
							}

							ctr.rightRowsMatched.Add(uint64(sel))
						} else {
							if hashJoin.IsSingle() && ctr.leftRowMatched {
								return moerr.NewInternalError(proc.Ctx, "scalar subquery returns more than 1 row")
							}
						}

						ctr.leftRowMatched = true

						if !hashJoin.IsRightSemi() && !hashJoin.IsAnti() {
							if err := ctr.appendOneMatch(hashJoin, proc, int64(row), idx1, idx2); err != nil {
								return err
							}
							resRowCnt++
						}

						if hashJoin.IsLeftSemi() {
							ctr.sels = nil
							break
						}
						if ctr.outputFull() {
							break
						}
					}
				}

				if len(ctr.sels) == 0 &&
					!ctr.leftRowMatched && hashJoin.EmitUnmatchedProbe() {
					if err := ctr.appendOneNotMatch(hashJoin, proc, int64(row)); err != nil {
						return err
					}
					resRowCnt++
				}
			}

			if len(ctr.sels) > 0 {
				ctr.probeState = psSelsForOneRow
			} else if ctr.vsIdx < len(ctr.vs) {
				ctr.probeState = psBatchRow
			} else {
				ctr.probeState = psNextBatch
			}

			if resRowCnt >= colexec.DefaultBatchSize || ctr.outputFull() {
				ctr.resBat.AddRowCount(resRowCnt)
				result.Batch = ctr.resBat
				return nil
			}
		}
	}
}

func (ctr *container) emptyProbe(hashJoin *HashJoin, proc *process.Process, result *vm.CallResult) error {
	if ctr.outputTarget == 0 {
		// Preserve the ordinary bulk-copy path, including repeated/shared varlen
		// values. Large child batches still obey the join's row bound.
		rows := min(ctr.leftBat.RowCount()-ctr.lastIdx, colexec.DefaultBatchSize)
		for i, rp := range hashJoin.ResultCols {
			if rp.Rel == 0 {
				if err := ctr.resBat.Vecs[i].UnionBatch(ctr.leftBat.Vecs[rp.Pos], int64(ctr.lastIdx), rows, nil, proc.Mp()); err != nil {
					return err
				}
			} else if err := vector.SetConstNull(ctr.resBat.Vecs[i], rows, proc.Mp()); err != nil {
				return err
			}
		}
		ctr.resBat.AddRowCount(rows)
		result.Batch = ctr.resBat
		ctr.lastIdx += rows
		if ctr.lastIdx == ctr.leftBat.RowCount() {
			ctr.lastIdx, ctr.leftBat = 0, nil
		}
		return nil
	}
	rows := 0
	for ctr.lastIdx < ctr.leftBat.RowCount() {
		row := int64(ctr.lastIdx)
		for i, rp := range hashJoin.ResultCols {
			if rp.Rel == 0 {
				if ctr.outputTarget > 0 {
					bytes, err := preExtendJoinValue(ctr.resBat.Vecs[i], ctr.leftBat.Vecs[rp.Pos], row, proc)
					if err != nil {
						return err
					}
					ctr.outputBytes += bytes
				}
				if err := ctr.resBat.Vecs[i].UnionOne(ctr.leftBat.Vecs[rp.Pos], row, proc.Mp()); err != nil {
					return err
				}
			} else if ctr.outputTarget > 0 {
				ctr.outputBytes += int64(hashJoin.RightTypes[rp.Pos].TypeSize())
			}
		}
		ctr.lastIdx++
		rows++
		if rows >= colexec.DefaultBatchSize || ctr.outputFull() {
			break
		}
	}
	for i, rp := range hashJoin.ResultCols {
		if rp.Rel != 0 {
			if err := vector.SetConstNull(ctr.resBat.Vecs[i], rows, proc.Mp()); err != nil {
				return err
			}
		}
	}

	ctr.resBat.AddRowCount(rows)
	result.Batch = ctr.resBat
	if ctr.lastIdx == ctr.leftBat.RowCount() {
		ctr.lastIdx = 0
		ctr.leftBat = nil
	}

	return nil
}

func (ctr *container) syncBitmap(hashJoin *HashJoin, proc *process.Process) error {
	ctr.bitmapSynced = true

	if ctr.rightRowsMatched == nil {
		return nil
	}

	if hashJoin.NumCPU > 1 && !ctr.spillGeneration {
		if !hashJoin.IsMerger {
			hashJoin.Channel <- ctr.rightRowsMatched
			return nil
		} else {
			matchedCnt := ctr.rightRowsMatched.Count()

			for cnt := 1; cnt < int(hashJoin.NumCPU); cnt++ {
				v := colexec.ReceiveBitmapFromChannel(proc.Ctx, hashJoin.Channel)
				if v != nil {
					matchedCnt += v.Count()
					ctr.rightRowsMatched.Or(v)
				} else {
					return nil
				}
			}

			if hashJoin.IsSingle() && matchedCnt > ctr.rightRowsMatched.Count() {
				return moerr.NewInternalError(proc.Ctx, "scalar subquery returns more than 1 row")
			}

			close(hashJoin.Channel)
		}
	}

	if !hashJoin.IsSemi() {
		ctr.rightRowsMatched.Negate()
	}

	ctr.rightMatchedIter = ctr.rightRowsMatched.Iterator()

	return nil
}

func (ctr *container) finalize(hashJoin *HashJoin, proc *process.Process, result *vm.CallResult) error {
	hashJoin.resetResultBat()
	rowCnt := 0

	for ; rowCnt < colexec.DefaultBatchSize && ctr.rightMatchedIter.HasNext(); rowCnt++ {
		row := ctr.rightMatchedIter.Next()
		idx1, idx2 := row/colexec.DefaultBatchSize, row%colexec.DefaultBatchSize

		for i, rp := range hashJoin.ResultCols {
			if rp.Rel == 1 {
				if ctr.outputTarget > 0 {
					bytes, err := preExtendJoinValue(ctr.resBat.Vecs[i], ctr.rightBats[idx1].Vecs[rp.Pos], int64(idx2), proc)
					if err != nil {
						return err
					}
					ctr.outputBytes += bytes
				}
				err := ctr.resBat.Vecs[i].UnionOne(ctr.rightBats[idx1].Vecs[rp.Pos], int64(idx2), proc.Mp())
				if err != nil {
					return err
				}
			}
		}
		if ctr.outputTarget > 0 {
			for _, rp := range hashJoin.ResultCols {
				if rp.Rel == 0 {
					ctr.outputBytes += int64(hashJoin.LeftTypes[rp.Pos].TypeSize())
				}
			}
		}
		if ctr.outputFull() {
			rowCnt++
			break
		}
	}

	if rowCnt == 0 {
		result.Batch = nil
		return nil
	}

	for i, rp := range hashJoin.ResultCols {
		if rp.Rel == 0 {
			err := vector.AppendMultiFixed(ctr.resBat.Vecs[i], 0, true, rowCnt, proc.Mp())
			if err != nil {
				return err
			}
		}
	}

	ctr.resBat.AddRowCount(rowCnt)
	result.Batch = ctr.resBat

	return nil
}

func (ctr *container) appendOneNotMatch(hashJoin *HashJoin, proc *process.Process, row int64) error {
	for j, rp := range hashJoin.ResultCols {
		if rp.Rel == 0 {
			if ctr.outputTarget > 0 {
				bytes, err := preExtendJoinValue(ctr.resBat.Vecs[j], ctr.leftBat.Vecs[rp.Pos], row, proc)
				if err != nil {
					return err
				}
				ctr.outputBytes += bytes
			}
			err := ctr.resBat.Vecs[j].UnionOne(ctr.leftBat.Vecs[rp.Pos], row, proc.Mp())
			if err != nil {
				return err
			}
		} else {
			if ctr.outputTarget > 0 {
				ctr.outputBytes += int64(hashJoin.RightTypes[rp.Pos].TypeSize())
			}
			err := ctr.resBat.Vecs[j].UnionNull(proc.Mp())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (ctr *container) appendOneMatch(hashJoin *HashJoin, proc *process.Process, leftRow, rIdx1, rIdx2 int64) error {
	for j, rp := range hashJoin.ResultCols {
		if rp.Rel == 0 {
			if ctr.outputTarget > 0 {
				bytes, err := preExtendJoinValue(ctr.resBat.Vecs[j], ctr.leftBat.Vecs[rp.Pos], leftRow, proc)
				if err != nil {
					return err
				}
				ctr.outputBytes += bytes
			}
			err := ctr.resBat.Vecs[j].UnionOne(ctr.leftBat.Vecs[rp.Pos], leftRow, proc.Mp())
			if err != nil {
				return err
			}
		} else {
			if ctr.outputTarget > 0 {
				bytes, err := preExtendJoinValue(ctr.resBat.Vecs[j], ctr.rightBats[rIdx1].Vecs[rp.Pos], rIdx2, proc)
				if err != nil {
					return err
				}
				ctr.outputBytes += bytes
			}
			err := ctr.resBat.Vecs[j].UnionOne(ctr.rightBats[rIdx1].Vecs[rp.Pos], rIdx2, proc.Mp())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (ctr *container) evalNonEqCondition(bat *batch.Batch, row int64, proc *process.Process, idx1, idx2 int64) (bool, error) {
	err := colexec.SetJoinBatchValues(ctr.joinBats[0], bat, row, 1, ctr.cfs1)
	if err != nil {
		return false, err
	}

	err = colexec.SetJoinBatchValues(ctr.joinBats[1], ctr.rightBats[idx1], idx2, 1, ctr.cfs2)
	if err != nil {
		return false, err
	}

	vec, err := ctr.nonEqCondExec.Eval(proc, ctr.joinBats, nil)
	if err != nil {
		return false, err
	}

	return !vec.IsConstNull() &&
		!vec.GetNulls().Contains(0) &&
		vector.MustFixedColWithTypeCheck[bool](vec)[0], nil
}

func (ctr *container) evalJoinCondition(bat *batch.Batch, proc *process.Process) error {
	bats := []*batch.Batch{bat}
	for i := range ctr.eqCondExecs {
		vec, err := ctr.eqCondExecs[i].Eval(proc, bats, nil)
		if err != nil {
			return err
		}
		ctr.eqCondVecs[i] = vec
	}
	return nil
}

func (hashJoin *HashJoin) resetResultBat() {
	ctr := &hashJoin.ctr
	ctr.outputBytes = 0
	if ctr.resBat != nil {
		ctr.resBat.CleanOnlyData()
		for i := range ctr.resBat.Vecs {
			ctr.resBat.Vecs[i].SetClass(vector.FLAT)
			ctr.resBat.Vecs[i].SetLength(0)
		}
	} else {
		ctr.resBat = batch.NewOffHeapWithSize(len(hashJoin.ResultCols))

		for i, rp := range hashJoin.ResultCols {
			if rp.Rel == 0 {
				ctr.resBat.Vecs[i] = vector.NewOffHeapVecWithType(hashJoin.LeftTypes[rp.Pos])
			} else {
				ctr.resBat.Vecs[i] = vector.NewOffHeapVecWithType(hashJoin.RightTypes[rp.Pos])
			}
		}
	}
}
