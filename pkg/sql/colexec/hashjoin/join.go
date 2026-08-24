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
	"math/bits"
	"slices"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
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

const (
	opName                       = "hash_join"
	asofLinearOnlyGroupSizeLimit = 4
)

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
	case plan.Node_MARK:
		buf.WriteString(": hash mark join ")
	case plan.Node_OUTER:
		buf.WriteString(": full outer join ")
	case plan.Node_ASOF:
		buf.WriteString(": asof join ")
	case plan.Node_ASOF_LEFT:
		buf.WriteString(": asof left join ")
	}
}

func (hashJoin *HashJoin) OpType() vm.OpType {
	return vm.HashJoin
}

func (hashJoin *HashJoin) Prepare(proc *process.Process) (err error) {
	if hashJoin.IsAsof() {
		if hashJoin.NonEqCond == nil || len(hashJoin.EqConds) != 2 || len(hashJoin.EqConds[0]) == 0 ||
			len(hashJoin.EqConds[0]) != len(hashJoin.EqConds[1]) || hashJoin.HashOnPK ||
			hashJoin.AsofRightCol < 0 || int(hashJoin.AsofRightCol) >= len(hashJoin.RightTypes) ||
			!isAsofTemporalType(hashJoin.RightTypes[hashJoin.AsofRightCol].Oid) {
			return moerr.NewInternalError(proc.Ctx, "invalid ASOF join physical contract")
		}
		leftCol, rightCol, strict := asofTemporalMetadata(hashJoin.NonEqCond)
		if leftCol < 0 || leftCol >= len(hashJoin.LeftTypes) ||
			rightCol != int(hashJoin.AsofRightCol) ||
			hashJoin.LeftTypes[leftCol].Oid != hashJoin.RightTypes[hashJoin.AsofRightCol].Oid {
			return moerr.NewInternalError(proc.Ctx, "invalid ASOF temporal predicate metadata")
		}
		hashJoin.ctr.asofLeftCol = leftCol
		hashJoin.ctr.asofStrict = strict
	}
	if hashJoin.IsMark() {
		if err := hashJoin.validateMarkJoin(proc); err != nil {
			return err
		}
	}
	if hashJoin.allocationAccount == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	hashJoin.recursiveProbe = false
	if hashJoin.NumChildren() > 0 {
		_ = vm.HandleAllOp(hashJoin.GetChildren(0), func(_ vm.Operator, op vm.Operator) error {
			if op.OpType() == vm.MergeRecursive {
				hashJoin.recursiveProbe = true
			}
			return nil
		})
	}

	if hashJoin.OpAnalyzer == nil {
		hashJoin.OpAnalyzer = process.NewAnalyzer(hashJoin.GetIdx(), hashJoin.IsFirst, hashJoin.IsLast, opName)
	} else {
		hashJoin.OpAnalyzer.Reset()
	}

	ctr := &hashJoin.ctr
	ctr.setSpillThreshold(hashJoin.SpillThreshold)

	if hashJoin.NonEqCond != nil && len(ctr.joinBats) == 0 {
		ctr.joinBats = make([]*batch.Batch, 2)
	}

	if len(ctr.eqCondVecs) == 0 {
		eqCondExecs, err := hashbuild.NewExpressionExecutors(
			proc,
			hashJoin.EqConds[0],
			hashJoin.allocationAccount,
		)
		if err != nil {
			return err
		}

		var nonEqCondExec colexec.ExpressionExecutor
		if hashJoin.NonEqCond != nil {
			var nonEqExecs []colexec.ExpressionExecutor
			nonEqExecs, err = hashbuild.NewExpressionExecutors(
				proc,
				[]*plan.Expr{hashJoin.NonEqCond},
				hashJoin.allocationAccount,
			)
			if err != nil {
				for _, exec := range eqCondExecs {
					exec.Free()
				}
				return err
			}
			nonEqCondExec = nonEqExecs[0]
		}

		ctr.eqCondVecs = make([]*vector.Vector, len(hashJoin.EqConds[0]))
		ctr.eqCondExecs = eqCondExecs
		ctr.nonEqCondExec = nonEqCondExec
	}

	return err
}

func isAsofTemporalType(typeID types.T) bool {
	switch typeID {
	case types.T_date, types.T_datetime, types.T_timestamp, types.T_time:
		return true
	default:
		return false
	}
}

func (hashJoin *HashJoin) validateMarkJoin(proc *process.Process) error {
	if hashJoin.NonEqCond != nil {
		return moerr.NewInternalError(proc.Ctx, "hash MARK join does not support residual conditions")
	}
	if len(hashJoin.EqConds) != 2 || len(hashJoin.EqConds[0]) == 0 || len(hashJoin.EqConds[0]) != len(hashJoin.EqConds[1]) {
		return moerr.NewInternalError(proc.Ctx, "hash MARK join requires matching non-empty probe and build keys")
	}
	if len(hashJoin.EqConds[0]) > 1 {
		for i := range hashJoin.EqConds[0] {
			if !hashJoin.EqConds[0][i].Typ.NotNullable || !hashJoin.EqConds[1][i].Typ.NotNullable {
				return moerr.NewInternalError(proc.Ctx, "hash MARK join requires composite keys to be not nullable")
			}
		}
	}
	for _, result := range hashJoin.ResultCols {
		if result.Rel != 0 && result.Rel != -1 {
			return moerr.NewInternalErrorf(proc.Ctx, "hash MARK join has unexpected result relation %d", result.Rel)
		}
	}
	return nil
}

func (hashJoin *HashJoin) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := hashJoin.OpAnalyzer

	ctr := &hashJoin.ctr
	input := vm.NewCallResult()
	result := vm.NewCallResult()
	var err error

	for {
		switch ctr.state {
		case Build:
			err = hashJoin.build(analyzer, proc)
			if err != nil {
				return result, hashbuild.TerminalBudgetError(proc.Ctx, err)
			}

			if ctr.mp == nil && ctr.spillEngine == nil && !hashJoin.EmitUnmatchedProbe() && !hashJoin.IsMark() && !hashJoin.recursiveProbe {
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
					return result, hashbuild.TerminalBudgetError(proc.Ctx, err)
				}
				bat := input.Batch

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

				if ctr.mp == nil && !ctr.probeEmitUnmatched && !ctr.probeMark {
					continue
				}

				ctr.leftBat = bat
				ctr.lastIdx = 0
			}

			if err = hashJoin.resetResultBat(); err != nil {
				return result, err
			}
			for i, rp := range hashJoin.ResultCols {
				if rp.Rel == 0 {
					ctr.resBat.Vecs[i].SetSorted(ctr.leftBat.Vecs[rp.Pos].GetSorted())
				}
			}

			if hashJoin.canEmitMatchCountOnly() {
				err = ctr.probeMatchCountOnly(hashJoin, proc, &result)
				if err != nil {
					return result, hashbuild.TerminalBudgetError(proc.Ctx, err)
				}
				return result, nil
			}

			if ctr.skipProbe {
				rowCount := ctr.leftBat.RowCount()
				var srcVec *vector.Vector
				var targetVec *vector.Vector

				for i, rp := range hashJoin.ResultCols {
					srcVec = ctr.leftBat.Vecs[rp.Pos]
					targetVec = ctr.resBat.Vecs[i]
					err = targetVec.UnionBatch(srcVec, 0, rowCount, nil, proc.Mp())
					if err != nil {
						return result, hashbuild.TerminalBudgetError(proc.Ctx, err)
					}
				}

				ctr.leftBat = nil
				ctr.resBat.SetRowCount(rowCount)
				result.Batch = ctr.resBat

				return result, nil
			}

			startRow := ctr.lastIdx

			if ctr.mp == nil {
				err = ctr.emptyProbe(hashJoin, proc, &result)
			} else {
				err = ctr.probe(hashJoin, proc, &result)
			}
			if err != nil {
				return result, hashbuild.TerminalBudgetError(proc.Ctx, err)
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
				return result, hashbuild.TerminalBudgetError(proc.Ctx, err)
			}

			// Only enter Finalize when syncBitmap ran to completion and set
			// the iterator. It stays nil for non-merger workers, when there
			// is no bitmap at all, or when the merger observed teardown (a
			// worker sent a nil bitmap on Reset, or the context was
			// canceled) — in all these cases there is nothing to finalize.
			if ctr.rightMatchedIter == nil {
				ctr.state = End
			} else {
				ctr.state = Finalize
			}

			continue

		case Finalize:
			err := ctr.finalize(hashJoin, proc, &result)
			if err != nil {
				return result, hashbuild.TerminalBudgetError(proc.Ctx, err)
			}

			if result.Batch == nil {
				ctr.state = End

				// For spilled join, clean up current bucket and move to next
				if (ctr.spillEngine != nil) && (ctr.spillEngine.HasMoreBuckets() || ctr.spillEngine.IsProbing()) {
					ctr.freeRightRowsMatched(proc)
					ctr.cleanHashMap()
					ctr.state = Probe
				}
				continue
			}

			return result, nil

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

// canEmitMatchCountOnly checks the explicit planner/executor contract and the
// currently loaded hashmap. Spilled joins install one in-memory hashmap per
// ready bucket; empty spilled buckets never enter this path.
func (hashJoin *HashJoin) canEmitMatchCountOnly() bool {
	return hashJoin.EmitCompressedRowCount && hashJoin.IsInner() &&
		hashJoin.NonEqCond == nil && len(hashJoin.ResultCols) == 0 &&
		hashJoin.ctr.mp != nil
}

// probeMatchCountOnly emits one zero-column batch for the complete probe
// batch.  In particular, duplicate build keys no longer force one operator
// call per DefaultBatchSize matches.  A zero-column batch is the existing
// row-count carrier used after projection pruning.
func (ctr *container) probeMatchCountOnly(
	hashJoin *HashJoin,
	proc *process.Process,
	result *vm.CallResult,
) error {
	if err := ctr.evalJoinCondition(ctr.leftBat, proc); err != nil {
		return err
	}
	if ctr.itr == nil {
		ctr.itr = ctr.mp.NewIterator()
	}

	matchCount := 0
	rowCount := ctr.leftBat.RowCount()
	maxInt := int(^uint(0) >> 1)
	for offset := 0; offset < rowCount; offset += hashmap.UnitLimit {
		count := min(rowCount-offset, hashmap.UnitLimit)
		values, zValues, err := ctr.itr.Find(offset, count, ctr.eqCondVecs)
		if err != nil {
			return err
		}
		for i, value := range values {
			if zValues[i] == 0 || value == 0 {
				continue
			}
			matches := 1
			if !ctr.probeHashOnPK {
				matches = len(ctr.mp.GetSels(value - 1))
			}
			if matches > maxInt-matchCount {
				return moerr.NewInternalErrorNoCtx("hash join match count overflows int")
			}
			matchCount += matches
		}
	}

	ctr.resBat.SetRowCount(matchCount)
	result.Batch = ctr.resBat
	ctr.leftBat = nil
	ctr.lastIdx = 0
	ctr.probeState = psNextBatch
	return nil
}

func (hashJoin *HashJoin) build(analyzer process.Analyzer, proc *process.Process) (err error) {
	ctr := &hashJoin.ctr
	dep, err := process.MeasureWait(analyzer, resource.WaitOther, func() (message.JoinMapResult, error) {
		return message.ReceiveJoinMapResult(hashJoin.JoinMapTag, hashJoin.IsShuffle, hashJoin.ShuffleIdx, proc.GetMessageBoard(), proc.Ctx)
	})
	if err != nil {
		return err
	}
	if buildErr := dep.BuildError(); buildErr != nil {
		// A terminal BuildError is a failed dependency, never an empty build.
		// Return before consuming probe input so no successful rows can escape.
		return buildErr.AsMoErr()
	}
	// Close the previous metadata generation before adopting a different
	// JoinMap. Reset normally makes this empty; keeping the transition local
	// also makes restart and spill/error reuse fail-safe.
	ctr.cleanAsofIndexes(proc)
	ctr.mp = dep.JoinMap()

	// Pre-compute per-query flags for the probe loop.
	ctr.probeEmitUnmatched = hashJoin.EmitUnmatchedProbe()
	ctr.probeRightSemiAnti = !hashJoin.IsRightSemi() && !hashJoin.IsAnti()
	ctr.probeTrackBuildMatches = hashJoin.EmitUnmatchedBuild()
	ctr.probeSingle = hashJoin.IsSingle()
	ctr.probeLeftSingle = hashJoin.IsLeftSingle()
	ctr.probeLeftSemi = hashJoin.IsLeftSemi()
	ctr.probeLeftAnti = hashJoin.IsLeftAnti()
	ctr.probeMark = hashJoin.IsMark()
	ctr.buildHasNullKey = false
	ctr.globalBuildRowCnt = 0

	if ctr.mp != nil {
		ctr.maxAllocSize = max(ctr.maxAllocSize, ctr.mp.Size())
		ctr.buildHasNullKey = ctr.mp.HasNullKey()
		ctr.globalBuildRowCnt = ctr.mp.GetRowCount()

		// Handle spilled build side
		if ctr.mp.IsSpilled() {
			payload, budget, takeErr := spillutil.TakeSpillBuildPayload(proc, ctr.mp)
			if takeErr != nil {
				return takeErr
			}
			if hashJoin.allocationAccount == nil {
				_ = payload.Close()
				ctr.mp.Free()
				ctr.mp = nil
				return mpool.ErrAllocationAccountInvalid
			}
			engine, engineErr := spillutil.NewSpillEngine(spillutil.SpillEngineConfig{
				BuildKeyExprs:           hashJoin.EqConds[1],
				ProbeKeyExprs:           hashJoin.EqConds[0],
				SpillThreshold:          ctr.spillThreshold,
				NeedsProbeForEmptyBuild: hashJoin.EmitUnmatchedProbe() || hashJoin.IsMark(),
				NeedsBuildForEmptyProbe: hashJoin.EmitUnmatchedBuild(),
				HashOnPK:                hashJoin.HashOnPK,
				NeedAllocateSels:        !hashJoin.HashOnPK,
				NeedBatches:             hashJoin.NeedBuildBatches(),
				Budget:                  budget,
			}, hashJoin.allocationAccount, mpool.AllocationOwnerHashBuild)
			if engineErr != nil {
				_ = payload.Close()
				ctr.mp.Free()
				ctr.mp = nil
				ctr.cleanEqCondExecutors()
				return engineErr
			}
			engine.InitFromSpilledFiles(payload.Files)
			ctr.spillEngine = engine
			if err := engine.ScatterProbeTable(proc,
				func() (*batch.Batch, error) {
					input, err := vm.ChildrenCall(hashJoin.GetChildren(0), proc, analyzer)
					return input.Batch, err
				},
				analyzer,
				func(bat *batch.Batch) ([]*vector.Vector, error) {
					if err := ctr.evalJoinCondition(bat, proc); err != nil {
						return nil, err
					}
					return ctr.eqCondVecs, nil
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
			return nil
		}
	}

	if ctr.mp == nil {
		return
	}
	ctr.rightBats = ctr.mp.GetBatches()
	ctr.rightRowCnt = ctr.mp.GetRowCount()
	ctr.probeHashOnPK = hashJoin.HashOnPK || ctr.mp.HashOnUnique()

	if hashJoin.EmitUnmatchedBuild() {
		if ctr.rightRowCnt > 0 {
			ctr.rightRowsMatched, err = colexec.NewAccountedBitmap(
				ctr.rightRowCnt,
				proc.Mp(),
				hashJoin.allocationAccount,
				mpool.AllocationOwnerHashBuild,
				hashJoinAllocationSiteMatchedRows,
			)
			if err != nil {
				return err
			}
		}
	}

	return
}

func (hashJoin *HashJoin) getInputBatch(proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	if hashJoin.ctr.spillEngine == nil {
		return vm.ChildrenCall(hashJoin.GetChildren(0), proc, analyzer)
	}
	return hashJoin.getSpilledInputBatch(proc, analyzer)
}

func (hashJoin *HashJoin) getSpilledInputBatch(proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	var result vm.CallResult
	ctr := &hashJoin.ctr
	engine := ctr.spillEngine

	for {
		// Read next probe batch from current bucket.
		if ctr.probeBucketActive {
			bat, err := engine.NextProbeBatch(proc)
			if err != nil {
				return result, err
			}
			if bat != nil {
				result.Batch = bat
				return result, nil
			}
			// EOF on probe file.
			engine.FinishBucket()
			ctr.probeBucketActive = false
			if ctr.rightRowsMatched != nil {
				return result, nil // trigger Finalize for unmatched right rows
			}
			// The index contains ordinals into the completed bucket. Release it
			// before the spill engine allocates the next bucket's JoinMap so the
			// two generations cannot overlap in the query memory account.
			ctr.cleanAsofIndexes(proc)
			ctr.cleanHashMap()
		}

		// Load next bucket via engine convenience method.
		if ctr.mp == nil {
			var allocationErr error
			ok, err := engine.AdvanceToNextBucket(proc, analyzer,
				func(jm *message.JoinMap, res spillutil.BucketResult) {
					if res == spillutil.BucketReady {
						ctr.cleanAsofIndexes(proc)
						ctr.mp = jm
						ctr.rightBats = jm.GetBatches()
						ctr.rightRowCnt = jm.GetRowCount()
						ctr.probeHashOnPK = hashJoin.HashOnPK || ctr.mp.HashOnUnique()
						if hashJoin.EmitUnmatchedBuild() && ctr.rightRowCnt > 0 {
							ctr.rightRowsMatched, allocationErr =
								colexec.NewAccountedBitmap(
									ctr.rightRowCnt,
									proc.Mp(),
									hashJoin.allocationAccount,
									mpool.AllocationOwnerHashBuild,
									hashJoinAllocationSiteMatchedRows,
								)
							ctr.rightMatchedIter = nil
						}
					}
				})
			if err != nil {
				return result, err
			}
			if allocationErr != nil {
				return result, allocationErr
			}
			if !ok {
				return result, nil
			}
			hashmap.IteratorClearOwner(ctr.itr)
			ctr.itr = nil
			ctr.probeState = psNextBatch
			ctr.lastIdx = 0
			ctr.vsIdx = 0
			ctr.probeBucketActive = true
		}
	}
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
				var err error
				ctr.vs, ctr.zvs, err = ctr.itr.Find(
					ctr.lastIdx,
					hashBatch,
					ctr.eqCondVecs,
				)
				if err != nil {
					return err
				}
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

			if ctr.probeMark {
				markValue := z != 0 && v != 0
				markNull := !markValue && (z == 0 || ctr.buildHasNullKey)
				if err = ctr.appendOneMark(hashJoin, proc, row, markValue, markNull); err != nil {
					return err
				}
				resRowCnt++

				if ctr.vsIdx < len(ctr.vs) {
					ctr.probeState = psBatchRow
				} else {
					ctr.probeState = psNextBatch
				}

				if resRowCnt >= colexec.DefaultBatchSize {
					ctr.resBat.AddRowCount(resRowCnt)
					result.Batch = ctr.resBat
					return nil
				}
				continue
			}

			if z == 0 || v == 0 {
				if ctr.probeEmitUnmatched {
					if err = ctr.appendOneNotMatch(hashJoin, proc, row); err != nil {
						return err
					}
					resRowCnt++
				}

				if ctr.vsIdx >= len(ctr.vs) {
					ctr.probeState = psNextBatch
				}

				if resRowCnt >= colexec.DefaultBatchSize {
					ctr.resBat.AddRowCount(resRowCnt)
					result.Batch = ctr.resBat
					return nil
				}

				continue
			}

			if hashJoin.IsAsof() {
				candidates := []int32{int32(idx)}
				if !ctr.probeHashOnPK {
					candidates = ctr.mp.GetSels(uint64(idx))
				}
				best, found, findErr := ctr.findAsofPredecessor(hashJoin, proc, row, uint64(idx), candidates)
				if findErr != nil {
					return findErr
				}
				if found {
					bestBatch := int64(best / colexec.DefaultBatchSize)
					bestRow := int64(best % colexec.DefaultBatchSize)
					// Predecessor selection is complete before output. Append exactly
					// one final row; intermediate candidates never reach the result.
					if err = ctr.appendOneMatch(hashJoin, proc, row, bestBatch, bestRow); err != nil {
						return err
					}
					resRowCnt++
				} else if ctr.probeEmitUnmatched {
					if err = ctr.appendOneNotMatch(hashJoin, proc, row); err != nil {
						return err
					}
					resRowCnt++
				}
				if ctr.vsIdx < len(ctr.vs) {
					ctr.probeState = psBatchRow
				} else {
					ctr.probeState = psNextBatch
				}
				if resRowCnt >= colexec.DefaultBatchSize {
					ctr.resBat.AddRowCount(resRowCnt)
					result.Batch = ctr.resBat
					return nil
				}
				continue
			}

			if ctr.probeHashOnPK {
				if hashJoin.NonEqCond == nil {
					if ctr.probeRightSemiAnti {
						err = ctr.appendOneMatch(hashJoin, proc, row, idx1, idx2)
						if err != nil {
							return err
						}

						resRowCnt++
					}

					if ctr.probeTrackBuildMatches {
						if ctr.probeSingle && ctr.rightRowsMatched.Contains(uint64(idx)) {
							return moerr.NewErrSubqueryNo1Row(proc.Ctx)
						}

						ctr.rightRowsMatched.Add(uint64(idx))
					}
				} else {
					ok, err := ctr.evalNonEqCondition(ctr.leftBat, row, proc, idx1, idx2)
					if err != nil {
						return err
					}

					if ok {
						if ctr.probeRightSemiAnti {
							err = ctr.appendOneMatch(hashJoin, proc, row, idx1, idx2)
							if err != nil {
								return err
							}

							resRowCnt++
						}

						if ctr.probeTrackBuildMatches {
							if ctr.probeSingle && ctr.rightRowsMatched.Contains(uint64(idx)) {
								return moerr.NewErrSubqueryNo1Row(proc.Ctx)
							}

							ctr.rightRowsMatched.Add(uint64(idx))
						}
					} else if ctr.probeEmitUnmatched {
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
					if ctr.probeLeftSingle {
						if len(ctr.sels) > 1 {
							return moerr.NewErrSubqueryNo1Row(proc.Ctx)
						}
					} else if ctr.probeLeftSemi {
						if err = ctr.appendOneNotMatch(hashJoin, proc, row); err != nil {
							return err
						}
						resRowCnt++
						ctr.sels = nil
					} else if ctr.probeLeftAnti {
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

			if resRowCnt >= colexec.DefaultBatchSize {
				ctr.resBat.AddRowCount(resRowCnt)
				result.Batch = ctr.resBat
				return nil
			}

		case psSelsForOneRow:
			row := int64(ctr.lastIdx - 1)
			processCount := min(len(ctr.sels), colexec.DefaultBatchSize-resRowCnt)
			sels := ctr.sels[:processCount]
			// remove processed sels
			ctr.sels = ctr.sels[processCount:]
			if hashJoin.NonEqCond == nil {
				if ctr.probeTrackBuildMatches {
					for _, sel := range sels {
						if ctr.probeSingle && ctr.rightRowsMatched.Contains(uint64(sel)) {
							return moerr.NewErrSubqueryNo1Row(proc.Ctx)
						}

						ctr.rightRowsMatched.Add(uint64(sel))
					}
				}

				if ctr.probeRightSemiAnti {
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
					idx1 := int64(sel / colexec.DefaultBatchSize)
					idx2 := int64(sel % colexec.DefaultBatchSize)
					ok, err := ctr.evalNonEqCondition(ctr.leftBat, int64(row), proc, idx1, idx2)
					if err != nil {
						return err
					}

					if ok {
						if ctr.probeTrackBuildMatches {
							if ctr.probeSingle && ctr.rightRowsMatched.Contains(uint64(sel)) {
								return moerr.NewErrSubqueryNo1Row(proc.Ctx)
							}

							ctr.rightRowsMatched.Add(uint64(sel))
						} else {
							if ctr.probeSingle && ctr.leftRowMatched {
								return moerr.NewErrSubqueryNo1Row(proc.Ctx)
							}
						}

						ctr.leftRowMatched = true

						if ctr.probeRightSemiAnti {
							if err = ctr.appendOneMatch(hashJoin, proc, int64(row), idx1, idx2); err != nil {
								return err
							}
							resRowCnt++
						}

						if ctr.probeLeftSemi {
							ctr.sels = nil
							break
						}
					}
				}

				if len(ctr.sels) == 0 &&
					!ctr.leftRowMatched && ctr.probeEmitUnmatched {
					if err = ctr.appendOneNotMatch(hashJoin, proc, int64(row)); err != nil {
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

			if resRowCnt >= colexec.DefaultBatchSize {
				ctr.resBat.AddRowCount(resRowCnt)
				result.Batch = ctr.resBat
				return nil
			}
		}
	}
}

func (ctr *container) findAsofPredecessor(
	hashJoin *HashJoin,
	proc *process.Process,
	leftRow int64,
	groupKey uint64,
	candidates []int32,
) (best int32, found bool, err error) {
	if len(candidates) == 0 {
		return -1, false, nil
	}
	if ctr.asofLeftCol < 0 || ctr.asofLeftCol >= len(ctr.leftBat.Vecs) {
		return -1, false, moerr.NewInternalErrorNoCtx("ASOF left temporal column is out of range")
	}
	leftValue, leftValid := asofTemporalValue(ctr.leftBat.Vecs[ctr.asofLeftCol], leftRow)
	if !leftValid {
		return -1, false, nil
	}

	// A bounded scan is cheaper than retaining per-group metadata for tiny
	// equality groups, even when they are reused. This is the literal
	// one-best-row path: no index table and no per-row search state.
	if len(candidates) <= asofLinearOnlyGroupSizeLimit {
		best = ctr.scanAsofBest(hashJoin, candidates, leftValue, ctr.asofStrict)
		if best < 0 {
			return -1, false, nil
		}
		batchIdx := int64(best / colexec.DefaultBatchSize)
		rowIdx := int64(best % colexec.DefaultBatchSize)
		qualified, evalErr := ctr.evalNonEqCondition(ctr.leftBat, leftRow, proc, batchIdx, rowIdx)
		return best, qualified, evalErr
	}

	index, firstProbe, indexErr := ctr.getOrCreateAsofIndex(hashJoin, proc, groupKey, candidates)
	if indexErr != nil {
		return -1, false, indexErr
	}
	if firstProbe {
		// The first probe follows the one-best-row algorithm directly: scan the
		// group once, keep only the closest candidate, and classify the immutable
		// selection for possible reuse. No per-row search index is built here.
		best = ctr.classifyAndScanAsofGroup(hashJoin, index, candidates, leftValue, ctr.asofStrict)
	} else {
		switch index.order {
		case asofIndexEmpty:
			return -1, false, nil
		case asofIndexAscending:
			best = ctr.searchAscendingAsof(hashJoin, candidates, leftValue, ctr.asofStrict)
		case asofIndexDescending:
			best = ctr.searchDescendingAsof(hashJoin, candidates, leftValue, ctr.asofStrict)
		case asofIndexLinear:
			if index.linearProbes >= asofIndexPromotionScans(index.candidateCount, index.validCount) {
				if buildErr := ctr.buildSortedAsofIndex(hashJoin, proc, index, candidates); buildErr != nil {
					return -1, false, buildErr
				}
				best = searchSortedAsof(index, leftValue, ctr.asofStrict)
			} else {
				best = ctr.scanAsofBest(hashJoin, candidates, leftValue, ctr.asofStrict)
				index.linearProbes++
			}
		case asofIndexSorted:
			best = searchSortedAsof(index, leftValue, ctr.asofStrict)
		default:
			return -1, false, moerr.NewInternalErrorNoCtx("invalid ASOF predecessor index")
		}
	}
	if best < 0 {
		return -1, false, nil
	}
	batchIdx := int64(best / colexec.DefaultBatchSize)
	rowIdx := int64(best % colexec.DefaultBatchSize)
	qualified, evalErr := ctr.evalNonEqCondition(ctr.leftBat, leftRow, proc, batchIdx, rowIdx)
	if evalErr != nil {
		return -1, false, evalErr
	}
	return best, qualified, nil
}

func asofTemporalValue(vec *vector.Vector, row int64) (int64, bool) {
	if vec.IsConstNull() || vec.GetNulls().Contains(uint64(row)) || vec.GetGrouping().Contains(uint64(row)) {
		return 0, false
	}
	dataRow := row
	if vec.IsConst() {
		dataRow = 0
	}
	switch vec.GetType().Oid {
	case types.T_date:
		return int64(vector.MustFixedColWithTypeCheck[types.Date](vec)[dataRow]), true
	case types.T_datetime:
		return int64(vector.MustFixedColWithTypeCheck[types.Datetime](vec)[dataRow]), true
	case types.T_timestamp:
		return int64(vector.MustFixedColWithTypeCheck[types.Timestamp](vec)[dataRow]), true
	case types.T_time:
		return int64(vector.MustFixedColWithTypeCheck[types.Time](vec)[dataRow]), true
	default:
		return 0, false
	}
}

func (ctr *container) asofRightTemporalValue(hashJoin *HashJoin, candidate int32) (int64, bool) {
	batchIdx := int(candidate / colexec.DefaultBatchSize)
	rowIdx := int64(candidate % colexec.DefaultBatchSize)
	if batchIdx < 0 || batchIdx >= len(ctr.rightBats) || hashJoin.AsofRightCol < 0 ||
		int(hashJoin.AsofRightCol) >= len(ctr.rightBats[batchIdx].Vecs) {
		return 0, false
	}
	return asofTemporalValue(ctr.rightBats[batchIdx].Vecs[hashJoin.AsofRightCol], rowIdx)
}

func asofPredecessorEligible(right, left int64, strict bool) bool {
	if strict {
		return right < left
	}
	return right <= left
}

func (ctr *container) getOrCreateAsofIndex(
	hashJoin *HashJoin,
	proc *process.Process,
	groupKey uint64,
	candidates []int32,
) (*asofIndex, bool, error) {
	if err := ctr.ensureAsofIndexTable(hashJoin, proc); err != nil {
		return nil, false, err
	}
	indexPos := ctr.findAsofIndexSlot(groupKey)
	if ctr.asofIndexes[indexPos].occupied &&
		int(ctr.asofIndexes[indexPos].candidateCount) == len(candidates) {
		return &ctr.asofIndexes[indexPos], false, nil
	}
	if ctr.asofIndexes[indexPos].occupied {
		// Production JoinMaps are immutable, so a changed group length denotes a
		// different generation. Clearing one open-addressed slot would break the
		// collision chain; invalidate the complete generation instead.
		ctr.cleanAsofIndexes(proc)
		if err := ctr.ensureAsofIndexTable(hashJoin, proc); err != nil {
			return nil, false, err
		}
		indexPos = ctr.findAsofIndexSlot(groupKey)
	}

	index := asofIndex{
		key:            groupKey,
		candidateCount: int32(len(candidates)),
		order:          asofIndexLinear,
		occupied:       true,
	}
	if ctr.asofIndexCount+1 > len(ctr.asofIndexes)*3/4 {
		capacity := len(ctr.asofIndexes) * 2
		newIndexes, allocErr := mpool.MakeSliceAccounted[asofIndex](capacity, proc.Mp(), hashJoin.allocationAccount, mpool.AllocationOwnerHashBuild, hashJoinAllocationSiteAsofIndex)
		if allocErr != nil {
			return nil, false, allocErr
		}
		for _, old := range ctr.asofIndexes {
			if !old.occupied {
				continue
			}
			i := hashAsofIndex(old.key, len(newIndexes))
			for newIndexes[i].occupied {
				i = (i + 1) % len(newIndexes)
			}
			newIndexes[i] = old
		}
		mpool.FreeSlice(proc.Mp(), ctr.asofIndexes)
		ctr.asofIndexes = newIndexes
		indexPos = ctr.findAsofIndexSlot(groupKey)
	}
	ctr.asofIndexes[indexPos] = index
	ctr.asofIndexCount++
	return &ctr.asofIndexes[indexPos], true, nil
}

func (ctr *container) ensureAsofIndexTable(hashJoin *HashJoin, proc *process.Process) error {
	if len(ctr.asofIndexes) != 0 {
		return nil
	}
	indexes, err := mpool.MakeSliceAccounted[asofIndex](
		8, proc.Mp(), hashJoin.allocationAccount,
		mpool.AllocationOwnerHashBuild, hashJoinAllocationSiteAsofIndex,
	)
	if err != nil {
		return err
	}
	ctr.asofIndexes = indexes
	return nil
}

func (ctr *container) classifyAndScanAsofGroup(
	hashJoin *HashJoin,
	index *asofIndex,
	candidates []int32,
	leftValue int64,
	strict bool,
) int32 {
	best := int32(-1)
	var bestValue int64
	ascending, descending := true, true
	nonNullCount := 0
	hasPrevious := false
	hasNull := false
	var previous int64
	for _, candidate := range candidates {
		value, valid := ctr.asofRightTemporalValue(hashJoin, candidate)
		if !valid {
			hasNull = true
			continue
		}
		nonNullCount++
		if hasPrevious {
			if value < previous {
				ascending = false
			}
			if value > previous {
				descending = false
			}
		}
		previous = value
		hasPrevious = true
		if asofPredecessorEligible(value, leftValue, strict) &&
			(best < 0 || value > bestValue) {
			best = candidate
			bestValue = value
		}
	}
	index.validCount = int32(nonNullCount)
	index.linearProbes = 1
	if nonNullCount == 0 {
		index.order = asofIndexEmpty
		return -1
	}
	// Binary search over the JoinMap selection is valid only when every
	// candidate has a temporal value. A NULL hole would break its ordering.
	if !hasNull && ascending {
		index.order = asofIndexAscending
		return best
	}
	if !hasNull && descending {
		index.order = asofIndexDescending
		return best
	}
	index.order = asofIndexLinear
	return best
}

func (ctr *container) scanAsofBest(
	hashJoin *HashJoin,
	candidates []int32,
	leftValue int64,
	strict bool,
) int32 {
	best := int32(-1)
	var bestValue int64
	for _, candidate := range candidates {
		value, valid := ctr.asofRightTemporalValue(hashJoin, candidate)
		if !valid || !asofPredecessorEligible(value, leftValue, strict) {
			continue
		}
		if best < 0 || value > bestValue {
			best = candidate
			bestValue = value
		}
	}
	return best
}

// asofIndexPromotionScans is an online rent-or-buy boundary. One linear probe
// costs candidateCount visits. Building the compact index costs one fill scan
// plus approximately validCount*ceil(log2(validCount)) sort comparisons.
// The comparison estimate is bounded by the accompanying focused benchmark:
// sorting copied integers is much cheaper than revisiting vector-backed rows.
// A group used once or a few times therefore avoids eager sort/allocation.
func asofIndexPromotionScans(candidateCount, validCount int32) uint32 {
	if candidateCount <= 0 || validCount <= 0 {
		return ^uint32(0)
	}
	levels := bits.Len32(uint32(validCount - 1))
	if levels == 0 {
		levels = 1
	}
	buildWork := int64(candidateCount) + int64(validCount)*int64(levels)
	scans := (buildWork + int64(candidateCount) - 1) / int64(candidateCount)
	if scans < 2 {
		scans = 2
	}
	// The accompanying benchmark covers 16..4095 unordered rows. Sorting the
	// copied integer entries costs no more than four vector-backed scans there;
	// cap the comparison-count estimate so it does not overprice cache-local
	// sorting and leave a repeatedly probed group on the linear path too long.
	if scans > 4 {
		scans = 4
	}
	return uint32(scans)
}

func (ctr *container) buildSortedAsofIndex(
	hashJoin *HashJoin,
	proc *process.Process,
	index *asofIndex,
	candidates []int32,
) error {
	entries, allocErr := mpool.MakeSliceAccounted[asofIndexEntry](
		int(index.validCount), proc.Mp(), hashJoin.allocationAccount,
		mpool.AllocationOwnerHashBuild, hashJoinAllocationSiteAsofIndex,
	)
	if allocErr != nil {
		return allocErr
	}
	next := 0
	for ordinal, candidate := range candidates {
		value, valid := ctr.asofRightTemporalValue(hashJoin, candidate)
		if !valid {
			continue
		}
		if next >= len(entries) {
			mpool.FreeSlice(proc.Mp(), entries)
			return moerr.NewInternalErrorNoCtx("ASOF group changed while building predecessor index")
		}
		entries[next] = asofIndexEntry{value: value, row: candidate, ordinal: int32(ordinal)}
		next++
	}
	if next != len(entries) {
		mpool.FreeSlice(proc.Mp(), entries)
		return moerr.NewInternalErrorNoCtx("ASOF group changed while building predecessor index")
	}
	slices.SortFunc(entries, func(left, right asofIndexEntry) int {
		if left.value < right.value {
			return -1
		}
		if left.value > right.value {
			return 1
		}
		// Reverse ordinal order makes the final entry for an equal timestamp
		// the first row in the materialized JoinMap selection.
		if left.ordinal > right.ordinal {
			return -1
		}
		if left.ordinal < right.ordinal {
			return 1
		}
		return 0
	})
	index.entries = entries
	index.order = asofIndexSorted
	return nil
}

func (ctr *container) searchAscendingAsof(
	hashJoin *HashJoin,
	candidates []int32,
	leftValue int64,
	strict bool,
) int32 {
	lo, hi := 0, len(candidates)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		value, _ := ctr.asofRightTemporalValue(hashJoin, candidates[mid])
		if asofPredecessorEligible(value, leftValue, strict) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	last := lo - 1
	if last < 0 {
		return -1
	}
	bestValue, _ := ctr.asofRightTemporalValue(hashJoin, candidates[last])
	// Equal timestamps choose the first row in this materialized JoinMap.
	lo, hi = 0, last+1
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		value, _ := ctr.asofRightTemporalValue(hashJoin, candidates[mid])
		if value < bestValue {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return candidates[lo]
}

func (ctr *container) searchDescendingAsof(
	hashJoin *HashJoin,
	candidates []int32,
	leftValue int64,
	strict bool,
) int32 {
	lo, hi := 0, len(candidates)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		value, _ := ctr.asofRightTemporalValue(hashJoin, candidates[mid])
		if asofPredecessorEligible(value, leftValue, strict) {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	if lo == len(candidates) {
		return -1
	}
	return candidates[lo]
}

func searchSortedAsof(index *asofIndex, leftValue int64, strict bool) int32 {
	lo, hi := 0, len(index.entries)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if asofPredecessorEligible(index.entries[mid].value, leftValue, strict) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == 0 {
		return -1
	}
	return index.entries[lo-1].row
}

func hashAsofIndex(key uint64, size int) int {
	// Mix the equality hash before reducing it to keep sequential keys from
	// clustering in the open-addressed table.
	key ^= key >> 33
	key *= 0xff51afd7ed558ccd
	key ^= key >> 33
	return int(key % uint64(size))
}

func (ctr *container) findAsofIndexSlot(key uint64) int {
	if len(ctr.asofIndexes) == 0 {
		return 0
	}
	i := hashAsofIndex(key, len(ctr.asofIndexes))
	for {
		if !ctr.asofIndexes[i].occupied || ctr.asofIndexes[i].key == key {
			return i
		}
		i = (i + 1) % len(ctr.asofIndexes)
	}
}

func asofTemporalMetadata(expr *plan.Expr) (leftCol, rightCol int, strict bool) {
	leftCol, rightCol = -1, -1
	if expr == nil {
		return
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func == nil {
			for _, arg := range fn.Args {
				if left, right, isStrict := asofTemporalMetadata(arg); left >= 0 {
					return left, right, isStrict
				}
			}
			return
		}
		if strings.EqualFold(fn.Func.ObjName, "and") {
			for _, arg := range fn.Args {
				if left, right, isStrict := asofTemporalMetadata(arg); left >= 0 {
					return left, right, isStrict
				}
			}
			return
		}
		if len(fn.Args) == 2 {
			left, right := fn.Args[0].GetCol(), fn.Args[1].GetCol()
			if left != nil && right != nil {
				if left.RelPos == 0 && right.RelPos == 1 && (fn.Func.ObjName == ">" || fn.Func.ObjName == ">=") {
					return int(left.ColPos), int(right.ColPos), fn.Func.ObjName == ">"
				}
				if left.RelPos == 1 && right.RelPos == 0 && (fn.Func.ObjName == "<" || fn.Func.ObjName == "<=") {
					return int(right.ColPos), int(left.ColPos), fn.Func.ObjName == "<"
				}
			}
		}
		for _, arg := range fn.Args {
			if left, right, isStrict := asofTemporalMetadata(arg); left >= 0 {
				return left, right, isStrict
			}
		}
	}
	return
}

func (ctr *container) emptyProbe(hashJoin *HashJoin, proc *process.Process, result *vm.CallResult) error {
	rowCnt := ctr.leftBat.RowCount()
	for i, rp := range hashJoin.ResultCols {
		if rp.Rel == 0 {
			err := ctr.resBat.Vecs[i].UnionBatch(ctr.leftBat.Vecs[rp.Pos], 0, rowCnt, nil, proc.Mp())
			if err != nil {
				return err
			}
		} else if hashJoin.IsMark() {
			if rp.Rel != -1 {
				return moerr.NewInternalErrorNoCtxf("hash mark join has unexpected result relation %d", rp.Rel)
			}
			if err := ctr.appendMarkForEmptyBuildBucket(ctr.resBat.Vecs[i], proc, rowCnt); err != nil {
				return err
			}
		} else {
			if err := vector.SetConstNull(ctr.resBat.Vecs[i], rowCnt, proc.Mp()); err != nil {
				return err
			}
		}
	}
	ctr.resBat.AddRowCount(rowCnt)
	result.Batch = ctr.resBat
	ctr.lastIdx = 0
	ctr.leftBat = nil
	return nil
}

// appendMarkForEmptyBuildBucket evaluates a MARK join when the current spill
// bucket has no build rows. A local empty bucket does not imply a globally
// empty build side: global build emptiness and global build NULLs still decide
// the SQL three-valued result.
func (ctr *container) appendMarkForEmptyBuildBucket(marker *vector.Vector, proc *process.Process, rowCnt int) error {
	if ctr.globalBuildRowCnt == 0 {
		return vector.SetConstFixed(marker, false, rowCnt, proc.Mp())
	}
	if ctr.buildHasNullKey {
		return vector.SetConstNull(marker, rowCnt, proc.Mp())
	}

	if err := ctr.evalJoinCondition(ctr.leftBat, proc); err != nil {
		return err
	}
	if err := vector.AppendMultiFixed(marker, false, false, rowCnt, proc.Mp()); err != nil {
		return err
	}
	for _, vec := range ctr.eqCondVecs {
		if vec.IsConstNull() {
			if err := marker.PreExtendNulls(rowCnt, proc.Mp()); err != nil {
				return err
			}
			marker.GetNulls().AddRange(0, uint64(rowCnt))
			return nil
		}
		if !vec.GetNulls().Any() {
			continue
		}
		if err := marker.PreExtendNulls(rowCnt, proc.Mp()); err != nil {
			return err
		}
		nulls.Or(marker.GetNulls(), vec.GetNulls(), marker.GetNulls())
	}
	return nil
}

func (ctr *container) syncBitmap(hashJoin *HashJoin, proc *process.Process) error {
	ctr.bitmapSynced = true

	if ctr.rightRowsMatched == nil {
		return nil
	}

	if hashJoin.NumCPU > 1 {
		if !hashJoin.IsMerger {
			if hashJoin.Mailbox.Send(ctr.rightRowsMatched) {
				ctr.rightRowsMatched = nil
			}
			return nil
		} else {
			matchedCnt := ctr.rightRowsMatched.Count()

			for cnt := 1; cnt < int(hashJoin.NumCPU); cnt++ {
				v, received := hashJoin.Mailbox.Receive(proc.Ctx)
				if !received || v == nil {
					// A worker was torn down before syncing (its Reset sends
					// nil) or the context was canceled. Sealing transfers all
					// already-published values to cleanup and makes late
					// publishers retain their own value. Bail out without initializing the
					// iterator — Call routes to End and nothing is finalized.
					hashJoin.Mailbox.SealAndDrain(proc.Mp())
					return nil
				}
				matchedCnt += v.Count()
				ctr.rightRowsMatched.Or(v)
				colexec.FreeAccountedBitmap(v, proc.Mp())
			}

			if ctr.probeSingle && matchedCnt > ctr.rightRowsMatched.Count() {
				return moerr.NewErrSubqueryNo1Row(proc.Ctx)
			}

			hashJoin.Mailbox.SealAndDrain(proc.Mp())
		}
	}

	if !hashJoin.IsSemi() {
		ctr.rightRowsMatched.Negate()
	}

	ctr.rightMatchedIter = ctr.rightRowsMatched.Iterator()

	return nil
}

func (ctr *container) finalize(hashJoin *HashJoin, proc *process.Process, result *vm.CallResult) error {
	if err := hashJoin.resetResultBat(); err != nil {
		return err
	}
	rowCnt := 0

	for ; rowCnt < colexec.DefaultBatchSize && ctr.rightMatchedIter.HasNext(); rowCnt++ {
		row := ctr.rightMatchedIter.Next()
		idx1, idx2 := row/colexec.DefaultBatchSize, row%colexec.DefaultBatchSize

		for i, rp := range hashJoin.ResultCols {
			if rp.Rel == 1 {
				err := ctr.resBat.Vecs[i].UnionOne(ctr.rightBats[idx1].Vecs[rp.Pos], int64(idx2), proc.Mp())
				if err != nil {
					return err
				}
			}
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
			err := ctr.resBat.Vecs[j].UnionOne(ctr.leftBat.Vecs[rp.Pos], row, proc.Mp())
			if err != nil {
				return err
			}
		} else {
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
			err := ctr.resBat.Vecs[j].UnionOne(ctr.leftBat.Vecs[rp.Pos], leftRow, proc.Mp())
			if err != nil {
				return err
			}
		} else {
			err := ctr.resBat.Vecs[j].UnionOne(ctr.rightBats[rIdx1].Vecs[rp.Pos], rIdx2, proc.Mp())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (ctr *container) appendOneMark(
	hashJoin *HashJoin,
	proc *process.Process,
	leftRow int64,
	value bool,
	isNull bool,
) error {
	for i, rp := range hashJoin.ResultCols {
		switch rp.Rel {
		case 0:
			if err := ctr.resBat.Vecs[i].UnionOne(ctr.leftBat.Vecs[rp.Pos], leftRow, proc.Mp()); err != nil {
				return err
			}
		case -1:
			if err := vector.AppendFixed(ctr.resBat.Vecs[i], value, isNull, proc.Mp()); err != nil {
				return err
			}
		default:
			return moerr.NewInternalErrorNoCtxf("hash mark join has unexpected result relation %d", rp.Rel)
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

func (hashJoin *HashJoin) resetResultBat() error {
	ctr := &hashJoin.ctr
	if ctr.resBat != nil {
		ctr.resBat.CleanOnlyData()
		for i := range ctr.resBat.Vecs {
			ctr.resBat.Vecs[i].SetClass(vector.FLAT)
			ctr.resBat.Vecs[i].SetLength(0)
		}
	} else {
		ctr.resBat = batch.NewOffHeapWithSize(len(hashJoin.ResultCols))

		for i, rp := range hashJoin.ResultCols {
			switch rp.Rel {
			case 0:
				ctr.resBat.Vecs[i] = vector.NewOffHeapVecWithType(hashJoin.LeftTypes[rp.Pos])
			case 1:
				ctr.resBat.Vecs[i] = vector.NewOffHeapVecWithType(hashJoin.RightTypes[rp.Pos])
			case -1:
				ctr.resBat.Vecs[i] = vector.NewOffHeapVecWithType(types.T_bool.ToType())
			}
		}
		if err := ctr.resBat.SetAllocationAccount(hashJoin.resultAllocation); err != nil {
			ctr.resBat.Clean(nil)
			ctr.resBat = nil
			return err
		}
	}
	return nil
}
