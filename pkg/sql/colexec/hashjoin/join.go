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
	"time"

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
	}
}

func (hashJoin *HashJoin) OpType() vm.OpType {
	return vm.HashJoin
}

func (hashJoin *HashJoin) Prepare(proc *process.Process) (err error) {
	if hashJoin.OpAnalyzer == nil {
		hashJoin.OpAnalyzer = process.NewAnalyzer(hashJoin.GetIdx(), hashJoin.IsFirst, hashJoin.IsLast, opName)
	} else {
		hashJoin.OpAnalyzer.Reset()
	}

	if len(hashJoin.ctr.joinBats) == 0 {
		hashJoin.ctr.joinBats = make([]*batch.Batch, 2)
	}

	if len(hashJoin.ctr.eqCondVecs) == 0 {
		hashJoin.ctr.eqCondVecs = make([]*vector.Vector, len(hashJoin.EqConds[0]))
		hashJoin.ctr.eqCondExecs = make([]colexec.ExpressionExecutor, len(hashJoin.EqConds[0]))
		hashJoin.ctr.eqCondExecs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, hashJoin.EqConds[0])
		if err != nil {
			return err
		}

		if hashJoin.NonEqCond != nil {
			hashJoin.ctr.nonEqCondExec, err = colexec.NewExpressionExecutor(proc, hashJoin.NonEqCond)
			if err != nil {
				return err
			}
		}
	}

	hashJoin.ctr.handledLast = false

	return err
}

func (hashJoin *HashJoin) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := hashJoin.OpAnalyzer

	ctr := &hashJoin.ctr
	result := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {
		case Build:
			err = hashJoin.build(analyzer, proc)
			if err != nil {
				return result, err
			}

			if ctr.mp == nil && !hashJoin.IsShuffle {
				// for inner ,right and semi join, if hashmap is empty, we can finish this pipeline
				// shuffle join can't stop early for this moment
				if hashJoin.JoinType == plan.Node_INNER || hashJoin.JoinType == plan.Node_RIGHT || hashJoin.JoinType == plan.Node_SEMI {
					ctr.state = End
					continue
				}
			}

			ctr.state = Probe

		case Probe:
			if hashJoin.ctr.inbat == nil {
				result, err = vm.ChildrenCall(hashJoin.GetChildren(0), proc, analyzer)
				if err != nil {
					return result, err
				}
				bat := result.Batch

				if bat == nil {
					if hashJoin.IsRightJoin && (hashJoin.JoinType == plan.Node_RIGHT || hashJoin.JoinType == plan.Node_SEMI || hashJoin.JoinType == plan.Node_ANTI) {
						ctr.state = Finalize
					} else {
						ctr.state = End
					}

					continue
				}

				if bat.IsEmpty() {
					continue
				}

				if ctr.mp == nil && hashJoin.JoinType != plan.Node_LEFT {
					continue
				}

				ctr.inbat = bat
				ctr.lastRow = 0
			}

			hashJoin.resetRBat()
			for i, rp := range hashJoin.ResultCols {
				if rp.Rel == 0 {
					ctr.rbat.Vecs[i].SetSorted(ctr.inbat.Vecs[rp.Pos].GetSorted())
				}
			}

			startRow := ctr.lastRow
			if ctr.mp == nil {
				err = ctr.emptyProbe(hashJoin, proc, &result)
			} else {
				err = ctr.probe(hashJoin, proc, &result)
			}
			if err != nil {
				return result, err
			}
			if hashJoin.ctr.lastRow == startRow && ctr.inbat != nil &&
				(result.Batch == nil || result.Batch.IsEmpty()) {
				return result, moerr.NewInternalErrorNoCtx("hash join hanging")
			}

			return result, nil

		case Finalize:
			err := ctr.finalize(hashJoin, proc, &result)
			if err != nil {
				return result, err
			}

			ctr.state = End
			if result.Batch == nil {
				continue
			}

			result.Status = vm.ExecNext
			return result, nil

		default:
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

	if ctr.mp != nil {
		ctr.maxAllocSize = max(ctr.maxAllocSize, ctr.mp.Size())
	}
	ctr.batches = ctr.mp.GetBatches()
	ctr.batchRowCount = ctr.mp.GetRowCount()

	if hashJoin.IsRightJoin {
		if ctr.batchRowCount > 0 {
			ctr.rightMatched = &bitmap.Bitmap{}
			ctr.rightMatched.InitWithSize(ctr.batchRowCount)
		}
	}

	return nil
}

func (ctr *container) finalize(ap *HashJoin, proc *process.Process, result *vm.CallResult) error {
	ctr.handledLast = true

	if ctr.rightMatched == nil {
		result.Batch = nil
		return nil
	}

	if ap.NumCPU > 1 {
		if !ap.IsMerger {
			ap.Channel <- ctr.rightMatched
			result.Batch = nil
			return nil
		} else {
			for cnt := 1; cnt < int(ap.NumCPU); cnt++ {
				v := colexec.ReceiveBitmapFromChannel(proc.Ctx, ap.Channel)
				if v != nil {
					ctr.rightMatched.Or(v)
				} else {
					result.Batch = nil
					return nil
				}
			}
			close(ap.Channel)
		}
	}

	count := ctr.batchRowCount - int64(ctr.rightMatched.Count())
	ctr.rightMatched.Negate()
	sels := make([]int32, 0, count)
	itr := ctr.rightMatched.Iterator()
	for itr.HasNext() {
		r := itr.Next()
		sels = append(sels, int32(r))
	}

	ap.resetRBat()
	if err := ctr.rbat.PreExtend(proc.Mp(), len(sels)); err != nil {
		return err
	}

	for i, rp := range ap.ResultCols {
		if rp.Rel == 0 {
			if err := vector.AppendMultiFixed(ctr.rbat.Vecs[i], 0, true, int(count), proc.Mp()); err != nil {
				return err
			}
		} else {
			for _, sel := range sels {
				idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
				if err := ctr.rbat.Vecs[i].UnionOne(ctr.batches[idx1].Vecs[rp.Pos], int64(idx2), proc.Mp()); err != nil {
					return err
				}
			}
		}
	}

	ctr.rbat.AddRowCount(len(sels))
	result.Batch = ctr.rbat
	return nil
}

func (ctr *container) probe(ap *HashJoin, proc *process.Process, result *vm.CallResult) error {
	if err := ctr.evalJoinCondition(ap.ctr.inbat, proc); err != nil {
		return err
	}

	if ctr.joinBats[0] == nil {
		ctr.joinBats[0], ctr.cfs1 = colexec.NewJoinBatch(ap.ctr.inbat, proc.Mp())
	}
	if ctr.joinBats[1] == nil && ctr.batchRowCount > 0 {
		ctr.joinBats[1], ctr.cfs2 = colexec.NewJoinBatch(ctr.batches[0], proc.Mp())
	}

	if ctr.itr == nil {
		ctr.itr = ctr.mp.NewIterator()
	}
	inputRowCount := ap.ctr.inbat.RowCount()
	rowCount := 0

	for {
		switch ctr.probeState {
		case psNextBatch:
			if ctr.lastRow < inputRowCount {
				hashBatch := min(inputRowCount-ctr.lastRow, hashmap.UnitLimit)
				ctr.vs, ctr.zvs = ctr.itr.Find(ctr.lastRow, hashBatch, ctr.eqCondVecs)
				ctr.vsidx = 0
				ctr.probeState = psBatchRow
			} else {
				ctr.rbat.AddRowCount(rowCount)
				result.Batch = ctr.rbat
				ap.ctr.lastRow = 0
				ap.ctr.inbat = nil
				ctr.probeState = psNextBatch
				return nil
			}

		case psSelsForOneRow:
			row := int64(ctr.lastRow - 1)
			processCount := min(len(ctr.sels), colexec.DefaultBatchSize-rowCount)
			sels := ctr.sels[:processCount]
			// remove processed sels
			ctr.sels = ctr.sels[processCount:]
			if ap.NonEqCond == nil {
				for j, rp := range ap.ResultCols {
					if rp.Rel == 0 {
						if err := ctr.rbat.Vecs[j].UnionMulti(ap.ctr.inbat.Vecs[rp.Pos], row, processCount, proc.Mp()); err != nil {
							return err
						}
					} else {
						for _, sel := range sels {
							idx1 := sel / colexec.DefaultBatchSize
							idx2 := sel % colexec.DefaultBatchSize
							if err := ctr.rbat.Vecs[j].UnionOne(ctr.batches[idx1].Vecs[rp.Pos], int64(idx2), proc.Mp()); err != nil {
								return err
							}
						}
					}
				}

				if ap.IsRightJoin {
					for _, sel := range sels {
						ctr.rightMatched.Add(uint64(sel))
					}
				}

				rowCount += processCount
			} else {
				for _, sel := range sels {
					idx1 := int64(sel / colexec.DefaultBatchSize)
					idx2 := int64(sel % colexec.DefaultBatchSize)
					ok, err := ctr.evalNonEqCondition(ap.ctr.inbat, int64(row), proc, idx1, idx2)
					if err != nil {
						return err
					}

					if ok {
						if ap.JoinType == plan.Node_SINGLE && ctr.leftMatched {
							return moerr.NewInternalError(proc.Ctx, "scalar subquery returns more than 1 row")
						}

						ctr.leftMatched = true
						ctr.appendOneMatch(ap, proc, int64(row), idx1, idx2)
						if ap.IsRightJoin {
							ctr.rightMatched.Add(uint64(sel))
						}
						rowCount++
					}
				}
			}

			if ap.NonEqCond != nil && len(ctr.sels) == 0 &&
				!ctr.leftMatched && ap.JoinType == plan.Node_LEFT {
				ctr.appendOneNotMatch(ap, proc, int64(row))
				rowCount++
			}

			if len(ctr.sels) > 0 {
				ctr.probeState = psSelsForOneRow
			} else if ctr.vsidx < len(ctr.vs) {
				ctr.probeState = psBatchRow
			} else {
				ctr.probeState = psNextBatch
			}

			if rowCount >= colexec.DefaultBatchSize {
				ctr.rbat.AddRowCount(rowCount)
				result.Batch = ctr.rbat
				return nil
			}

		case psBatchRow:
			z, v := ctr.zvs[ctr.vsidx], ctr.vs[ctr.vsidx]
			row := int64(ctr.lastRow)
			idx := int64(v) - 1
			ctr.lastRow++
			ctr.vsidx++
			if z == 0 || v == 0 {
				if ap.JoinType == plan.Node_LEFT {
					ctr.appendOneNotMatch(ap, proc, row)
					rowCount++
				}

				if ctr.vsidx >= len(ctr.vs) {
					ctr.probeState = psNextBatch
				}

				if rowCount >= colexec.DefaultBatchSize {
					ctr.rbat.AddRowCount(rowCount)
					result.Batch = ctr.rbat
					return nil
				}

				continue
			}

			if ap.HashOnPK || ctr.mp.HashOnUnique() {
				idx1 := idx / colexec.DefaultBatchSize
				idx2 := idx % colexec.DefaultBatchSize
				if ap.NonEqCond == nil {
					if err := ctr.appendOneMatch(ap, proc, row, idx1, idx2); err != nil {
						return err
					}
					if ap.IsRightJoin {
						ctr.rightMatched.Add(uint64(idx))
					}
					rowCount++
				} else {
					ok, err := ctr.evalNonEqCondition(
						ap.ctr.inbat, row, proc, idx1, idx2,
					)
					if err != nil {
						return err
					}
					if ok {
						if err := ctr.appendOneMatch(ap, proc, row, idx1, idx2); err != nil {
							return err
						}
						if ap.IsRightJoin {
							ctr.rightMatched.Add(uint64(idx))
						}
						rowCount++
					} else if ap.JoinType == plan.Node_LEFT {
						if err := ctr.appendOneNotMatch(ap, proc, row); err != nil {
							return err
						}
						rowCount++
					}
				}
			} else {
				ctr.sels = ctr.mp.GetSels(uint64(idx))
				ctr.leftMatched = false

				if ap.JoinType == plan.Node_SINGLE && ap.NonEqCond == nil && len(ctr.sels) > 1 {
					return moerr.NewInternalError(proc.Ctx, "scalar subquery returns more than 1 row")
				}
			}

			if len(ctr.sels) > 0 {
				ctr.probeState = psSelsForOneRow
			} else if ctr.vsidx < len(ctr.vs) {
				ctr.probeState = psBatchRow
			} else {
				ctr.probeState = psNextBatch
			}

			if rowCount >= colexec.DefaultBatchSize {
				ctr.rbat.AddRowCount(rowCount)
				result.Batch = ctr.rbat
				return nil
			}
		}
	}
}

func (ctr *container) emptyProbe(ap *HashJoin, proc *process.Process, result *vm.CallResult) error {
	for i, rp := range ap.ResultCols {
		if rp.Rel == 0 {
			if err := vector.GetUnionAllFunction(
				*ctr.rbat.Vecs[i].GetType(),
				proc.Mp(),
			)(ctr.rbat.Vecs[i], ap.ctr.inbat.Vecs[rp.Pos]); err != nil {
				return err
			}
		} else {
			ctr.rbat.Vecs[i].SetClass(vector.CONSTANT)
			ctr.rbat.Vecs[i].SetLength(ctr.inbat.RowCount())
		}
	}
	ctr.rbat.AddRowCount(ap.ctr.inbat.RowCount())
	result.Batch = ctr.rbat
	ap.ctr.lastRow = 0
	ap.ctr.inbat = nil
	return nil
}

func (ctr *container) appendOneNotMatch(ap *HashJoin, proc *process.Process, row int64) error {
	for j, rp := range ap.ResultCols {
		if rp.Rel == 0 {
			if err := ctr.rbat.Vecs[j].UnionOne(
				ap.ctr.inbat.Vecs[rp.Pos],
				row,
				proc.Mp(),
			); err != nil {
				return err
			}
		} else {
			if err := ctr.rbat.Vecs[j].UnionNull(
				proc.Mp(),
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ctr *container) appendOneMatch(ap *HashJoin, proc *process.Process, leftRow, rIdx1, rIdx2 int64) error {
	for j, rp := range ap.ResultCols {
		if rp.Rel == 0 {
			if err := ctr.rbat.Vecs[j].UnionOne(
				ap.ctr.inbat.Vecs[rp.Pos], leftRow, proc.Mp(),
			); err != nil {
				return err
			}
		} else {
			if err := ctr.rbat.Vecs[j].UnionOne(
				ctr.batches[rIdx1].Vecs[rp.Pos], rIdx2, proc.Mp(),
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ctr *container) evalNonEqCondition(
	bat *batch.Batch, row int64, proc *process.Process, idx1, idx2 int64,
) (bool, error) {
	mpbat := ctr.mp.GetBatches()

	if err := colexec.SetJoinBatchValues(
		ctr.joinBats[0], bat, row, 1, ctr.cfs1,
	); err != nil {
		return false, err
	}

	if err := colexec.SetJoinBatchValues(
		ctr.joinBats[1], mpbat[idx1], idx2, 1, ctr.cfs2,
	); err != nil {
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
	for i := range ctr.eqCondExecs {
		vec, err := ctr.eqCondExecs[i].Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			return err
		}
		ctr.eqCondVecs[i] = vec
	}
	return nil
}

func (hashJoin *HashJoin) resetRBat() {
	ctr := &hashJoin.ctr
	if ctr.rbat != nil {
		ctr.rbat.CleanOnlyData()
	} else {
		ctr.rbat = batch.NewWithSize(len(hashJoin.ResultCols))

		for i, rp := range hashJoin.ResultCols {
			if rp.Rel == 0 {
				ctr.rbat.Vecs[i] = vector.NewOffHeapVecWithType(hashJoin.LeftTypes[rp.Pos])
			} else {
				ctr.rbat.Vecs[i] = vector.NewOffHeapVecWithType(hashJoin.RightTypes[rp.Pos])
			}
		}
	}
}
