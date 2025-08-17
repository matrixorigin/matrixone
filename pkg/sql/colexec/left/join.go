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

package left

import (
	"bytes"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "left"

func (leftJoin *LeftJoin) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": left join ")
}

func (leftJoin *LeftJoin) OpType() vm.OpType {
	return vm.Left
}

func (leftJoin *LeftJoin) Prepare(proc *process.Process) (err error) {
	if leftJoin.OpAnalyzer == nil {
		leftJoin.OpAnalyzer = process.NewAnalyzer(leftJoin.GetIdx(), leftJoin.IsFirst, leftJoin.IsLast, "left join")
	} else {
		leftJoin.OpAnalyzer.Reset()
	}

	if len(leftJoin.ctr.joinBats) == 0 {
		leftJoin.ctr.joinBats = make([]*batch.Batch, 2)
	}
	if leftJoin.ctr.vecs == nil {
		leftJoin.ctr.vecs = make([]*vector.Vector, len(leftJoin.Conditions[0]))
		leftJoin.ctr.executor = make([]colexec.ExpressionExecutor, len(leftJoin.Conditions[0]))
		for i := range leftJoin.ctr.executor {
			leftJoin.ctr.executor[i], err = colexec.NewExpressionExecutor(proc, leftJoin.Conditions[0][i])
			if err != nil {
				return err
			}
		}
		if leftJoin.Cond != nil {
			leftJoin.ctr.expr, err = colexec.NewExpressionExecutor(proc, leftJoin.Cond)
			if err != nil {
				return err
			}
		}
	}
	return err
}

func (leftJoin *LeftJoin) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := leftJoin.OpAnalyzer

	ctr := &leftJoin.ctr
	input := vm.NewCallResult()
	result := vm.NewCallResult()
	probeResult := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {
		case Build:
			err = leftJoin.build(analyzer, proc)
			if err != nil {
				return result, err
			}
			ctr.state = Probe

		case Probe:
			if leftJoin.ctr.inbat == nil {
				input, err = vm.ChildrenCall(leftJoin.GetChildren(0), proc, analyzer)
				if err != nil {
					return result, err

				}
				bat := input.Batch
				if bat == nil {
					ctr.state = End
					continue
				}
				if bat.IsEmpty() {
					continue
				}
				ctr.inbat = bat
				ctr.lastRow = 0
			}

			if ctr.rbat == nil {
				ctr.rbat = batch.NewWithSize(len(leftJoin.Result))
				for i, rp := range leftJoin.Result {
					if rp.Rel == 0 {
						ctr.rbat.Vecs[i] = vector.NewVec(*ctr.inbat.Vecs[rp.Pos].GetType())
						// for left join, if left batch is sorted , then output batch is sorted
						ctr.rbat.Vecs[i].SetSorted(ctr.inbat.Vecs[rp.Pos].GetSorted())
					} else {
						ctr.rbat.Vecs[i] = vector.NewVec(leftJoin.Typs[rp.Pos])
					}
				}
			} else {
				ctr.rbat.CleanOnlyData()
				for i, rp := range leftJoin.Result {
					if rp.Rel == 0 {
						ctr.rbat.Vecs[i].SetSorted(ctr.inbat.Vecs[rp.Pos].GetSorted())
					}
				}
			}

			startRow := leftJoin.ctr.lastRow
			if ctr.mp == nil {
				err = ctr.emptyProbe(leftJoin, proc, &probeResult)
			} else {
				err = ctr.probe(leftJoin, proc, &probeResult)
			}
			if err != nil {
				return result, err
			}
			if leftJoin.ctr.lastRow == startRow && ctr.inbat != nil &&
				(probeResult.Batch == nil || probeResult.Batch.IsEmpty()) {
				return result, moerr.NewInternalErrorNoCtx("left join hanging")
			}

			result.Batch = probeResult.Batch
			return result, nil

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (leftJoin *LeftJoin) build(analyzer process.Analyzer, proc *process.Process) (err error) {
	ctr := &leftJoin.ctr
	start := time.Now()
	defer analyzer.WaitStop(start)
	ctr.mp, err = message.ReceiveJoinMap(leftJoin.JoinMapTag, leftJoin.IsShuffle, leftJoin.ShuffleIdx, proc.GetMessageBoard(), proc.Ctx)
	if err != nil {
		return err
	}
	if ctr.mp != nil {
		ctr.maxAllocSize = max(ctr.maxAllocSize, ctr.mp.Size())
	}
	ctr.batchRowCount = ctr.mp.GetRowCount()
	return nil
}

func (ctr *container) emptyProbe(ap *LeftJoin, proc *process.Process, result *vm.CallResult) error {

	for i, rp := range ap.Result {
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

func (ctr *container) probe(ap *LeftJoin, proc *process.Process, result *vm.CallResult) error {
	mpbat := ctr.mp.GetBatches()

	if err := ctr.evalJoinCondition(ap.ctr.inbat, proc); err != nil {
		return err
	}

	if ctr.joinBats[0] == nil {
		ctr.joinBats[0], ctr.cfs1 = colexec.NewJoinBatch(ap.ctr.inbat, proc.Mp())
	}
	if ctr.joinBats[1] == nil && ctr.batchRowCount > 0 {
		ctr.joinBats[1], ctr.cfs2 = colexec.NewJoinBatch(mpbat[0], proc.Mp())
	}

	if ctr.itr == nil {
		ctr.itr = ctr.mp.NewIterator()
	}
	inputRowCount := ap.ctr.inbat.RowCount()
	rowCount := 0

	appendOneNotMatch := func(row int64) error {
		for j, rp := range ap.Result {
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

	appendOneMatch := func(leftRow, rIdx1, rIdx2 int64) error {
		for j, rp := range ap.Result {
			if rp.Rel == 0 {
				if err := ctr.rbat.Vecs[j].UnionOne(
					ap.ctr.inbat.Vecs[rp.Pos], leftRow, proc.Mp(),
				); err != nil {
					return err
				}
			} else {
				if err := ctr.rbat.Vecs[j].UnionOne(
					mpbat[rIdx1].Vecs[rp.Pos], rIdx2, proc.Mp(),
				); err != nil {
					return err
				}
			}
		}
		return nil
	}

	for {
		switch ctr.probeState {
		case psSelsForOneRow:
			row := int64(ctr.lastRow - 1)
			processCount := min(len(ctr.sels), colexec.DefaultBatchSize-rowCount)
			sels := ctr.sels[:processCount]
			// remove processed sels
			ctr.sels = ctr.sels[processCount:]
			if ap.Cond == nil {
				for j, rp := range ap.Result {
					if rp.Rel == 0 {
						if err := ctr.rbat.Vecs[j].UnionMulti(
							ap.ctr.inbat.Vecs[rp.Pos],
							row,
							len(sels),
							proc.Mp(),
						); err != nil {
							return err
						}
					} else {
						for _, sel := range sels {
							idx1 := sel / colexec.DefaultBatchSize
							idx2 := sel % colexec.DefaultBatchSize
							if err := ctr.rbat.Vecs[j].UnionOne(
								mpbat[idx1].Vecs[rp.Pos],
								int64(idx2),
								proc.Mp(),
							); err != nil {
								return err
							}
						}
					}
				}
				rowCount += processCount
			} else {
				for _, sel := range sels {
					idx1 := int64(sel / colexec.DefaultBatchSize)
					idx2 := int64(sel % colexec.DefaultBatchSize)
					ok, err := ctr.evalNonEqCondition(
						ap.ctr.inbat, int64(row), proc, idx1, idx2,
					)
					if err != nil {
						return err
					}
					if ok {
						ctr.anySelNEqMatchForOneRow = true
						appendOneMatch(int64(row), idx1, idx2)
						rowCount++
					}
				}
			}

			if ap.Cond != nil && len(ctr.sels) == 0 &&
				!ctr.anySelNEqMatchForOneRow {
				appendOneNotMatch(int64(row))
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
				appendOneNotMatch(int64(row))
				rowCount++
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
				if ap.Cond == nil {
					if err := appendOneMatch(row, idx1, idx2); err != nil {
						return err
					}
				} else {
					ok, err := ctr.evalNonEqCondition(
						ap.ctr.inbat, row, proc, idx1, idx2,
					)
					if err != nil {
						return err
					}
					if ok {
						if err := appendOneMatch(row, idx1, idx2); err != nil {
							return err
						}
					} else {
						if err := appendOneNotMatch(row); err != nil {
							return err
						}
					}
				}
				rowCount++
			} else {
				ctr.sels = ctr.mp.GetSels(uint64(idx))
				ctr.anySelNEqMatchForOneRow = false
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

		case psNextBatch:
			if ctr.lastRow < inputRowCount {
				hashBatch := min(inputRowCount-ctr.lastRow, hashmap.UnitLimit)
				ctr.vs, ctr.zvs = ctr.itr.Find(ctr.lastRow, hashBatch, ctr.vecs)
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
		}
	}
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
	vec, err := ctr.expr.Eval(proc, ctr.joinBats, nil)
	if err != nil {
		return false, err
	}
	return !vec.IsConstNull() &&
		!vec.GetNulls().Contains(0) &&
		vector.MustFixedColWithTypeCheck[bool](vec)[0], nil
}

func (ctr *container) evalJoinCondition(bat *batch.Batch, proc *process.Process) error {
	for i := range ctr.executor {
		vec, err := ctr.executor[i].Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			return err
		}
		ctr.vecs[i] = vec
	}
	return nil
}
