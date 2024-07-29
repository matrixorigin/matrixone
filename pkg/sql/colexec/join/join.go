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

package join

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "join"

func (innerJoin *InnerJoin) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": inner join ")
}

func (innerJoin *InnerJoin) OpType() vm.OpType {
	return vm.Join
}

func (innerJoin *InnerJoin) Prepare(proc *process.Process) (err error) {
	innerJoin.ctr = new(container)
	innerJoin.ctr.InitReceiver(proc, true)
	innerJoin.ctr.vecs = make([]*vector.Vector, len(innerJoin.Conditions[0]))
	innerJoin.ctr.evecs = make([]evalVector, len(innerJoin.Conditions[0]))
	for i := range innerJoin.ctr.evecs {
		innerJoin.ctr.evecs[i].executor, err = colexec.NewExpressionExecutor(proc, innerJoin.Conditions[0][i])
		if err != nil {
			return err
		}
	}

	if innerJoin.Cond != nil {
		innerJoin.ctr.expr, err = colexec.NewExpressionExecutor(proc, innerJoin.Cond)
	}
	return err
}

func (innerJoin *InnerJoin) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(innerJoin.GetIdx(), innerJoin.GetParallelIdx(), innerJoin.GetParallelMajor())
	anal.Start()
	defer anal.Stop()
	ctr := innerJoin.ctr
	result := vm.NewCallResult()
	for {
		switch ctr.state {
		case Build:
			innerJoin.build(anal, proc)

			if ctr.mp == nil && !innerJoin.IsShuffle {
				// for inner ,right and semi join, if hashmap is empty, we can finish this pipeline
				// shuffle join can't stop early for this moment
				ctr.state = End
			} else {
				ctr.state = Probe
			}
		case Probe:
			if innerJoin.ctr.bat == nil {
				msg := ctr.ReceiveFromAllRegs(anal)
				if msg.Err != nil {
					return result, msg.Err
				}
				bat := msg.Batch
				if bat == nil {
					ctr.state = End
					continue
				}
				if bat.Last() {
					result.Batch = bat
					return result, nil
				}
				if bat.IsEmpty() {
					proc.PutBatch(bat)
					continue
				}
				if ctr.mp == nil {
					proc.PutBatch(bat)
					continue
				}
				innerJoin.ctr.bat = bat
				innerJoin.ctr.lastrow = 0
			}

			startrow := innerJoin.ctr.lastrow
			if err := ctr.probe(innerJoin, proc, anal, innerJoin.GetIsFirst(), innerJoin.GetIsLast(), &result); err != nil {
				return result, err
			}
			if innerJoin.ctr.lastrow == 0 {
				proc.PutBatch(innerJoin.ctr.bat)
				innerJoin.ctr.bat = nil
			} else if innerJoin.ctr.lastrow == startrow {
				return result, moerr.NewInternalErrorNoCtx("inner join hanging")
			}
			return result, nil

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (innerJoin *InnerJoin) build(anal process.Analyze, proc *process.Process) {
	ctr := innerJoin.ctr
	ctr.mp = proc.ReceiveJoinMap(anal, innerJoin.JoinMapTag, innerJoin.IsShuffle, innerJoin.ShuffleIdx)
	if ctr.mp != nil {
		ctr.maxAllocSize = max(ctr.maxAllocSize, ctr.mp.Size())
	}
	ctr.batches = ctr.mp.GetBatches()
	ctr.batchRowCount = ctr.mp.GetRowCount()
}

func (ctr *container) probe(ap *InnerJoin, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool, result *vm.CallResult) error {

	anal.Input(ap.ctr.bat, isFirst)
	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	ctr.rbat = batch.NewWithSize(len(ap.Result))
	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			ctr.rbat.Vecs[i] = proc.GetVector(*ap.ctr.bat.Vecs[rp.Pos].GetType())
			// for inner join, if left batch is sorted , then output batch is sorted
			ctr.rbat.Vecs[i].SetSorted(ap.ctr.bat.Vecs[rp.Pos].GetSorted())
		} else {
			ctr.rbat.Vecs[i] = proc.GetVector(*ctr.batches[0].Vecs[rp.Pos].GetType())
		}
	}

	if err := ctr.evalJoinCondition(ap.ctr.bat, proc); err != nil {
		return err
	}
	if ctr.joinBat1 == nil {
		ctr.joinBat1, ctr.cfs1 = colexec.NewJoinBatch(ap.ctr.bat, proc.Mp())
	}
	if ctr.joinBat2 == nil && ctr.batchRowCount > 0 {
		ctr.joinBat2, ctr.cfs2 = colexec.NewJoinBatch(ctr.batches[0], proc.Mp())
	}

	mSels := ctr.mp.Sels()
	count := ap.ctr.bat.RowCount()
	itr := ctr.mp.NewIterator()
	rowCount := 0
	for i := ap.ctr.lastrow; i < count; i += hashmap.UnitLimit {
		if rowCount >= colexec.DefaultBatchSize {
			ctr.rbat.AddRowCount(rowCount)
			anal.Output(ctr.rbat, isLast)
			result.Batch = ctr.rbat
			ap.ctr.lastrow = i
			return nil
		}
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		vals, zvals := itr.Find(i, n, ctr.vecs)
		for k := 0; k < n; k++ {
			if zvals[k] == 0 || vals[k] == 0 {
				continue
			}
			idx := vals[k] - 1

			if ap.Cond == nil {
				if ap.HashOnPK {
					for j, rp := range ap.Result {
						if rp.Rel == 0 {
							if err := ctr.rbat.Vecs[j].UnionOne(ap.ctr.bat.Vecs[rp.Pos], int64(i+k), proc.Mp()); err != nil {
								return err
							}
						} else {
							idx1, idx2 := idx/colexec.DefaultBatchSize, idx%colexec.DefaultBatchSize
							if err := ctr.rbat.Vecs[j].UnionOne(ctr.batches[idx1].Vecs[rp.Pos], int64(idx2), proc.Mp()); err != nil {
								return err
							}
						}
					}
					rowCount++
				} else {
					sels := mSels[idx]
					for j, rp := range ap.Result {
						if rp.Rel == 0 {
							if err := ctr.rbat.Vecs[j].UnionMulti(ap.ctr.bat.Vecs[rp.Pos], int64(i+k), len(sels), proc.Mp()); err != nil {
								return err
							}
						} else {
							for _, sel := range sels {
								idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
								if err := ctr.rbat.Vecs[j].UnionOne(ctr.batches[idx1].Vecs[rp.Pos], int64(idx2), proc.Mp()); err != nil {
									return err
								}
							}
						}
					}
					rowCount += len(sels)
				}
			} else {
				if ap.HashOnPK {
					if err := ctr.evalApCondForOneSel(ap.ctr.bat, ctr.rbat, ap, proc, int64(i+k), int64(idx)); err != nil {
						return err
					}
					rowCount++
				} else {
					sels := mSels[idx]
					for _, sel := range sels {
						if err := ctr.evalApCondForOneSel(ap.ctr.bat, ctr.rbat, ap, proc, int64(i+k), int64(sel)); err != nil {
							return err
						}
					}
					rowCount += len(sels)
				}
			}
		}
	}

	ctr.rbat.AddRowCount(rowCount)
	anal.Output(ctr.rbat, isLast)
	result.Batch = ctr.rbat
	ap.ctr.lastrow = 0
	return nil
}

func (ctr *container) evalApCondForOneSel(bat, rbat *batch.Batch, ap *InnerJoin, proc *process.Process, row, sel int64) error {
	if err := colexec.SetJoinBatchValues(ctr.joinBat1, bat, row,
		1, ctr.cfs1); err != nil {
		return err
	}
	idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
	if err := colexec.SetJoinBatchValues(ctr.joinBat2, ctr.batches[idx1], idx2,
		1, ctr.cfs2); err != nil {
		return err
	}
	vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat1, ctr.joinBat2}, nil)
	if err != nil {
		rbat.Clean(proc.Mp())
		return err
	}
	if vec.IsConstNull() || vec.GetNulls().Contains(0) {
		return nil
	}
	bs := vector.MustFixedCol[bool](vec)
	if !bs[0] {
		return nil
	}
	for j, rp := range ap.Result {
		if rp.Rel == 0 {
			if err := rbat.Vecs[j].UnionOne(bat.Vecs[rp.Pos], row, proc.Mp()); err != nil {
				rbat.Clean(proc.Mp())
				return err
			}
		} else {
			if err := rbat.Vecs[j].UnionOne(ctr.batches[idx1].Vecs[rp.Pos], idx2, proc.Mp()); err != nil {
				rbat.Clean(proc.Mp())
				return err
			}
		}
	}
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
