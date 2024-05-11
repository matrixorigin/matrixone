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

const argName = "join"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": inner join ")
}

func (arg *Argument) Prepare(proc *process.Process) (err error) {
	arg.ctr = new(container)
	arg.ctr.InitReceiver(proc, false)
	arg.ctr.inBuckets = make([]uint8, hashmap.UnitLimit)
	arg.ctr.vecs = make([]*vector.Vector, len(arg.Conditions[0]))
	arg.ctr.evecs = make([]evalVector, len(arg.Conditions[0]))
	for i := range arg.ctr.evecs {
		arg.ctr.evecs[i].executor, err = colexec.NewExpressionExecutor(proc, arg.Conditions[0][i])
		if err != nil {
			return err
		}
	}

	if arg.Cond != nil {
		arg.ctr.expr, err = colexec.NewExpressionExecutor(proc, arg.Cond)
	}
	return err
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	anal.Start()
	defer anal.Stop()
	ctr := arg.ctr
	result := vm.NewCallResult()
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(anal); err != nil {
				return result, err
			}
			if ctr.mp == nil && !arg.IsShuffle {
				// for inner ,right and semi join, if hashmap is empty, we can finish this pipeline
				// shuffle join can't stop early for this moment
				ctr.state = End
			} else {
				ctr.state = Probe
			}
		case Probe:
			if arg.bat == nil {
				bat, _, err := ctr.ReceiveFromSingleReg(0, anal)
				if err != nil {
					return result, err
				}
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
				arg.bat = bat
				arg.lastrow = 0
			}

			startrow := arg.lastrow
			if err := ctr.probe(arg, proc, anal, arg.GetIsFirst(), arg.GetIsLast(), &result); err != nil {
				return result, err
			}
			if arg.lastrow == 0 {
				proc.PutBatch(arg.bat)
				arg.bat = nil
			} else if arg.lastrow == startrow {
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

func (ctr *container) receiveHashMap(anal process.Analyze) error {
	bat, _, err := ctr.ReceiveFromSingleReg(1, anal)
	if err != nil {
		return err
	}
	if bat != nil && bat.AuxData != nil {
		ctr.mp = bat.DupJmAuxData()
		ctr.maxAllocSize = max(ctr.maxAllocSize, ctr.mp.Size())
	}
	return nil
}

func (ctr *container) receiveBatch(anal process.Analyze) error {
	for {
		bat, _, err := ctr.ReceiveFromSingleReg(1, anal)
		if err != nil {
			return err
		}
		if bat != nil {
			ctr.batchRowCount += bat.RowCount()
			ctr.batches = append(ctr.batches, bat)
		} else {
			break
		}
	}
	for i := 0; i < len(ctr.batches)-1; i++ {
		if ctr.batches[i].RowCount() != colexec.DefaultBatchSize {
			panic("wrong batch received for hash build!")
		}
	}
	return nil
}

func (ctr *container) build(anal process.Analyze) error {
	err := ctr.receiveHashMap(anal)
	if err != nil {
		return err
	}
	return ctr.receiveBatch(anal)
}

func (ctr *container) probe(ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool, result *vm.CallResult) error {

	anal.Input(ap.bat, isFirst)
	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	ctr.rbat = batch.NewWithSize(len(ap.Result))
	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			ctr.rbat.Vecs[i] = proc.GetVector(*ap.bat.Vecs[rp.Pos].GetType())
			// for inner join, if left batch is sorted , then output batch is sorted
			ctr.rbat.Vecs[i].SetSorted(ap.bat.Vecs[rp.Pos].GetSorted())
		} else {
			ctr.rbat.Vecs[i] = proc.GetVector(*ctr.batches[0].Vecs[rp.Pos].GetType())
		}
	}

	if err := ctr.evalJoinCondition(ap.bat, proc); err != nil {
		return err
	}
	if ctr.joinBat1 == nil {
		ctr.joinBat1, ctr.cfs1 = colexec.NewJoinBatch(ap.bat, proc.Mp())
	}
	if ctr.joinBat2 == nil && ctr.batchRowCount > 0 {
		ctr.joinBat2, ctr.cfs2 = colexec.NewJoinBatch(ctr.batches[0], proc.Mp())
	}

	mSels := ctr.mp.Sels()
	count := ap.bat.RowCount()
	itr := ctr.mp.NewIterator()
	rowCount := 0
	for i := ap.lastrow; i < count; i += hashmap.UnitLimit {
		if rowCount >= colexec.DefaultBatchSize {
			ctr.rbat.AddRowCount(rowCount)
			anal.Output(ctr.rbat, isLast)
			result.Batch = ctr.rbat
			ap.lastrow = i
			return nil
		}
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		copy(ctr.inBuckets, hashmap.OneUInt8s)

		vals, zvals := itr.Find(i, n, ctr.vecs, ctr.inBuckets)
		for k := 0; k < n; k++ {
			if ctr.inBuckets[k] == 0 || zvals[k] == 0 || vals[k] == 0 {
				continue
			}
			idx := vals[k] - 1

			if ap.Cond == nil {
				if ap.HashOnPK {
					for j, rp := range ap.Result {
						if rp.Rel == 0 {
							if err := ctr.rbat.Vecs[j].UnionOne(ap.bat.Vecs[rp.Pos], int64(i+k), proc.Mp()); err != nil {
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
							if err := ctr.rbat.Vecs[j].UnionMulti(ap.bat.Vecs[rp.Pos], int64(i+k), len(sels), proc.Mp()); err != nil {
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
					if err := ctr.evalApCondForOneSel(ap.bat, ctr.rbat, ap, proc, int64(i+k), int64(idx)); err != nil {
						return err
					}
					rowCount++
				} else {
					sels := mSels[idx]
					for _, sel := range sels {
						if err := ctr.evalApCondForOneSel(ap.bat, ctr.rbat, ap, proc, int64(i+k), int64(sel)); err != nil {
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
	ap.lastrow = 0
	return nil
}

func (ctr *container) evalApCondForOneSel(bat, rbat *batch.Batch, ap *Argument, proc *process.Process, row, sel int64) error {
	if err := colexec.SetJoinBatchValues(ctr.joinBat1, bat, row,
		1, ctr.cfs1); err != nil {
		return err
	}
	idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
	if err := colexec.SetJoinBatchValues(ctr.joinBat2, ctr.batches[idx1], idx2,
		1, ctr.cfs2); err != nil {
		return err
	}
	vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat1, ctr.joinBat2})
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
		vec, err := ctr.evecs[i].executor.Eval(proc, []*batch.Batch{bat})
		if err != nil {
			return err
		}
		ctr.vecs[i] = vec
		ctr.evecs[i].vec = vec
	}
	return nil
}
