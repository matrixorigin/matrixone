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

package anti

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "anti"

func (antiJoin *AntiJoin) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": anti join ")
}

func (antiJoin *AntiJoin) OpType() vm.OpType {
	return vm.Anti
}

func (antiJoin *AntiJoin) Prepare(proc *process.Process) (err error) {
	antiJoin.ctr = new(container)
	antiJoin.ctr.InitReceiver(proc, true)
	antiJoin.ctr.vecs = make([]*vector.Vector, len(antiJoin.Conditions[0]))
	antiJoin.ctr.executorForVecs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, antiJoin.Conditions[0])
	if err != nil {
		return err
	}

	if antiJoin.Cond != nil {
		antiJoin.ctr.expr, err = colexec.NewExpressionExecutor(proc, antiJoin.Cond)
	}
	return err
}

func (antiJoin *AntiJoin) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(antiJoin.GetIdx(), antiJoin.GetParallelIdx(), antiJoin.GetParallelMajor())
	anal.Start()
	defer anal.Stop()
	ap := antiJoin
	result := vm.NewCallResult()
	ctr := ap.ctr
	for {
		switch ctr.state {
		case Build:
			antiJoin.build(anal, proc)
			ctr.state = Probe

		case Probe:
			if ap.ctr.bat == nil {
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
					continue
				}

				ap.ctr.bat = bat
				ap.ctr.lastrow = 0
			}

			if ctr.mp == nil {
				err := ctr.emptyProbe(ap, proc, anal, antiJoin.GetIsFirst(), antiJoin.GetIsLast(), &result)
				return result, err
			} else {
				err := ctr.probe(ap, proc, anal, antiJoin.GetIsFirst(), antiJoin.GetIsLast(), &result)
				if ap.ctr.lastrow == 0 {
					proc.PutBatch(ap.ctr.bat)
					ap.ctr.bat = nil
				}
				return result, err
			}

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (antiJoin *AntiJoin) build(anal process.Analyze, proc *process.Process) {
	ctr := antiJoin.ctr
	ctr.mp = proc.ReceiveJoinMap(anal, antiJoin.JoinMapTag, antiJoin.IsShuffle, antiJoin.ShuffleIdx)
	if ctr.mp != nil {
		ctr.maxAllocSize = max(ctr.maxAllocSize, ctr.mp.Size())
	}
	ctr.batches = ctr.mp.GetBatches()
	ctr.batchRowCount = ctr.mp.GetRowCount()
}

func (ctr *container) emptyProbe(ap *AntiJoin, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool, result *vm.CallResult) error {
	anal.Input(ap.ctr.bat, isFirst)
	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	ctr.rbat = batch.NewWithSize(len(ap.Result))
	for i, pos := range ap.Result {
		ctr.rbat.Vecs[i] = proc.GetVector(*ap.ctr.bat.Vecs[pos].GetType())
		// for anti join, if left batch is sorted , then output batch is sorted
		ctr.rbat.Vecs[i].SetSorted(ap.ctr.bat.Vecs[pos].GetSorted())
	}
	count := ap.ctr.bat.RowCount()
	for i := ap.ctr.lastrow; i < count; i += hashmap.UnitLimit {
		if ctr.rbat.RowCount() >= colexec.DefaultBatchSize {
			anal.Output(ctr.rbat, isLast)
			result.Batch = ctr.rbat
			ap.ctr.lastrow = i
			return nil
		}
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		for k := 0; k < n; k++ {
			for j, pos := range ap.Result {
				if err := ctr.rbat.Vecs[j].UnionOne(ap.ctr.bat.Vecs[pos], int64(i+k), proc.Mp()); err != nil {
					return err
				}
			}
		}
		ctr.rbat.AddRowCount(n)
	}
	anal.Output(ctr.rbat, isLast)
	result.Batch = ctr.rbat
	proc.PutBatch(ap.ctr.bat)
	ap.ctr.lastrow = 0
	ap.ctr.bat = nil
	return nil
}

func (ctr *container) probe(ap *AntiJoin, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool, result *vm.CallResult) error {

	anal.Input(ap.ctr.bat, isFirst)
	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	ctr.rbat = batch.NewWithSize(len(ap.Result))
	for i, pos := range ap.Result {
		ctr.rbat.Vecs[i] = proc.GetVector(*ap.ctr.bat.Vecs[pos].GetType())
		// for anti join, if left batch is sorted , then output batch is sorted
		ctr.rbat.Vecs[i].SetSorted(ap.ctr.bat.Vecs[pos].GetSorted())
	}
	if ctr.batchRowCount == 1 && ctr.hasNull {
		result.Batch = ctr.rbat
		anal.Output(ctr.rbat, isLast)
		return nil
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

	count := ap.ctr.bat.RowCount()
	mSels := ctr.mp.Sels()
	itr := ctr.mp.NewIterator()
	eligible := make([]int32, 0, hashmap.UnitLimit)
	for i := ap.ctr.lastrow; i < count; i += hashmap.UnitLimit {
		if ctr.rbat.RowCount() >= colexec.DefaultBatchSize {
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

		rowCountIncrease := 0
		for k := 0; k < n; k++ {
			if zvals[k] == 0 {
				continue
			}
			if vals[k] == 0 {
				eligible = append(eligible, int32(i+k))
				rowCountIncrease++
				continue
			}
			if ap.Cond != nil {
				if ap.HashOnPK {
					idx1, idx2 := int64(vals[k]-1)/colexec.DefaultBatchSize, int64(vals[k]-1)%colexec.DefaultBatchSize
					if err := colexec.SetJoinBatchValues(ctr.joinBat1, ap.ctr.bat, int64(i+k),
						1, ctr.cfs1); err != nil {
						return err
					}
					if err := colexec.SetJoinBatchValues(ctr.joinBat2, ctr.batches[idx1], idx2,
						1, ctr.cfs2); err != nil {
						return err
					}
					vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat1, ctr.joinBat2}, nil)
					if err != nil {
						return err
					}
					if vec.IsConstNull() || vec.GetNulls().Contains(0) {
						continue
					}
					bs := vector.MustFixedCol[bool](vec)
					if bs[0] {
						continue
					}
				} else {
					matched := false // mark if any tuple satisfies the condition
					sels := mSels[vals[k]-1]
					for _, sel := range sels {
						idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
						if err := colexec.SetJoinBatchValues(ctr.joinBat1, ap.ctr.bat, int64(i+k),
							1, ctr.cfs1); err != nil {
							return err
						}
						if err := colexec.SetJoinBatchValues(ctr.joinBat2, ctr.batches[idx1], int64(idx2),
							1, ctr.cfs2); err != nil {
							return err
						}
						vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat1, ctr.joinBat2}, nil)
						if err != nil {
							return err
						}
						if vec.IsConstNull() || vec.GetNulls().Contains(0) {
							continue
						}
						bs := vector.MustFixedCol[bool](vec)
						if bs[0] {
							matched = true
							break
						}
					}
					if matched {
						continue
					}
				}
				eligible = append(eligible, int32(i+k))
				rowCountIncrease++
			}
		}
		ctr.rbat.SetRowCount(ctr.rbat.RowCount() + rowCountIncrease)

		for j, pos := range ap.Result {
			if err := ctr.rbat.Vecs[j].Union(ap.ctr.bat.Vecs[pos], eligible, proc.Mp()); err != nil {
				return err
			}
		}
		eligible = eligible[:0]
	}
	anal.Output(ctr.rbat, isLast)
	result.Batch = ctr.rbat
	ap.ctr.lastrow = 0
	return nil
}

func (ctr *container) evalJoinCondition(bat *batch.Batch, proc *process.Process) error {
	for i := range ctr.executorForVecs {
		vec, err := ctr.executorForVecs[i].Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			return err
		}
		ctr.vecs[i] = vec
	}
	return nil
}
