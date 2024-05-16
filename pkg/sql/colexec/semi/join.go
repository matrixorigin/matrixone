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

package semi

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const argName = "semi"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": semi join ")
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
			if ctr.mp != nil && ctr.mp.PushedRuntimeFilterIn() && arg.Cond == nil {
				ctr.skipProbe = true
			}

		case Probe:
			bat, _, err := ctr.ReceiveFromSingleReg(0, anal)
			if err != nil {
				return result, err
			}

			if bat == nil {
				ctr.state = End
				continue
			}
			if bat.IsEmpty() {
				proc.PutBatch(bat)
				continue
			}
			if ctr.skipProbe {
				vecused := make([]bool, len(bat.Vecs))
				newvecs := make([]*vector.Vector, len(arg.Result))
				for i, pos := range arg.Result {
					vecused[pos] = true
					newvecs[i] = bat.Vecs[pos]
				}
				for i := range bat.Vecs {
					if !vecused[i] {
						bat.Vecs[i].Free(proc.Mp())
					}
				}
				bat.Vecs = newvecs
				result.Batch = bat
				anal.Output(bat, arg.GetIsLast())
				return result, nil
			}
			if ctr.mp == nil {
				proc.PutBatch(bat)
				continue
			}
			if err := ctr.probe(bat, arg, proc, anal, arg.GetIsFirst(), arg.GetIsLast(), &result); err != nil {
				bat.Clean(proc.Mp())
				return result, err
			}
			proc.PutBatch(bat)
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

func (ctr *container) probe(bat *batch.Batch, ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool, result *vm.CallResult) error {
	anal.Input(bat, isFirst)
	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	ctr.rbat = batch.NewWithSize(len(ap.Result))
	for i, pos := range ap.Result {
		ctr.rbat.Vecs[i] = proc.GetVector(*bat.Vecs[pos].GetType())
		// for semi join, if left batch is sorted , then output batch is sorted
		ctr.rbat.Vecs[i].SetSorted(bat.Vecs[pos].GetSorted())
	}
	if err := ctr.evalJoinCondition(bat, proc); err != nil {
		return err
	}
	if ctr.joinBat1 == nil {
		ctr.joinBat1, ctr.cfs1 = colexec.NewJoinBatch(bat, proc.Mp())
	}
	if ctr.joinBat2 == nil && ctr.batchRowCount > 0 {
		ctr.joinBat2, ctr.cfs2 = colexec.NewJoinBatch(ctr.batches[0], proc.Mp())
	}
	count := bat.RowCount()
	mSels := ctr.mp.Sels()
	itr := ctr.mp.NewIterator()

	rowCountIncrease := 0
	eligible := make([]int32, 0, hashmap.UnitLimit)
	for i := 0; i < count; i += hashmap.UnitLimit {
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
			if ap.Cond != nil {
				matched := false // mark if any tuple satisfies the condition
				if ap.HashOnPK {
					idx1, idx2 := int64(vals[k]-1)/colexec.DefaultBatchSize, int64(vals[k]-1)%colexec.DefaultBatchSize
					if err := colexec.SetJoinBatchValues(ctr.joinBat1, bat, int64(i+k),
						1, ctr.cfs1); err != nil {
						return err
					}
					if err := colexec.SetJoinBatchValues(ctr.joinBat2, ctr.batches[idx1], idx2,
						1, ctr.cfs2); err != nil {
						return err
					}
					vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat1, ctr.joinBat2})
					if err != nil {
						return err
					}
					if vec.IsConstNull() || vec.GetNulls().Contains(0) {
						continue
					}
					bs := vector.MustFixedCol[bool](vec)
					if bs[0] {
						matched = true
					}
				} else {
					sels := mSels[vals[k]-1]
					for _, sel := range sels {
						idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
						if err := colexec.SetJoinBatchValues(ctr.joinBat1, bat, int64(i+k),
							1, ctr.cfs1); err != nil {
							return err
						}
						if err := colexec.SetJoinBatchValues(ctr.joinBat2, ctr.batches[idx1], int64(idx2),
							1, ctr.cfs2); err != nil {
							return err
						}
						vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat1, ctr.joinBat2})
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

				}
				if !matched {
					continue
				}
			}
			eligible = append(eligible, int32(i+k))
			rowCountIncrease++
		}
		for j, pos := range ap.Result {
			if err := ctr.rbat.Vecs[j].Union(bat.Vecs[pos], eligible, proc.Mp()); err != nil {
				return err
			}
		}
		eligible = eligible[:0]
	}

	ctr.rbat.AddRowCount(rowCountIncrease)
	anal.Output(ctr.rbat, isLast)
	result.Batch = ctr.rbat
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
