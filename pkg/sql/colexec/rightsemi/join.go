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

package rightsemi

import (
	"bytes"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const argName = "right_semi"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": right semi join ")
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
	arg.ctr.tmpBatches = make([]*batch.Batch, 2)
	return err
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	analyze := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	analyze.Start()
	defer analyze.Stop()
	ctr := arg.ctr
	result := vm.NewCallResult()
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(analyze); err != nil {
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
			bat, _, err := ctr.ReceiveFromSingleReg(0, analyze)
			if err != nil {
				return result, err
			}

			if bat == nil {
				ctr.state = SendLast
				continue
			}
			if bat.IsEmpty() {
				proc.PutBatch(bat)
				continue
			}

			if ctr.batchRowCount == 0 {
				proc.PutBatch(bat)
				continue
			}

			if err = ctr.probe(bat, arg, proc, analyze, arg.GetIsFirst(), arg.GetIsLast()); err != nil {
				bat.Clean(proc.Mp())
				return result, err
			}
			proc.PutBatch(bat)
			continue

		case SendLast:
			if arg.rbat == nil {
				arg.lastpos = 0
				setNil, err := ctr.sendLast(arg, proc, analyze, arg.GetIsFirst(), arg.GetIsLast())
				if err != nil {
					return result, err
				}
				if setNil {
					ctr.state = End
				}
				continue
			} else {
				if arg.lastpos >= len(arg.rbat) {
					ctr.state = End
					continue
				}
				result.Batch = arg.rbat[arg.lastpos]
				arg.lastpos++
				return result, nil
			}

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
	if ctr.batchRowCount > 0 {
		ctr.matched = &bitmap.Bitmap{}
		ctr.matched.InitWithSize(int64(ctr.batchRowCount))
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

func (ctr *container) sendLast(ap *Argument, proc *process.Process, analyze process.Analyze, _ bool, isLast bool) (bool, error) {
	ctr.handledLast = true

	if ctr.matched == nil {
		return true, nil
	}

	if ap.NumCPU > 1 {
		if !ap.IsMerger {

			sendStart := time.Now()
			ap.Channel <- ctr.matched
			analyze.WaitStop(sendStart)

			return true, nil
		} else {
			cnt := 1

			receiveStart := time.Now()

			// The original code didn't handle the context correctly and would cause the system to HUNG!
			for completed := true; completed; {
				select {
				case <-proc.Ctx.Done():
					return true, moerr.NewInternalError(proc.Ctx, "query has been closed early")
				case v := <-ap.Channel:
					ctr.matched.Or(v)
					cnt++
					if cnt == int(ap.NumCPU) {
						close(ap.Channel)
						completed = false
					}
				}
			}
			analyze.WaitStop(receiveStart)

		}
	}

	count := ctr.matched.Count()
	sels := make([]int32, 0, count)
	itr := ctr.matched.Iterator()
	for itr.HasNext() {
		r := itr.Next()
		sels = append(sels, int32(r))
	}

	if len(sels) <= colexec.DefaultBatchSize {
		if ctr.rbat != nil {
			proc.PutBatch(ctr.rbat)
			ctr.rbat = nil
		}
		ctr.rbat = batch.NewWithSize(len(ap.Result))

		for i, pos := range ap.Result {
			ctr.rbat.Vecs[i] = proc.GetVector(ap.RightTypes[pos])
		}
		for j, pos := range ap.Result {
			for _, sel := range sels {
				idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
				if err := ctr.rbat.Vecs[j].UnionOne(ctr.batches[idx1].Vecs[pos], int64(idx2), proc.Mp()); err != nil {
					return false, err
				}
			}
		}
		ctr.rbat.AddRowCount(len(sels))

		analyze.Output(ctr.rbat, isLast)
		ap.rbat = []*batch.Batch{ctr.rbat}
		return false, nil
	} else {
		n := (len(sels)-1)/colexec.DefaultBatchSize + 1
		ap.rbat = make([]*batch.Batch, n)
		for k := range ap.rbat {
			ap.rbat[k] = batch.NewWithSize(len(ap.Result))
			for i, pos := range ap.Result {
				ap.rbat[k].Vecs[i] = proc.GetVector(ap.RightTypes[pos])
			}
			var newsels []int32
			if (k+1)*colexec.DefaultBatchSize <= len(sels) {
				newsels = sels[k*colexec.DefaultBatchSize : (k+1)*colexec.DefaultBatchSize]
			} else {
				newsels = sels[k*colexec.DefaultBatchSize:]
			}
			for i, pos := range ap.Result {
				for _, sel := range newsels {
					idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
					if err := ap.rbat[k].Vecs[i].UnionOne(ctr.batches[idx1].Vecs[pos], int64(idx2), proc.Mp()); err != nil {
						return false, err
					}
				}
			}
			ap.rbat[k].SetRowCount(len(newsels))
			analyze.Output(ap.rbat[k], isLast)
		}
		return false, nil
	}

}

func (ctr *container) probe(bat *batch.Batch, ap *Argument, proc *process.Process, analyze process.Analyze, isFirst bool, _ bool) error {
	analyze.Input(bat, isFirst)

	if err := ctr.evalJoinCondition(bat, proc); err != nil {
		return err
	}
	if ctr.joinBat1 == nil {
		ctr.joinBat1, ctr.cfs1 = colexec.NewJoinBatch(bat, proc.Mp())
	}
	if ctr.joinBat2 == nil {
		ctr.joinBat2, ctr.cfs2 = colexec.NewJoinBatch(ctr.batches[0], proc.Mp())
	}
	count := bat.RowCount()
	mSels := ctr.mp.Sels()
	itr := ctr.mp.NewIterator()
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
			if ap.HashOnPK {
				idx1, idx2 := int64(vals[k]-1)/colexec.DefaultBatchSize, int64(vals[k]-1)%colexec.DefaultBatchSize
				if ctr.matched.Contains(vals[k] - 1) {
					continue
				}
				if ap.Cond != nil {
					if err := colexec.SetJoinBatchValues(ctr.joinBat1, bat, int64(i+k),
						1, ctr.cfs1); err != nil {
						return err
					}
					if err := colexec.SetJoinBatchValues(ctr.joinBat2, ctr.batches[idx1], idx2,
						1, ctr.cfs2); err != nil {
						return err
					}
					ctr.tmpBatches[0] = ctr.joinBat1
					ctr.tmpBatches[1] = ctr.joinBat2
					vec, err := ctr.expr.Eval(proc, ctr.tmpBatches)
					if err != nil {
						return err
					}
					if vec.IsConstNull() || vec.GetNulls().Contains(0) {
						continue
					} else {
						vcol := vector.MustFixedCol[bool](vec)
						if !vcol[0] {
							continue
						}
					}
				}
				ctr.matched.Add(vals[k] - 1)
			} else {
				sels := mSels[vals[k]-1]
				for _, sel := range sels {
					if ctr.matched.Contains(uint64(sel)) {
						continue
					}
					idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
					if ap.Cond != nil {
						if err := colexec.SetJoinBatchValues(ctr.joinBat1, bat, int64(i+k),
							1, ctr.cfs1); err != nil {
							return err
						}
						if err := colexec.SetJoinBatchValues(ctr.joinBat2, ctr.batches[idx1], int64(idx2),
							1, ctr.cfs2); err != nil {
							return err
						}
						ctr.tmpBatches[0] = ctr.joinBat1
						ctr.tmpBatches[1] = ctr.joinBat2
						vec, err := ctr.expr.Eval(proc, ctr.tmpBatches)
						if err != nil {
							return err
						}
						if vec.IsConstNull() || vec.GetNulls().Contains(0) {
							continue
						} else {
							vcol := vector.MustFixedCol[bool](vec)
							if !vcol[0] {
								continue
							}
						}
					}
					ctr.matched.Add(uint64(sel))
				}
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
