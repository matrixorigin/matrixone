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

package rightanti

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "right_anti"

func (rightAnti *RightAnti) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": right anti join ")
}

func (rightAnti *RightAnti) OpType() vm.OpType {
	return vm.RightAnti
}

func (rightAnti *RightAnti) Prepare(proc *process.Process) (err error) {
	rightAnti.ctr = new(container)
	rightAnti.ctr.InitReceiver(proc, true)
	rightAnti.ctr.vecs = make([]*vector.Vector, len(rightAnti.Conditions[0]))
	rightAnti.ctr.evecs = make([]evalVector, len(rightAnti.Conditions[0]))
	for i := range rightAnti.ctr.evecs {
		rightAnti.ctr.evecs[i].executor, err = colexec.NewExpressionExecutor(proc, rightAnti.Conditions[0][i])
		if err != nil {
			return err
		}
	}
	if rightAnti.Cond != nil {
		rightAnti.ctr.expr, err = colexec.NewExpressionExecutor(proc, rightAnti.Cond)
	}

	rightAnti.ctr.tmpBatches = make([]*batch.Batch, 2)
	return err
}

func (rightAnti *RightAnti) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	analyze := proc.GetAnalyze(rightAnti.GetIdx(), rightAnti.GetParallelIdx(), rightAnti.GetParallelMajor())
	analyze.Start()
	defer analyze.Stop()
	ctr := rightAnti.ctr
	result := vm.NewCallResult()
	for {
		switch ctr.state {
		case Build:
			rightAnti.build(analyze, proc)
			// for inner ,right and semi join, if hashmap is empty, we can finish this pipeline
			// shuffle join can't stop early for this moment
			if ctr.mp == nil && !rightAnti.IsShuffle {
				ctr.state = End
			} else {
				ctr.state = Probe
			}

		case Probe:
			msg := ctr.ReceiveFromAllRegs(analyze)
			if msg.Err != nil {
				return result, msg.Err
			}
			bat := msg.Batch
			if bat == nil {
				ctr.state = SendLast
				rightAnti.ctr.buf = nil
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

			if err := ctr.probe(bat, rightAnti, proc, analyze, rightAnti.GetIsFirst(), rightAnti.GetIsLast()); err != nil {
				return result, err
			}

			continue

		case SendLast:
			if rightAnti.ctr.buf == nil {
				rightAnti.ctr.lastpos = 0
				setNil, err := ctr.sendLast(rightAnti, proc, analyze, rightAnti.GetIsFirst(), rightAnti.GetIsLast())
				if err != nil {
					return result, err
				}
				if setNil {
					ctr.state = End
				}
				continue
			} else {
				if rightAnti.ctr.lastpos >= len(rightAnti.ctr.buf) {
					ctr.state = End
					continue
				}
				result.Batch = rightAnti.ctr.buf[rightAnti.ctr.lastpos]
				rightAnti.ctr.lastpos++
				return result, nil
			}

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (rightAnti *RightAnti) build(anal process.Analyze, proc *process.Process) {
	ctr := rightAnti.ctr
	ctr.mp = proc.ReceiveJoinMap(anal, rightAnti.JoinMapTag, rightAnti.IsShuffle, rightAnti.ShuffleIdx)
	if ctr.mp != nil {
		ctr.maxAllocSize = max(ctr.maxAllocSize, ctr.mp.Size())
	}
	ctr.batches = ctr.mp.GetBatches()
	ctr.batchRowCount = ctr.mp.GetRowCount()
	if ctr.batchRowCount > 0 {
		ctr.matched = &bitmap.Bitmap{}
		ctr.matched.InitWithSize(ctr.batchRowCount)
	}
}

func (ctr *container) sendLast(ap *RightAnti, proc *process.Process, analyze process.Analyze, _ bool, isLast bool) (bool, error) {
	ctr.handledLast = true

	if ctr.matched == nil {
		return true, nil
	}

	if ap.NumCPU > 1 {
		if !ap.IsMerger {
			ap.Channel <- ctr.matched
			return true, nil
		} else {
			for cnt := 1; cnt < int(ap.NumCPU); cnt++ {
				v := ctr.ReceiveBitmapFromChannel(ap.Channel)
				if v != nil {
					ctr.matched.Or(v)
				} else {
					return true, nil
				}
			}
			close(ap.Channel)
		}
	}

	count := ctr.batchRowCount - int64(ctr.matched.Count())
	ctr.matched.Negate()
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
		ap.ctr.buf = []*batch.Batch{ctr.rbat}
		return false, nil
	} else {
		n := (len(sels)-1)/colexec.DefaultBatchSize + 1
		ap.ctr.buf = make([]*batch.Batch, n)
		for k := range ap.ctr.buf {
			ap.ctr.buf[k] = batch.NewWithSize(len(ap.Result))
			for i, pos := range ap.Result {
				ap.ctr.buf[k].Vecs[i] = proc.GetVector(ap.RightTypes[pos])
			}
			var newsels []int32
			if (k+1)*colexec.DefaultBatchSize <= len(sels) {
				newsels = sels[k*colexec.DefaultBatchSize : (k+1)*colexec.DefaultBatchSize]
			} else {
				newsels = sels[k*colexec.DefaultBatchSize:]
			}
			for j, pos := range ap.Result {
				for _, sel := range newsels {
					idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
					if err := ap.ctr.buf[k].Vecs[j].UnionOne(ctr.batches[idx1].Vecs[pos], int64(idx2), proc.Mp()); err != nil {
						return false, err
					}
				}
			}
			ap.ctr.buf[k].SetRowCount(len(newsels))
			analyze.Output(ap.ctr.buf[k], isLast)
		}
		return false, nil
	}

}

func (ctr *container) probe(bat *batch.Batch, ap *RightAnti, proc *process.Process, analyze process.Analyze, isFirst bool, _ bool) error {
	defer proc.PutBatch(bat)
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
		vals, zvals := itr.Find(i, n, ctr.vecs)
		for k := 0; k < n; k++ {
			if zvals[k] == 0 || vals[k] == 0 {
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
					vec, err := ctr.expr.Eval(proc, ctr.tmpBatches, nil)
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
						vec, err := ctr.expr.Eval(proc, ctr.tmpBatches, nil)
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
		vec, err := ctr.evecs[i].executor.Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			return err
		}
		ctr.vecs[i] = vec
		ctr.evecs[i].vec = vec
	}
	return nil
}
