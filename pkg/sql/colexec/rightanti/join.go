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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
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
	if rightAnti.OpAnalyzer == nil {
		rightAnti.OpAnalyzer = process.NewAnalyzer(rightAnti.GetIdx(), rightAnti.IsFirst, rightAnti.IsLast, "right anti join")
	} else {
		rightAnti.OpAnalyzer.Reset()
	}

	if len(rightAnti.ctr.tmpBatches) == 0 {
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
	}

	return err
}

func (rightAnti *RightAnti) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := rightAnti.OpAnalyzer

	ctr := &rightAnti.ctr
	result := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {
		case Build:
			err = rightAnti.build(analyzer, proc)
			if err != nil {
				return result, err
			}
			// for inner ,right and semi join, if hashmap is empty, we can finish this pipeline
			// shuffle join can't stop early for this moment
			if ctr.mp == nil && !rightAnti.IsShuffle {
				ctr.state = End
			} else {
				ctr.state = Probe
			}

		case Probe:
			result, err = vm.ChildrenCall(rightAnti.GetChildren(0), proc, analyzer)
			if err != nil {
				return result, err
			}
			bat := result.Batch
			if bat == nil {
				ctr.state = SendLast
				rightAnti.ctr.buf = nil
				continue
			}
			if bat.IsEmpty() {
				continue
			}

			if ctr.batchRowCount == 0 {
				continue
			}

			if err := ctr.probe(bat, rightAnti, proc, analyzer); err != nil {
				return result, err
			}

			continue

		case SendLast:
			if ctr.buf == nil {
				ctr.lastPos = 0
				err := ctr.finalize(rightAnti, proc)
				if err != nil {
					return result, err
				}
			}

			if ctr.lastPos >= len(ctr.buf) {
				ctr.state = End
				continue
			}

			result.Batch = ctr.buf[ctr.lastPos]
			ctr.lastPos++
			result.Status = vm.ExecHasMore
			return result, nil

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (rightAnti *RightAnti) build(analyzer process.Analyzer, proc *process.Process) (err error) {
	ctr := &rightAnti.ctr
	start := time.Now()
	defer analyzer.WaitStop(start)
	ctr.mp, err = message.ReceiveJoinMap(rightAnti.JoinMapTag, rightAnti.IsShuffle, rightAnti.ShuffleIdx, proc.GetMessageBoard(), proc.Ctx)
	if err != nil {
		return err
	}
	if ctr.mp != nil {
		ctr.maxAllocSize = max(ctr.maxAllocSize, ctr.mp.Size())
	}
	ctr.batches = ctr.mp.GetBatches()
	ctr.batchRowCount = ctr.mp.GetRowCount()
	if ctr.batchRowCount > 0 {
		ctr.matched = &bitmap.Bitmap{}
		ctr.matched.InitWithSize(ctr.batchRowCount)
	}
	return nil
}

func (ctr *container) finalize(ap *RightAnti, proc *process.Process) error {
	ctr.handledLast = true

	if ctr.matched == nil {
		return nil
	}

	if ap.NumCPU > 1 {
		if !ap.IsMerger {
			ap.Channel <- ctr.matched
			return nil
		} else {
			for cnt := 1; cnt < int(ap.NumCPU); cnt++ {
				v := colexec.ReceiveBitmapFromChannel(proc.Ctx, ap.Channel)
				if v != nil {
					ctr.matched.Or(v)
				} else {
					return nil
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
			ctr.rbat.CleanOnlyData()
		} else {
			ctr.rbat = batch.NewWithSize(len(ap.Result))

			for i, pos := range ap.Result {
				ctr.rbat.Vecs[i] = vector.NewOffHeapVecWithType(ap.RightTypes[pos])
			}
		}
		if err := ctr.rbat.PreExtend(proc.Mp(), len(sels)); err != nil {
			return err
		}
		for j, pos := range ap.Result {
			for _, sel := range sels {
				idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
				if err := ctr.rbat.Vecs[j].UnionOne(ctr.batches[idx1].Vecs[pos], int64(idx2), proc.Mp()); err != nil {
					return err
				}
			}
		}
		ctr.rbat.AddRowCount(len(sels))
		ctr.buf = []*batch.Batch{ctr.rbat}
		return nil
	} else {
		n := (len(sels)-1)/colexec.DefaultBatchSize + 1
		ctr.buf = make([]*batch.Batch, n)
		for k := range ctr.buf {
			ctr.buf[k] = batch.NewWithSize(len(ap.Result))
			for i, pos := range ap.Result {
				ctr.buf[k].Vecs[i] = vector.NewOffHeapVecWithType(ap.RightTypes[pos])
			}
			if err := ctr.buf[k].PreExtend(proc.Mp(), colexec.DefaultBatchSize); err != nil {
				return err
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
					if err := ctr.buf[k].Vecs[j].UnionOne(ctr.batches[idx1].Vecs[pos], int64(idx2), proc.Mp()); err != nil {
						return err
					}
				}
			}
			ctr.buf[k].SetRowCount(len(newsels))
		}
		return nil
	}

}

func (ctr *container) probe(bat *batch.Batch, ap *RightAnti, proc *process.Process, analyzer process.Analyzer) error {
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
	if ctr.itr == nil {
		ctr.itr = ctr.mp.NewIterator()
	}
	itr := ctr.itr
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
			if ap.HashOnPK || ctr.mp.HashOnUnique() {
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
						vcol := vector.MustFixedColWithTypeCheck[bool](vec)
						if !vcol[0] {
							continue
						}
					}
				}
				ctr.matched.Add(vals[k] - 1)
			} else {
				sels := ctr.mp.GetSels(vals[k] - 1)
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
							vcol := vector.MustFixedColWithTypeCheck[bool](vec)
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
