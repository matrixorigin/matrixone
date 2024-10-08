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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "right_semi"

func (rightSemi *RightSemi) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": right semi join ")
}

func (rightSemi *RightSemi) OpType() vm.OpType {
	return vm.RightSemi
}

func (rightSemi *RightSemi) Prepare(proc *process.Process) (err error) {
	if rightSemi.OpAnalyzer == nil {
		rightSemi.OpAnalyzer = process.NewAnalyzer(rightSemi.GetIdx(), rightSemi.IsFirst, rightSemi.IsLast, "right semi join")
	} else {
		rightSemi.OpAnalyzer.Reset()
	}

	if len(rightSemi.ctr.tmpBatches) == 0 {
		rightSemi.ctr.vecs = make([]*vector.Vector, len(rightSemi.Conditions[0]))
		rightSemi.ctr.evecs = make([]evalVector, len(rightSemi.Conditions[0]))
		for i := range rightSemi.ctr.evecs {
			rightSemi.ctr.evecs[i].executor, err = colexec.NewExpressionExecutor(proc, rightSemi.Conditions[0][i])
			if err != nil {
				return err
			}
		}

		if rightSemi.Cond != nil {
			rightSemi.ctr.expr, err = colexec.NewExpressionExecutor(proc, rightSemi.Cond)
		}
		rightSemi.ctr.tmpBatches = make([]*batch.Batch, 2)
	}
	return err
}

func (rightSemi *RightSemi) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	analyzer := rightSemi.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	ctr := &rightSemi.ctr
	result := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {
		case Build:
			err = rightSemi.build(analyzer, proc)
			if err != nil {
				return result, err
			}
			if ctr.mp == nil && !rightSemi.IsShuffle {
				// for inner ,right and semi join, if hashmap is empty, we can finish this pipeline
				// shuffle join can't stop early for this moment
				ctr.state = End
			} else {
				ctr.state = Probe
			}

		case Probe:
			result, err = vm.ChildrenCall(rightSemi.GetChildren(0), proc, analyzer)
			if err != nil {
				return result, err
			}
			bat := result.Batch

			if bat == nil {
				ctr.state = SendLast
				continue
			}
			if bat.IsEmpty() {
				continue
			}

			if ctr.batchRowCount == 0 {
				continue
			}

			if err = ctr.probe(bat, rightSemi, proc, analyzer); err != nil {
				return result, err
			}
			continue

		case SendLast:
			if rightSemi.ctr.buf == nil {
				rightSemi.ctr.lastpos = 0
				setNil, err := ctr.sendLast(rightSemi, proc, analyzer)
				if err != nil {
					return result, err
				}
				if setNil {
					ctr.state = End
				}
				continue
			} else {
				if rightSemi.ctr.lastpos >= len(rightSemi.ctr.buf) {
					ctr.state = End
					continue
				}
				result.Batch = rightSemi.ctr.buf[rightSemi.ctr.lastpos]
				rightSemi.ctr.lastpos++
				result.Status = vm.ExecHasMore
				analyzer.Output(result.Batch)
				return result, nil
			}

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (rightSemi *RightSemi) build(analyzer process.Analyzer, proc *process.Process) (err error) {
	ctr := &rightSemi.ctr
	start := time.Now()
	defer analyzer.WaitStop(start)
	ctr.mp, err = message.ReceiveJoinMap(rightSemi.JoinMapTag, rightSemi.IsShuffle, rightSemi.ShuffleIdx, proc.GetMessageBoard(), proc.Ctx)
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

func (ctr *container) sendLast(ap *RightSemi, proc *process.Process, analyzer process.Analyzer) (bool, error) {
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
				v := colexec.ReceiveBitmapFromChannel(proc.Ctx, ap.Channel)
				if v != nil {
					ctr.matched.Or(v)
				} else {
					return true, nil
				}
			}
			close(ap.Channel)
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
			ctr.rbat.CleanOnlyData()
		} else {
			ctr.rbat = batch.NewWithSize(len(ap.Result))

			for i, pos := range ap.Result {
				ctr.rbat.Vecs[i] = vector.NewVec(ap.RightTypes[pos])
			}
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

		ap.ctr.buf = []*batch.Batch{ctr.rbat}
		return false, nil
	} else {
		n := (len(sels)-1)/colexec.DefaultBatchSize + 1
		ap.ctr.buf = make([]*batch.Batch, n)
		for k := range ap.ctr.buf {
			ap.ctr.buf[k] = batch.NewWithSize(len(ap.Result))
			for i, pos := range ap.Result {
				ap.ctr.buf[k].Vecs[i] = vector.NewVec(ap.RightTypes[pos])
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
					if err := ap.ctr.buf[k].Vecs[i].UnionOne(ctr.batches[idx1].Vecs[pos], int64(idx2), proc.Mp()); err != nil {
						return false, err
					}
				}
			}
			ap.ctr.buf[k].SetRowCount(len(newsels))
		}
		return false, nil
	}

}

func (ctr *container) probe(bat *batch.Batch, ap *RightSemi, proc *process.Process, analyzer process.Analyzer) error {
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
						vcol := vector.MustFixedColWithTypeCheck[bool](vec)
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
