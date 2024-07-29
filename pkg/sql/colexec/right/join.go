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

package right

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "right"

func (rightJoin *RightJoin) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": right join ")
}

func (rightJoin *RightJoin) OpType() vm.OpType {
	return vm.Right
}

func (rightJoin *RightJoin) Prepare(proc *process.Process) (err error) {
	rightJoin.ctr = new(container)
	rightJoin.ctr.InitReceiver(proc, true)
	rightJoin.ctr.vecs = make([]*vector.Vector, len(rightJoin.Conditions[0]))

	rightJoin.ctr.evecs = make([]evalVector, len(rightJoin.Conditions[0]))
	for i := range rightJoin.Conditions[0] {
		rightJoin.ctr.evecs[i].executor, err = colexec.NewExpressionExecutor(proc, rightJoin.Conditions[0][i])
		if err != nil {
			return err
		}
	}
	if rightJoin.Cond != nil {
		rightJoin.ctr.expr, err = colexec.NewExpressionExecutor(proc, rightJoin.Cond)
	}
	rightJoin.ctr.handledLast = false
	return err
}

func (rightJoin *RightJoin) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	analyze := proc.GetAnalyze(rightJoin.GetIdx(), rightJoin.GetParallelIdx(), rightJoin.GetParallelMajor())
	analyze.Start()
	defer analyze.Stop()
	ctr := rightJoin.ctr
	result := vm.NewCallResult()
	for {
		switch ctr.state {
		case Build:
			rightJoin.build(analyze, proc)
			if ctr.mp == nil && !rightJoin.IsShuffle {
				// for inner ,right and semi join, if hashmap is empty, we can finish this pipeline
				// shuffle join can't stop early for this moment
				ctr.state = End
			} else {
				ctr.state = Probe
			}

		case Probe:
			if rightJoin.ctr.buf == nil {
				msg := ctr.ReceiveFromAllRegs(analyze)
				if msg.Err != nil {
					return result, msg.Err
				}
				bat := msg.Batch

				if bat == nil {
					ctr.state = SendLast
					rightJoin.rbat = nil
					continue
				}
				if bat.IsEmpty() {
					proc.PutBatch(bat)
					continue
				}
				if ctr.mp == nil {
					proc.PutBatch(bat)
					continue
				}
				rightJoin.ctr.buf = bat
				rightJoin.ctr.lastpos = 0
			}

			startrow := rightJoin.ctr.lastpos
			if err := ctr.probe(rightJoin, proc, analyze, rightJoin.GetIsFirst(), rightJoin.GetIsLast(), &result); err != nil {
				return result, err
			}
			if rightJoin.ctr.lastpos == 0 {
				proc.PutBatch(rightJoin.ctr.buf)
				rightJoin.ctr.buf = nil
			} else if rightJoin.ctr.lastpos == startrow {
				return result, moerr.NewInternalErrorNoCtx("right join hanging")
			}
			return result, nil

		case SendLast:
			setNil, err := ctr.sendLast(rightJoin, proc, analyze, rightJoin.GetIsFirst(), rightJoin.GetIsLast(), &result)
			if err != nil {
				return result, err
			}

			ctr.state = End
			if setNil {
				continue
			}
			return result, nil

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (rightJoin *RightJoin) build(anal process.Analyze, proc *process.Process) {
	ctr := rightJoin.ctr
	ctr.mp = proc.ReceiveJoinMap(anal, rightJoin.JoinMapTag, rightJoin.IsShuffle, rightJoin.ShuffleIdx)
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

func (ctr *container) sendLast(ap *RightJoin, proc *process.Process, analyze process.Analyze, _ bool, isLast bool, result *vm.CallResult) (bool, error) {
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

	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	ctr.rbat = batch.NewWithSize(len(ap.Result))

	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			ctr.rbat.Vecs[i] = proc.GetVector(ap.LeftTypes[rp.Pos])
		} else {
			ctr.rbat.Vecs[i] = proc.GetVector(ap.RightTypes[rp.Pos])
		}
	}

	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			if err := vector.AppendMultiFixed(ctr.rbat.Vecs[i], 0, true, int(count), proc.Mp()); err != nil {
				return false, err
			}
		} else {
			for _, sel := range sels {
				idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
				if err := ctr.rbat.Vecs[i].UnionOne(ctr.batches[idx1].Vecs[rp.Pos], int64(idx2), proc.Mp()); err != nil {
					return false, err
				}
			}
		}

	}
	ctr.rbat.AddRowCount(len(sels))
	analyze.Output(ctr.rbat, isLast)
	result.Batch = ctr.rbat
	return false, nil
}

func (ctr *container) probe(ap *RightJoin, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool, result *vm.CallResult) error {
	anal.Input(ap.ctr.buf, isFirst)
	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	ctr.rbat = batch.NewWithSize(len(ap.Result))
	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			ctr.rbat.Vecs[i] = proc.GetVector(*ap.ctr.buf.Vecs[rp.Pos].GetType())
		} else {
			ctr.rbat.Vecs[i] = proc.GetVector(ap.RightTypes[rp.Pos])
		}
	}

	if err := ctr.evalJoinCondition(ap.ctr.buf, proc); err != nil {
		return err
	}
	if ctr.joinBat1 == nil {
		ctr.joinBat1, ctr.cfs1 = colexec.NewJoinBatch(ap.ctr.buf, proc.Mp())
	}
	if ctr.joinBat2 == nil {
		ctr.joinBat2, ctr.cfs2 = colexec.NewJoinBatch(ctr.batches[0], proc.Mp())
	}
	count := ap.ctr.buf.RowCount()
	mSels := ctr.mp.Sels()
	itr := ctr.mp.NewIterator()

	rowCountIncrese := 0
	for i := ap.ctr.lastpos; i < count; i += hashmap.UnitLimit {
		if rowCountIncrese >= colexec.DefaultBatchSize {
			ctr.rbat.AddRowCount(rowCountIncrese)
			anal.Output(ctr.rbat, isLast)
			result.Batch = ctr.rbat
			ap.ctr.lastpos = i
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
			if ap.HashOnPK {
				idx1, idx2 := int64(vals[k]-1)/colexec.DefaultBatchSize, int64(vals[k]-1)%colexec.DefaultBatchSize
				if ap.Cond != nil {
					if err := colexec.SetJoinBatchValues(ctr.joinBat1, ap.ctr.buf, int64(i+k),
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
						for j, rp := range ap.Result {
							if rp.Rel == 0 {
								if err := ctr.rbat.Vecs[j].UnionOne(ap.ctr.buf.Vecs[rp.Pos], int64(i+k), proc.Mp()); err != nil {
									return err
								}
							} else {
								if err := ctr.rbat.Vecs[j].UnionOne(ctr.batches[idx1].Vecs[rp.Pos], idx2, proc.Mp()); err != nil {
									return err
								}
							}
						}
						ctr.matched.Add(vals[k] - 1)
						rowCountIncrese++
					}
				} else {
					for j, rp := range ap.Result {
						if rp.Rel == 0 {
							if err := ctr.rbat.Vecs[j].UnionMulti(ap.ctr.buf.Vecs[rp.Pos], int64(i+k), 1, proc.Mp()); err != nil {
								return err
							}
						} else {
							if err := ctr.rbat.Vecs[j].UnionOne(ctr.batches[idx1].Vecs[rp.Pos], idx2, proc.Mp()); err != nil {
								return err
							}
						}
					}
					ctr.matched.Add(vals[k] - 1)
					rowCountIncrese++
				}
			} else {
				sels := mSels[vals[k]-1]
				if ap.Cond != nil {
					for _, sel := range sels {
						idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
						if err := colexec.SetJoinBatchValues(ctr.joinBat1, ap.ctr.buf, int64(i+k),
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
						if !bs[0] {
							continue
						}
						for j, rp := range ap.Result {
							if rp.Rel == 0 {
								if err := ctr.rbat.Vecs[j].UnionOne(ap.ctr.buf.Vecs[rp.Pos], int64(i+k), proc.Mp()); err != nil {
									return err
								}
							} else {
								if err := ctr.rbat.Vecs[j].UnionOne(ctr.batches[idx1].Vecs[rp.Pos], int64(idx2), proc.Mp()); err != nil {
									return err
								}
							}
						}
						ctr.matched.Add(uint64(sel))
						rowCountIncrese++
					}
				} else {
					for j, rp := range ap.Result {
						if rp.Rel == 0 {
							if err := ctr.rbat.Vecs[j].UnionMulti(ap.ctr.buf.Vecs[rp.Pos], int64(i+k), len(sels), proc.Mp()); err != nil {
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
					for _, sel := range sels {
						ctr.matched.Add(uint64(sel))
					}
					rowCountIncrese += len(sels)
				}
			}

		}
	}

	ctr.rbat.AddRowCount(rowCountIncrese)
	anal.Output(ctr.rbat, isLast)
	result.Batch = ctr.rbat
	ap.ctr.lastpos = 0
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
