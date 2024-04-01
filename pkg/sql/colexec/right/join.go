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
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(" right join ")
}

func (arg *Argument) Prepare(proc *process.Process) (err error) {
	ap := arg
	ap.ctr = new(container)
	ap.ctr.InitReceiver(proc, false)
	ap.ctr.inBuckets = make([]uint8, hashmap.UnitLimit)
	ap.ctr.vecs = make([]*vector.Vector, len(ap.Conditions[0]))
	ap.ctr.bat = batch.NewWithSize(len(ap.RightTypes))
	for i, typ := range ap.RightTypes {
		ap.ctr.bat.Vecs[i] = proc.GetVector(typ)
	}

	ap.ctr.evecs = make([]evalVector, len(ap.Conditions[0]))
	for i := range ap.Conditions[0] {
		ap.ctr.evecs[i].executor, err = colexec.NewExpressionExecutor(proc, ap.Conditions[0][i])
		if err != nil {
			return err
		}
	}
	if ap.Cond != nil {
		ap.ctr.expr, err = colexec.NewExpressionExecutor(proc, ap.Cond)
	}
	ap.ctr.handledLast = false
	return err
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	analyze := proc.GetAnalyze(arg.info.Idx, arg.info.ParallelIdx, arg.info.ParallelMajor)
	analyze.Start()
	defer analyze.Stop()
	ap := arg
	ctr := ap.ctr
	result := vm.NewCallResult()
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(ap, proc, analyze); err != nil {
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

			if ctr.bat == nil || ctr.bat.IsEmpty() {
				proc.PutBatch(bat)
				continue
			}

			if err := ctr.probe(bat, ap, proc, analyze, arg.info.IsFirst, arg.info.IsLast, &result); err != nil {
				bat.Clean(proc.Mp())
				return result, err
			}
			proc.PutBatch(bat)
			return result, nil

		case SendLast:
			setNil, err := ctr.sendLast(ap, proc, analyze, arg.info.IsFirst, arg.info.IsLast, &result)
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

func (ctr *container) build(ap *Argument, proc *process.Process, analyze process.Analyze) error {
	bat, _, err := ctr.ReceiveFromSingleReg(1, analyze)
	if err != nil {
		return err
	}

	if bat != nil {
		if ctr.bat != nil {
			proc.PutBatch(ctr.bat)
			ctr.bat = nil
		}
		ctr.bat = bat
		ctr.mp = bat.DupJmAuxData()
		ctr.matched = &bitmap.Bitmap{}
		ctr.matched.InitWithSize(int64(bat.RowCount()))
		mpSize := ctr.mp.Size()
		if mpSize > ctr.maxAllocSize {
			ctr.maxAllocSize = mpSize
		}
	}
	return nil
}

func (ctr *container) sendLast(ap *Argument, proc *process.Process, analyze process.Analyze, isFirst bool, isLast bool, result *vm.CallResult) (bool, error) {
	ctr.handledLast = true

	if ctr.matched == nil {
		return true, nil
	}

	if ap.NumCPU > 1 {
		if !ap.IsMerger {
			ap.Channel <- ctr.matched
			return true, nil
		} else {
			cnt := 1
			for v := range ap.Channel {
				ctr.matched.Or(v)
				cnt++
				if cnt == int(ap.NumCPU) {
					close(ap.Channel)
					break
				}
			}
		}
	}

	count := ctr.bat.RowCount() - ctr.matched.Count()
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
			if err := vector.AppendMultiFixed(ctr.rbat.Vecs[i], 0, true, count, proc.Mp()); err != nil {
				return false, err
			}
		} else {
			if err := ctr.rbat.Vecs[i].Union(ctr.bat.Vecs[rp.Pos], sels, proc.Mp()); err != nil {
				return false, err
			}
		}

	}
	ctr.rbat.AddRowCount(len(sels))
	analyze.Output(ctr.rbat, isLast)
	result.Batch = ctr.rbat
	return false, nil
}

func (ctr *container) probe(bat *batch.Batch, ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool, result *vm.CallResult) error {
	anal.Input(bat, isFirst)
	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	ctr.rbat = batch.NewWithSize(len(ap.Result))
	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			ctr.rbat.Vecs[i] = proc.GetVector(*bat.Vecs[rp.Pos].GetType())
		} else {
			ctr.rbat.Vecs[i] = proc.GetVector(*ctr.bat.Vecs[rp.Pos].GetType())
		}
	}

	if err := ctr.evalJoinCondition(bat, proc); err != nil {
		return err
	}
	if ctr.joinBat1 == nil {
		ctr.joinBat1, ctr.cfs1 = colexec.NewJoinBatch(bat, proc.Mp())
	}
	if ctr.joinBat2 == nil {
		ctr.joinBat2, ctr.cfs2 = colexec.NewJoinBatch(ctr.bat, proc.Mp())
	}
	count := bat.RowCount()
	mSels := ctr.mp.Sels()
	itr := ctr.mp.NewIterator()

	rowCountIncrese := 0
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
				if ap.Cond != nil {
					if err := colexec.SetJoinBatchValues(ctr.joinBat1, bat, int64(i+k),
						1, ctr.cfs1); err != nil {
						return err
					}
					if err := colexec.SetJoinBatchValues(ctr.joinBat2, ctr.bat, int64(vals[k]-1),
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
						for j, rp := range ap.Result {
							if rp.Rel == 0 {
								if err := ctr.rbat.Vecs[j].UnionOne(bat.Vecs[rp.Pos], int64(i+k), proc.Mp()); err != nil {
									return err
								}
							} else {
								if err := ctr.rbat.Vecs[j].UnionOne(ctr.bat.Vecs[rp.Pos], int64(vals[k]-1), proc.Mp()); err != nil {
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
							if err := ctr.rbat.Vecs[j].UnionMulti(bat.Vecs[rp.Pos], int64(i+k), 1, proc.Mp()); err != nil {
								return err
							}
						} else {
							if err := ctr.rbat.Vecs[j].Union(ctr.bat.Vecs[rp.Pos], []int32{int32(vals[k] - 1)}, proc.Mp()); err != nil {
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
						if err := colexec.SetJoinBatchValues(ctr.joinBat1, bat, int64(i+k),
							1, ctr.cfs1); err != nil {
							return err
						}
						if err := colexec.SetJoinBatchValues(ctr.joinBat2, ctr.bat, int64(sel),
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
						if !bs[0] {
							continue
						}
						for j, rp := range ap.Result {
							if rp.Rel == 0 {
								if err := ctr.rbat.Vecs[j].UnionOne(bat.Vecs[rp.Pos], int64(i+k), proc.Mp()); err != nil {
									return err
								}
							} else {
								if err := ctr.rbat.Vecs[j].UnionOne(ctr.bat.Vecs[rp.Pos], int64(sel), proc.Mp()); err != nil {
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
							if err := ctr.rbat.Vecs[j].UnionMulti(bat.Vecs[rp.Pos], int64(i+k), len(sels), proc.Mp()); err != nil {
								return err
							}
						} else {
							if err := ctr.rbat.Vecs[j].Union(ctr.bat.Vecs[rp.Pos], sels, proc.Mp()); err != nil {
								return err
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
	return nil
}

func (ctr *container) evalJoinCondition(bat *batch.Batch, proc *process.Process) error {
	for i := range ctr.evecs {
		vec, err := ctr.evecs[i].executor.Eval(proc, []*batch.Batch{bat})
		if err != nil {
			ctr.cleanEvalVectors(proc.Mp())
			return err
		}
		ctr.vecs[i] = vec
		ctr.evecs[i].vec = vec
	}
	return nil
}
