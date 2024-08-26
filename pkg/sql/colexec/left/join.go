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

package left

import (
	"bytes"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/message"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "left"

func (leftJoin *LeftJoin) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": left join ")
}

func (leftJoin *LeftJoin) OpType() vm.OpType {
	return vm.Left
}

func (leftJoin *LeftJoin) Prepare(proc *process.Process) (err error) {
	leftJoin.OpAnalyzer = process.NewAnalyzer(leftJoin.GetIdx(), leftJoin.IsFirst, leftJoin.IsLast, "left join")

	if leftJoin.ctr.vecs == nil {
		leftJoin.ctr.vecs = make([]*vector.Vector, len(leftJoin.Conditions[0]))
		leftJoin.ctr.executor = make([]colexec.ExpressionExecutor, len(leftJoin.Conditions[0]))
		for i := range leftJoin.ctr.executor {
			leftJoin.ctr.executor[i], err = colexec.NewExpressionExecutor(proc, leftJoin.Conditions[0][i])
			if err != nil {
				return err
			}
		}
		if leftJoin.Cond != nil {
			leftJoin.ctr.expr, err = colexec.NewExpressionExecutor(proc, leftJoin.Cond)
			if err != nil {
				return err
			}
		}
		return leftJoin.PrepareProjection(proc)
	}
	return err
}

func (leftJoin *LeftJoin) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	//anal := proc.GetAnalyze(leftJoin.GetIdx(), leftJoin.GetParallelIdx(), leftJoin.GetParallelMajor())
	//anal.Start()
	//defer anal.Stop()
	analyzer := leftJoin.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	ctr := &leftJoin.ctr
	input := vm.NewCallResult()
	result := vm.NewCallResult()
	probeResult := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {
		case Build:
			leftJoin.build(analyzer, proc)
			ctr.state = Probe

		case Probe:
			if leftJoin.ctr.inbat == nil {
				//input, err = leftJoin.Children[0].Call(proc)
				input, err = vm.ChildrenCallV1(leftJoin.GetChildren(0), proc, analyzer)
				if err != nil {
					return result, err

				}
				bat := input.Batch
				if bat == nil {
					ctr.state = End
					continue
				}
				if bat.IsEmpty() {
					continue
				}
				ctr.inbat = bat
				ctr.lastrow = 0
				//anal.Input(bat, leftJoin.GetIsFirst())
			}

			if ctr.rbat == nil {
				ctr.rbat = batch.NewWithSize(len(leftJoin.Result))
				for i, rp := range leftJoin.Result {
					if rp.Rel == 0 {
						ctr.rbat.Vecs[i] = vector.NewVec(*ctr.inbat.Vecs[rp.Pos].GetType())
						// for left join, if left batch is sorted , then output batch is sorted
						ctr.rbat.Vecs[i].SetSorted(ctr.inbat.Vecs[rp.Pos].GetSorted())
					} else {
						ctr.rbat.Vecs[i] = vector.NewVec(leftJoin.Typs[rp.Pos])
					}
				}
			} else {
				ctr.rbat.CleanOnlyData()
				for i, rp := range leftJoin.Result {
					if rp.Rel == 0 {
						ctr.rbat.Vecs[i].SetSorted(ctr.inbat.Vecs[rp.Pos].GetSorted())
					}
				}
			}

			startrow := leftJoin.ctr.lastrow
			if ctr.mp == nil {
				err = ctr.emptyProbe(leftJoin, proc, &probeResult)
			} else {
				err = ctr.probe(leftJoin, proc, &probeResult)
			}
			if err != nil {
				return result, err
			}
			if leftJoin.ctr.lastrow == 0 {
				leftJoin.ctr.inbat = nil
			} else if leftJoin.ctr.lastrow == startrow {
				return result, moerr.NewInternalErrorNoCtx("left join hanging")
			}

			result.Batch, err = leftJoin.EvalProjection(probeResult.Batch, proc)
			if err != nil {
				return result, err
			}
			//anal.Output(result.Batch, leftJoin.GetIsLast())
			analyzer.Output(result.Batch)
			return result, nil

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (leftJoin *LeftJoin) build(analyzer process.Analyzer, proc *process.Process) {
	ctr := &leftJoin.ctr
	start := time.Now()
	defer analyzer.WaitStop(start)
	ctr.mp = message.ReceiveJoinMap(leftJoin.JoinMapTag, leftJoin.IsShuffle, leftJoin.ShuffleIdx, proc.GetMessageBoard(), proc.Ctx)
	if ctr.mp != nil {
		ctr.maxAllocSize = max(ctr.maxAllocSize, ctr.mp.Size())
	}
	ctr.batchRowCount = ctr.mp.GetRowCount()
}

func (ctr *container) emptyProbe(ap *LeftJoin, proc *process.Process, result *vm.CallResult) error {

	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			if err := vector.GetUnionAllFunction(*ctr.rbat.Vecs[i].GetType(), proc.Mp())(ctr.rbat.Vecs[i], ap.ctr.inbat.Vecs[rp.Pos]); err != nil {
				return err
			}
		} else {
			ctr.rbat.Vecs[i].SetClass(vector.CONSTANT)
			ctr.rbat.Vecs[i].SetLength(ctr.inbat.RowCount())
		}
	}
	ctr.rbat.AddRowCount(ap.ctr.inbat.RowCount())
	result.Batch = ctr.rbat
	ap.ctr.lastrow = 0
	return nil
}

func (ctr *container) probe(ap *LeftJoin, proc *process.Process, result *vm.CallResult) error {
	mpbat := ctr.mp.GetBatches()

	if err := ctr.evalJoinCondition(ap.ctr.inbat, proc); err != nil {
		return err
	}

	if ctr.joinBat1 == nil {
		ctr.joinBat1, ctr.cfs1 = colexec.NewJoinBatch(ap.ctr.inbat, proc.Mp())
	}
	if ctr.joinBat2 == nil && ctr.batchRowCount > 0 {
		ctr.joinBat2, ctr.cfs2 = colexec.NewJoinBatch(mpbat[0], proc.Mp())
	}

	count := ap.ctr.inbat.RowCount()
	mSels := ctr.mp.Sels()
	itr := ctr.mp.NewIterator()
	for i := ap.ctr.lastrow; i < count; i += hashmap.UnitLimit {
		if ctr.rbat.RowCount() >= colexec.DefaultBatchSize {
			result.Batch = ctr.rbat
			ap.ctr.lastrow = i
			return nil
		}
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		vals, zvals := itr.Find(i, n, ctr.vecs)
		rowCount := 0
		for k := 0; k < n; k++ {
			// if null or not found.
			// the result row was  [left cols, null cols]
			if zvals[k] == 0 || vals[k] == 0 {
				for j, rp := range ap.Result {
					// columns from the left table.
					if rp.Rel == 0 {
						if err := ctr.rbat.Vecs[j].UnionOne(ap.ctr.inbat.Vecs[rp.Pos], int64(i+k), proc.Mp()); err != nil {
							return err
						}
					} else {
						if err := ctr.rbat.Vecs[j].UnionNull(proc.Mp()); err != nil {
							return err
						}
					}
				}
				rowCount++
				continue
			}

			matched := false
			if ap.HashOnPK {
				idx1, idx2 := int64(vals[k]-1)/colexec.DefaultBatchSize, int64(vals[k]-1)%colexec.DefaultBatchSize
				if ap.Cond != nil {
					if err := colexec.SetJoinBatchValues(ctr.joinBat1, ap.ctr.inbat, int64(i+k),
						1, ctr.cfs1); err != nil {
						return err
					}
					if err := colexec.SetJoinBatchValues(ctr.joinBat2, mpbat[idx1], idx2,
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
						for j, rp := range ap.Result {
							if rp.Rel == 0 {
								if err := ctr.rbat.Vecs[j].UnionOne(ap.ctr.inbat.Vecs[rp.Pos], int64(i+k), proc.Mp()); err != nil {
									return err
								}
							} else {
								if err := ctr.rbat.Vecs[j].UnionOne(mpbat[idx1].Vecs[rp.Pos], idx2, proc.Mp()); err != nil {
									return err
								}
							}
						}
						rowCount++
					}
				} else {
					matched = true
					for j, rp := range ap.Result {
						if rp.Rel == 0 {
							if err := ctr.rbat.Vecs[j].UnionMulti(ap.ctr.inbat.Vecs[rp.Pos], int64(i+k), 1, proc.Mp()); err != nil {
								return err
							}
						} else {
							if err := ctr.rbat.Vecs[j].UnionOne(mpbat[idx1].Vecs[rp.Pos], idx2, proc.Mp()); err != nil {
								return err
							}
						}
					}
					rowCount++
				}
			} else {
				sels := mSels[vals[k]-1]
				if ap.Cond != nil {
					if err := colexec.SetJoinBatchValues(ctr.joinBat1, ap.ctr.inbat, int64(i+k),
						1, ctr.cfs1); err != nil {
						return err
					}

					for _, sel := range sels {
						idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
						if err := colexec.SetJoinBatchValues(ctr.joinBat2, mpbat[idx1], int64(idx2),
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
						matched = true
						for j, rp := range ap.Result {
							if rp.Rel == 0 {
								if err := ctr.rbat.Vecs[j].UnionOne(ap.ctr.inbat.Vecs[rp.Pos], int64(i+k), proc.Mp()); err != nil {
									return err
								}
							} else {
								if err := ctr.rbat.Vecs[j].UnionOne(mpbat[idx1].Vecs[rp.Pos], int64(idx2), proc.Mp()); err != nil {
									return err
								}
							}
						}
						rowCount++
					}
				} else {
					matched = true
					for j, rp := range ap.Result {
						if rp.Rel == 0 {
							if err := ctr.rbat.Vecs[j].UnionMulti(ap.ctr.inbat.Vecs[rp.Pos], int64(i+k), len(sels), proc.Mp()); err != nil {
								return err
							}
						} else {
							for _, sel := range sels {
								idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
								if err := ctr.rbat.Vecs[j].UnionOne(mpbat[idx1].Vecs[rp.Pos], int64(idx2), proc.Mp()); err != nil {
									return err
								}
							}
						}
					}
					rowCount += len(sels)
				}
			}

			if !matched {
				for j, rp := range ap.Result {
					if rp.Rel == 0 {
						if err := ctr.rbat.Vecs[j].UnionOne(ap.ctr.inbat.Vecs[rp.Pos], int64(i+k), proc.Mp()); err != nil {
							return err
						}
					} else {
						if err := ctr.rbat.Vecs[j].UnionNull(proc.Mp()); err != nil {
							return err
						}
					}
				}
				rowCount++
				continue
			}
		}

		ctr.rbat.SetRowCount(ctr.rbat.RowCount() + rowCount)
	}
	result.Batch = ctr.rbat
	ap.ctr.lastrow = 0
	return nil
}

func (ctr *container) evalJoinCondition(bat *batch.Batch, proc *process.Process) error {
	for i := range ctr.executor {
		vec, err := ctr.executor[i].Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			return err
		}
		ctr.vecs[i] = vec
	}
	return nil
}
