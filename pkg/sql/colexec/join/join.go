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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "join"

func (innerJoin *InnerJoin) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": inner join ")
}

func (innerJoin *InnerJoin) OpType() vm.OpType {
	return vm.Join
}

func (innerJoin *InnerJoin) Prepare(proc *process.Process) (err error) {

	if innerJoin.OpAnalyzer == nil {
		innerJoin.OpAnalyzer = process.NewAnalyzer(innerJoin.GetIdx(), innerJoin.IsFirst, innerJoin.IsLast, "innerJoin")
	} else {
		innerJoin.OpAnalyzer.Reset()
	}
	if len(innerJoin.ctr.vecs) == 0 {
		innerJoin.ctr.vecs = make([]*vector.Vector, len(innerJoin.Conditions[0]))
		innerJoin.ctr.executor = make([]colexec.ExpressionExecutor, len(innerJoin.Conditions[0]))
		for i := range innerJoin.ctr.executor {
			innerJoin.ctr.executor[i], err = colexec.NewExpressionExecutor(proc, innerJoin.Conditions[0][i])
			if err != nil {
				return err
			}
		}
		if innerJoin.Cond != nil {
			innerJoin.ctr.expr, err = colexec.NewExpressionExecutor(proc, innerJoin.Cond)
			if err != nil {
				return err
			}
		}
		return innerJoin.PrepareProjection(proc)
	}
	return err
}

func (innerJoin *InnerJoin) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := innerJoin.OpAnalyzer
	ctr := &innerJoin.ctr
	input := vm.NewCallResult()
	result := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {
		case Build:
			err = innerJoin.build(analyzer, proc)
			if err != nil {
				return result, err
			}

			if ctr.mp == nil && !innerJoin.IsShuffle {
				// for inner ,right and semi join, if hashmap is empty, we can finish this pipeline
				// shuffle join can't stop early for this moment
				ctr.state = End
			} else {
				ctr.state = Probe
			}
		case Probe:
			if innerJoin.ctr.inbat == nil {
				input, err = vm.ChildrenCall(innerJoin.Children[0], proc, analyzer)
				if err != nil {
					return input, err
				}
				bat := input.Batch
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
				if ctr.mp == nil {
					continue
				}
				ctr.inbat = bat
				ctr.lastRow = 0
			}

			startrow := innerJoin.ctr.lastRow
			if err := ctr.probe(innerJoin, proc, &result); err != nil {
				return result, err
			}
			if innerJoin.ctr.lastRow == 0 {
				innerJoin.ctr.inbat = nil
			} else if innerJoin.ctr.lastRow == startrow {
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

func (innerJoin *InnerJoin) build(analyzer process.Analyzer, proc *process.Process) (err error) {
	ctr := &innerJoin.ctr
	start := time.Now()
	defer analyzer.WaitStop(start)
	ctr.mp, err = message.ReceiveJoinMap(innerJoin.JoinMapTag, innerJoin.IsShuffle, innerJoin.ShuffleIdx, proc.GetMessageBoard(), proc.Ctx)
	if err != nil {
		return err
	}
	if ctr.mp != nil {
		ctr.maxAllocSize = max(ctr.maxAllocSize, ctr.mp.Size())
	}
	ctr.batchRowCount = ctr.mp.GetRowCount()
	return nil
}

func (ctr *container) probe(ap *InnerJoin, proc *process.Process, result *vm.CallResult) error {

	mpbat := ctr.mp.GetBatches()
	if ctr.rbat == nil {
		ctr.rbat = batch.NewOffHeapWithSize(len(ap.Result))
		for i, rp := range ap.Result {
			if rp.Rel == 0 {
				ctr.rbat.Vecs[i] = vector.NewOffHeapVecWithType(*ap.ctr.inbat.Vecs[rp.Pos].GetType())
				// for inner join, if left batch is sorted , then output batch is sorted
				ctr.rbat.Vecs[i].SetSorted(ap.ctr.inbat.Vecs[rp.Pos].GetSorted())
			} else {
				ctr.rbat.Vecs[i] = vector.NewOffHeapVecWithType(*mpbat[0].Vecs[rp.Pos].GetType())
			}
		}
	} else {
		ctr.rbat.CleanOnlyData()
		for i, rp := range ap.Result {
			if rp.Rel == 0 {
				ctr.rbat.Vecs[i].SetSorted(ap.ctr.inbat.Vecs[rp.Pos].GetSorted())
			}
		}
	}

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
	if ctr.itr == nil {
		ctr.itr = ctr.mp.NewIterator()
	}
	itr := ctr.itr
	rowCount := 0
	for i := ap.ctr.lastRow; i < count; i += hashmap.UnitLimit {
		if rowCount >= colexec.DefaultBatchSize {
			ctr.rbat.AddRowCount(rowCount)
			result.Batch = ctr.rbat
			ap.ctr.lastRow = i
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
			idx := vals[k] - 1

			if ap.Cond == nil {
				if ap.HashOnPK || ctr.mp.HashOnUnique() {
					for j, rp := range ap.Result {
						if rp.Rel == 0 {
							if err := ctr.rbat.Vecs[j].UnionOne(ap.ctr.inbat.Vecs[rp.Pos], int64(i+k), proc.Mp()); err != nil {
								return err
							}
						} else {
							idx1, idx2 := idx/colexec.DefaultBatchSize, idx%colexec.DefaultBatchSize
							if err := ctr.rbat.Vecs[j].UnionOne(mpbat[idx1].Vecs[rp.Pos], int64(idx2), proc.Mp()); err != nil {
								return err
							}
						}
					}
					rowCount++
				} else {
					sels := ctr.mp.GetSels(idx)
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
			} else {
				if ap.HashOnPK || ctr.mp.HashOnUnique() {
					if err := ctr.evalApCondForOneSel(ap.ctr.inbat, ctr.rbat, ap, proc, int64(i+k), int64(idx)); err != nil {
						return err
					}
					rowCount++
				} else {
					sels := ctr.mp.GetSels(idx)
					for _, sel := range sels {
						if err := ctr.evalApCondForOneSel(ap.ctr.inbat, ctr.rbat, ap, proc, int64(i+k), int64(sel)); err != nil {
							return err
						}
					}
					rowCount += len(sels)
				}
			}
		}
	}

	ctr.rbat.AddRowCount(rowCount)
	result.Batch = ctr.rbat
	ap.ctr.lastRow = 0
	return nil
}

func (ctr *container) evalApCondForOneSel(bat, rbat *batch.Batch, ap *InnerJoin, proc *process.Process, row, sel int64) error {
	mpbat := ctr.mp.GetBatches()
	if err := colexec.SetJoinBatchValues(ctr.joinBat1, bat, row,
		1, ctr.cfs1); err != nil {
		return err
	}
	idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
	if err := colexec.SetJoinBatchValues(ctr.joinBat2, mpbat[idx1], idx2,
		1, ctr.cfs2); err != nil {
		return err
	}
	vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat1, ctr.joinBat2}, nil)
	if err != nil {
		return err
	}
	if vec.IsConstNull() || vec.GetNulls().Contains(0) {
		return nil
	}
	bs := vector.MustFixedColWithTypeCheck[bool](vec)
	if !bs[0] {
		return nil
	}
	for j, rp := range ap.Result {
		if rp.Rel == 0 {
			if err := rbat.Vecs[j].UnionOne(bat.Vecs[rp.Pos], row, proc.Mp()); err != nil {
				return err
			}
		} else {
			if err := rbat.Vecs[j].UnionOne(mpbat[idx1].Vecs[rp.Pos], idx2, proc.Mp()); err != nil {
				return err
			}
		}
	}
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
