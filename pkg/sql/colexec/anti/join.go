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
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/message"

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
	if antiJoin.OpAnalyzer == nil {
		antiJoin.OpAnalyzer = process.NewAnalyzer(antiJoin.GetIdx(), antiJoin.IsFirst, antiJoin.IsLast, "anti join")
	} else {
		antiJoin.OpAnalyzer.Reset()
	}

	if antiJoin.ctr.vecs == nil {
		antiJoin.ctr.vecs = make([]*vector.Vector, len(antiJoin.Conditions[0]))
		antiJoin.ctr.executor, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, antiJoin.Conditions[0])
		if err != nil {
			return err
		}

		if antiJoin.Cond != nil {
			antiJoin.ctr.expr, err = colexec.NewExpressionExecutor(proc, antiJoin.Cond)
			if err != nil {
				return err
			}
		}

		return antiJoin.PrepareProjection(proc)
	}
	return nil
}

func (antiJoin *AntiJoin) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	analyzer := antiJoin.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	ap := antiJoin
	input := vm.NewCallResult()
	result := vm.NewCallResult()
	probeResult := vm.NewCallResult()
	var err error
	ctr := &ap.ctr
	for {
		switch ctr.state {
		case Build:
			antiJoin.build(analyzer, proc)
			ctr.state = Probe

		case Probe:
			input, err = vm.ChildrenCall(antiJoin.GetChildren(0), proc, analyzer)
			if err != nil {
				return result, err
			}
			inbat := input.Batch
			if inbat == nil {
				ctr.state = End
				continue
			}
			if inbat.Last() {
				result.Batch = inbat
				analyzer.Output(result.Batch)
				return result, nil
			}
			if inbat.IsEmpty() {
				continue
			}
			//anal.Input(inbat, antiJoin.GetIsFirst())

			if ctr.rbat == nil {
				ctr.rbat = batch.NewWithSize(len(ap.Result))
				for i, pos := range ap.Result {
					ctr.rbat.Vecs[i] = vector.NewVec(*inbat.Vecs[pos].GetType())
					// for anti join, if left batch is sorted , then output batch is sorted
					ctr.rbat.Vecs[i].SetSorted(inbat.Vecs[pos].GetSorted())
				}
			} else {
				ctr.rbat.CleanOnlyData()
				for i, pos := range ap.Result {
					ctr.rbat.Vecs[i].SetSorted(inbat.Vecs[pos].GetSorted())
				}
			}

			if ctr.mp == nil {
				err = ctr.emptyProbe(ap, inbat, proc, &probeResult)
			} else {
				err = ctr.probe(ap, inbat, proc, &probeResult)
			}
			if err != nil {
				return result, err
			}

			result.Batch, err = ap.EvalProjection(probeResult.Batch, proc)
			if err != nil {
				return result, err
			}

			//anal.Output(result.Batch, antiJoin.GetIsLast())
			analyzer.Output(result.Batch)
			return result, nil

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (antiJoin *AntiJoin) build(analyzer process.Analyzer, proc *process.Process) {
	ctr := &antiJoin.ctr
	start := time.Now()
	defer analyzer.WaitStop(start)
	ctr.mp = message.ReceiveJoinMap(antiJoin.JoinMapTag, antiJoin.IsShuffle, antiJoin.ShuffleIdx, proc.GetMessageBoard(), proc.Ctx)
	if ctr.mp != nil {
		ctr.maxAllocSize = max(ctr.maxAllocSize, ctr.mp.Size())
	}
	ctr.batchRowCount = ctr.mp.GetRowCount()
}

func (ctr *container) emptyProbe(ap *AntiJoin, inbat *batch.Batch, proc *process.Process, result *vm.CallResult) error {

	count := inbat.RowCount()
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		for k := 0; k < n; k++ {
			for j, pos := range ap.Result {
				if err := ctr.rbat.Vecs[j].UnionOne(inbat.Vecs[pos], int64(i+k), proc.Mp()); err != nil {
					return err
				}
			}
		}
		ctr.rbat.AddRowCount(n)
	}

	result.Batch = ctr.rbat
	return nil
}

func (ctr *container) probe(ap *AntiJoin, inbat *batch.Batch, proc *process.Process, result *vm.CallResult) error {
	mpbat := ctr.mp.GetBatches()

	if err := ctr.evalJoinCondition(inbat, proc); err != nil {
		return err
	}

	if ctr.joinBat1 == nil {
		ctr.joinBat1, ctr.cfs1 = colexec.NewJoinBatch(inbat, proc.Mp())
	}
	if ctr.joinBat2 == nil && ctr.batchRowCount > 0 {
		ctr.joinBat2, ctr.cfs2 = colexec.NewJoinBatch(mpbat[0], proc.Mp())
	}

	count := inbat.RowCount()
	mSels := ctr.mp.Sels()
	itr := ctr.mp.NewIterator()
	eligible := make([]int64, 0, hashmap.UnitLimit)
	for i := 0; i < count; i += hashmap.UnitLimit {
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
				eligible = append(eligible, int64(i+k))
				rowCountIncrease++
				continue
			}
			if ap.Cond != nil {
				if ap.HashOnPK {
					idx1, idx2 := int64(vals[k]-1)/colexec.DefaultBatchSize, int64(vals[k]-1)%colexec.DefaultBatchSize
					if err := colexec.SetJoinBatchValues(ctr.joinBat1, inbat, int64(i+k),
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
						continue
					}
				} else {
					matched := false // mark if any tuple satisfies the condition
					sels := mSels[vals[k]-1]
					for _, sel := range sels {
						idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
						if err := colexec.SetJoinBatchValues(ctr.joinBat1, inbat, int64(i+k),
							1, ctr.cfs1); err != nil {
							return err
						}
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
						if bs[0] {
							matched = true
							break
						}
					}
					if matched {
						continue
					}
				}
				eligible = append(eligible, int64(i+k))
				rowCountIncrease++
			}
		}
		ctr.rbat.SetRowCount(ctr.rbat.RowCount() + rowCountIncrease)

		for j, pos := range ap.Result {
			if err := ctr.rbat.Vecs[j].Union(inbat.Vecs[pos], eligible, proc.Mp()); err != nil {
				return err
			}
		}
		eligible = eligible[:0]
	}

	result.Batch = ctr.rbat
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
