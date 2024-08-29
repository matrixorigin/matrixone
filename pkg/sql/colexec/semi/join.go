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
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/message"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "semi"

func (semiJoin *SemiJoin) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": semi join ")
}

func (semiJoin *SemiJoin) OpType() vm.OpType {
	return vm.Semi
}

func (semiJoin *SemiJoin) Prepare(proc *process.Process) (err error) {
	if semiJoin.OpAnalyzer == nil {
		semiJoin.OpAnalyzer = process.NewAnalyzer(semiJoin.GetIdx(), semiJoin.IsFirst, semiJoin.IsLast, "semi join")
	} else {
		semiJoin.OpAnalyzer.Reset()
	}

	if semiJoin.ctr.vecs == nil {
		semiJoin.ctr.vecs = make([]*vector.Vector, len(semiJoin.Conditions[0]))
		semiJoin.ctr.executor = make([]colexec.ExpressionExecutor, len(semiJoin.Conditions[0]))
		for i := range semiJoin.ctr.executor {
			semiJoin.ctr.executor[i], err = colexec.NewExpressionExecutor(proc, semiJoin.Conditions[0][i])
			if err != nil {
				return err
			}
		}

		if semiJoin.Cond != nil {
			semiJoin.ctr.expr, err = colexec.NewExpressionExecutor(proc, semiJoin.Cond)
			if err != nil {
				return err
			}
		}

		if semiJoin.ProjectList != nil {
			err = semiJoin.PrepareProjection(proc)
		}
	}
	return err
}

func (semiJoin *SemiJoin) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	analyzer := semiJoin.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	ctr := &semiJoin.ctr
	input := vm.NewCallResult()
	result := vm.NewCallResult()
	probeResult := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {
		case Build:
			semiJoin.build(analyzer, proc)
			if ctr.mp == nil && !semiJoin.IsShuffle {
				// for inner ,right and semi join, if hashmap is empty, we can finish this pipeline
				// shuffle join can't stop early for this moment
				ctr.state = End
			} else {
				ctr.state = Probe
			}
			if ctr.mp != nil && ctr.mp.PushedRuntimeFilterIn() && semiJoin.Cond == nil {
				ctr.skipProbe = true
			}

		case Probe:
			//input, err = semiJoin.Children[0].Call(proc)
			input, err = vm.ChildrenCall(semiJoin.GetChildren(0), proc, analyzer)
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
			//anal.Input(bat, semiJoin.GetIsFirst())

			if ctr.skipProbe {
				newvecs := make([]*vector.Vector, len(semiJoin.Result))
				for i, pos := range semiJoin.Result {
					newvecs[i] = bat.Vecs[pos]
				}
				bat.Vecs = newvecs
				result.Batch, err = semiJoin.EvalProjection(bat, proc)
				if err != nil {
					return result, err
				}
				//anal.Output(result.Batch, semiJoin.GetIsLast())
				analyzer.Output(result.Batch)
				return result, nil
			}

			if ctr.mp == nil {
				continue
			}

			if ctr.rbat == nil {
				ctr.rbat = batch.NewWithSize(len(semiJoin.Result))
				for i, pos := range semiJoin.Result {
					ctr.rbat.Vecs[i] = vector.NewVec(*bat.Vecs[pos].GetType())
					// for semi join, if left batch is sorted , then output batch is sorted
					ctr.rbat.Vecs[i].SetSorted(bat.Vecs[pos].GetSorted())
				}
			} else {
				ctr.rbat.CleanOnlyData()
				for i, pos := range semiJoin.Result {
					ctr.rbat.Vecs[i].SetSorted(bat.Vecs[pos].GetSorted())
				}
			}

			if err := ctr.probe(bat, semiJoin, proc, &probeResult); err != nil {
				return result, err
			}

			result.Batch, err = semiJoin.EvalProjection(probeResult.Batch, proc)
			if err != nil {
				return result, err
			}

			//anal.Output(result.Batch, semiJoin.GetIsLast())
			analyzer.Output(result.Batch)
			return result, nil

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (semiJoin *SemiJoin) build(analyzer process.Analyzer, proc *process.Process) {
	ctr := &semiJoin.ctr
	start := time.Now()
	defer analyzer.WaitStop(start)
	ctr.mp = message.ReceiveJoinMap(semiJoin.JoinMapTag, semiJoin.IsShuffle, semiJoin.ShuffleIdx, proc.GetMessageBoard(), proc.Ctx)
	if ctr.mp != nil {
		ctr.maxAllocSize = max(ctr.maxAllocSize, ctr.mp.Size())
	}
}

func (ctr *container) probe(bat *batch.Batch, ap *SemiJoin, proc *process.Process, result *vm.CallResult) error {
	mpbat := ctr.mp.GetBatches()
	if err := ctr.evalJoinCondition(bat, proc); err != nil {
		return err
	}
	if ctr.joinBat1 == nil {
		ctr.joinBat1, ctr.cfs1 = colexec.NewJoinBatch(bat, proc.Mp())
	}
	if ctr.joinBat2 == nil && ctr.mp.GetRowCount() > 0 {
		ctr.joinBat2, ctr.cfs2 = colexec.NewJoinBatch(mpbat[0], proc.Mp())
	}
	count := bat.RowCount()
	mSels := ctr.mp.Sels()
	itr := ctr.mp.NewIterator()

	rowCountIncrease := 0
	eligible := make([]int64, 0) // eligible := make([]int32, 0, hashmap.UnitLimit)
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
			if ap.Cond != nil {
				matched := false // mark if any tuple satisfies the condition
				if ap.HashOnPK {
					idx1, idx2 := int64(vals[k]-1)/colexec.DefaultBatchSize, int64(vals[k]-1)%colexec.DefaultBatchSize
					if err := colexec.SetJoinBatchValues(ctr.joinBat1, bat, int64(i+k),
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
					}
				} else {
					sels := mSels[vals[k]-1]
					for _, sel := range sels {
						idx1, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
						if err := colexec.SetJoinBatchValues(ctr.joinBat1, bat, int64(i+k),
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

				}
				if !matched {
					continue
				}
			}
			eligible = append(eligible, int64(i+k))
			rowCountIncrease++
		}
		//eligible = eligible[:0]
	}

	for j, pos := range ap.Result {
		if err := ctr.rbat.Vecs[j].PreExtendWithArea(len(eligible), len(bat.Vecs[pos].GetArea()), proc.Mp()); err != nil {
			return err
		}
	}

	for j, pos := range ap.Result {
		if err := ctr.rbat.Vecs[j].Union(bat.Vecs[pos], eligible, proc.Mp()); err != nil {
			return err
		}
	}

	ctr.rbat.AddRowCount(rowCountIncrease)
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
