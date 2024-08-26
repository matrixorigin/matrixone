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

package loopsingle

import (
	"bytes"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/message"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "loop_single"

func (loopSingle *LoopSingle) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": loop single join ")
}

func (loopSingle *LoopSingle) OpType() vm.OpType {
	return vm.LoopSingle
}

func (loopSingle *LoopSingle) Prepare(proc *process.Process) error {
	var err error
	loopSingle.OpAnalyzer = process.NewAnalyzer(loopSingle.GetIdx(), loopSingle.IsFirst, loopSingle.IsLast, "loop_single")
	if loopSingle.Cond != nil && loopSingle.ctr.expr == nil {
		loopSingle.ctr.expr, err = colexec.NewExpressionExecutor(proc, loopSingle.Cond)
		if err != nil {
			return err
		}
	}

	if loopSingle.ProjectList != nil && loopSingle.ProjectExecutors == nil {
		err = loopSingle.PrepareProjection(proc)
	}
	return err
}

func (loopSingle *LoopSingle) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	//anal := proc.GetAnalyze(loopSingle.GetIdx(), loopSingle.GetParallelIdx(), loopSingle.GetParallelMajor())
	//anal.Start()
	//defer anal.Stop()
	analyzer := loopSingle.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	ctr := &loopSingle.ctr
	input := vm.NewCallResult()
	result := vm.NewCallResult()
	probeResult := vm.NewCallResult()
	for {
		switch ctr.state {
		case Build:
			if err := loopSingle.build(proc, analyzer); err != nil {
				return result, err
			}
			ctr.state = Probe

		case Probe:
			var err error
			//input, err = loopSingle.Children[0].Call(proc)
			input, err = vm.ChildrenCallV1(loopSingle.GetChildren(0), proc, analyzer)
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
			//anal.Input(bat, loopSingle.GetIsFirst())

			if ctr.rbat == nil {
				ctr.rbat = batch.NewWithSize(len(loopSingle.Result))
				for i, rp := range loopSingle.Result {
					if rp.Rel != 0 {
						ctr.rbat.Vecs[i] = vector.NewVec(loopSingle.Typs[rp.Pos])
					} else {
						ctr.rbat.Vecs[i] = vector.NewVec(*bat.Vecs[rp.Pos].GetType())
					}
				}
			} else {
				ctr.rbat.CleanOnlyData()
			}
			for i, rp := range loopSingle.Result {
				if rp.Rel == 0 {
					if err = vector.GetUnionAllFunction(*bat.Vecs[rp.Pos].GetType(), proc.Mp())(ctr.rbat.Vecs[i], bat.Vecs[rp.Pos]); err != nil {
						return result, err
					}
				}
			}

			if ctr.bat == nil || ctr.bat.RowCount() == 0 {
				err = ctr.emptyProbe(bat, loopSingle, &probeResult)
			} else {
				err = ctr.probe(bat, loopSingle, proc, &probeResult)
			}
			if err != nil {
				return result, err
			}

			result.Batch, err = loopSingle.EvalProjection(probeResult.Batch, proc)
			if err != nil {
				return result, err
			}

			//anal.Output(result.Batch, loopSingle.GetIsLast())
			analyzer.Output(result.Batch)
			return result, err

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (loopSingle *LoopSingle) build(proc *process.Process, analyzer process.Analyzer) error {
	ctr := &loopSingle.ctr
	start := time.Now()
	defer analyzer.WaitStop(start)
	mp := message.ReceiveJoinMap(loopSingle.JoinMapTag, false, 0, proc.GetMessageBoard(), proc.Ctx)
	if mp == nil {
		return nil
	}
	batches := mp.GetBatches()
	var err error
	//maybe optimize this in the future
	for i := range batches {
		ctr.bat, err = ctr.bat.AppendWithCopy(proc.Ctx, proc.Mp(), batches[i])
		if err != nil {
			return err
		}
	}
	mp.Free()
	return nil
}

func (ctr *container) emptyProbe(bat *batch.Batch, ap *LoopSingle, result *vm.CallResult) error {
	for i, rp := range ap.Result {
		if rp.Rel != 0 {
			ctr.rbat.Vecs[i].SetClass(vector.CONSTANT)
			ctr.rbat.Vecs[i].SetLength(bat.RowCount())
		}
	}
	ctr.rbat.SetRowCount(ctr.rbat.RowCount() + bat.RowCount())
	result.Batch = ctr.rbat
	return nil
}

func (ctr *container) probe(bat *batch.Batch, ap *LoopSingle, proc *process.Process, result *vm.CallResult) error {
	count := bat.RowCount()
	if ctr.expr == nil {
		switch ctr.bat.RowCount() {
		case 0:
			for i, rp := range ap.Result {
				if rp.Rel != 0 {
					err := vector.AppendMultiFixed(ctr.rbat.Vecs[i], 0, true, count, proc.Mp())
					if err != nil {
						return err
					}
				}
			}
		case 1:
			for i, rp := range ap.Result {
				if rp.Rel != 0 {
					err := ctr.rbat.Vecs[i].UnionMulti(ctr.bat.Vecs[rp.Pos], 0, count, proc.Mp())
					if err != nil {
						return err
					}
				}
			}
		default:
			return moerr.NewInternalError(proc.Ctx, "scalar subquery returns more than 1 row")
		}
	} else {
		if ctr.joinBat == nil {
			ctr.joinBat, ctr.cfs = colexec.NewJoinBatch(bat, proc.Mp())
		}
		for i := 0; i < count; i++ {
			if err := colexec.SetJoinBatchValues(ctr.joinBat, bat, int64(i),
				ctr.bat.RowCount(), ctr.cfs); err != nil {
				return err
			}
			unmatched := true
			vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat, ctr.bat}, nil)
			if err != nil {
				return err
			}

			rs := vector.GenerateFunctionFixedTypeParameter[bool](vec)
			if vec.IsConst() {
				b, null := rs.GetValue(0)
				if !null && b {
					if ctr.bat.RowCount() > 1 {
						return moerr.NewInternalError(proc.Ctx, "scalar subquery returns more than 1 row")
					}
					unmatched = false
					for k, rp := range ap.Result {
						if rp.Rel != 0 {
							if err := ctr.rbat.Vecs[k].UnionOne(ctr.bat.Vecs[rp.Pos], 0, proc.Mp()); err != nil {
								return err
							}
						}
					}
				}
			} else {
				l := vec.Length()
				for j := uint64(0); j < uint64(l); j++ {
					b, null := rs.GetValue(j)
					if !null && b {
						if !unmatched {
							return moerr.NewInternalError(proc.Ctx, "scalar subquery returns more than 1 row")
						}
						unmatched = false
						for k, rp := range ap.Result {
							if rp.Rel != 0 {
								if err := ctr.rbat.Vecs[k].UnionOne(ctr.bat.Vecs[rp.Pos], int64(j), proc.Mp()); err != nil {
									return err
								}
							}
						}
					}
				}
			}
			if unmatched {
				for k, rp := range ap.Result {
					if rp.Rel != 0 {
						if err := ctr.rbat.Vecs[k].UnionNull(proc.Mp()); err != nil {
							return err
						}
					}
				}
			}
		}
	}
	ctr.rbat.AddRowCount(bat.RowCount())
	result.Batch = ctr.rbat
	return nil
}
