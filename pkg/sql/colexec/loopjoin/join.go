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

package loopjoin

import (
	"bytes"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/message"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "loop_join"

func (loopJoin *LoopJoin) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": loop join ")
}

func (loopJoin *LoopJoin) OpType() vm.OpType {
	return vm.LoopJoin
}

func (loopJoin *LoopJoin) Prepare(proc *process.Process) error {
	var err error

	if loopJoin.Cond != nil && loopJoin.ctr.expr == nil {
		loopJoin.ctr.expr, err = colexec.NewExpressionExecutor(proc, loopJoin.Cond)
		if err != nil {
			return err
		}
	}

	if loopJoin.ProjectList != nil && loopJoin.ProjectExecutors == nil {
		err = loopJoin.PrepareProjection(proc)
	}
	return err
}

func (loopJoin *LoopJoin) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(loopJoin.GetIdx(), loopJoin.GetParallelIdx(), loopJoin.GetParallelMajor())
	anal.Start()
	defer anal.Stop()
	ctr := &loopJoin.ctr
	input := vm.NewCallResult()
	result := vm.NewCallResult()
	probeResult := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {
		case Build:
			if err := loopJoin.build(proc, anal); err != nil {
				return result, err
			}
			if ctr.bat == nil {
				// for inner ,right and semi join, if hashmap is empty, we can finish this pipeline
				ctr.state = End
			} else {
				ctr.state = Probe
			}

		case Probe:
			input, err = loopJoin.Children[0].Call(proc)
			if err != nil {
				return result, err
			}
			inbat := input.Batch
			if inbat == nil {
				ctr.state = End
				continue
			}
			if inbat.IsEmpty() {
				continue
			}
			if ctr.bat == nil || ctr.bat.RowCount() == 0 {
				continue
			}
			anal.Input(inbat, loopJoin.GetIsFirst())

			if ctr.rbat == nil {
				ctr.rbat = batch.NewWithSize(len(loopJoin.Result))
				for i, rp := range loopJoin.Result {
					if rp.Rel == 0 {
						ctr.rbat.Vecs[i] = vector.NewVec(*inbat.Vecs[rp.Pos].GetType())
					} else {
						ctr.rbat.Vecs[i] = vector.NewVec(*ctr.bat.Vecs[rp.Pos].GetType())
					}
				}
			} else {
				ctr.rbat.CleanOnlyData()
			}

			err = ctr.probe(loopJoin, inbat, proc, &probeResult)
			if err != nil {
				return result, err
			}

			result.Batch, err = loopJoin.EvalProjection(probeResult.Batch, proc)
			if err != nil {
				return result, err
			}

			anal.Output(result.Batch, loopJoin.GetIsLast())
			return result, err
		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (loopJoin *LoopJoin) build(proc *process.Process, anal process.Analyze) error {
	ctr := &loopJoin.ctr
	start := time.Now()
	defer anal.WaitStop(start)
	mp := message.ReceiveJoinMap(loopJoin.JoinMapTag, false, 0, proc.GetMessageBoard(), proc.Ctx)
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

func (ctr *container) probe(ap *LoopJoin, inbat *batch.Batch, proc *process.Process, result *vm.CallResult) error {
	count := inbat.RowCount()
	if ctr.joinBat == nil {
		ctr.joinBat, ctr.cfs = colexec.NewJoinBatch(inbat, proc.Mp())
	}

	rowCountIncrease := 0
	for i := ctr.probeIdx; i < count; i++ {
		if err := colexec.SetJoinBatchValues(ctr.joinBat, inbat, int64(i),
			ctr.bat.RowCount(), ctr.cfs); err != nil {
			return err
		}
		vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat, ctr.bat}, nil)
		if err != nil {
			return err
		}

		rs := vector.GenerateFunctionFixedTypeParameter[bool](vec)
		if vec.IsConst() {
			b, null := rs.GetValue(0)
			if !null && b {
				for j := 0; j < ctr.bat.RowCount(); j++ {
					for k, rp := range ap.Result {
						if rp.Rel == 0 {
							if err = ctr.rbat.Vecs[k].UnionOne(inbat.Vecs[rp.Pos], int64(i), proc.Mp()); err != nil {
								return err
							}
						} else {
							if err = ctr.rbat.Vecs[k].UnionOne(ctr.bat.Vecs[rp.Pos], int64(j), proc.Mp()); err != nil {
								return err
							}
						}
					}
					rowCountIncrease++
				}
			}
		} else {
			l := uint64(ctr.bat.RowCount())
			for j := uint64(0); j < l; j++ {
				b, null := rs.GetValue(j)
				if !null && b {
					for k, rp := range ap.Result {
						if rp.Rel == 0 {
							if err = ctr.rbat.Vecs[k].UnionOne(inbat.Vecs[rp.Pos], int64(i), proc.Mp()); err != nil {
								return err
							}
						} else {
							if err = ctr.rbat.Vecs[k].UnionOne(ctr.bat.Vecs[rp.Pos], int64(j), proc.Mp()); err != nil {
								return err
							}
						}
					}
					rowCountIncrease++
				}
			}
		}
		if rowCountIncrease >= colexec.DefaultBatchSize {
			result.Batch = ctr.rbat
			ctr.rbat.SetRowCount(rowCountIncrease)
			ctr.probeIdx = i + 1
			return nil
		}
	}

	ctr.probeIdx = 0
	ctr.rbat.SetRowCount(rowCountIncrease)
	result.Batch = ctr.rbat
	return nil
}
