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

package loopsemi

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

const opName = "loop_semi"

func (loopSemi *LoopSemi) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": ⨝ ")
}

func (loopSemi *LoopSemi) OpType() vm.OpType {
	return vm.LoopSemi
}

func (loopSemi *LoopSemi) Prepare(proc *process.Process) error {
	var err error
	loopSemi.OpAnalyzer = process.NewAnalyzer(loopSemi.GetIdx(), loopSemi.IsFirst, loopSemi.IsLast, "loop semi join")
	if loopSemi.Cond != nil && loopSemi.ctr.expr == nil {
		loopSemi.ctr.expr, err = colexec.NewExpressionExecutor(proc, loopSemi.Cond)
		if err != nil {
			return err
		}
	}

	err = loopSemi.PrepareProjection(proc)
	return err
}

func (loopSemi *LoopSemi) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	//anal := proc.GetAnalyze(loopSemi.GetIdx(), loopSemi.GetParallelIdx(), loopSemi.GetParallelMajor())
	//anal.Start()
	//defer anal.Stop()
	analyzer := loopSemi.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	ctr := &loopSemi.ctr
	input := vm.NewCallResult()
	result := vm.NewCallResult()
	probeResult := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {
		case Build:
			if err := loopSemi.build(proc, analyzer); err != nil {
				return result, err
			}
			if ctr.bat == nil {
				// for inner ,right and semi join, if hashmap is empty, we can finish this pipeline
				ctr.state = End
			} else {
				ctr.state = Probe
			}

		case Probe:
			if loopSemi.ctr.buf == nil {
				//input, err = loopSemi.Children[0].Call(proc)
				input, err = vm.ChildrenCall(loopSemi.GetChildren(0), proc, analyzer)
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
				if ctr.bat == nil || ctr.bat.RowCount() == 0 {
					continue
				}
				loopSemi.ctr.buf = bat
				loopSemi.ctr.lastrow = 0
				//anal.Input(loopSemi.ctr.buf, loopSemi.GetIsFirst())
			}

			if ctr.rbat == nil {
				ctr.rbat = batch.NewWithSize(len(loopSemi.Result))
				for i, pos := range loopSemi.Result {
					ctr.rbat.Vecs[i] = vector.NewVec(*loopSemi.ctr.buf.Vecs[pos].GetType())
				}
			} else {
				ctr.rbat.CleanOnlyData()
			}

			err := ctr.probe(loopSemi, proc, &probeResult)
			if err != nil {
				return result, err
			}

			result.Batch, err = loopSemi.EvalProjection(probeResult.Batch, proc)
			if err != nil {
				return result, err
			}

			//anal.Output(result.Batch, loopSemi.GetIsLast())
			analyzer.Output(result.Batch)
			return result, err

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (loopSemi *LoopSemi) build(proc *process.Process, analyzer process.Analyzer) error {
	ctr := &loopSemi.ctr
	start := time.Now()
	defer analyzer.WaitStop(start)
	mp := message.ReceiveJoinMap(loopSemi.JoinMapTag, false, 0, proc.GetMessageBoard(), proc.Ctx)
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

func (ctr *container) probe(ap *LoopSemi, proc *process.Process, result *vm.CallResult) error {
	count := ap.ctr.buf.RowCount()
	if ctr.joinBat == nil {
		ctr.joinBat, ctr.cfs = colexec.NewJoinBatch(ap.ctr.buf, proc.Mp())
	}

	rowCountIncrease := 0
	for i := ap.ctr.lastrow; i < count; i++ {
		if rowCountIncrease >= colexec.DefaultBatchSize {
			ctr.rbat.SetRowCount(ctr.rbat.RowCount() + rowCountIncrease)
			result.Batch = ctr.rbat
			ap.ctr.lastrow = i
			return nil
		}
		if err := colexec.SetJoinBatchValues(ctr.joinBat, ap.ctr.buf, int64(i),
			ctr.bat.RowCount(), ctr.cfs); err != nil {
			return err
		}
		vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat, ctr.bat}, nil)
		if err != nil {
			return err
		}

		rs := vector.GenerateFunctionFixedTypeParameter[bool](vec)
		for k := uint64(0); k < uint64(vec.Length()); k++ {
			b, null := rs.GetValue(k)
			if !null && b {
				for k, pos := range ap.Result {
					if err = ctr.rbat.Vecs[k].UnionOne(ap.ctr.buf.Vecs[pos], int64(i), proc.Mp()); err != nil {
						return err
					}
				}
				rowCountIncrease++
				break
			}
		}
	}
	ctr.rbat.SetRowCount(ctr.rbat.RowCount() + rowCountIncrease)
	result.Batch = ctr.rbat
	ap.ctr.lastrow = 0
	ctr.buf = nil
	return nil
}
