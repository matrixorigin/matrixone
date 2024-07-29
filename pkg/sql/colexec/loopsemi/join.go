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

	loopSemi.ctr = new(container)
	loopSemi.ctr.InitReceiver(proc, true)

	if loopSemi.Cond != nil {
		loopSemi.ctr.expr, err = colexec.NewExpressionExecutor(proc, loopSemi.Cond)
	}
	return err
}

func (loopSemi *LoopSemi) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(loopSemi.GetIdx(), loopSemi.GetParallelIdx(), loopSemi.GetParallelMajor())
	anal.Start()
	defer anal.Stop()
	ctr := loopSemi.ctr
	result := vm.NewCallResult()
	for {
		switch ctr.state {
		case Build:
			if err := loopSemi.build(proc, anal); err != nil {
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
				msg := ctr.ReceiveFromAllRegs(anal)
				if msg.Err != nil {
					return result, msg.Err
				}

				bat := msg.Batch
				if bat == nil {
					ctr.state = End
					continue
				}
				if bat.IsEmpty() {
					proc.PutBatch(bat)
					continue
				}
				if ctr.bat == nil || ctr.bat.RowCount() == 0 {
					proc.PutBatch(bat)
					continue
				}
				loopSemi.ctr.buf = bat
				loopSemi.ctr.lastrow = 0
			}

			err := ctr.probe(loopSemi, proc, anal, loopSemi.GetIsFirst(), loopSemi.GetIsLast(), &result)
			if loopSemi.ctr.lastrow == 0 {
				proc.PutBatch(loopSemi.ctr.buf)
				loopSemi.ctr.buf = nil
			}
			return result, err

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (loopSemi *LoopSemi) build(proc *process.Process, anal process.Analyze) error {
	ctr := loopSemi.ctr
	mp := proc.ReceiveJoinMap(anal, loopSemi.JoinMapTag, false, 0)
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
	return nil
}

func (ctr *container) probe(ap *LoopSemi, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool, result *vm.CallResult) error {
	anal.Input(ap.ctr.buf, isFirst)
	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	ctr.rbat = batch.NewWithSize(len(ap.Result))
	for i, pos := range ap.Result {
		ctr.rbat.Vecs[i] = proc.GetVector(*ap.ctr.buf.Vecs[pos].GetType())
	}
	count := ap.ctr.buf.RowCount()
	if ctr.joinBat == nil {
		ctr.joinBat, ctr.cfs = colexec.NewJoinBatch(ap.ctr.buf, proc.Mp())
	}

	rowCountIncrease := 0
	for i := ap.ctr.lastrow; i < count; i++ {
		if rowCountIncrease >= colexec.DefaultBatchSize {
			ctr.rbat.SetRowCount(ctr.rbat.RowCount() + rowCountIncrease)
			anal.Output(ctr.rbat, isLast)
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
	anal.Output(ctr.rbat, isLast)
	result.Batch = ctr.rbat
	ap.ctr.lastrow = 0
	return nil
}
