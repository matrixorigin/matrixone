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

package loopanti

import (
	"bytes"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/message"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "loop_anti"

func (loopAnti *LoopAnti) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": loop anti join ")
}

func (loopAnti *LoopAnti) OpType() vm.OpType {
	return vm.LoopAnti
}

func (loopAnti *LoopAnti) Prepare(proc *process.Process) error {
	var err error

	if loopAnti.Cond != nil {
		loopAnti.ctr.expr, err = colexec.NewExpressionExecutor(proc, loopAnti.Cond)
		if err != nil {
			return err
		}
	}

	if loopAnti.ProjectList != nil {
		err = loopAnti.PrepareProjection(proc)
	}
	return err
}

func (loopAnti *LoopAnti) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(loopAnti.GetIdx(), loopAnti.GetParallelIdx(), loopAnti.GetParallelMajor())
	anal.Start()
	defer anal.Stop()
	ctr := &loopAnti.ctr
	input := vm.NewCallResult()
	result := vm.NewCallResult()
	probeResult := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {
		case Build:
			if err = loopAnti.build(proc, anal); err != nil {
				return result, err
			}
			ctr.state = Probe

		case Probe:

			input, err = loopAnti.Children[0].Call(proc)
			if err != nil {
				return result, err
			}
			inbat := input.Batch
			if inbat == nil {
				ctr.state = End
				continue
			}
			if inbat.RowCount() == 0 {
				continue
			}
			anal.Input(inbat, loopAnti.GetIsFirst())

			if ctr.rbat == nil {
				ctr.rbat = batch.NewWithSize(len(loopAnti.Result))
				for i, pos := range loopAnti.Result {
					ctr.rbat.Vecs[i] = vector.NewVec(*inbat.Vecs[pos].GetType())
				}
			} else {
				ctr.rbat.CleanOnlyData()
			}

			if ctr.bat == nil || ctr.bat.RowCount() == 0 {
				err = ctr.emptyProbe(loopAnti, inbat, proc, &probeResult)
			} else {
				err = ctr.probe(loopAnti, inbat, proc, &probeResult)
			}
			if err != nil {
				return result, err
			}

			result.Batch, err = loopAnti.EvalProjection(probeResult.Batch, proc)
			if err != nil {
				return result, err
			}

			anal.Output(result.Batch, loopAnti.GetIsLast())
			return result, err
		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (loopAnti *LoopAnti) build(proc *process.Process, anal process.Analyze) error {
	ctr := loopAnti.ctr
	start := time.Now()
	defer anal.WaitStop(start)
	mp := message.ReceiveJoinMap(loopAnti.JoinMapTag, false, 0, proc.GetMessageBoard(), proc.Ctx)
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

func (ctr *container) emptyProbe(ap *LoopAnti, inbat *batch.Batch, proc *process.Process, result *vm.CallResult) error {
	for i, pos := range ap.Result {
		if err := vector.GetUnionAllFunction(*ctr.rbat.Vecs[i].GetType(), proc.Mp())(ctr.rbat.Vecs[i], inbat.Vecs[pos]); err != nil {
			return err
		}
	}
	ctr.rbat.AddRowCount(inbat.RowCount())
	result.Batch = ctr.rbat

	return nil
}

func (ctr *container) probe(ap *LoopAnti, inbat *batch.Batch, proc *process.Process, result *vm.CallResult) error {
	count := inbat.RowCount()
	if ctr.joinBat == nil {
		ctr.joinBat, ctr.cfs = colexec.NewJoinBatch(inbat, proc.Mp())
	}

	rowCountIncrease := 0
	for i := 0; i < count; i++ {
		if err := colexec.SetJoinBatchValues(ctr.joinBat, inbat, int64(i),
			ctr.bat.RowCount(), ctr.cfs); err != nil {
			return err
		}
		matched := false
		vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat, ctr.bat}, nil)
		if err != nil {
			return err
		}

		rs := vector.GenerateFunctionFixedTypeParameter[bool](vec)
		for k := uint64(0); k < uint64(vec.Length()); k++ {
			b, null := rs.GetValue(k)
			if !null && b {
				matched = true
				break
			}
		}
		if !matched && !nulls.Any(vec.GetNulls()) {
			for k, pos := range ap.Result {
				if err := ctr.rbat.Vecs[k].UnionOne(inbat.Vecs[pos], int64(i), proc.Mp()); err != nil {
					return err
				}
			}
			rowCountIncrease++
		}
	}
	ctr.rbat.SetRowCount(ctr.rbat.RowCount() + rowCountIncrease)

	result.Batch = ctr.rbat
	return nil
}
