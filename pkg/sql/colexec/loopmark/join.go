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

package loopmark

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "loop_mark"

func (loopMark *LoopMark) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": loop mark join ")
}

func (loopMark *LoopMark) OpType() vm.OpType {
	return vm.LoopMark
}

func (loopMark *LoopMark) Prepare(proc *process.Process) error {
	var err error

	loopMark.ctr = new(container)
	loopMark.ctr.InitReceiver(proc, true)
	loopMark.ctr.bat = batch.NewWithSize(len(loopMark.Typs))
	for i, typ := range loopMark.Typs {
		loopMark.ctr.bat.Vecs[i] = proc.GetVector(typ)
	}

	if loopMark.Cond != nil {
		loopMark.ctr.expr, err = colexec.NewExpressionExecutor(proc, loopMark.Cond)
	}
	return err
}

func (loopMark *LoopMark) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(loopMark.GetIdx(), loopMark.GetParallelIdx(), loopMark.GetParallelMajor())
	anal.Start()
	defer anal.Stop()
	ctr := loopMark.ctr
	result := vm.NewCallResult()
	for {
		switch ctr.state {
		case Build:
			if err := loopMark.build(proc, anal); err != nil {
				return result, err
			}
			ctr.state = Probe

		case Probe:
			var err error
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
			if ctr.bat.RowCount() == 0 {
				err = ctr.emptyProbe(bat, loopMark, proc, anal, loopMark.GetIsFirst(), loopMark.GetIsLast(), &result)
			} else {
				err = ctr.probe(bat, loopMark, proc, anal, loopMark.GetIsFirst(), loopMark.GetIsLast(), &result)
			}
			proc.PutBatch(bat)
			return result, err

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (loopMark *LoopMark) build(proc *process.Process, anal process.Analyze) error {
	ctr := loopMark.ctr
	mp := proc.ReceiveJoinMap(anal, loopMark.JoinMapTag, false, 0)
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

func (ctr *container) emptyProbe(bat *batch.Batch, ap *LoopMark, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool, result *vm.CallResult) (err error) {
	anal.Input(bat, isFirst)
	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	ctr.rbat = batch.NewWithSize(len(ap.Result))
	count := bat.RowCount()
	for i, rp := range ap.Result {
		if rp >= 0 {
			typ := *bat.Vecs[rp].GetType()
			ctr.rbat.Vecs[i] = proc.GetVector(typ)
			if err = vector.GetUnionAllFunction(typ, proc.Mp())(ctr.rbat.Vecs[i], bat.Vecs[rp]); err != nil {
				return err
			}
		} else {
			if ctr.rbat.Vecs[i], err = vector.NewConstFixed(types.T_bool.ToType(), false, count, proc.Mp()); err != nil {
				return err
			}
		}
	}
	ctr.rbat.AddRowCount(bat.RowCount())
	anal.Output(ctr.rbat, isLast)

	result.Batch = ctr.rbat
	return nil
}

func (ctr *container) probe(bat *batch.Batch, ap *LoopMark, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool, result *vm.CallResult) error {
	anal.Input(bat, isFirst)
	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	ctr.rbat = batch.NewWithSize(len(ap.Result))
	markPos := -1
	for i, pos := range ap.Result {
		if pos == -1 {
			ctr.rbat.Vecs[i] = proc.GetVector(types.T_bool.ToType())
			if err := ctr.rbat.Vecs[i].PreExtend(bat.RowCount(), proc.Mp()); err != nil {
				return err
			}
			markPos = i
			break
		}
	}
	if markPos == -1 {
		return moerr.NewInternalError(proc.Ctx, "MARK join must output mark column")
	}
	count := bat.RowCount()
	if ctr.joinBat == nil {
		ctr.joinBat, ctr.cfs = colexec.NewJoinBatch(bat, proc.Mp())
	}
	for i := 0; i < count; i++ {
		if err := colexec.SetJoinBatchValues(ctr.joinBat, bat, int64(i),
			ctr.bat.RowCount(), ctr.cfs); err != nil {
			return err
		}
		vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat, ctr.bat}, nil)
		if err != nil {
			return err
		}

		rs := vector.GenerateFunctionFixedTypeParameter[bool](vec)
		hasTrue := false
		hasNull := false
		if vec.IsConst() {
			v, null := rs.GetValue(0)
			if null {
				hasNull = true
			} else {
				hasTrue = v
			}
		} else {
			for j := uint64(0); j < uint64(vec.Length()); j++ {
				val, null := rs.GetValue(j)
				if null {
					hasNull = true
				} else if val {
					hasTrue = true
				}
			}
		}
		if hasTrue {
			err = vector.AppendFixed(ctr.rbat.Vecs[markPos], true, false, proc.Mp())
		} else if hasNull {
			err = vector.AppendFixed(ctr.rbat.Vecs[markPos], false, true, proc.Mp())
		} else {
			err = vector.AppendFixed(ctr.rbat.Vecs[markPos], false, false, proc.Mp())
		}
		if err != nil {
			return err
		}
	}
	for i, rp := range ap.Result {
		if rp >= 0 {
			ctr.rbat.Vecs[i] = bat.Vecs[rp]
			bat.Vecs[rp] = nil
		}
	}
	ctr.rbat.AddRowCount(bat.RowCount())
	anal.Output(ctr.rbat, isLast)
	result.Batch = ctr.rbat
	return nil
}
