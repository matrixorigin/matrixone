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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" loop mark join ")
}

func Prepare(proc *process.Process, arg any) error {
	var err error

	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.InitReceiver(proc, false)
	ap.ctr.bat = batch.NewWithSize(len(ap.Typs))
	for i, typ := range ap.Typs {
		ap.ctr.bat.Vecs[i] = vector.NewVec(typ)
	}

	if ap.Cond != nil {
		ap.ctr.expr, err = colexec.NewExpressionExecutor(proc, ap.Cond)
	}
	return err
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (process.ExecStatus, error) {
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	ap := arg.(*Argument)
	ctr := ap.ctr
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(ap, proc, anal); err != nil {
				return process.ExecNext, err
			}
			ctr.state = Probe

		case Probe:
			var err error
			bat, _, err := ctr.ReceiveFromSingleReg(0, anal)
			if err != nil {
				return process.ExecNext, err
			}

			if bat == nil {
				ctr.state = End
				continue
			}
			if bat.RowCount() == 0 {
				bat.Clean(proc.Mp())
				continue
			}
			if ctr.bat.RowCount() == 0 {
				err = ctr.emptyProbe(bat, ap, proc, anal, isFirst, isLast)
			} else {
				err = ctr.probe(bat, ap, proc, anal, isFirst, isLast)
			}
			proc.PutBatch(bat)
			return process.ExecNext, err

		default:
			proc.SetInputBatch(nil)
			return process.ExecStop, nil
		}
	}
}

func (ctr *container) build(ap *Argument, proc *process.Process, anal process.Analyze) error {
	bat, _, err := ctr.ReceiveFromSingleReg(1, anal)
	if err != nil {
		return err
	}

	if bat != nil {
		ctr.bat = bat
	}
	return nil
}

func (ctr *container) emptyProbe(bat *batch.Batch, ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool) error {
	anal.Input(bat, isFirst)
	rbat := batch.NewWithSize(len(ap.Result))
	count := bat.RowCount()
	for i, rp := range ap.Result {
		if rp >= 0 {
			// rbat.Vecs[i] = bat.Vecs[rp]
			// bat.Vecs[rp] = nil
			typ := *bat.Vecs[rp].GetType()
			rbat.Vecs[i] = vector.NewVec(typ)
			if err := vector.GetUnionAllFunction(typ, proc.Mp())(rbat.Vecs[i], bat.Vecs[rp]); err != nil {
				return err
			}
		} else {
			rbat.Vecs[i] = vector.NewConstFixed(types.T_bool.ToType(), false, count, proc.Mp())
		}
	}
	rbat.AddRowCount(bat.RowCount())
	anal.Output(rbat, isLast)

	proc.SetInputBatch(rbat)
	return nil
}

func (ctr *container) probe(bat *batch.Batch, ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool) error {
	anal.Input(bat, isFirst)
	rbat := batch.NewWithSize(len(ap.Result))
	markPos := -1
	for i, pos := range ap.Result {
		if pos == -1 {
			rbat.Vecs[i] = vector.NewVec(types.T_bool.ToType())
			rbat.Vecs[i].PreExtend(bat.RowCount(), proc.Mp())
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
			rbat.Clean(proc.Mp())
			return err
		}
		vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat, ctr.bat})
		if err != nil {
			rbat.Clean(proc.Mp())
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
			vector.AppendFixed(rbat.Vecs[markPos], true, false, proc.Mp())
		} else if hasNull {
			vector.AppendFixed(rbat.Vecs[markPos], false, true, proc.Mp())
		} else {
			vector.AppendFixed(rbat.Vecs[markPos], false, false, proc.Mp())
		}
	}
	for i, rp := range ap.Result {
		if rp >= 0 {
			rbat.Vecs[i] = bat.Vecs[rp]
			bat.Vecs[rp] = nil
		}
	}
	rbat.AddRowCount(bat.RowCount())
	anal.Output(rbat, isLast)
	proc.SetInputBatch(rbat)
	return nil
}
