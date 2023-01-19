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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" loop single join ")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.bat = batch.NewWithSize(len(ap.Typs))
	ap.ctr.bat.Zs = proc.Mp().GetSels()
	for i, typ := range ap.Typs {
		ap.ctr.bat.Vecs[i] = vector.New(typ)
	}
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	ap := arg.(*Argument)
	ctr := ap.ctr
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(ap, proc, anal); err != nil {
				return false, err
			}
			ctr.state = Probe

		case Probe:
			var err error
			start := time.Now()
			bat := <-proc.Reg.MergeReceivers[0].Ch
			anal.WaitStop(start)
			if bat == nil {
				ctr.state = End
				continue
			}
			if bat.Length() == 0 {
				continue
			}
			if ctr.bat.Length() == 0 {
				err = ctr.emptyProbe(bat, ap, proc, anal, isFirst, isLast)
			} else {
				err = ctr.probe(bat, ap, proc, anal, isFirst, isLast)
			}
			bat.Clean(proc.Mp())
			return false, err

		default:
			ap.Free(proc, false)
			proc.SetInputBatch(nil)
			return true, nil
		}
	}
}

func (ctr *container) build(ap *Argument, proc *process.Process, anal process.Analyze) error {
	start := time.Now()
	bat := <-proc.Reg.MergeReceivers[1].Ch
	anal.WaitStop(start)

	if bat != nil {
		ctr.bat = bat
	}
	return nil
}

func (ctr *container) emptyProbe(bat *batch.Batch, ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool) error {
	anal.Input(bat, isFirst)
	rbat := batch.NewWithSize(len(ap.Result))
	count := bat.Length()
	for i, rp := range ap.Result {
		if rp >= 0 {
			rbat.Vecs[i] = bat.Vecs[rp]
			bat.Vecs[rp] = nil
		} else {
			rbat.Vecs[i] = vector.NewConstFixed(types.T_bool.ToType(), count, false, proc.Mp())
		}
	}
	rbat.Zs = bat.Zs
	bat.Zs = nil
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
			rbat.Vecs[i] = vector.New(types.T_bool.ToType())
			markPos = i
			break
		}
	}
	if markPos == -1 {
		return moerr.NewInternalError(proc.Ctx, "MARK join must output mark column")
	}
	count := bat.Length()
	for i := 0; i < count; i++ {
		vec, err := colexec.JoinFilterEvalExpr(bat, ctr.bat, i, proc, ap.Cond)
		if err != nil {
			rbat.Clean(proc.Mp())
			return err
		}
		exprVals := vector.MustTCols[bool](vec)
		hasTrue := false
		hasNull := false
		for j := range exprVals {
			if vec.Nsp.Contains(uint64(j)) {
				hasNull = true
			} else if exprVals[j] {
				hasTrue = true
			}
		}
		if hasTrue {
			rbat.Vecs[markPos].Append(true, false, proc.Mp())
		} else if hasNull {
			rbat.Vecs[markPos].Append(false, true, proc.Mp())
		} else {
			rbat.Vecs[markPos].Append(false, false, proc.Mp())
		}
		vec.Free(proc.Mp())
	}
	for i, rp := range ap.Result {
		if rp >= 0 {
			rbat.Vecs[i] = bat.Vecs[rp]
			bat.Vecs[rp] = nil
		}
	}
	rbat.Zs = bat.Zs
	bat.Zs = nil
	rbat.ExpandNulls()
	anal.Output(rbat, isLast)
	proc.SetInputBatch(rbat)
	return nil
}
