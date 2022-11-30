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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" loop anti join ")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	return nil
}

func Call(idx int, proc *process.Process, arg any) (bool, error) {
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
			bat := <-proc.Reg.MergeReceivers[0].Ch
			if bat == nil {
				ctr.state = End
				continue
			}
			if bat.Length() == 0 {
				continue
			}
			if ctr.bat == nil || ctr.bat.Length() == 0 {
				err = ctr.emptyProbe(bat, ap, proc, anal)
			} else {
				err = ctr.probe(bat, ap, proc, anal)
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
	bat := <-proc.Reg.MergeReceivers[1].Ch
	if bat != nil {
		ctr.bat = bat
	}
	return nil
}

func (ctr *container) emptyProbe(bat *batch.Batch, ap *Argument, proc *process.Process, anal process.Analyze) error {
	anal.Input(bat)
	rbat := batch.NewWithSize(len(ap.Result))
	for i, pos := range ap.Result {
		rbat.Vecs[i] = bat.Vecs[pos]
		bat.Vecs[pos] = nil
	}
	rbat.Zs = bat.Zs
	bat.Zs = nil
	anal.Output(rbat)
	proc.SetInputBatch(rbat)
	return nil
}

func (ctr *container) probe(bat *batch.Batch, ap *Argument, proc *process.Process, anal process.Analyze) error {
	anal.Input(bat)
	rbat := batch.NewWithSize(len(ap.Result))
	rbat.Zs = proc.Mp().GetSels()
	for i, pos := range ap.Result {
		rbat.Vecs[i] = vector.New(bat.Vecs[pos].Typ)
	}
	count := bat.Length()
	for i := 0; i < count; i++ {
		matched := false
		vec, err := colexec.JoinFilterEvalExpr(bat, ctr.bat, i, proc, ap.Cond)
		if err != nil {
			rbat.Clean(proc.Mp())
			return err
		}
		bs := vector.MustTCols[bool](vec)
		for _, b := range bs {
			if b {
				matched = true
				break
			}
		}
		if !matched && !nulls.Any(vec.Nsp) {
			for k, pos := range ap.Result {
				if err := vector.UnionOne(rbat.Vecs[k], bat.Vecs[pos], int64(i), proc.Mp()); err != nil {
					rbat.Clean(proc.Mp())
					vec.Free(proc.Mp())
					return err
				}
			}
			rbat.Zs = append(rbat.Zs, bat.Zs[i])
		}
		vec.Free(proc.Mp())
	}
	rbat.ExpandNulls()
	anal.Output(rbat)
	proc.SetInputBatch(rbat)
	return nil
}
