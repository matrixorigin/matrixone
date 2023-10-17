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

package product

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" cross join ")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.InitReceiver(proc, false)
	return nil
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
			bat, _, err := ctr.ReceiveFromSingleReg(0, anal)
			if err != nil {
				return process.ExecNext, err
			}

			if bat == nil {
				ctr.state = End
				continue
			}
			if bat.IsEmpty() {
				proc.PutBatch(bat)
				continue
			}
			if ctr.bat == nil {
				proc.PutBatch(bat)
				continue
			}
			if err = ctr.probe(bat, ap, proc, anal, isFirst, isLast); err != nil {
				bat.Clean(proc.Mp())
				return process.ExecNext, err
			}
			proc.PutBatch(bat)
			return process.ExecNext, nil

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

func (ctr *container) probe(bat *batch.Batch, ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool) error {
	anal.Input(bat, isFirst)
	rbat := batch.NewWithSize(len(ap.Result))
	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			rbat.Vecs[i] = vector.NewVec(*bat.Vecs[rp.Pos].GetType())
		} else {
			rbat.Vecs[i] = vector.NewVec(*ctr.bat.Vecs[rp.Pos].GetType())
		}
	}
	count := bat.RowCount()
	count2 := ctr.bat.RowCount()
	for i := 0; i < count; i++ {
		for j := 0; j < count2; j++ {
			for k, rp := range ap.Result {
				if rp.Rel == 0 {
					if err := rbat.Vecs[k].UnionOne(bat.Vecs[rp.Pos], int64(i), proc.Mp()); err != nil {
						rbat.Clean(proc.Mp())
						return err
					}
				} else {
					if err := rbat.Vecs[k].UnionOne(ctr.bat.Vecs[rp.Pos], int64(j), proc.Mp()); err != nil {
						rbat.Clean(proc.Mp())
						return err
					}
				}
			}
		}
	}
	rbat.AddRowCount(count * count2)
	anal.Output(rbat, isLast)
	proc.SetInputBatch(rbat)
	return nil
}
