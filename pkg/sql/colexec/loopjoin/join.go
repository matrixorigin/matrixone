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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" loop join ")
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
				ctr.state = End
				return true, err
			}
			ctr.state = Probe
		case Probe:
			bat := <-proc.Reg.MergeReceivers[0].Ch
			if bat == nil {
				ctr.state = End
				if ctr.bat != nil {
					ctr.bat.Clean(proc.Mp())
				}
				continue
			}
			if bat.Length() == 0 {
				continue
			}
			if ctr.bat == nil || ctr.bat.Length() == 0 {
				bat.Clean(proc.Mp())
				continue
			}
			if err := ctr.probe(bat, ap, proc, anal); err != nil {
				ctr.state = End
				proc.SetInputBatch(nil)
				return true, err
			}
			return false, nil
		default:
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

func (ctr *container) probe(bat *batch.Batch, ap *Argument, proc *process.Process, anal process.Analyze) error {
	defer bat.Clean(proc.Mp())
	anal.Input(bat)
	rbat := batch.NewWithSize(len(ap.Result))
	rbat.Zs = proc.Mp().GetSels()
	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			rbat.Vecs[i] = vector.New(bat.Vecs[rp.Pos].Typ)
		} else {
			rbat.Vecs[i] = vector.New(ctr.bat.Vecs[rp.Pos].Typ)
		}
	}
	count := bat.Length()
	for i := 0; i < count; i++ {
		vec, err := colexec.JoinFilterEvalExpr(bat, ctr.bat, i, proc, ap.Cond)
		if err != nil {
			return err
		}
		bs := vec.Col.([]bool)
		if len(bs) == 1 {
			if bs[0] {
				for j := 0; j < len(ctr.bat.Zs); j++ {
					for k, rp := range ap.Result {
						if rp.Rel == 0 {
							if err := vector.UnionOne(rbat.Vecs[k], bat.Vecs[rp.Pos], int64(i), proc.Mp()); err != nil {
								rbat.Clean(proc.Mp())
								return err
							}
						} else {
							if err := vector.UnionOne(rbat.Vecs[k], ctr.bat.Vecs[rp.Pos], int64(j), proc.Mp()); err != nil {
								rbat.Clean(proc.Mp())
								return err
							}
						}
					}
					rbat.Zs = append(rbat.Zs, ctr.bat.Zs[j])
				}
			}
		} else {
			for j, b := range bs {
				if b {
					for k, rp := range ap.Result {
						if rp.Rel == 0 {
							if err := vector.UnionOne(rbat.Vecs[k], bat.Vecs[rp.Pos], int64(i), proc.Mp()); err != nil {
								rbat.Clean(proc.Mp())
								return err
							}
						} else {
							if err := vector.UnionOne(rbat.Vecs[k], ctr.bat.Vecs[rp.Pos], int64(j), proc.Mp()); err != nil {
								rbat.Clean(proc.Mp())
								return err
							}
						}
					}
					rbat.Zs = append(rbat.Zs, ctr.bat.Zs[j])
				}
			}
		}
		vector.Clean(vec, proc.Mp())
	}
	rbat.ExpandNulls()
	anal.Output(rbat)
	proc.SetInputBatch(rbat)
	return nil
}
