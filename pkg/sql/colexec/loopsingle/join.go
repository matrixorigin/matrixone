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

package loopsingle

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
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
			if ctr.bat.Length() == 0 {
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
	count := bat.Length()
	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			rbat.Vecs[i] = bat.Vecs[rp.Pos]
			bat.Vecs[rp.Pos] = nil
		} else {
			rbat.Vecs[i] = vector.NewConstNull(ctr.bat.Vecs[rp.Pos].Typ, count)
		}
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
	for i, rp := range ap.Result {
		if rp.Rel != 0 {
			rbat.Vecs[i] = vector.New(ctr.bat.Vecs[rp.Pos].Typ)
		}
	}
	count := bat.Length()
	for i := 0; i < count; i++ {
		unmatched := true
		vec, err := colexec.JoinFilterEvalExpr(bat, ctr.bat, i, proc, ap.Cond)
		if err != nil {
			rbat.Clean(proc.Mp())
			return err
		}
		bs := vector.MustTCols[bool](vec)
		if len(bs) == 1 {
			if bs[0] {
				if len(ctr.bat.Zs) > 1 {
					return moerr.NewInternalError(proc.Ctx, "scalar subquery returns more than 1 row")
				}
				unmatched = false
				for k, rp := range ap.Result {
					if rp.Rel != 0 {
						if err := vector.UnionOne(rbat.Vecs[k], ctr.bat.Vecs[rp.Pos], 0, proc.Mp()); err != nil {
							vec.Free(proc.Mp())
							rbat.Clean(proc.Mp())
							return err
						}
					}
				}
			}
		} else {
			for j, b := range bs {
				if b {
					if !unmatched {
						return moerr.NewInternalError(proc.Ctx, "scalar subquery returns more than 1 row")
					}
					unmatched = false
					for k, rp := range ap.Result {
						if rp.Rel != 0 {
							if err := vector.UnionOne(rbat.Vecs[k], ctr.bat.Vecs[rp.Pos], int64(j), proc.Mp()); err != nil {
								vec.Free(proc.Mp())
								rbat.Clean(proc.Mp())
								return err
							}
						}
					}
				}
			}
		}
		if unmatched {
			for k, rp := range ap.Result {
				if rp.Rel != 0 {
					if err := vector.UnionNull(rbat.Vecs[k], ctr.bat.Vecs[rp.Pos], proc.Mp()); err != nil {
						vec.Free(proc.Mp())
						rbat.Clean(proc.Mp())
						return err
					}
				}
			}
		}
		vec.Free(proc.Mp())
	}
	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			rbat.Vecs[i] = bat.Vecs[rp.Pos]
			bat.Vecs[rp.Pos] = nil
		}
	}
	rbat.Zs = bat.Zs
	bat.Zs = nil
	rbat.ExpandNulls()
	anal.Output(rbat)
	proc.SetInputBatch(rbat)
	return nil
}
