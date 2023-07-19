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
			if bat.Length() == 0 {
				bat.Clean(proc.Mp())
				continue
			}
			if ctr.bat.Length() == 0 {
				err = ctr.emptyProbe(bat, ap, proc, anal, isFirst, isLast)
			} else {
				err = ctr.probe(bat, ap, proc, anal, isFirst, isLast)
			}
			bat.Clean(proc.Mp())
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
	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			rbat.Vecs[i] = bat.Vecs[rp.Pos]
			bat.Vecs[rp.Pos] = nil
		} else {
			rbat.Vecs[i] = vector.NewConstNull(ap.Typs[rp.Pos], bat.Length(), proc.Mp())
		}
	}
	rbat.SetRowCount(rbat.RowCount() + bat.RowCount())
	anal.Output(rbat, isLast)
	proc.SetInputBatch(rbat)
	return nil
}

func (ctr *container) probe(bat *batch.Batch, ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool) error {
	anal.Input(bat, isFirst)
	rbat := batch.NewWithSize(len(ap.Result))
	for i, rp := range ap.Result {
		if rp.Rel != 0 {
			rbat.Vecs[i] = vector.NewVec(ap.Typs[rp.Pos])
		}
	}
	count := bat.Length()
	if ctr.expr == nil {
		switch ctr.bat.Length() {
		case 0:
			for i, rp := range ap.Result {
				if rp.Rel != 0 {
					err := vector.AppendMultiFixed(rbat.Vecs[i], 0, true, count, proc.Mp())
					if err != nil {
						rbat.Clean(proc.Mp())
						return err
					}
				}
			}
		case 1:
			for i, rp := range ap.Result {
				if rp.Rel != 0 {
					err := rbat.Vecs[i].UnionMulti(ctr.bat.Vecs[rp.Pos], 0, count, proc.Mp())
					if err != nil {
						rbat.Clean(proc.Mp())
						return err
					}
				}
			}
		default:
			return moerr.NewInternalError(proc.Ctx, "scalar subquery returns more than 1 row")
		}
	} else {
		if ctr.joinBat == nil {
			ctr.joinBat, ctr.cfs = colexec.NewJoinBatch(bat, proc.Mp())
		}
		for i := 0; i < count; i++ {
			if err := colexec.SetJoinBatchValues(ctr.joinBat, bat, int64(i),
				ctr.bat.Length(), ctr.cfs); err != nil {
				rbat.Clean(proc.Mp())
				return err
			}
			unmatched := true
			vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat, ctr.bat})
			if err != nil {
				rbat.Clean(proc.Mp())
				return err
			}
			defer vec.Free(proc.Mp())

			rs := vector.GenerateFunctionFixedTypeParameter[bool](vec)
			if vec.IsConst() {
				b, null := rs.GetValue(0)
				if !null && b {
					if ctr.bat.Length() > 1 {
						return moerr.NewInternalError(proc.Ctx, "scalar subquery returns more than 1 row")
					}
					unmatched = false
					for k, rp := range ap.Result {
						if rp.Rel != 0 {
							if err := rbat.Vecs[k].UnionOne(ctr.bat.Vecs[rp.Pos], 0, proc.Mp()); err != nil {
								rbat.Clean(proc.Mp())
								return err
							}
						}
					}
				}
			} else {
				l := vec.Length()
				for j := uint64(0); j < uint64(l); j++ {
					b, null := rs.GetValue(j)
					if !null && b {
						if !unmatched {
							return moerr.NewInternalError(proc.Ctx, "scalar subquery returns more than 1 row")
						}
						unmatched = false
						for k, rp := range ap.Result {
							if rp.Rel != 0 {
								if err := rbat.Vecs[k].UnionOne(ctr.bat.Vecs[rp.Pos], int64(j), proc.Mp()); err != nil {
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
						if err := rbat.Vecs[k].UnionNull(proc.Mp()); err != nil {
							rbat.Clean(proc.Mp())
							return err
						}
					}
				}
			}
		}
	}
	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			// rbat.Vecs[i] = bat.Vecs[rp.Pos]
			// bat.Vecs[rp.Pos] = nil
			typ := *bat.Vecs[rp.Pos].GetType()
			rbat.Vecs[i] = vector.NewVec(typ)
			if err := vector.GetUnionAllFunction(typ, proc.Mp())(rbat.Vecs[i], bat.Vecs[rp.Pos]); err != nil {
				return err
			}
		}
	}
	rbat.AddRowCount(bat.RowCount())
	anal.Output(rbat, isLast)
	proc.SetInputBatch(rbat)
	return nil
}
