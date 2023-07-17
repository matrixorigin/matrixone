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
	var err error

	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.InitReceiver(proc, false)

	if ap.Cond != nil {
		ap.ctr.expr, err = colexec.NewExpressionExecutor(proc, ap.Cond)
	}
	return err
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
			if ctr.bat == nil {
				// for inner ,right and semi join, if hashmap is empty, we can finish this pipeline
				ctr.state = End
			} else {
				ctr.state = Probe
			}

		case Probe:
			bat, _, err := ctr.ReceiveFromSingleReg(0, anal)
			if err != nil {
				return false, err
			}

			if bat == nil {
				ctr.state = End
				continue
			}
			if bat.Length() == 0 {
				bat.Clean(proc.Mp())
				continue
			}
			if ctr.bat == nil || ctr.bat.Length() == 0 {
				proc.PutBatch(bat)
				continue
			}
			err = ctr.probe(bat, ap, proc, anal, isFirst, isLast)
			proc.PutBatch(bat)
			return false, err
		default:
			proc.SetInputBatch(nil)
			return true, nil
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
			rbat.Vecs[i] = proc.GetVector(*bat.Vecs[rp.Pos].GetType())
		} else {
			rbat.Vecs[i] = proc.GetVector(*ctr.bat.Vecs[rp.Pos].GetType())
		}
	}
	count := bat.RowCount()
	if ctr.joinBat == nil {
		ctr.joinBat, ctr.cfs = colexec.NewJoinBatch(bat, proc.Mp())
	}

	rowCountIncrease := 0
	for i := 0; i < count; i++ {
		if err := colexec.SetJoinBatchValues(ctr.joinBat, bat, int64(i),
			ctr.bat.Length(), ctr.cfs); err != nil {
			rbat.Clean(proc.Mp())
			return err
		}
		vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat, ctr.bat})
		if err != nil {
			rbat.Clean(proc.Mp())
			return err
		}

		rs := vector.GenerateFunctionFixedTypeParameter[bool](vec)
		if vec.IsConst() {
			b, null := rs.GetValue(0)
			if !null && b {
				for j := 0; j < ctr.bat.RowCount(); j++ {
					for k, rp := range ap.Result {
						if rp.Rel == 0 {
							if err = rbat.Vecs[k].UnionOne(bat.Vecs[rp.Pos], int64(i), proc.Mp()); err != nil {
								vec.Free(proc.Mp())
								rbat.Clean(proc.Mp())
								return err
							}
						} else {
							if err = rbat.Vecs[k].UnionOne(ctr.bat.Vecs[rp.Pos], int64(j), proc.Mp()); err != nil {
								vec.Free(proc.Mp())
								rbat.Clean(proc.Mp())
								return err
							}
						}
					}
					rowCountIncrease++
				}
			}
		} else {
			l := vec.Length()
			for j := uint64(0); j < uint64(l); j++ {
				b, null := rs.GetValue(j)
				if !null && b {
					for k, rp := range ap.Result {
						if rp.Rel == 0 {
							if err := rbat.Vecs[k].UnionOne(bat.Vecs[rp.Pos], int64(i), proc.Mp()); err != nil {
								rbat.Clean(proc.Mp())
								vec.Free(proc.Mp())
								return err
							}
						} else {
							if err := rbat.Vecs[k].UnionOne(ctr.bat.Vecs[rp.Pos], int64(j), proc.Mp()); err != nil {
								rbat.Clean(proc.Mp())
								vec.Free(proc.Mp())
								return err
							}
						}
					}
					rowCountIncrease++
				}
			}
		}
	}

	rbat.SetRowCount(rowCountIncrease)
	anal.Output(rbat, isLast)
	proc.SetInputBatch(rbat)
	return nil
}
