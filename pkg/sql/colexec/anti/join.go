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

package anti

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" anti join ")
}

func Prepare(proc *process.Process, arg any) (err error) {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.InitReceiver(proc, colexec.JoinReceiver)
	ap.ctr.inBuckets = make([]uint8, hashmap.UnitLimit)

	ap.ctr.vecs = make([]*vector.Vector, len(ap.Conditions[0]))
	ap.ctr.executorForVecs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, ap.Conditions[0])
	if err != nil {
		return err
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
			bat, _, err := ctr.ReceiveProbe(anal)
			if err != nil {
				return process.ExecNext, err
			}

			if bat == nil {
				ctr.state = End
				continue
			}
			if bat.Last() {
				proc.SetInputBatch(bat)
				return process.ExecNext, nil
			}
			if bat.IsEmpty() {
				bat.Clean(proc.Mp())
				continue
			}

			if ctr.bat == nil || ctr.bat.IsEmpty() {
				err = ctr.emptyProbe(bat, ap, proc, anal, isFirst, isLast)
			} else {
				err = ctr.probe(bat, ap, proc, anal, isFirst, isLast)
			}
			return process.ExecNext, err

		default:
			proc.SetInputBatch(nil)
			return process.ExecStop, nil
		}
	}
}

func (ctr *container) build(ap *Argument, proc *process.Process, anal process.Analyze) error {
	bat, _, err := ctr.ReceiveBuild(anal)
	if err != nil {
		return err
	}

	if bat != nil {
		ctr.bat = bat
		ctr.mp = bat.DupJmAuxData()
		ctr.hasNull = ctr.mp.HasNull()
		anal.Alloc(ctr.mp.Map().Size())
	}
	return nil
}

func (ctr *container) emptyProbe(bat *batch.Batch, ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool) error {
	defer proc.PutBatch(bat)
	anal.Input(bat, isFirst)
	rbat := batch.NewWithSize(len(ap.Result))
	for i, pos := range ap.Result {
		rbat.Vecs[i] = proc.GetVector(*bat.Vecs[pos].GetType())
	}
	count := bat.RowCount()
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		for k := 0; k < n; k++ {
			for j, pos := range ap.Result {
				if err := rbat.Vecs[j].UnionOne(bat.Vecs[pos], int64(i+k), proc.Mp()); err != nil {
					rbat.Clean(proc.Mp())
					return err
				}
			}
		}
		rbat.SetRowCount(rbat.RowCount() + n)
	}
	anal.Output(rbat, isLast)
	proc.SetInputBatch(rbat)
	return nil
}

func (ctr *container) probe(bat *batch.Batch, ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool) error {
	defer proc.PutBatch(bat)
	anal.Input(bat, isFirst)
	rbat := batch.NewWithSize(len(ap.Result))
	for i, pos := range ap.Result {
		rbat.Vecs[i] = proc.GetVector(*bat.Vecs[pos].GetType())
	}
	if (ctr.bat.RowCount() == 1 && ctr.hasNull) || ctr.bat.RowCount() == 0 {
		anal.Output(rbat, isLast)
		proc.SetInputBatch(rbat)
		return nil
	}

	if err := ctr.evalJoinCondition(bat, proc); err != nil {
		return err
	}

	if ctr.joinBat1 == nil {
		ctr.joinBat1, ctr.cfs1 = colexec.NewJoinBatch(bat, proc.Mp())
	}
	if ctr.joinBat2 == nil {
		ctr.joinBat2, ctr.cfs2 = colexec.NewJoinBatch(ctr.bat, proc.Mp())
	}

	count := bat.RowCount()
	mSels := ctr.mp.Sels()
	itr := ctr.mp.Map().NewIterator()
	eligible := make([]int32, 0, hashmap.UnitLimit)
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		copy(ctr.inBuckets, hashmap.OneUInt8s)
		vals, zvals := itr.Find(i, n, ctr.vecs, ctr.inBuckets)

		rowCountIncrease := 0
		for k := 0; k < n; k++ {
			if ctr.inBuckets[k] == 0 || zvals[k] == 0 {
				continue
			}
			if vals[k] == 0 {
				eligible = append(eligible, int32(i+k))
				rowCountIncrease++
				continue
			}
			if ap.Cond != nil {
				matched := false // mark if any tuple satisfies the condition
				sels := mSels[vals[k]-1]
				for _, sel := range sels {
					if err := colexec.SetJoinBatchValues(ctr.joinBat1, bat, int64(i+k),
						1, ctr.cfs1); err != nil {
						return err
					}
					if err := colexec.SetJoinBatchValues(ctr.joinBat2, ctr.bat, int64(sel),
						1, ctr.cfs2); err != nil {
						return err
					}
					vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat1, ctr.joinBat2})
					if err != nil {
						return err
					}
					if vec.IsConstNull() || vec.GetNulls().Contains(0) {
						continue
					}
					bs := vector.MustFixedCol[bool](vec)
					if bs[0] {
						matched = true
						break
					}
				}
				if matched {
					continue
				}
				eligible = append(eligible, int32(i+k))
				rowCountIncrease++
			}
		}
		rbat.SetRowCount(rbat.RowCount() + rowCountIncrease)

		for j, pos := range ap.Result {
			if err := rbat.Vecs[j].Union(bat.Vecs[pos], eligible, proc.Mp()); err != nil {
				rbat.Clean(proc.Mp())
				return err
			}
		}
		eligible = eligible[:0]
	}
	anal.Output(rbat, isLast)
	proc.SetInputBatch(rbat)
	return nil
}

func (ctr *container) evalJoinCondition(bat *batch.Batch, proc *process.Process) error {
	for i := range ctr.executorForVecs {
		vec, err := ctr.executorForVecs[i].Eval(proc, []*batch.Batch{bat})
		if err != nil {
			return err
		}
		ctr.vecs[i] = vec
	}
	return nil
}
