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

package rightanti

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" right anti join ")
}

func Prepare(proc *process.Process, arg any) (err error) {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	//ap.ctr.InitReceiver(proc, false)
	ap.ctr.InitReceiver2(proc, colexec.JoinReceiver)
	ap.ctr.inBuckets = make([]uint8, hashmap.UnitLimit)
	ap.ctr.vecs = make([]*vector.Vector, len(ap.Conditions[0]))
	ap.ctr.bat = batch.NewWithSize(len(ap.RightTypes))
	for i, typ := range ap.RightTypes {
		ap.ctr.bat.Vecs[i] = vector.NewVec(typ)
	}

	ap.ctr.evecs = make([]evalVector, len(ap.Conditions[0]))
	for i := range ap.ctr.evecs {
		ap.ctr.evecs[i].executor, err = colexec.NewExpressionExecutor(proc, ap.Conditions[0][i])
		if err != nil {
			return err
		}
	}
	if ap.Cond != nil {
		ap.ctr.expr, err = colexec.NewExpressionExecutor(proc, ap.Cond)
	}
	return err
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (process.ExecStatus, error) {
	analyze := proc.GetAnalyze(idx)
	analyze.Start()
	defer analyze.Stop()
	ap := arg.(*Argument)
	ctr := ap.ctr
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(ap, proc, analyze); err != nil {
				return process.ExecNext, err
			}
			if ctr.mp == nil {
				ctr.state = End
			} else {
				ctr.state = Probe
			}

		case Probe:

			//bat, _, err := ctr.ReceiveFromSingleReg(0, analyze)
			bat, _, err := ctr.ReceiveProbe(analyze)
			if err != nil {
				return process.ExecNext, err
			}

			if bat == nil {
				ctr.state = SendLast
				continue
			}
			if bat.IsEmpty() {
				bat.Clean(proc.Mp())
				continue
			}

			if ctr.bat == nil || ctr.bat.IsEmpty() {
				proc.PutBatch(bat)
				continue
			}

			if err := ctr.probe(bat, ap, proc, analyze, isFirst, isLast); err != nil {
				return process.ExecNext, err
			}

			continue

		case SendLast:
			setNil, err := ctr.sendLast(ap, proc, analyze, isFirst, isLast)
			if err != nil {
				return process.ExecNext, err
			}

			ctr.state = End
			if setNil {
				continue
			}

			return process.ExecNext, nil

		default:
			proc.SetInputBatch(nil)
			return process.ExecStop, nil
		}
	}
}

func (ctr *container) build(ap *Argument, proc *process.Process, analyze process.Analyze) error {
	//bat, _, err := ctr.ReceiveFromSingleReg(1, analyze)
	bat, _, err := ctr.ReceiveBuild(analyze)
	if err != nil {
		return err
	}

	if bat != nil {
		ctr.bat = bat
		ctr.mp = bat.DupJmAuxData()
		ctr.matched = &bitmap.Bitmap{}
		ctr.matched.InitWithSize(bat.RowCount())
		analyze.Alloc(ctr.mp.Map().Size())
	}
	return nil
}

func (ctr *container) sendLast(ap *Argument, proc *process.Process, analyze process.Analyze, isFirst bool, isLast bool) (bool, error) {
	ctr.handledLast = true

	if ap.NumCPU > 1 {
		if !ap.IsMerger {
			ap.Channel <- ctr.matched
			return true, nil
		}

		cnt := 1
		for v := range ap.Channel {
			ctr.matched.Or(v)
			cnt++
			if cnt == int(ap.NumCPU) {
				close(ap.Channel)
				break
			}
		}
	}

	rbat := batch.NewWithSize(len(ap.Result))

	for i, pos := range ap.Result {
		rbat.Vecs[i] = proc.GetVector(ap.RightTypes[pos])
	}

	count := ctr.bat.RowCount() - ctr.matched.Count()
	ctr.matched.Negate()
	sels := make([]int32, 0, count)
	itr := ctr.matched.Iterator()
	for itr.HasNext() {
		r := itr.Next()
		sels = append(sels, int32(r))
	}

	for j, pos := range ap.Result {
		if err := rbat.Vecs[j].Union(ctr.bat.Vecs[pos], sels, proc.Mp()); err != nil {
			rbat.Clean(proc.Mp())
			return false, err
		}
	}
	rbat.AddRowCount(len(sels))

	analyze.Output(rbat, isLast)
	proc.SetInputBatch(rbat)
	return false, nil
}

func (ctr *container) probe(bat *batch.Batch, ap *Argument, proc *process.Process, analyze process.Analyze, isFirst bool, isLast bool) error {
	defer proc.PutBatch(bat)
	analyze.Input(bat, isFirst)

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
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		copy(ctr.inBuckets, hashmap.OneUInt8s)
		vals, zvals := itr.Find(i, n, ctr.vecs, ctr.inBuckets)
		for k := 0; k < n; k++ {
			if ctr.inBuckets[k] == 0 || zvals[k] == 0 || vals[k] == 0 {
				continue
			}
			sels := mSels[vals[k]-1]
			for _, sel := range sels {
				if ctr.matched.Contains(uint64(sel)) {
					continue
				}
				if ap.Cond != nil {
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

					result := vector.GenerateFunctionFixedTypeParameter[bool](vec)
					b, null := result.GetValue(0)
					if null || !b {
						continue
					}
				}
				ctr.matched.Add(uint64(sel))
			}
		}
	}

	// proc.SetInputBatch(batch.EmptyBatch)
	return nil
}

func (ctr *container) evalJoinCondition(bat *batch.Batch, proc *process.Process) error {
	for i := range ctr.evecs {
		vec, err := ctr.evecs[i].executor.Eval(proc, []*batch.Batch{bat})
		if err != nil {
			ctr.cleanEvalVectors()
			return err
		}
		ctr.vecs[i] = vec
		ctr.evecs[i].vec = vec
	}
	return nil
}
