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

package hashbuild

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" hash build ")
}

func Prepare(proc *process.Process, arg any) (err error) {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.InitReceiver(proc, false)
	if ap.NeedHashMap {
		if ap.ctr.mp, err = hashmap.NewStrMap(false, ap.Ibucket, ap.Nbucket, proc.Mp()); err != nil {
			return err
		}
		ap.ctr.vecs = make([]*vector.Vector, len(ap.Conditions))

		ctr := ap.ctr
		ctr.evecs = make([]evalVector, len(ap.Conditions))
		for i := range ctr.evecs {
			ctr.evecs[i].executor, err = colexec.NewExpressionExecutor(proc, ap.Conditions[i])
			if err != nil {
				return err
			}
		}
	}
	ap.ctr.bat = batch.NewWithSize(len(ap.Typs))
	ap.ctr.bat.Zs = proc.Mp().GetSels()
	for i, typ := range ap.Typs {
		ap.ctr.bat.Vecs[i] = vector.NewVec(typ)
	}

	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, _ bool) (bool, error) {
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	ap := arg.(*Argument)
	ctr := ap.ctr
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(ap, proc, anal, isFirst); err != nil {
				return false, err
			}
			if ap.ctr.mp != nil {
				anal.Alloc(ap.ctr.mp.Size())
			}
			ctr.state = End
		default:
			if ctr.bat != nil {
				if ap.NeedHashMap {
					ctr.bat.Ht = hashmap.NewJoinMap(ctr.sels, nil, ctr.mp, ctr.hasNull)
				}
				proc.SetInputBatch(ctr.bat)
				ctr.mp = nil
				ctr.bat = nil
				ctr.sels = nil
			} else {
				proc.SetInputBatch(nil)
			}
			return true, nil
		}
	}
}

func (ctr *container) build(ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool) error {
	var err error

	for {
		bat, _, err := ctr.ReceiveFromSingleReg(0, anal)
		if err != nil {
			return err
		}

		if bat == nil {
			break
		}
		if bat.Length() == 0 {
			bat.Clean(proc.Mp())
			continue
		}
		anal.Input(bat, isFirst)
		anal.Alloc(int64(bat.Size()))
		if ctr.bat, err = ctr.bat.Append(proc.Ctx, proc.Mp(), bat); err != nil {
			return err
		}
		bat.Clean(proc.Mp())
	}
	if ctr.bat == nil || ctr.bat.Length() == 0 || !ap.NeedHashMap {
		return nil
	}

	if err = ctr.evalJoinCondition(ctr.bat, proc); err != nil {
		return err
	}

	itr := ctr.mp.NewIterator()
	count := ctr.bat.Length()
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		rows := ctr.mp.GroupCount()
		vals, zvals, err := itr.Insert(i, n, ctr.vecs)
		if err != nil {
			return err
		}
		for k, v := range vals[:n] {
			if zvals[k] == 0 {
				ctr.hasNull = true
				continue
			}
			if v == 0 {
				continue
			}
			if v > rows {
				ctr.sels = append(ctr.sels, make([]int32, 0))
			}
			ai := int64(v) - 1
			ctr.sels[ai] = append(ctr.sels[ai], int32(i+k))
		}
	}

	return nil
}

func (ctr *container) evalJoinCondition(bat *batch.Batch, proc *process.Process) error {
	for i := range ctr.evecs {
		vec, err := ctr.evecs[i].executor.Eval(proc, []*batch.Batch{bat})
		if err != nil {
			ctr.cleanEvalVectors(proc.Mp())
			return err
		}
		ctr.vecs[i] = vec
		ctr.evecs[i].vec = vec
	}
	return nil
}
