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

package join

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/joincondition"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ interface{}, buf *bytes.Buffer) {
	buf.WriteString(" inner join ")
}

func Prepare(proc *process.Process, arg interface{}) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.mp = hashmap.NewStrMap(false)
	ap.ctr.inBuckets = make([]uint8, hashmap.UnitLimit)
	ap.ctr.vecs = make([]*vector.Vector, len(ap.Conditions[0]))
	ap.ctr.evecs = make([]evalVector, len(ap.Conditions[0]))
	return nil
}

func Call(idx int, proc *process.Process, arg interface{}) (bool, error) {
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
				ctr.freeSels(proc)
				return true, err
			}
			ctr.state = Probe
		case Probe:
			bat := <-proc.Reg.MergeReceivers[0].Ch
			if bat == nil {
				ctr.state = End
				ctr.freeSels(proc)
				if ctr.bat != nil {
					ctr.bat.Clean(proc.GetMheap())
				}
				continue
			}
			if bat.Length() == 0 {
				continue
			}
			if ctr.bat == nil {
				bat.Clean(proc.GetMheap())
				continue
			}
			if err := ctr.probe(bat, ap, proc, anal); err != nil {
				ctr.state = End
				ctr.freeSels(proc)
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
	var err error

	for {
		bat := <-proc.Reg.MergeReceivers[1].Ch
		if bat == nil {
			break
		}
		if bat.Length() == 0 {
			continue
		}
		if ctr.bat == nil {
			ctr.bat = batch.NewWithSize(len(bat.Vecs))
			for i, vec := range bat.Vecs {
				ctr.bat.Vecs[i] = vector.New(vec.Typ)
			}
			ctr.bat.Zs = proc.GetMheap().GetSels()
		}
		anal.Input(bat)
		anal.Alloc(int64(bat.Size()))
		if ctr.bat, err = ctr.bat.Append(proc.GetMheap(), bat); err != nil {
			bat.Clean(proc.GetMheap())
			ctr.bat.Clean(proc.GetMheap())
			return err
		}
		bat.Clean(proc.GetMheap())
	}
	if ctr.bat == nil || ctr.bat.Length() == 0 {
		return nil
	}
	if err := ctr.evalJoinCondition(ctr.bat, ap.Conditions[1], proc); err != nil {
		return err
	}
	defer ctr.freeJoinCondition(proc)
	rows := ctr.mp.GroupCount()
	itr := ctr.mp.NewIterator(ap.Ibucket, ap.Nbucket)
	count := ctr.bat.Length()
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		vals, zvals := itr.Insert(i, n, ctr.vecs)
		for k, v := range vals {
			if zvals[k] == 0 {
				continue
			}
			if v > rows {
				ctr.mp.AddGroup()
				ctr.sels = append(ctr.sels, proc.GetMheap().GetSels())
			}
			ai := int64(v) - 1
			ctr.sels[ai] = append(ctr.sels[ai], int64(i+k))
		}
	}
	return nil
}

func (ctr *container) probe(bat *batch.Batch, ap *Argument, proc *process.Process, anal process.Analyze) error {
	defer bat.Clean(proc.Mp)
	anal.Input(bat)
	rbat := batch.NewWithSize(len(ap.Result))
	rbat.Zs = proc.GetMheap().GetSels()
	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			rbat.Vecs[i] = vector.New(bat.Vecs[rp.Pos].Typ)
		} else {
			rbat.Vecs[i] = vector.New(ctr.bat.Vecs[rp.Pos].Typ)
		}
	}
	if err := ctr.evalJoinCondition(bat, ap.Conditions[0], proc); err != nil {
		return err
	}
	defer ctr.freeJoinCondition(proc)
	count := bat.Length()
	itr := ctr.mp.NewIterator(ap.Ibucket, ap.Nbucket)
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		copy(ctr.inBuckets, hashmap.OneUInt8s)
		vals, zvals := itr.Find(i, n, ctr.vecs, ctr.inBuckets)
		for k := 0; k < n; k++ {
			if ctr.inBuckets[k] == 0 {
				continue
			}
			if zvals[k] == 0 {
				continue
			}
			if vals[k] == 0 {
				continue
			}
			sels := ctr.sels[vals[k]-1]
			for _, sel := range sels {
				for j, rp := range ap.Result {
					if rp.Rel == 0 {
						if err := vector.UnionOne(rbat.Vecs[j], bat.Vecs[rp.Pos], int64(i+k), proc.Mp); err != nil {
							rbat.Clean(proc.Mp)
							return err
						}
					} else {
						if err := vector.UnionOne(rbat.Vecs[j], ctr.bat.Vecs[rp.Pos], sel, proc.Mp); err != nil {
							rbat.Clean(proc.Mp)
							return err
						}
					}
				}
				rbat.Zs = append(rbat.Zs, ctr.bat.Zs[sel])
			}
		}
	}
	rbat.ExpandNulls()
	anal.Output(rbat)
	proc.SetInputBatch(rbat)
	return nil
}

func (ctr *container) evalJoinCondition(bat *batch.Batch, conds []joincondition.Condition, proc *process.Process) error {
	for i, cond := range conds {
		vec, err := colexec.EvalExpr(bat, proc, cond.Expr)
		if err != nil || vec.ConstExpand(proc.GetMheap()) == nil {
			for j := 0; j < i; j++ {
				if ctr.evecs[j].needFree {
					vector.Clean(ctr.evecs[j].vec, proc.GetMheap())
				}
			}
			return err
		}
		ctr.vecs[i] = vec
		ctr.evecs[i].vec = vec
		ctr.evecs[i].needFree = true
		for j := range bat.Vecs {
			if bat.Vecs[j] == vec {
				ctr.evecs[i].needFree = false
				break
			}
		}
	}
	return nil
}

func (ctr *container) freeJoinCondition(proc *process.Process) {
	for i := range ctr.evecs {
		if ctr.evecs[i].needFree {
			ctr.evecs[i].vec.Free(proc.GetMheap())
		}
	}
}

func (ctr *container) freeSels(proc *process.Process) {
	for i := range ctr.sels {
		proc.GetMheap().PutSels(ctr.sels[i])
	}
	ctr.sels = nil
}
