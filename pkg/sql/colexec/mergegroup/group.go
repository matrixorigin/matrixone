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

package mergegroup

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ interface{}, buf *bytes.Buffer) {
	buf.WriteString("γ()")
}

func Prepare(_ *process.Process, arg interface{}) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.inserted = make([]uint8, hashmap.UnitLimit)
	ap.ctr.zInserted = make([]uint8, hashmap.UnitLimit)
	return nil
}

func Call(idx int, proc *process.Process, arg interface{}) (bool, error) {
	ap := arg.(*Argument)
	ctr := ap.ctr
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(proc, anal); err != nil {
				ctr.state = End
				return true, err
			}
			ctr.state = Eval
		case Eval:
			ctr.state = End
			if ctr.bat != nil {
				if ap.NeedEval {
					for i, agg := range ctr.bat.Aggs {
						vec, err := agg.Eval(proc.GetMheap())
						if err != nil {
							ctr.state = End
							ctr.bat.Clean(proc.GetMheap())
							return false, err
						}
						ctr.bat.Aggs[i] = nil
						ctr.bat.Vecs = append(ctr.bat.Vecs, vec)
					}
					ctr.bat.Aggs = nil
					for i := range ctr.bat.Zs { // reset zs
						ctr.bat.Zs[i] = 1
					}
				}
				anal.Output(ctr.bat)
				ctr.bat.ExpandNulls()
			}
			proc.SetInputBatch(ctr.bat)
			ctr.bat = nil
			return true, nil
		case End:
			proc.SetInputBatch(nil)
			return true, nil
		}
	}
}

func (ctr *container) build(proc *process.Process, anal process.Analyze) error {
	if len(proc.Reg.MergeReceivers) == 1 {
		for {
			bat := <-proc.Reg.MergeReceivers[0].Ch
			if bat == nil {
				return nil
			}
			if bat.Length() == 0 {
				continue
			}
			anal.Input(bat)
			ctr.bat = bat
			return nil
		}
	}
	for i := 0; i < len(proc.Reg.MergeReceivers); i++ {
		bat := <-proc.Reg.MergeReceivers[i].Ch
		if bat == nil {
			continue
		}
		if len(bat.Zs) == 0 {
			i--
			continue
		}
		anal.Input(bat)
		if err := ctr.process(bat, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) process(bat *batch.Batch, proc *process.Process) error {
	var err error

	if ctr.bat == nil {
		size := 0
		for _, vec := range bat.Vecs {
			switch vec.Typ.TypeSize() {
			case 1:
				size += 1 + 1
			case 2:
				size += 2 + 1
			case 4:
				size += 4 + 1
			case 8:
				size += 8 + 1
			case 16:
				size += 16 + 1
			default:
				size = 128
			}
		}
		switch {
		case size == 0:
			ctr.typ = H0
		case size <= 8:
			ctr.typ = H8
			ctr.intHashMap = hashmap.NewIntHashMap(true)
		default:
			ctr.typ = HStr
			ctr.strHashMap = hashmap.NewStrMap(true)
		}
	}
	switch ctr.typ {
	case H0:
		err = ctr.processH0(bat, proc)
	case H8:
		err = ctr.processH8(bat, proc)
	default:
		err = ctr.processHStr(bat, proc)
	}
	if err != nil {
		ctr.bat.Clean(proc.Mp)
		ctr.bat = nil
		return err
	}
	return nil
}

func (ctr *container) processH0(bat *batch.Batch, proc *process.Process) error {
	if ctr.bat == nil {
		ctr.bat = bat
		return nil
	}
	defer bat.Clean(proc.Mp)
	for _, z := range bat.Zs {
		ctr.bat.Zs[0] += z
	}
	for i, agg := range ctr.bat.Aggs {
		agg.Merge(bat.Aggs[i], 0, 0)
	}
	return nil
}

func (ctr *container) processH8(bat *batch.Batch, proc *process.Process) error {
	count := bat.Length()
	itr := ctr.intHashMap.NewIterator(0, 0)
	flg := ctr.bat == nil
	if !flg {
		defer bat.Clean(proc.Mp)
	}
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		vals, _ := itr.Insert(i, n, bat.Vecs)
		if !flg {
			if err := ctr.batchFill(i, n, bat, vals, ctr.intHashMap, proc); err != nil {
				return err
			}
		}
	}
	if flg {
		ctr.bat = bat
		ctr.intHashMap.AddGroups(ctr.intHashMap.Cardinality())
	}
	return nil
}

func (ctr *container) processHStr(bat *batch.Batch, proc *process.Process) error {
	count := bat.Length()
	itr := ctr.strHashMap.NewIterator(0, 0)
	flg := ctr.bat == nil
	if !flg {
		defer bat.Clean(proc.Mp)
	}
	for i := 0; i < count; i += hashmap.UnitLimit { // batch
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		vals, _ := itr.Insert(i, n, bat.Vecs)
		if !flg {
			if err := ctr.batchFill(i, n, bat, vals, ctr.strHashMap, proc); err != nil {
				return err
			}
		}
	}
	if flg {
		ctr.bat = bat
		ctr.strHashMap.AddGroups(ctr.strHashMap.Cardinality())
	}
	return nil
}

func (ctr *container) batchFill(i int, n int, bat *batch.Batch, vals []uint64, mp hashmap.HashMap, proc *process.Process) error {
	cnt := 0
	copy(ctr.inserted[:n], ctr.zInserted[:n])
	for k, v := range vals {
		if v > mp.GroupCount() {
			ctr.inserted[k] = 1
			mp.AddGroup()
			cnt++
			ctr.bat.Zs = append(ctr.bat.Zs, 0)
		}
		ai := int64(v) - 1
		ctr.bat.Zs[ai] += bat.Zs[i+k]
	}
	if cnt > 0 {
		for j, vec := range ctr.bat.Vecs {
			if err := vector.UnionBatch(vec, bat.Vecs[j], int64(i), cnt, ctr.inserted[:n], proc.GetMheap()); err != nil {
				return err
			}
		}
		for _, agg := range ctr.bat.Aggs {
			if err := agg.Grows(cnt, proc.Mp); err != nil {
				return err
			}
		}
	}
	for j, agg := range ctr.bat.Aggs {
		agg.BatchMerge(bat.Aggs[j], int64(i), ctr.inserted[:n], vals)
	}
	return nil
}
