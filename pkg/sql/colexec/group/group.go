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

package group

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	ap := arg.(*Argument)
	buf.WriteString("group([")
	for i, expr := range ap.Exprs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%v", expr))
	}
	buf.WriteString("], [")
	for i, ag := range ap.Aggs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%v(%v)", agg.Names[ag.Op], ag.E))
	}
	buf.WriteString("])")
}

func Prepare(_ *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.inserted = make([]uint8, hashmap.UnitLimit)
	ap.ctr.zInserted = make([]uint8, hashmap.UnitLimit)
	return nil
}

func Call(idx int, proc *process.Process, arg any) (bool, error) {
	ap := arg.(*Argument)
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	if len(ap.Exprs) == 0 {
		return ap.ctr.process(ap, proc, anal)
	}
	return ap.ctr.processWithGroup(ap, proc, anal)
}

func (ctr *container) process(ap *Argument, proc *process.Process, anal process.Analyze) (bool, error) {
	bat := proc.InputBatch()
	if bat == nil {
		if len(ctr.aggVecs) == 0 {
			bat := batch.NewWithSize(len(ap.Types))
			for i := range bat.Vecs {
				bat.Vecs[i] = vector.New(ap.Types[i])
			}
			proc.SetInputBatch(bat)
			if _, err := ctr.process(ap, proc, anal); err != nil {
				return false, err
			}
		}
		if ctr.bat != nil {
			ctr.bat.ExpandNulls()
			anal.Output(ctr.bat)
			proc.SetInputBatch(ctr.bat)
			ctr.bat = nil
			return true, nil
		}
		proc.SetInputBatch(nil)
		return true, nil
	}
	defer bat.Clean(proc.Mp())
	if len(bat.Vecs) == 0 {
		return false, nil
	}
	anal.Input(bat)
	proc.SetInputBatch(&batch.Batch{})
	if len(ctr.aggVecs) == 0 {
		ctr.aggVecs = make([]evalVector, len(ap.Aggs))
	}
	if err := ctr.evalAggVector(bat, ap.Aggs, proc); err != nil {
		return false, err
	}
	defer ctr.cleanAggVectors(proc.Mp())
	if ctr.bat == nil {
		var err error

		ctr.bat = batch.NewWithSize(0)
		ctr.bat.Zs = proc.Mp().GetSels()
		ctr.bat.Zs = append(ctr.bat.Zs, 0)
		ctr.bat.Aggs = make([]agg.Agg[any], len(ap.Aggs))
		for i, ag := range ap.Aggs {
			if ctr.bat.Aggs[i], err = agg.New(ag.Op, ag.Dist, ctr.aggVecs[i].vec.Typ); err != nil {
				return false, err
			}
		}
		for _, ag := range ctr.bat.Aggs {
			if err := ag.Grows(1, proc.Mp()); err != nil {
				return false, err
			}
		}
	}
	if bat.Length() == 0 {
		return false, nil
	}
	if err := ctr.processH0(bat, ap, proc); err != nil {
		return false, err
	}
	return false, nil
}

func (ctr *container) processWithGroup(ap *Argument, proc *process.Process, anal process.Analyze) (bool, error) {
	var err error

	bat := proc.InputBatch()
	if bat == nil {
		if ctr.bat != nil {
			if ap.NeedEval {
				for i, ag := range ctr.bat.Aggs {
					vec, err := ag.Eval(proc.Mp())
					if err != nil {
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
			ctr.bat.ExpandNulls()
			anal.Output(ctr.bat)
			proc.SetInputBatch(ctr.bat)
			ctr.bat = nil
			return true, nil
		}
		proc.SetInputBatch(nil)
		return true, nil
	}
	if bat.Length() == 0 {
		return false, nil
	}
	defer bat.Clean(proc.Mp())
	anal.Input(bat)
	proc.SetInputBatch(&batch.Batch{})
	if len(ctr.aggVecs) == 0 {
		ctr.aggVecs = make([]evalVector, len(ap.Aggs))
	}
	if err := ctr.evalAggVector(bat, ap.Aggs, proc); err != nil {
		return false, err
	}
	defer ctr.cleanAggVectors(proc.Mp())
	if len(ctr.groupVecs) == 0 {
		ctr.vecs = make([]*vector.Vector, len(ap.Exprs))
		ctr.groupVecs = make([]evalVector, len(ap.Exprs))
	}
	for i, expr := range ap.Exprs {
		vec, err := colexec.EvalExpr(bat, proc, expr)
		if err != nil || vec.ConstExpand(proc.Mp()) == nil {
			return false, err
		}
		ctr.groupVecs[i].vec = vec
		ctr.groupVecs[i].needFree = true
		for j := range bat.Vecs {
			if bat.Vecs[j] == vec {
				ctr.groupVecs[i].needFree = false
				break
			}
		}
		ctr.vecs[i] = vec
	}
	if ctr.bat == nil {
		size := 0
		ctr.bat = batch.NewWithSize(len(ap.Exprs))
		ctr.bat.Zs = proc.Mp().GetSels()
		for i := range ctr.groupVecs {
			vec := ctr.groupVecs[i].vec
			ctr.bat.Vecs[i] = vector.New(vec.Typ)
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
		ctr.bat.Aggs = make([]agg.Agg[any], len(ap.Aggs))
		for i, ag := range ap.Aggs {
			if ctr.bat.Aggs[i], err = agg.New(ag.Op, ag.Dist, ctr.aggVecs[i].vec.Typ); err != nil {
				ctr.bat = nil
				return false, err
			}
		}
		switch {
		case size <= 8:
			ctr.typ = H8
			if ctr.intHashMap, err = hashmap.NewIntHashMap(true, ap.Ibucket, ap.Nbucket, proc.Mp()); err != nil {
				return false, err
			}
		default:
			ctr.typ = HStr
			if ctr.strHashMap, err = hashmap.NewStrMap(true, ap.Ibucket, ap.Nbucket, proc.Mp()); err != nil {
				return false, err
			}
		}
	}
	switch ctr.typ {
	case H8:
		err = ctr.processH8(bat, proc)
	default:
		err = ctr.processHStr(bat, proc)
	}
	if err != nil {
		return false, err
	}
	return false, err
}

func (ctr *container) processH0(bat *batch.Batch, ap *Argument, proc *process.Process) error {
	for _, z := range bat.Zs {
		ctr.bat.Zs[0] += z
	}
	for i, ag := range ctr.bat.Aggs {
		err := ag.BulkFill(0, bat.Zs, []*vector.Vector{ctr.aggVecs[i].vec})
		if err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) processH8(bat *batch.Batch, proc *process.Process) error {
	count := bat.Length()
	itr := ctr.intHashMap.NewIterator()
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		rows := ctr.intHashMap.GroupCount()
		vals, _, err := itr.Insert(i, n, ctr.vecs)
		if err != nil {
			return err
		}
		if err := ctr.batchFill(i, n, bat, vals, rows, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) processHStr(bat *batch.Batch, proc *process.Process) error {
	count := bat.Length()
	itr := ctr.strHashMap.NewIterator()
	for i := 0; i < count; i += hashmap.UnitLimit { // batch
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		rows := ctr.strHashMap.GroupCount()
		vals, _, err := itr.Insert(i, n, ctr.vecs)
		if err != nil {
			return err
		}
		if err := ctr.batchFill(i, n, bat, vals, rows, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) batchFill(i int, n int, bat *batch.Batch, vals []uint64, hashRows uint64, proc *process.Process) error {
	cnt := 0
	valCnt := 0
	copy(ctr.inserted[:n], ctr.zInserted[:n])
	for k, v := range vals[:n] {
		if v == 0 {
			continue
		}
		if v > hashRows {
			ctr.inserted[k] = 1
			hashRows++
			cnt++
			ctr.bat.Zs = append(ctr.bat.Zs, 0)
		}
		valCnt++
		ai := int64(v) - 1
		ctr.bat.Zs[ai] += bat.Zs[i+k]
	}
	if cnt > 0 {
		for j, vec := range ctr.bat.Vecs {
			if err := vector.UnionBatch(vec, ctr.groupVecs[j].vec, int64(i), cnt, ctr.inserted[:n], proc.Mp()); err != nil {
				return err
			}
		}
		for _, ag := range ctr.bat.Aggs {
			if err := ag.Grows(cnt, proc.Mp()); err != nil {
				return err
			}
		}
	}
	if valCnt == 0 {
		return nil
	}
	for j, ag := range ctr.bat.Aggs {
		err := ag.BatchFill(int64(i), ctr.inserted[:n], vals, bat.Zs, []*vector.Vector{ctr.aggVecs[j].vec})
		if err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) evalAggVector(bat *batch.Batch, aggs []agg.Aggregate, proc *process.Process) error {
	for i, ag := range aggs {
		vec, err := colexec.EvalExpr(bat, proc, ag.E)
		if err != nil || vec.ConstExpand(proc.Mp()) == nil {
			return err
		}
		ctr.aggVecs[i].vec = vec
		ctr.aggVecs[i].needFree = true
		for j := range bat.Vecs {
			if bat.Vecs[j] == vec {
				ctr.aggVecs[i].needFree = false
				break
			}
		}
	}
	return nil
}
