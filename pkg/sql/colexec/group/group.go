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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_col/group_concat"
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
	if len(ap.MultiAggs) != 0 {
		if len(ap.Aggs) > 0 {
			buf.WriteString(",")
		}
		for i, ag := range ap.MultiAggs {
			if i > 0 {
				buf.WriteString(",")
			}
			buf.WriteString("group_concat(")
			for _, expr := range ag.GroupExpr {
				buf.WriteString(fmt.Sprintf("%v ", expr))
			}
			buf.WriteString(")")
		}
	}
	buf.WriteString("])")
}

func Prepare(_ *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.mapAggType = make(map[int32]int)
	ap.ctr.inserted = make([]uint8, hashmap.UnitLimit)
	ap.ctr.zInserted = make([]uint8, hashmap.UnitLimit)
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	var end bool
	var err error
	ap := arg.(*Argument)
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	if len(ap.Exprs) == 0 {
		end, err = ap.ctr.process(ap, proc, anal, isFirst, isLast)
	} else {
		end, err = ap.ctr.processWithGroup(ap, proc, anal, isFirst, isLast)
	}
	if err != nil {
		ap.Free(proc, true)
	}
	if end {
		ap.Free(proc, false)
	}
	return end, err
}

func (ctr *container) getBatchAggs(ap *Argument) error {
	var err error
	i := 0
	j := 0
	idx := int32(0)
	for i < len(ap.Aggs) || j < len(ap.MultiAggs) {
		if j < len(ap.MultiAggs) && ap.MultiAggs[j].OrderId == idx {
			if ctr.bat.Aggs[idx] = group_concat.NewGroupConcat(&ap.MultiAggs[j], ctr.ToInputType(j)); err != nil {
				return err
			}
			ctr.mapAggType[idx] = MultiAgg
			j++
		} else {
			if ctr.bat.Aggs[idx], err = agg.New(ap.Aggs[i].Op, ap.Aggs[i].Dist, *ctr.aggVecs[i].vec.GetType()); err != nil {
				ctr.bat = nil
				return err
			}
			ctr.mapAggType[idx] = UnaryAgg
			i++
		}
		idx++
	}
	return nil
}

func (ctr *container) process(ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool) (bool, error) {
	bat := proc.InputBatch()
	if bat == nil {
		// if the result vectors are empty, process again. because the result of Agg can't be empty but 0 or NULL.
		if len(ctr.aggVecs) == 0 && len(ctr.multiVecs) == 0 {
			b := batch.NewWithSize(len(ap.Types))
			for i := range b.Vecs {
				b.Vecs[i] = vector.NewVec(ap.Types[i])
			}
			proc.SetInputBatch(b)
			if _, err := ctr.process(ap, proc, anal, isFirst, isLast); err != nil {
				return false, err
			}
		}
		if ctr.bat != nil {
			anal.Alloc(int64(ctr.bat.Size()))
			anal.Output(ctr.bat, isLast)
			proc.SetInputBatch(ctr.bat)
			ctr.bat = nil
			return true, nil
		}
		proc.SetInputBatch(nil)
		return true, nil
	}
	defer proc.PutBatch(bat)
	if len(bat.Vecs) == 0 {
		return false, nil
	}
	anal.Input(bat, isFirst)
	proc.SetInputBatch(&batch.Batch{})
	if len(ctr.aggVecs) == 0 {
		ctr.aggVecs = make([]evalVector, len(ap.Aggs))
	}

	if err := ctr.evalAggVector(bat, ap.Aggs, proc, anal); err != nil {
		return false, err
	}
	defer ctr.cleanAggVectors(proc.Mp())

	if len(ctr.multiVecs) == 0 {
		ctr.multiVecs = make([][]evalVector, len(ap.MultiAggs))
		for i, agg := range ap.MultiAggs {
			ctr.multiVecs[i] = make([]evalVector, len(agg.GroupExpr))
		}
	}
	if err := ctr.evalMultiAggs(bat, ap.MultiAggs, proc, anal); err != nil {
		return false, err
	}
	defer ctr.cleanMultiAggVecs(proc.Mp())

	if ctr.bat == nil {
		var err error

		ctr.bat = batch.NewWithSize(0)
		ctr.bat.Zs = proc.Mp().GetSels()
		ctr.bat.Zs = append(ctr.bat.Zs, 0)
		ctr.bat.Aggs = make([]agg.Agg[any], len(ap.Aggs)+len(ap.MultiAggs))
		if err = ctr.getBatchAggs(ap); err != nil {
			return false, err
		}
		for _, ag := range ctr.bat.Aggs {
			if err := ag.Grows(1, proc.Mp()); err != nil {
				return false, err
			}
		}
	}
	if bat.Length() == 0 {
		bat.Clean(proc.Mp())
		return false, nil
	}
	if err := ctr.processH0(bat, ap, proc); err != nil {
		return false, err
	}
	return false, nil
}

func (ctr *container) processWithGroup(ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool) (bool, error) {
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
					anal.Alloc(int64(vec.Size()))
				}
				ctr.bat.Aggs = nil
				for i := range ctr.bat.Zs { // reset zs
					ctr.bat.Zs[i] = 1
				}
			}
			anal.Output(ctr.bat, isLast)
			proc.SetInputBatch(ctr.bat)
			ctr.bat = nil
			return true, nil
		}
		proc.SetInputBatch(nil)
		return true, nil
	}
	if bat.Length() == 0 {
		bat.Clean(proc.Mp())
		return false, nil
	}
	defer proc.PutBatch(bat)
	anal.Input(bat, isFirst)
	proc.SetInputBatch(&batch.Batch{})
	if len(ctr.aggVecs) == 0 {
		ctr.aggVecs = make([]evalVector, len(ap.Aggs))
	}

	if err := ctr.evalAggVector(bat, ap.Aggs, proc, anal); err != nil {
		return false, err
	}
	defer ctr.cleanAggVectors(proc.Mp())

	if len(ctr.multiVecs) == 0 {
		ctr.multiVecs = make([][]evalVector, len(ap.MultiAggs))
		for i, agg := range ap.MultiAggs {
			ctr.multiVecs[i] = make([]evalVector, len(agg.GroupExpr))
		}
	}
	if err := ctr.evalMultiAggs(bat, ap.MultiAggs, proc, anal); err != nil {
		return false, err
	}
	defer ctr.cleanMultiAggVecs(proc.Mp())
	if len(ctr.groupVecs) == 0 {
		ctr.vecs = make([]*vector.Vector, len(ap.Exprs))
		ctr.groupVecs = make([]evalVector, len(ap.Exprs))
	}

	groupVecsNullable := false
	for i, expr := range ap.Exprs {
		groupVecsNullable = groupVecsNullable || (!expr.Typ.NotNullable)
		vec, err := colexec.EvalExpr(bat, proc, expr)
		if err != nil {
			ctr.cleanGroupVectors(proc.Mp())
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
		if ctr.groupVecs[i].needFree && vec != nil {
			anal.Alloc(int64(vec.Size()))
		}
		ctr.vecs[i] = vec
	}

	if ctr.bat == nil {
		size := 0
		ctr.bat = batch.NewWithSize(len(ap.Exprs))
		ctr.bat.Zs = proc.Mp().GetSels()
		for i := range ctr.groupVecs {
			vec := ctr.groupVecs[i].vec
			ctr.bat.Vecs[i] = proc.GetVector(*vec.GetType())
			ctr.bat.Vecs[i].GetType().SetNotNull(!groupVecsNullable)
			currentSize := vec.GetType().TypeSize()
			switch currentSize {
			case 1, 2, 4, 8, 16:
				size += currentSize
				if groupVecsNullable {
					size += 1
				}
			default:
				size = 128
			}
		}
		ctr.bat.Aggs = make([]agg.Agg[any], len(ap.Aggs)+len(ap.MultiAggs))
		if err = ctr.getBatchAggs(ap); err != nil {
			return false, err
		}
		switch {
		//case ctr.idx != nil:
		//	ctr.typ = HIndex
		case size <= 8:
			ctr.typ = H8
			if ctr.intHashMap, err = hashmap.NewIntHashMap(groupVecsNullable, ap.Ibucket, ap.Nbucket, proc.Mp()); err != nil {
				return false, err
			}
		default:
			ctr.typ = HStr
			if ctr.strHashMap, err = hashmap.NewStrMap(groupVecsNullable, ap.Ibucket, ap.Nbucket, proc.Mp()); err != nil {
				return false, err
			}
		}
	}
	switch ctr.typ {
	case H8:
		err = ctr.processH8(bat, proc)
	case HStr:
		err = ctr.processHStr(bat, proc)
	default:
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
	mulAggIdx := 0
	unaryAggIdx := 0
	for i, ag := range ctr.bat.Aggs {
		if ctr.mapAggType[int32(i)] == UnaryAgg {
			err := ag.BulkFill(0, bat.Zs, []*vector.Vector{ctr.aggVecs[unaryAggIdx].vec})
			if err != nil {
				return err
			}
			unaryAggIdx++
		} else {
			err := ag.BulkFill(0, bat.Zs, ctr.ToVectors(mulAggIdx))
			if err != nil {
				return err
			}
			mulAggIdx++
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

/*
func (ctr *container) processHIndex(bat *batch.Batch, proc *process.Process) error {
	mSels := make([][]int64, index.MaxLowCardinality+1)
	poses := vector.MustFixedCol[uint16](ctr.idx.GetPoses())
	for k, v := range poses {
		if len(mSels[v]) == 0 {
			mSels[v] = make([]int64, 0, 64)
		}
		mSels[v] = append(mSels[v], int64(k))
	}
	if len(mSels[0]) == 0 { // hasNotNull == true
		mSels = mSels[1:]
	}

	var groups []int64
	for i, sels := range mSels {
		if len(sels) > 0 {
			groups = append(groups, sels[0])
			ctr.bat.Zs = append(ctr.bat.Zs, 0)
			for _, k := range sels {
				ctr.bat.Zs[i] += bat.Zs[k]
			}
		}
	}

	for _, ag := range ctr.bat.Aggs {
		if err := ag.Grows(len(groups), proc.Mp()); err != nil {
			return err
		}
	}
	if err := ctr.bat.Vecs[0].Union(ctr.vecs[0], groups, proc.Mp()); err != nil {
		return err
	}
	for i, ag := range ctr.bat.Aggs {

		for j, sels := range mSels {
			for _, sel := range sels {
				if i < len(ctr.aggVecs) {
					aggVecs := []*vector.Vector{ctr.aggVecs[i].vec}
					if err := ag.Fill(int64(j), sel, 1, aggVecs); err != nil {
						return err
					}
				} else {
					if err := ag.Fill(int64(j), sel, 1, ctr.ToVecotrs(i-len(ctr.aggVecs))); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}
*/

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
			if err := vec.UnionBatch(ctr.groupVecs[j].vec, int64(i), cnt, ctr.inserted[:n], proc.Mp()); err != nil {
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
	mulAggIdx := 0
	unaryAggIdx := 0
	for j, ag := range ctr.bat.Aggs {
		if ctr.mapAggType[int32(j)] == UnaryAgg {
			err := ag.BatchFill(int64(i), ctr.inserted[:n], vals, bat.Zs, []*vector.Vector{ctr.aggVecs[unaryAggIdx].vec})
			if err != nil {
				return err
			}
			unaryAggIdx++
		} else {
			err := ag.BatchFill(int64(i), ctr.inserted[:n], vals, bat.Zs, ctr.ToVectors(mulAggIdx))
			if err != nil {
				return err
			}
			mulAggIdx++
		}
	}
	return nil
}

func (ctr *container) evalAggVector(bat *batch.Batch, aggs []agg.Aggregate, proc *process.Process, analyze process.Analyze) error {
	for i, ag := range aggs {
		vec, err := colexec.EvalExpr(bat, proc, ag.E)
		if err != nil {
			ctr.cleanAggVectors(proc.Mp())
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
		if ctr.aggVecs[i].needFree && vec != nil {
			analyze.Alloc(int64(vec.Size()))
		}
	}
	return nil
}

func (ctr *container) evalMultiAggs(bat *batch.Batch, multiAggs []group_concat.Argument, proc *process.Process, analyze process.Analyze) error {
	for i := range multiAggs {
		for j, expr := range multiAggs[i].GroupExpr {
			vec, err := colexec.EvalExpr(bat, proc, expr)
			if err != nil {
				ctr.cleanMultiAggVecs(proc.Mp())
				return err
			}
			ctr.multiVecs[i][j].vec = vec
			ctr.multiVecs[i][j].needFree = true
			for k := range bat.Vecs {
				if bat.Vecs[k] == vec {
					ctr.multiVecs[i][j].needFree = false
					break
				}
			}
			if ctr.multiVecs[i][j].needFree && vec != nil {
				analyze.Alloc(int64(vec.Size()))
			}
		}
	}
	return nil
}
