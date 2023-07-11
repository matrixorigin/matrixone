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
	"github.com/matrixorigin/matrixone/pkg/container/types"
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

func Prepare(proc *process.Process, arg any) (err error) {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.mapAggType = make(map[int32]int)
	ap.ctr.inserted = make([]uint8, hashmap.UnitLimit)
	ap.ctr.zInserted = make([]uint8, hashmap.UnitLimit)

	ctr := ap.ctr

	// if operator has multi columns aggregation agg(expr1, expr2 ...)
	// then create expression executor for each parameter expr.
	if ap.MultiAggs != nil {
		ctr.multiVecs = make([][]evalVector, len(ap.MultiAggs))
		for i, ag := range ap.MultiAggs {
			ctr.multiVecs[i] = make([]evalVector, len(ag.GroupExpr))
			for j := range ctr.multiVecs[i] {
				ctr.multiVecs[i][j].executor, err = colexec.NewExpressionExecutor(proc, ag.GroupExpr[j])
				if err != nil {
					return err
				}
				// very bad code.
				exprTyp := ag.GroupExpr[j].Typ
				typ := types.New(types.T(exprTyp.Id), exprTyp.Width, exprTyp.Scale)
				ctr.multiVecs[i][j].vec = vector.NewVec(typ)
			}
		}
	}

	// if operator has single column aggregation agg(expr)
	// then create expression executor for the parameter expr.
	if ap.Aggs != nil {
		ctr.aggVecs = make([]evalVector, len(ap.Aggs))
		for i, ag := range ap.Aggs {
			ctr.aggVecs[i].executor, err = colexec.NewExpressionExecutor(proc, ag.E)
			if err != nil {
				return err
			}
			// very bad code.
			exprTyp := ag.E.Typ
			typ := types.New(types.T(exprTyp.Id), exprTyp.Width, exprTyp.Scale)
			ctr.aggVecs[i].vec = vector.NewVec(typ)
		}
	}

	// if operator has group by clause group by expr1, expr2 ...
	// then create expression executor for each expr.
	ctr.keyWidth = 0
	if ap.Exprs != nil {
		ctr.groupVecs = make([]evalVector, len(ap.Exprs))
		ctr.groupVecsNullable = false

		for i, gv := range ap.Exprs {
			ctr.groupVecsNullable = ctr.groupVecsNullable || (!gv.Typ.NotNullable)

			ctr.groupVecs[i].executor, err = colexec.NewExpressionExecutor(proc, gv)
			if err != nil {
				return err
			}
		}
		ctr.vecs = make([]*vector.Vector, len(ap.Exprs))

		for _, expr := range ap.Exprs {
			typ := expr.Typ
			width := types.T(typ.Id).TypeLen()
			if types.T(typ.Id).FixedLength() < 0 {
				if typ.Width == 0 {
					width = 128
				} else {
					width = int(typ.Width)
				}
			}
			ctr.keyWidth += width
			if ctr.groupVecsNullable {
				ctr.keyWidth += 1
			}
		}
	}
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (end bool, err error) {
	ap := arg.(*Argument)
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()

	// if operator has no group by clause.
	if len(ap.Exprs) == 0 {
		// if operator has no group by clause.
		return ap.ctr.processWithoutGroup(ap, proc, anal, isFirst, isLast)
	}
	return ap.ctr.processWithGroup(ap, proc, anal, isFirst, isLast)
}

func (ctr *container) generateAggStructures(ap *Argument) error {
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

func (ctr *container) processWithoutGroup(ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool) (bool, error) {
	bat := proc.InputBatch()
	if bat == nil {
		// the result of Agg can't be empty but 0 or NULL.
		if !ctr.hasAggResult {
			// very bad code.
			if err := initCtrBatchForProcessWithoutGroup(ap, proc, ctr); err != nil {
				return false, err
			}
		}
		if ctr.bat != nil {
			anal.Alloc(int64(ctr.bat.Size()))
			anal.Output(ctr.bat, isLast)
		}

		// todo(cms): i reset zs here.
		for i := range ctr.bat.Zs {
			ctr.bat.Zs[i] = 1
		}

		ctr.bat.CheckForRemoveZs("group")
		proc.SetInputBatch(ctr.bat)
		ctr.bat = nil
		return true, nil
	}

	bat.FixedForRemoveZs()

	if bat.IsEmpty() {
		bat.Clean(proc.Mp())
		return false, nil
	}

	defer proc.PutBatch(bat)
	anal.Input(bat, isFirst)
	proc.SetInputBatch(batch.EmptyBatch)

	if err := ctr.evalAggVector(bat, proc); err != nil {
		return false, err
	}

	if err := ctr.evalMultiAggs(bat, proc); err != nil {
		return false, err
	}

	if ctr.bat == nil {
		if err := initCtrBatchForProcessWithoutGroup(ap, proc, ctr); err != nil {
			return false, err
		}
	}

	if err := ctr.processH0(bat); err != nil {
		return false, err
	}
	return false, nil
}

func initCtrBatchForProcessWithoutGroup(ap *Argument, proc *process.Process, ctr *container) (err error) {
	ctr.bat = batch.NewWithSize(0)
	ctr.bat.Zs = proc.Mp().GetSels()
	ctr.bat.Zs = append(ctr.bat.Zs, 0)
	ctr.bat.SetRowCount(1)

	ctr.bat.Aggs = make([]agg.Agg[any], len(ap.Aggs)+len(ap.MultiAggs))
	if err = ctr.generateAggStructures(ap); err != nil {
		return err
	}
	for _, ag := range ctr.bat.Aggs {
		if err = ag.Grows(1, proc.Mp()); err != nil {
			return err
		}
	}
	return err
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
		}

		ctr.bat.CheckForRemoveZs("group")
		proc.SetInputBatch(ctr.bat)
		ctr.bat = nil
		return true, nil
	}

	bat.FixedForRemoveZs()
	if bat.IsEmpty() {
		bat.Clean(proc.Mp())
		return false, nil
	}

	defer proc.PutBatch(bat)
	anal.Input(bat, isFirst)
	proc.SetInputBatch(batch.EmptyBatch)

	if err = ctr.evalAggVector(bat, proc); err != nil {
		return false, err
	}

	if err = ctr.evalMultiAggs(bat, proc); err != nil {
		return false, err
	}

	for i := range ap.Exprs {
		ctr.groupVecs[i].vec, err = ctr.groupVecs[i].executor.Eval(proc, []*batch.Batch{bat})
		if err != nil {
			return false, err
		}
		ctr.vecs[i] = ctr.groupVecs[i].vec
	}

	if ctr.bat == nil {
		ctr.bat = batch.NewWithSize(len(ap.Exprs))
		ctr.bat.Zs = proc.Mp().GetSels()
		for i := range ctr.groupVecs {
			vec := ctr.groupVecs[i].vec
			ctr.bat.Vecs[i] = proc.GetVector(*vec.GetType())
			//ctr.bat.Vecs[i].GetType().SetNotNull(!groupVecsNullable)
		}
		ctr.bat.Aggs = make([]agg.Agg[any], len(ap.Aggs)+len(ap.MultiAggs))
		if err = ctr.generateAggStructures(ap); err != nil {
			return false, err
		}
		switch {
		//case ctr.idx != nil:
		//	ctr.typ = HIndex
		case ctr.keyWidth <= 8:
			ctr.typ = H8
			if ctr.intHashMap, err = hashmap.NewIntHashMap(ctr.groupVecsNullable, ap.Ibucket, ap.Nbucket, proc.Mp()); err != nil {
				return false, err
			}
		default:
			ctr.typ = HStr
			if ctr.strHashMap, err = hashmap.NewStrMap(ctr.groupVecsNullable, ap.Ibucket, ap.Nbucket, proc.Mp()); err != nil {
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

// processH8 use whole batch to fill the aggregation.
func (ctr *container) processH0(bat *batch.Batch) error {
	for _, z := range bat.Zs {
		ctr.bat.Zs[0] += z
	}
	// TODO(cms):
	ctr.bat.SetRowCount(1)

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

// processH8 do group by aggregation with int hashmap.
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

// processHStr do group by aggregation with string hashmap.
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
	ctr.bat.SetRowCount(ctr.bat.RowCount() + cnt)

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

func (ctr *container) evalAggVector(bat *batch.Batch, proc *process.Process) error {
	ctr.hasAggResult = true
	for i := range ctr.aggVecs {
		vec, err := ctr.aggVecs[i].executor.Eval(proc, []*batch.Batch{bat})
		if err != nil {
			return err
		}
		ctr.aggVecs[i].vec = vec
	}
	return nil
}

func (ctr *container) evalMultiAggs(bat *batch.Batch, proc *process.Process) error {
	ctr.hasAggResult = true
	for i := range ctr.multiVecs {
		for j := range ctr.multiVecs[i] {
			vec, err := ctr.multiVecs[i][j].executor.Eval(proc, []*batch.Batch{bat})
			if err != nil {
				return err
			}
			ctr.multiVecs[i][j].vec = vec
		}
	}
	return nil
}
