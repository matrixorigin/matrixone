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
	"runtime"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (arg *Argument) String(buf *bytes.Buffer) {
	ap := arg
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
		buf.WriteString(fmt.Sprintf("%v(%v)", function.GetAggFunctionNameByID(ag.Op), ag.E))
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

func (arg *Argument) Prepare(proc *process.Process) (err error) {
	ap := arg
	ap.ctr = new(container)
	ap.ctr.inserted = make([]uint8, hashmap.UnitLimit)
	ap.ctr.zInserted = make([]uint8, hashmap.UnitLimit)

	ctr := ap.ctr
	ctr.state = vm.Build

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
				ctr.multiVecs[i][j].vec = proc.GetVector(typ)
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
			ctr.aggVecs[i].vec = proc.GetVector(typ)
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

	ctr.tmpVecs = make([]*vector.Vector, 1)

	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	ap := arg
	anal := proc.GetAnalyze(arg.info.Idx, arg.info.ParallelIdx, arg.info.ParallelMajor)
	anal.Start()
	defer anal.Stop()

	// if operator has no group by clause.
	if len(ap.Exprs) == 0 {
		// if operator has no group by clause.
		return ap.ctr.processWithoutGroup(ap, proc, anal, arg.info.IsFirst, arg.info.IsLast)
	}
	return ap.ctr.processWithGroup(ap, proc, anal, arg.info.IsFirst, arg.info.IsLast)
}

func (ctr *container) generateAggStructures(arg *Argument) error {
	var err error
	i := 0
	for i < len(arg.Aggs) {
		if ctr.bat.Aggs[i], err = agg.NewAggWithConfig(arg.Aggs[i].Op, arg.Aggs[i].Dist, []types.Type{*ctr.aggVecs[i].vec.GetType()}, arg.Aggs[i].Config); err != nil {
			ctr.bat = nil
			return err
		}
		i++
	}
	return nil
}

func (ctr *container) processWithoutGroup(ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool) (vm.CallResult, error) {
	if ctr.state == vm.Build {
		for {
			result, err := vm.ChildrenCall(ap.children[0], proc, anal)
			if err != nil {
				return result, err
			}
			if result.Batch == nil {
				ctr.state = vm.Eval
				break
			}
			if result.Batch.IsEmpty() {
				continue
			}
			bat := result.Batch
			// defer bat.Clean(proc.Mp())
			anal.Input(bat, isFirst)

			if err := ctr.evalAggVector(bat, proc); err != nil {
				return result, err
			}

			if ctr.bat == nil {
				if err := initCtrBatchForProcessWithoutGroup(ap, proc, ctr); err != nil {
					return result, err
				}
			}

			if err := ctr.processH0(bat); err != nil {
				return result, err
			}
		}
	}

	result := vm.NewCallResult()
	if ctr.state == vm.Eval {

		// the result of Agg can't be empty but 0 or NULL.
		if !ctr.hasAggResult {
			// very bad code.
			if err := initCtrBatchForProcessWithoutGroup(ap, proc, ctr); err != nil {
				return result, err
			}
		}
		if ctr.bat != nil {
			anal.Alloc(int64(ctr.bat.Size()))
			anal.Output(ctr.bat, isLast)
		}

		result.Batch = ctr.bat
		ctr.state = vm.End
		return result, nil
	}

	if ctr.state == vm.End {
		return result, nil
	}

	panic("bug")
}

func initCtrBatchForProcessWithoutGroup(ap *Argument, proc *process.Process, ctr *container) (err error) {
	ctr.bat = batch.NewWithSize(0)
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

func (ctr *container) processWithGroup(ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool) (vm.CallResult, error) {
	if ctr.state == vm.Build {
		for {
			result, err := vm.ChildrenCall(ap.children[0], proc, anal)
			if err != nil {
				return result, err
			}
			if result.Batch == nil {
				ctr.state = vm.Eval
				break
			}
			if result.Batch.IsEmpty() {
				continue
			}
			bat := result.Batch
			// defer bat.Clean(proc.Mp())
			anal.Input(bat, isFirst)

			if err = ctr.evalAggVector(bat, proc); err != nil {
				return result, err
			}

			for i := range ap.Exprs {
				ctr.groupVecs[i].vec, err = ctr.groupVecs[i].executor.Eval(proc, []*batch.Batch{bat})
				if err != nil {
					return result, err
				}
				ctr.vecs[i] = ctr.groupVecs[i].vec
			}

			if ctr.bat == nil {
				ctr.bat = batch.NewWithSize(len(ap.Exprs))
				for i := range ctr.groupVecs {
					vec := ctr.groupVecs[i].vec
					ctr.bat.Vecs[i] = proc.GetVector(*vec.GetType())
				}
				if ap.PreAllocSize > 0 {
					err = ctr.bat.PreExtend(proc.Mp(), int(ap.PreAllocSize))
					if err != nil {
						return result, err
					}
				}
				ctr.bat.Aggs = make([]agg.Agg[any], len(ap.Aggs)+len(ap.MultiAggs))
				if err = ctr.generateAggStructures(ap); err != nil {
					return result, err
				}
				switch {
				//case ctr.idx != nil:
				//	ctr.typ = HIndex
				case ctr.keyWidth <= 8:
					ctr.typ = H8
					if ctr.intHashMap, err = hashmap.NewIntHashMap(ctr.groupVecsNullable, ap.Ibucket, ap.Nbucket, proc.Mp()); err != nil {
						return result, err
					}
					if ap.PreAllocSize > 0 {
						err = ctr.intHashMap.PreAlloc(ap.PreAllocSize, proc.Mp())
						if err != nil {
							return result, err
						}
					}
				default:
					ctr.typ = HStr
					if ctr.strHashMap, err = hashmap.NewStrMap(ctr.groupVecsNullable, ap.Ibucket, ap.Nbucket, proc.Mp()); err != nil {
						return result, err
					}
					if ap.PreAllocSize > 0 {
						err = ctr.strHashMap.PreAlloc(ap.PreAllocSize, proc.Mp())
						if err != nil {
							return result, err
						}
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
				return result, err
			}
		}
	}

	result := vm.NewCallResult()
	if ctr.state == vm.Eval {
		if ctr.bat != nil {
			if ap.NeedEval {
				for i, ag := range ctr.bat.Aggs {
					vec, err := ag.Eval(proc.Mp())
					if err != nil {
						return result, err
					}
					ctr.bat.Aggs[i] = nil
					ctr.bat.Vecs = append(ctr.bat.Vecs, vec)
					anal.Alloc(int64(vec.Size()))

					ag.Free(proc.Mp())
				}
				ctr.bat.Aggs = nil
			}
			anal.Output(ctr.bat, isLast)
		}

		result.Batch = ctr.bat
		ctr.state = vm.End
		return result, nil
	}

	if ctr.state == vm.End {
		return result, nil
	}

	panic("bug")
}

// processH8 use whole batch to fill the aggregation.
func (ctr *container) processH0(bat *batch.Batch) error {
	ctr.bat.SetRowCount(1)

	for i, ag := range ctr.bat.Aggs {
		err := ag.BulkFill(0, []*vector.Vector{ctr.aggVecs[i].vec})
		if err != nil {
			return err
		}
	}

	return nil
}

// processH8 do group by aggregation with int hashmap.
func (ctr *container) processH8(bat *batch.Batch, proc *process.Process) error {
	count := bat.RowCount()
	itr := ctr.intHashMap.NewIterator()
	for i := 0; i < count; i += hashmap.UnitLimit {
		if i%(hashmap.UnitLimit*32) == 0 {
			runtime.Gosched()
		}
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
	count := bat.RowCount()
	itr := ctr.strHashMap.NewIterator()
	for i := 0; i < count; i += hashmap.UnitLimit { // batch
		if i%(hashmap.UnitLimit*32) == 0 {
			runtime.Gosched()
		}
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
		}
		valCnt++
	}
	ctr.bat.AddRowCount(cnt)

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
	for j, ag := range ctr.bat.Aggs {
		ctr.tmpVecs[0] = ctr.aggVecs[j].vec
		err := ag.BatchFill(int64(i), ctr.inserted[:n], vals, ctr.tmpVecs)
		if err != nil {
			return err
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
