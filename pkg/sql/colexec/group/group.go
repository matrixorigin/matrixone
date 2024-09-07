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

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "group"

func (group *Group) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	ap := group
	buf.WriteString(": group([")
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
		buf.WriteString(fmt.Sprintf("%v(%v)", function.GetAggFunctionNameByID(ag.GetAggID()), ag.GetArgExpressions()))
	}
	buf.WriteString("])")
}

func (group *Group) OpType() vm.OpType {
	return vm.Group
}

func (group *Group) Prepare(proc *process.Process) (err error) {
	if !group.ctr.skipInitReusableMem {
		group.ctr.inserted = make([]uint8, hashmap.UnitLimit)
		group.ctr.zInserted = make([]uint8, hashmap.UnitLimit)

		// create executors for aggregation columns.
		if len(group.Aggs) > 0 {
			group.ctr.aggVecs = make([]ExprEvalVector, len(group.Aggs))
			for i, ag := range group.Aggs {
				expressions := ag.GetArgExpressions()
				if group.ctr.aggVecs[i], err = MakeEvalVector(proc, expressions); err != nil {
					return err
				}
			}
		}

		// calculate the key width and set the hashmap type.
		// create executors for group-by columns.
		group.ctr.keyWidth = 0
		if group.Exprs != nil {
			group.ctr.groupVecsNullable = false
			group.ctr.groupVecs, err = MakeEvalVector(proc, group.Exprs)
			if err != nil {
				return err
			}
			for _, gv := range group.Exprs {
				group.ctr.groupVecsNullable = group.ctr.groupVecsNullable || (!gv.Typ.NotNullable)
			}

			for _, expr := range group.Exprs {
				typ := expr.Typ
				width := types.T(typ.Id).TypeLen()
				if types.T(typ.Id).FixedLength() < 0 {
					if typ.Width == 0 {
						switch types.T(typ.Id) {
						case types.T_array_float32:
							width = 128 * 4
						case types.T_array_float64:
							width = 128 * 8
						default:
							width = 128
						}
					} else {
						switch types.T(typ.Id) {
						case types.T_array_float32:
							width = int(typ.Width) * 4
						case types.T_array_float64:
							width = int(typ.Width) * 8
						default:
							width = int(typ.Width)
						}
					}
				}
				group.ctr.keyWidth += width
				if group.ctr.groupVecsNullable {
					group.ctr.keyWidth += 1
				}
			}
		}
		if group.ctr.keyWidth <= 8 {
			group.ctr.typ = H8
		} else {
			group.ctr.typ = HStr
		}

		if err = group.ctr.initResultBat(proc, group); err != nil {
			return err
		}
		group.ctr.skipInitReusableMem = true
	}

	group.ctr.state = vm.Build
	// init the agg.
	if len(group.Exprs) == 0 {
		if err = group.ctr.aggWithoutGroupByCannotEmptySet(proc, group); err != nil {
			return err
		}
	} else {
		group.ctr.bat.Aggs = make([]aggexec.AggFuncExec, len(group.Aggs))
		if err = group.ctr.generateAggStructures(proc, group); err != nil {
			return err
		}
	}

	// init projection.
	if group.ProjectList != nil {
		err = group.PrepareProjection(proc)
		if err != nil {
			return
		}
	}
	return group.ctr.initHashMap(proc, group)
}

func (group *Group) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(group.GetIdx(), group.GetParallelIdx(), group.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	result, err := group.ctr.processGroupByAndAgg(group, proc, anal, group.GetIsFirst())
	if err != nil {
		return result, err
	}

	if group.ProjectList != nil {
		result.Batch, err = group.EvalProjection(result.Batch, proc)
		if err != nil {
			return result, err
		}
	}

	anal.Output(result.Batch, group.GetIsLast())
	return result, nil
}

// compute the `agg(expression)List group by expressionList`.
func (ctr *container) processGroupByAndAgg(ap *Group, proc *process.Process, anal process.Analyze, isFirst bool) (vm.CallResult, error) {
	for {
		switch ctr.state {
		// receive data from pre-operator.
		// evaluate the group-by columns and agg parameters.
		// do the group-by, and fill the agg.
		case vm.Build:
			batList := make([]*batch.Batch, 1)
			for {
				result, err := vm.ChildrenCall(ap.GetChildren(0), proc, anal)
				if err != nil {
					return result, err
				}
				if result.Batch == nil {
					if err = ctr.aggWithoutGroupByCannotEmptySet(proc, ap); err != nil {
						return result, err
					}
					ctr.state = vm.Eval
					break
				}
				if result.Batch.IsEmpty() {
					continue
				}
				anal.Input(result.Batch, isFirst)

				bat := result.Batch
				batList[0] = bat
				if err = ctr.evaluateAggAndGroupBy(proc, batList); err != nil {
					return result, err
				}

				if len(ap.Exprs) == 0 {
					// no group-by clause.
					err = ctr.processH0()

				} else {
					// with group-by clause
					switch ctr.typ {
					case H8:
						err = ctr.processH8(bat, proc)
					case HStr:
						err = ctr.processHStr(bat, proc)
					default:
						err = moerr.NewInternalError(proc.Ctx, "unexpected hashmap typ for group-operator.")
					}
				}

				if err != nil {
					return result, err
				}
			}

		// return the result one by one. todo: I have not modify that, we send result as a big batch now.
		// if NeedEval the agg, we should flush the agg first.
		case vm.Eval:
			// the result was empty.
			if ctr.bat.IsEmpty() {
				ctr.state = vm.End
				break
			}

			result := vm.NewCallResult()
			// there is no need to do agg merge. and we can get agg result here.
			if ap.NeedEval {
				aggVectors, err := ctr.getAggResult()
				if err != nil {
					return result, err
				}
				ctr.bat.Vecs = append(ctr.bat.Vecs, aggVectors...)

				// analyze.
				for _, vec := range aggVectors {
					anal.Alloc(int64(vec.Size()))
				}
			}
			result.Batch = ctr.bat

			ctr.state = vm.End
			return result, nil

		// send an End-Message to tell the next operator all were done.
		case vm.End:
			result := vm.NewCallResult()
			return result, nil

		default:
			return vm.NewCallResult(), moerr.NewInternalErrorf(proc.Ctx, "unexpected state %d for group operator.", ctr.state)
		}
	}
}

func (ctr *container) generateAggStructures(proc *process.Process, group *Group) error {

	for i, ag := range group.Aggs {
		ctr.bat.Aggs[i] = aggexec.MakeAgg(
			proc,
			ag.GetAggID(), ag.IsDistinct(), ctr.aggVecs[i].Typ...)

		if config := ag.GetExtraConfig(); config != nil {
			if err := ctr.bat.Aggs[i].SetExtraInformation(config, 0); err != nil {
				return err
			}
		}
	}

	if preAllocate := int(group.PreAllocSize); preAllocate > 0 {
		for _, ag := range ctr.bat.Aggs {
			if err := ag.PreAllocateGroups(preAllocate); err != nil {
				return err
			}
		}
	}
	return nil
}

// processH8 use whole batch to fill the aggregation.
func (ctr *container) processH0() error {
	ctr.bat.SetRowCount(1)

	for i, ag := range ctr.bat.Aggs {
		err := ag.BulkFill(0, ctr.aggVecs[i].Vec)
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
		vals, _, err := itr.Insert(i, n, ctr.groupVecs.Vec)
		if err != nil {
			return err
		}
		if err = ctr.batchFill(i, n, vals, rows, proc); err != nil {
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
		vals, _, err := itr.Insert(i, n, ctr.groupVecs.Vec)
		if err != nil {
			return err
		}
		if err = ctr.batchFill(i, n, vals, rows, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) batchFill(i int, n int, vals []uint64, hashRows uint64, proc *process.Process) error {
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
			if err := vec.UnionBatch(ctr.groupVecs.Vec[j], int64(i), cnt, ctr.inserted[:n], proc.Mp()); err != nil {
				return err
			}
		}
		for _, ag := range ctr.bat.Aggs {
			if err := ag.GroupGrow(cnt); err != nil {
				return err
			}
		}
	}
	if valCnt == 0 {
		return nil
	}
	for j, ag := range ctr.bat.Aggs {
		err := ag.BatchFill(i, vals[:n], ctr.aggVecs[j].Vec)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) evaluateAggAndGroupBy(
	proc *process.Process, batList []*batch.Batch) (err error) {
	// evaluate the agg.
	for i := range ctr.aggVecs {
		for j := range ctr.aggVecs[i].Executor {
			ctr.aggVecs[i].Vec[j], err = ctr.aggVecs[i].Executor[j].Eval(proc, batList, nil)
			if err != nil {
				return err
			}
		}
	}

	// evaluate the group-by.
	for i := range ctr.groupVecs.Vec {
		ctr.groupVecs.Vec[i], err = ctr.groupVecs.Executor[i].Eval(proc, batList, nil)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ctr *container) getAggResult() ([]*vector.Vector, error) {
	result := make([]*vector.Vector, len(ctr.bat.Aggs))

	var err error
	for i, ag := range ctr.bat.Aggs {
		result[i], err = ag.Flush()
		if err != nil {
			return nil, err
		}
		ag.Free()
	}
	ctr.bat.Aggs = nil

	return result, nil
}

// init the container.bat to store the final result of group-operator
func (ctr *container) initResultBat(proc *process.Process, config *Group) (err error) {
	// init the batch to store the group-by.
	ctr.bat = batch.NewWithSize(len(config.Exprs))
	for i := range ctr.groupVecs.Typ {
		ctr.bat.Vecs[i] = vector.NewVec(ctr.groupVecs.Typ[i])
	}
	if config.PreAllocSize > 0 {
		if err = ctr.bat.PreExtend(proc.Mp(), int(config.PreAllocSize)); err != nil {
			return err
		}
	}
	return nil
}

// init the hashmap.
func (ctr *container) initHashMap(proc *process.Process, config *Group) (err error) {
	// init the hashmap.
	switch {
	case ctr.keyWidth <= 8:
		if ctr.intHashMap, err = hashmap.NewIntHashMap(ctr.groupVecsNullable); err != nil {
			return err
		}
		if config.PreAllocSize > 0 {
			if err = ctr.intHashMap.PreAlloc(config.PreAllocSize); err != nil {
				return err
			}
		}

	default:
		if ctr.strHashMap, err = hashmap.NewStrMap(ctr.groupVecsNullable); err != nil {
			return err
		}
		if config.PreAllocSize > 0 {
			if err = ctr.strHashMap.PreAlloc(config.PreAllocSize); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ctr *container) aggWithoutGroupByCannotEmptySet(proc *process.Process, config *Group) (err error) {
	if len(config.Exprs) != 0 {
		return nil
	}

	// if this was a query like `select agg(a) from t`, and t is empty.
	// agg(a) should return 0 for count, and return null for other agg.
	if ctr.bat == nil {
		ctr.bat = batch.NewWithSize(0)
	}
	if len(ctr.bat.Aggs) == 0 {
		// init the agg.
		ctr.bat.Aggs = make([]aggexec.AggFuncExec, len(config.Aggs))
		if err = ctr.generateAggStructures(proc, config); err != nil {
			return err
		}
		// if no group by, the group number must be 1.
		if len(config.Exprs) == 0 {
			for _, ag := range ctr.bat.Aggs {
				if err = ag.GroupGrow(1); err != nil {
					return err
				}
			}
			ctr.bat.SetRowCount(1)
		}
	}

	return nil
}
