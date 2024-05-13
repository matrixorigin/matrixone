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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const argName = "group"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	ap := arg
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

func (arg *Argument) Prepare(proc *process.Process) (err error) {
	arg.ctr = new(container)
	arg.ctr.inserted = make([]uint8, hashmap.UnitLimit)
	arg.ctr.zInserted = make([]uint8, hashmap.UnitLimit)

	ctr := arg.ctr
	ctr.state = vm.Build

	// create executors for aggregation functions.
	if len(arg.Aggs) > 0 {
		ctr.aggVecs = make([]ExprEvalVector, len(arg.Aggs))
		for i, ag := range arg.Aggs {
			expressions := ag.GetArgExpressions()
			if ctr.aggVecs[i], err = MakeEvalVector(proc, expressions); err != nil {
				return err
			}
		}
	}

	// create executors for group by columns.
	ctr.keyWidth = 0
	if arg.Exprs != nil {
		ctr.groupVecsNullable = false
		ctr.groupVecs, err = MakeEvalVector(proc, arg.Exprs)
		if err != nil {
			return err
		}
		for _, gv := range arg.Exprs {
			ctr.groupVecsNullable = ctr.groupVecsNullable || (!gv.Typ.NotNullable)
		}

		for _, expr := range arg.Exprs {
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
			ctr.keyWidth += width
			if ctr.groupVecsNullable {
				ctr.keyWidth += 1
			}
		}
	}

	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	ap := arg
	anal := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	// if operator has no group by clause.
	if len(ap.Exprs) == 0 {
		// if operator has no group by clause.
		return ap.ctr.processWithoutGroup(ap, proc, anal, arg.GetIsFirst(), arg.GetIsLast())
	}
	return ap.ctr.processWithGroup(ap, proc, anal, arg.GetIsFirst(), arg.GetIsLast())
}

func (ctr *container) generateAggStructures(proc *process.Process, arg *Argument) error {
	for i, ag := range arg.Aggs {
		ctr.bat.Aggs[i] = aggexec.MakeAgg(
			proc,
			ag.GetAggID(), ag.IsDistinct(), ctr.aggVecs[i].Typ...)

		if config := ag.GetExtraConfig(); config != nil {
			if err := ctr.bat.Aggs[i].SetExtraInformation(config, 0); err != nil {
				return err
			}
		}
	}

	if preAllocate := int(arg.PreAllocSize); preAllocate > 0 {
		for _, ag := range ctr.bat.Aggs {
			if err := ag.PreAllocateGroups(preAllocate); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ctr *container) processWithoutGroup(ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool) (vm.CallResult, error) {
	if ctr.state == vm.Build {
		for {
			result, err := vm.ChildrenCall(ap.GetChildren(0), proc, anal)
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
			anal.Input(bat, isFirst)

			if err = ctr.evalAggVector(bat, proc); err != nil {
				return result, err
			}

			if ctr.bat == nil {
				if err = initCtrBatchForProcessWithoutGroup(ap, proc, ctr); err != nil {
					return result, err
				}
			}

			if err = ctr.processH0(); err != nil {
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

	ctr.bat.Aggs = make([]aggexec.AggFuncExec, len(ap.Aggs))
	if err = ctr.generateAggStructures(proc, ap); err != nil {
		return err
	}
	for _, ag := range ctr.bat.Aggs {
		if err = ag.GroupGrow(1); err != nil {
			return err
		}
	}
	return err
}

func (ctr *container) processWithGroup(ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool, isLast bool) (vm.CallResult, error) {
	if ctr.state == vm.Build {
		for {
			result, err := vm.ChildrenCall(ap.GetChildren(0), proc, anal)
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
				ctr.groupVecs.Vec[i], err = ctr.groupVecs.Executor[i].Eval(proc, []*batch.Batch{bat})
				if err != nil {
					return result, err
				}
			}

			if ctr.bat == nil {
				ctr.bat = batch.NewWithSize(len(ap.Exprs))
				for i, vec := range ctr.groupVecs.Vec {
					ctr.bat.Vecs[i] = proc.GetVector(*vec.GetType())
				}
				if ap.PreAllocSize > 0 {
					err = ctr.bat.PreExtend(proc.Mp(), int(ap.PreAllocSize))
					if err != nil {
						return result, err
					}
				}
				ctr.bat.Aggs = make([]aggexec.AggFuncExec, len(ap.Aggs))
				if err = ctr.generateAggStructures(proc, ap); err != nil {
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
					vec, err := ag.Flush()
					if err != nil {
						return result, err
					}
					ctr.bat.Aggs[i] = nil
					ctr.bat.Vecs = append(ctr.bat.Vecs, vec)
					anal.Alloc(int64(vec.Size()))

					ag.Free()
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

func (ctr *container) evalAggVector(bat *batch.Batch, proc *process.Process) (err error) {
	ctr.hasAggResult = true
	input := []*batch.Batch{bat}

	for i := range ctr.aggVecs {
		for j := range ctr.aggVecs[i].Executor {
			ctr.aggVecs[i].Vec[j], err = ctr.aggVecs[i].Executor[j].Eval(proc, input)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
