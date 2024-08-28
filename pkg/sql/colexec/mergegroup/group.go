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
	"runtime"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "merge_group"

func (mergeGroup *MergeGroup) String(buf *bytes.Buffer) {
	buf.WriteString(opName)

}

func (mergeGroup *MergeGroup) OpType() vm.OpType {
	return vm.MergeGroup
}

func (mergeGroup *MergeGroup) Prepare(proc *process.Process) error {
	if mergeGroup.ProjectList != nil {
		err := mergeGroup.PrepareProjection(proc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (mergeGroup *MergeGroup) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	ctr := &mergeGroup.ctr
	anal := proc.GetAnalyze(mergeGroup.GetIdx(), mergeGroup.GetParallelIdx(), mergeGroup.GetParallelMajor())
	anal.Start()
	defer anal.Stop()
	result := vm.NewCallResult()
	for {
		switch ctr.state {
		case Build:
			for {
				result, err := mergeGroup.GetChildren(0).Call(proc)
				if err != nil {
					return result, err
				}

				if result.Batch == nil {
					break
				}

				bat := result.Batch
				anal.Input(bat, mergeGroup.GetIsFirst())
				if err = ctr.process(bat, proc); err != nil {
					return result, err
				}
			}
			ctr.state = Eval

		case Eval:
			if ctr.bat != nil && !ctr.bat.IsEmpty() {
				if mergeGroup.NeedEval {
					for i, agg := range ctr.bat.Aggs {
						if len(mergeGroup.PartialResults) > i && mergeGroup.PartialResults[i] != nil {
							if err := agg.SetExtraInformation(mergeGroup.PartialResults[i], 0); err != nil {
								return result, err
							}
						}
						vec, err := agg.Flush()
						if err != nil {
							ctr.state = End
							return result, err
						}
						ctr.bat.Aggs[i] = nil
						ctr.bat.Vecs = append(ctr.bat.Vecs, vec)
						if vec != nil {
							anal.Alloc(int64(vec.Size()))
						}

						agg.Free()
					}
					ctr.bat.Aggs = nil
				}

				result.Batch = ctr.bat
				if mergeGroup.ProjectList != nil {
					var err error
					result.Batch, err = mergeGroup.EvalProjection(result.Batch, proc)
					if err != nil {
						return result, err
					}
				}
				anal.Output(result.Batch, mergeGroup.GetIsLast())
			}
			ctr.state = End
			return result, nil

		case End:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (ctr *container) process(bat *batch.Batch, proc *process.Process) error {
	var err error

	// calculate hash key width and nullability
	if ctr.hashKeyWidth == NeedCalculationForKeyWidth {
		ctr.hashKeyWidth = 0
		ctr.keyNullability = false
		ctr.groupByCol = len(bat.Vecs)

		for _, vec := range bat.Vecs {
			ctr.keyNullability = ctr.keyNullability || (!vec.GetType().GetNotNull())
		}

		for _, vec := range bat.Vecs {
			width := vec.GetType().TypeSize()
			if vec.GetType().IsVarlen() {
				if vec.GetType().Width == 0 {
					switch vec.GetType().Oid {
					case types.T_array_float32:
						width = 128 * 4
					case types.T_array_float64:
						width = 128 * 8
					default:
						width = 128
					}
				} else {
					switch vec.GetType().Oid {
					case types.T_array_float32:
						width = int(vec.GetType().Width) * 4
					case types.T_array_float64:
						width = int(vec.GetType().Width) * 8
					default:
						width = int(vec.GetType().Width)
					}
				}
			}
			ctr.hashKeyWidth += width
			if ctr.keyNullability {
				ctr.hashKeyWidth += 1
			}
		}

		switch {
		case ctr.hashKeyWidth == 0:
			// no group by.
			ctr.typ = H0

		case ctr.hashKeyWidth <= 8:
			ctr.inserted = make([]uint8, hashmap.UnitLimit)
			ctr.zInserted = make([]uint8, hashmap.UnitLimit)
			ctr.typ = H8

		default:
			ctr.inserted = make([]uint8, hashmap.UnitLimit)
			ctr.zInserted = make([]uint8, hashmap.UnitLimit)
			ctr.typ = HStr
		}
	}

	switch ctr.typ {
	case H0:
		return ctr.processH0(bat, proc)

	case H8:
		if ctr.intHashMap == nil {
			if ctr.intHashMap, err = hashmap.NewIntHashMap(ctr.keyNullability, proc.Mp()); err != nil {
				return err
			}
		}
		return ctr.processH8(bat, proc)

	default:
		if ctr.strHashMap == nil {
			if ctr.strHashMap, err = hashmap.NewStrMap(ctr.keyNullability, proc.Mp()); err != nil {
				return err
			}
		}
		return ctr.processHStr(bat, proc)
	}
}

func (ctr *container) processH0(bat *batch.Batch, _ *process.Process) error {
	ctr.initEmptyBatchFromInput(bat)
	if ctr.bat.IsEmpty() {
		ctr.bat.Aggs = bat.Aggs
		bat.Aggs = nil
		ctr.bat.SetRowCount(1)
		return nil
	}

	for i, agg := range ctr.bat.Aggs {
		err := agg.Merge(bat.Aggs[i], 0, 0)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) processH8(bat *batch.Batch, proc *process.Process) error {
	count := bat.RowCount()
	itr := ctr.intHashMap.NewIterator()

	ctr.initEmptyBatchFromInput(bat)
	noNeedToFill := ctr.bat.IsEmpty()
	if noNeedToFill {
		var err error
		if ctr.bat, err = ctr.bat.Append(proc.Ctx, proc.Mp(), bat); err != nil {
			return err
		}

		ctr.bat.Aggs = bat.Aggs
		bat.Aggs = nil
	}

	for i := 0; i < count; i += hashmap.UnitLimit {
		if i%(hashmap.UnitLimit*32) == 0 {
			runtime.Gosched()
		}
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		rowCount := ctr.intHashMap.GroupCount()
		vals, _, err := itr.Insert(i, n, bat.Vecs)
		if err != nil {
			return err
		}

		if noNeedToFill {
			continue
		}
		if err = ctr.batchFill(i, n, bat, vals, rowCount, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) processHStr(bat *batch.Batch, proc *process.Process) error {
	count := bat.RowCount()
	itr := ctr.strHashMap.NewIterator()

	ctr.initEmptyBatchFromInput(bat)
	noNeedToFill := ctr.bat.IsEmpty()
	if noNeedToFill {
		var err error
		if ctr.bat, err = ctr.bat.Append(proc.Ctx, proc.Mp(), bat); err != nil {
			return err
		}

		ctr.bat.Aggs = bat.Aggs
		bat.Aggs = nil
	}

	for i := 0; i < count; i += hashmap.UnitLimit { // batch
		if i%(hashmap.UnitLimit*32) == 0 {
			runtime.Gosched()
		}
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		rowCount := ctr.strHashMap.GroupCount()
		vals, _, err := itr.Insert(i, n, bat.Vecs)
		if err != nil {
			return err
		}
		if noNeedToFill {
			continue
		}
		if err = ctr.batchFill(i, n, bat, vals, rowCount, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) batchFill(i int, n int, bat *batch.Batch, vals []uint64, hashRows uint64, proc *process.Process) error {
	cnt := 0
	copy(ctr.inserted[:n], ctr.zInserted[:n])
	for k, v := range vals {
		if v > hashRows {
			ctr.inserted[k] = 1
			hashRows++
			cnt++
		}
	}
	ctr.bat.AddRowCount(cnt)

	if cnt > 0 {
		for j, vec := range ctr.bat.Vecs {
			if err := vec.UnionBatch(bat.Vecs[j], int64(i), cnt, ctr.inserted[:n], proc.Mp()); err != nil {
				return err
			}
		}
		for _, agg := range ctr.bat.Aggs {
			if err := agg.GroupGrow(cnt); err != nil {
				return err
			}
		}
	}
	for j, agg := range ctr.bat.Aggs {
		if err := agg.BatchMerge(bat.Aggs[j], i, vals[:n]); err != nil {
			return err
		}
	}
	return nil
}
