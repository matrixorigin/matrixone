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

package filter

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "filter"

func (filter *Filter) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(fmt.Sprintf("filter(%s)", filter.E))
}

func (filter *Filter) OpType() vm.OpType {
	return vm.Filter
}

func (filter *Filter) Prepare(proc *process.Process) (err error) {
	if filter.OpAnalyzer == nil {
		filter.OpAnalyzer = process.NewAnalyzer(filter.GetIdx(), filter.IsFirst, filter.IsLast, "filter")
	} else {
		filter.OpAnalyzer.Reset()
	}

	if len(filter.ctr.executors) == 0 && filter.E != nil {
		filter.ctr.executors, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, colexec.SplitAndExprs([]*plan.Expr{filter.E}))
	}

	if filter.ctr.allExecutors == nil {
		filter.ctr.allExecutors = make([]colexec.ExpressionExecutor, 0, len(filter.ctr.runtimeExecutors)+len(filter.ctr.executors))
	} else {
		filter.ctr.allExecutors = filter.ctr.allExecutors[:0]
	}
	filter.ctr.allExecutors = append(filter.ctr.allExecutors, filter.ctr.runtimeExecutors...)
	filter.ctr.allExecutors = append(filter.ctr.allExecutors, filter.ctr.executors...)

	return err
}

func (filter *Filter) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := filter.OpAnalyzer

	inputResult, err := vm.ChildrenCall(filter.GetChildren(0), proc, analyzer)
	if err != nil {
		return inputResult, err
	}

	if inputResult.Batch == nil || inputResult.Batch.IsEmpty() || inputResult.Batch.Last() || len(filter.ctr.allExecutors) == 0 {
		return inputResult, nil
	}

	filterBat := inputResult.Batch
	var sels []int64
	for i := range filter.ctr.allExecutors {
		if filterBat.IsEmpty() {
			break
		}

		var vec *vector.Vector
		vec, err = filter.ctr.allExecutors[i].Eval(proc, []*batch.Batch{filterBat}, nil)
		if err != nil {
			return vm.CancelResult, err
		}

		if proc.OperatorOutofMemory(int64(vec.Size())) {
			return vm.CancelResult, moerr.NewOOM(proc.Ctx)
		}
		analyzer.Alloc(int64(vec.Size()))

		if !vec.GetType().IsBoolean() {
			return vm.CancelResult, moerr.NewInvalidInput(proc.Ctx, "filter condition is not boolean")
		}

		if filter.ctr.bs == nil {
			filter.ctr.bs = vector.GenerateFunctionFixedTypeParameter[bool](vec)
		} else {
			ok := vector.ReuseFunctionFixedTypeParameter[bool](vec, filter.ctr.bs)
			if !ok {
				filter.ctr.bs = vector.GenerateFunctionFixedTypeParameter[bool](vec)
			}
		}
		bs := filter.ctr.bs
		if vec.IsConst() {
			v, null := bs.GetValue(0)
			if null || !v {
				filterBat, err = filter.ctr.shrinkWithSels(proc, filterBat, nil)
				if err != nil {
					return vm.CancelResult, err
				}
			}
		} else {
			if sels == nil {
				sels = proc.Mp().GetSels()
			}
			sels = sels[:0]

			l := uint64(vec.Length())
			if bs.WithAnyNullValue() {
				for j := uint64(0); j < l; j++ {
					v, null := bs.GetValue(j)
					if !null && v {
						sels = append(sels, int64(j))
					}
				}
			} else {
				for j := uint64(0); j < l; j++ {
					v, _ := bs.GetValue(j)
					if v {
						sels = append(sels, int64(j))
					}
				}
			}
			if len(sels) != filterBat.RowCount() {
				filterBat, err = filter.ctr.shrinkWithSels(proc, filterBat, sels)
				if err != nil {
					return vm.CancelResult, err
				}
			}
		}
	}

	if sels != nil {
		proc.Mp().PutSels(sels)
	}

	// bad design here. should compile a pipeline like `-> restrict -> output (just do clean work or memory reuse) -> `
	// but not use the IsEnd flag to do the clean work.
	result := vm.NewCallResult()
	if filter.IsEnd {
		result.Batch = nil
	} else {
		result.Batch = filterBat
	}
	return result, nil
}

func (ctr *container) shrinkWithSels(proc *process.Process, bat *batch.Batch, sels []int64) (*batch.Batch, error) {
	if len(sels) == 0 {
		return batch.EmptyBatch, nil
	}
	if bat == ctr.buf {
		ctr.buf.Shrink(sels, false)
	} else {
		if ctr.buf == nil {
			ctr.buf = batch.NewWithSize(len(bat.Vecs))
			ctr.buf.SetAttributes(bat.Attrs)
			ctr.buf.Recursive = bat.Recursive
			for j, vec := range bat.Vecs {
				typ := *bat.GetVector(int32(j)).GetType()
				ctr.buf.Vecs[j] = vector.NewOffHeapVecWithType(typ)
				ctr.buf.Vecs[j].SetSorted(vec.GetSorted())
			}
			ctr.buf.SetRowCount(bat.RowCount())
			ctr.buf.ShuffleIDX = bat.ShuffleIDX
			err := ctr.buf.PreExtend(proc.Mp(), len(sels))
			if err != nil {
				return nil, err
			}
		} else {
			ctr.buf.CleanOnlyData()
		}
		err := ctr.buf.Union(bat, sels, proc.Mp())
		if err != nil {
			return nil, err
		}
	}
	return ctr.buf, nil
}
