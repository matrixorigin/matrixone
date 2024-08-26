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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
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
	filter.OpAnalyzer = process.NewAnalyzer(filter.GetIdx(), filter.IsFirst, filter.IsLast, "filter")

	if filter.exeExpr == nil && filter.E == nil {
		return nil
	}

	var filterExpr *plan.Expr
	if filter.exeExpr == nil {
		filterExpr, err = plan2.ConstantFold(batch.EmptyForConstFoldBatch, plan2.DeepCopyExpr(filter.E), proc, true, true)
	} else {
		filterExpr, err = plan2.ConstantFold(batch.EmptyForConstFoldBatch, plan2.DeepCopyExpr(filter.exeExpr), proc, true, true)
	}
	if err != nil {
		return err
	}
	filter.ctr.executors, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, colexec.SplitAndExprs([]*plan.Expr{filterExpr}))
	return err

	// if len(filter.ctr.executors) == 0 {
	// 	if filter.exeExpr == nil {
	// 		filter.ctr.executors, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, colexec.SplitAndExprs([]*plan.Expr{filter.E}))
	// 	} else {
	// 		filter.ctr.executors, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, colexec.SplitAndExprs([]*plan.Expr{filter.exeExpr}))
	// 	}
	// }
	// return err
}

func (filter *Filter) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	//anal := proc.GetAnalyze(filter.GetIdx(), filter.GetParallelIdx(), filter.GetParallelMajor())
	//anal.Start()
	//defer anal.Stop()

	analyzer := filter.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	inputResult, err := vm.ChildrenCallV1(filter.GetChildren(0), proc, analyzer)
	if err != nil {
		return inputResult, err
	}
	//anal.Input(inputResult.Batch, filter.IsFirst)

	if inputResult.Batch == nil || inputResult.Batch.IsEmpty() || inputResult.Batch.Last() || len(filter.ctr.executors) == 0 {
		return inputResult, nil
	}

	filterBat := inputResult.Batch
	var sels []int64
	for i := range filter.ctr.executors {
		if filterBat.IsEmpty() {
			break
		}

		vec, err := filter.ctr.executors[i].Eval(proc, []*batch.Batch{filterBat}, nil)
		if err != nil {
			return vm.CancelResult, err
		}

		if proc.OperatorOutofMemory(int64(vec.Size())) {
			return vm.CancelResult, moerr.NewOOM(proc.Ctx)
		}
		//anal.Alloc(int64(vec.Size()))
		analyzer.Alloc(int64(vec.Size()))

		if !vec.GetType().IsBoolean() {
			return vm.CancelResult, moerr.NewInvalidInput(proc.Ctx, "filter condition is not boolean")
		}

		bs := vector.GenerateFunctionFixedTypeParameter[bool](vec)
		if vec.IsConst() {
			v, null := bs.GetValue(0)
			if null || !v {
				filterBat, err = tryDupBatch(&filter.ctr, proc, filterBat)
				if err != nil {
					return vm.CancelResult, err
				}
				filterBat.Shrink(nil, false)
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
				filterBat, err = tryDupBatch(&filter.ctr, proc, filterBat)
				if err != nil {
					return vm.CancelResult, err
				}
				filterBat.Shrink(sels, false)
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
		//anal.Output(filterBat, filter.GetIsLast())
		result.Batch = filterBat
	}
	analyzer.Output(result.Batch)
	return result, nil
}

func tryDupBatch(ctr *container, proc *process.Process, bat *batch.Batch) (*batch.Batch, error) {
	if bat == ctr.buf {
		return bat, nil
	}
	if ctr.buf != nil {
		ctr.buf.CleanOnlyData()
	}
	//copy input.Batch to ctr.buf
	var err error
	ctr.buf, err = ctr.buf.AppendWithCopy(proc.Ctx, proc.GetMPool(), bat)
	if err != nil {
		return nil, err
	}
	return ctr.buf, nil
}
