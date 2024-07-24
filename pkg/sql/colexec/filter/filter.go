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
	filter.ctr = new(container)
	var filterExpr *plan.Expr

	if filter.exeExpr == nil && filter.E == nil {
		return nil
	}

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
}

func (filter *Filter) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	result, err := filter.GetChildren(0).Call(proc)
	if err != nil {
		return result, err
	}

	anal := proc.GetAnalyze(filter.GetIdx(), filter.GetParallelIdx(), filter.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	if result.Batch == nil || result.Batch.IsEmpty() || result.Batch.Last() || len(filter.ctr.executors) == 0 {
		return result, nil
	}
	if filter.ctr.buf != nil {
		proc.PutBatch(filter.ctr.buf)
		filter.ctr.buf = nil
	}
	filter.ctr.buf = result.Batch

	anal.Input(filter.ctr.buf, filter.GetIsFirst())

	var sels []int64
	for i := range filter.ctr.executors {
		if filter.ctr.buf.IsEmpty() {
			break
		}

		vec, err := filter.ctr.executors[i].Eval(proc, []*batch.Batch{filter.ctr.buf}, nil)
		if err != nil {
			result.Batch = nil
			return result, err
		}

		if proc.OperatorOutofMemory(int64(vec.Size())) {
			return result, moerr.NewOOM(proc.Ctx)
		}
		anal.Alloc(int64(vec.Size()))
		if !vec.GetType().IsBoolean() {
			return result, moerr.NewInvalidInput(proc.Ctx, "filter condition is not boolean")
		}

		bs := vector.GenerateFunctionFixedTypeParameter[bool](vec)
		if vec.IsConst() {
			v, null := bs.GetValue(0)
			if null || !v {
				filter.ctr.buf, err = tryDupBatch(proc, filter.ctr.buf)
				if err != nil {
					return result, err
				}
				filter.ctr.buf.Shrink(nil, false)
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
			filter.ctr.buf, err = tryDupBatch(proc, filter.ctr.buf)
			if err != nil {
				return result, err
			}
			filter.ctr.buf.Shrink(sels, false)
		}
	}

	if sels != nil {
		proc.Mp().PutSels(sels)
	}

	// bad design here. should compile a pipeline like `-> restrict -> output (just do clean work or memory reuse) -> `
	// but not use the IsEnd flag to do the clean work.
	if filter.IsEnd {
		result.Batch = nil
	} else {
		anal.Output(filter.ctr.buf, filter.GetIsLast())
		if filter.ctr.buf == result.Batch {
			filter.ctr.buf = nil
		} else {
			result.Batch = filter.ctr.buf
		}
	}
	return result, nil
}

func tryDupBatch(proc *process.Process, bat *batch.Batch) (*batch.Batch, error) {
	cnt := bat.GetCnt()
	if cnt == 1 {
		return bat, nil
	}
	newBat, err := bat.Dup(proc.Mp())
	if err != nil {
		return nil, err
	}
	// proc.PutBatch(bat)
	return newBat, nil
}
