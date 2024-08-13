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

package order

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/partition"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "order"

func (ctr *container) appendBatch(proc *process.Process, bat *batch.Batch) (enoughToSend bool, err error) {
	s1, s2 := 0, bat.Size()
	if ctr.batWaitForSort != nil {
		s1 = ctr.batWaitForSort.Size()
	}
	all := s1 + s2

	if ctr.batWaitForSort == nil {
		ctr.batWaitForSort, err = bat.Dup(proc.Mp())
		if err != nil {
			return false, err
		}
	} else {
		ctr.batWaitForSort, err = ctr.batWaitForSort.Append(proc.Ctx, proc.Mp(), bat)
		if err != nil {
			return false, err
		}
	}
	return all >= maxBatchSizeToSort, nil
}

func (ctr *container) sortAndSend(proc *process.Process, result *vm.CallResult) (err error) {
	if ctr.batWaitForSort != nil {
		for i := range ctr.sortExprExecutor {
			ctr.sortVectors[i], err = ctr.sortExprExecutor[i].Eval(proc, []*batch.Batch{ctr.batWaitForSort}, nil)
			if err != nil {
				return err
			}
		}

		firstVec := ctr.sortVectors[0]
		if cap(ctr.resultOrderList) >= ctr.batWaitForSort.RowCount() {
			ctr.resultOrderList = ctr.resultOrderList[:ctr.batWaitForSort.RowCount()]
		} else {
			ctr.resultOrderList = make([]int64, ctr.batWaitForSort.RowCount())
		}

		for i := range ctr.resultOrderList {
			ctr.resultOrderList[i] = int64(i)
		}

		// skip sort for const vector
		if !firstVec.IsConst() {
			nullCnt := firstVec.GetNulls().Count()
			if nullCnt < firstVec.Length() {
				sort.Sort(ctr.desc[0], ctr.nullsLast[0], nullCnt > 0, ctr.resultOrderList, firstVec)
			}
		}

		sels := ctr.resultOrderList
		ovec := firstVec
		if len(ctr.sortVectors) != 1 {
			ps := make([]int64, 0, 16)
			ds := make([]bool, len(sels))
			for i, j := 1, len(ctr.sortVectors); i < j; i++ {
				vec := ctr.sortVectors[i]
				ps = partition.Partition(sels, ds, ps, ovec)

				// skip sort for const vector
				if !vec.IsConst() {
					desc := ctr.desc[i]
					nullsLast := ctr.nullsLast[i]

					nullCnt := vec.GetNulls().Count()
					if nullCnt < vec.Length() {
						for m, n := 0, len(ps); m < n; m++ {
							if m == n-1 {
								sort.Sort(desc, nullsLast, nullCnt > 0, sels[ps[m]:], vec)
							} else {
								sort.Sort(desc, nullsLast, nullCnt > 0, sels[ps[m]:ps[m+1]], vec)
							}
						}
					}
				}
				ovec = vec
			}
		}

		if err = ctr.batWaitForSort.Shuffle(ctr.resultOrderList, proc.Mp()); err != nil {
			return err
		}
	}
	ctr.rbat = ctr.batWaitForSort
	result.Batch = ctr.batWaitForSort
	ctr.batWaitForSort = nil
	return nil
}

func (order *Order) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	ap := order
	buf.WriteString(": τ([")
	for i, f := range ap.OrderBySpec {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	buf.WriteString("])")
}

func (order *Order) OpType() vm.OpType {
	return vm.Order
}

func (order *Order) Prepare(proc *process.Process) (err error) {
	ctr := &order.ctr
	if len(ctr.desc) == 0 {
		ctr.desc = make([]bool, len(order.OrderBySpec))
		ctr.nullsLast = make([]bool, len(order.OrderBySpec))
		ctr.sortVectors = make([]*vector.Vector, len(order.OrderBySpec))
		for i, f := range order.OrderBySpec {
			ctr.desc[i] = f.Flag&pbplan.OrderBySpec_DESC != 0
			if f.Flag&pbplan.OrderBySpec_NULLS_FIRST != 0 {
				order.ctr.nullsLast[i] = false
			} else if f.Flag&pbplan.OrderBySpec_NULLS_LAST != 0 {
				order.ctr.nullsLast[i] = true
			} else {
				order.ctr.nullsLast[i] = order.ctr.desc[i]
			}
		}

		ctr.sortVectors = make([]*vector.Vector, len(order.OrderBySpec))
		ctr.sortExprExecutor = make([]colexec.ExpressionExecutor, len(order.OrderBySpec))
		for i := range ctr.sortVectors {
			ctr.sortExprExecutor[i], err = colexec.NewExpressionExecutor(proc, order.OrderBySpec[i].Expr)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (order *Order) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	ctr := &order.ctr
	anal := proc.GetAnalyze(order.GetIdx(), order.GetParallelIdx(), order.GetParallelMajor())
	anal.Start()
	defer func() {
		anal.Stop()
	}()

	if ctr.rbat != nil {
		//rbat is a big memory, just clean
		ctr.rbat.Clean(proc.GetMPool())
		ctr.rbat = nil
	}

	if ctr.state == vm.Build {
		for {
			input, err := vm.ChildrenCall(order.GetChildren(0), proc, anal)
			if err != nil {
				return vm.CancelResult, err
			}
			if input.Batch == nil {
				ctr.state = vm.Eval
				break
			}
			if input.Batch.IsEmpty() {
				continue
			}

			anal.Input(input.Batch, order.IsFirst)
			enoughToSend, err := ctr.appendBatch(proc, input.Batch)
			if err != nil {
				return vm.CancelResult, err
			}

			if enoughToSend {
				err := ctr.sortAndSend(proc, &input)
				if err != nil {
					return vm.CancelResult, err
				}
				return input, nil
			}
		}
	}

	result := vm.NewCallResult()
	if ctr.state == vm.Eval {
		err := ctr.sortAndSend(proc, &result)
		if err != nil {
			return vm.CancelResult, err
		}
		ctr.state = vm.End
		return result, nil
	}

	if ctr.state == vm.End {
		return result, nil
	}

	panic("bug")
}
