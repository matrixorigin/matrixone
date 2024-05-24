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

const argName = "order"

func (ctr *container) appendBatch(proc *process.Process, bat *batch.Batch) (enoughToSend bool, err error) {
	s1, s2 := 0, bat.Size()
	if ctr.batWaitForSort != nil {
		s1 = ctr.batWaitForSort.Size()
	}
	all := s1 + s2

	// Maybe a bug that bat.Cnt is not 1.
	// can we instead the vec directly?
	for i := 0; i < bat.VectorCount(); i++ {
		vec := bat.GetVector(int32(i))
		if vec.NeedDup() {
			oldVec := bat.Vecs[i]
			nv, errDup := oldVec.Dup(proc.Mp())
			if errDup != nil {
				return false, errDup
			}
			bat.ReplaceVector(oldVec, nv)
			oldVec.Free(proc.Mp())
		}
	}
	if ctr.batWaitForSort == nil {
		ctr.batWaitForSort, err = bat.Dup(proc.Mp())
		if err != nil {
			return false, err
		}
	} else {
		// XXX flat const vector here.
		if ctr.batWaitForSort != nil {
			if ctr.flatFn == nil {
				ctr.flatFn = make([]func(v, w *vector.Vector) error, bat.VectorCount())
			}

			for i := 0; i < ctr.batWaitForSort.VectorCount(); i++ {
				typ := *ctr.batWaitForSort.Vecs[i].GetType()
				if ctr.batWaitForSort.Vecs[i].IsConst() {
					if ctr.flatFn[i] == nil {
						ctr.flatFn[i] = vector.GetUnionAllFunction(typ, proc.Mp())
					}

					v := ctr.batWaitForSort.Vecs[i]
					ctr.batWaitForSort.Vecs[i] = proc.GetVector(typ)
					err = ctr.flatFn[i](ctr.batWaitForSort.Vecs[i], v)
					v.Free(proc.Mp())
					if err != nil {
						return false, err
					}
				}
			}
		}
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
			ctr.sortVectors[i], err = ctr.sortExprExecutor[i].Eval(proc, []*batch.Batch{ctr.batWaitForSort})
			if err != nil {
				return err
			}
		}

		var strCol []string

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
				if firstVec.GetType().IsVarlen() {
					strCol = vector.MustStrCol(firstVec)
				} else {
					strCol = nil
				}
				sort.Sort(ctr.desc[0], ctr.nullsLast[0], nullCnt > 0, ctr.resultOrderList, firstVec, strCol)
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
						if vec.GetType().IsVarlen() {
							strCol = vector.MustStrCol(vec)
						} else {
							strCol = nil
						}
						for m, n := 0, len(ps); m < n; m++ {
							if m == n-1 {
								sort.Sort(desc, nullsLast, nullCnt > 0, sels[ps[m]:], vec, strCol)
							} else {
								sort.Sort(desc, nullsLast, nullCnt > 0, sels[ps[m]:ps[m+1]], vec, strCol)
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
	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	ctr.rbat = ctr.batWaitForSort
	result.Batch = ctr.batWaitForSort
	ctr.batWaitForSort = nil
	return nil
}

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	ap := arg
	buf.WriteString(": τ([")
	for i, f := range ap.OrderBySpec {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	buf.WriteString("])")
}

func (arg *Argument) Prepare(proc *process.Process) (err error) {
	arg.ctr = new(container)
	ctr := arg.ctr
	ctr.state = vm.Build
	{
		ctr.desc = make([]bool, len(arg.OrderBySpec))
		ctr.nullsLast = make([]bool, len(arg.OrderBySpec))
		ctr.sortVectors = make([]*vector.Vector, len(arg.OrderBySpec))
		for i, f := range arg.OrderBySpec {
			ctr.desc[i] = f.Flag&pbplan.OrderBySpec_DESC != 0
			if f.Flag&pbplan.OrderBySpec_NULLS_FIRST != 0 {
				arg.ctr.nullsLast[i] = false
			} else if f.Flag&pbplan.OrderBySpec_NULLS_LAST != 0 {
				arg.ctr.nullsLast[i] = true
			} else {
				arg.ctr.nullsLast[i] = arg.ctr.desc[i]
			}
		}
	}

	ctr.sortVectors = make([]*vector.Vector, len(arg.OrderBySpec))
	ctr.sortExprExecutor = make([]colexec.ExpressionExecutor, len(arg.OrderBySpec))
	for i := range ctr.sortVectors {
		ctr.sortExprExecutor[i], err = colexec.NewExpressionExecutor(proc, arg.OrderBySpec[i].Expr)
		if err != nil {
			return err
		}
	}

	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	ctr := arg.ctr
	anal := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	anal.Start()
	defer func() {
		anal.Stop()
	}()

	if ctr.state == vm.Build {
		for {
			result, err := vm.ChildrenCall(arg.GetChildren(0), proc, anal)

			if err != nil {
				result.Status = vm.ExecStop
				return result, err
			}
			if result.Batch == nil {
				ctr.state = vm.Eval
				break
			}
			if result.Batch.IsEmpty() {
				continue
			}

			enoughToSend, err := ctr.appendBatch(proc, result.Batch)
			if err != nil {
				result.Status = vm.ExecStop
				return result, err
			}

			if enoughToSend {
				err := ctr.sortAndSend(proc, &result)
				if err != nil {
					result.Status = vm.ExecStop
					return result, err
				}
				return result, nil
			}
		}
	}

	result := vm.NewCallResult()
	if ctr.state == vm.Eval {
		err := ctr.sortAndSend(proc, &result)
		if err != nil {
			result.Status = vm.ExecStop
			return result, err
		}
		ctr.state = vm.End
		return result, nil
	}

	if ctr.state == vm.End {
		return result, nil
	}

	panic("bug")
}
