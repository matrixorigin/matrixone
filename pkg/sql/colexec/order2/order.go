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

package order2

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/partition"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const maxBatchSizeToSort = 512 * mpool.MB

type Argument struct {
	ctr *container

	SortDirections []*plan.OrderBySpec
}

type container struct {
	batWaitForSort *batch.Batch
	extraBat       *batch.Batch

	desc      []bool // ds[i] == true: the attrs[i] are in descending order
	nullsLast []bool

	sortExprExecutor []colexec.ExpressionExecutor
	sortVectors      []*vector.Vector
	resultOrderList  []int64
}

func (arg *Argument) Free(proc *process.Process, _ bool) {
	ctr := arg.ctr
	if ctr != nil {
		for i := range ctr.sortExprExecutor {
			if ctr.sortExprExecutor[i] != nil {
				ctr.sortExprExecutor[i].Free()
			}
		}
		ctr.batWaitForSort.Clean(proc.Mp())
		ctr.extraBat.Clean(proc.Mp())
		ctr.resultOrderList = nil
	}
}

func (ctr *container) appendBatch(proc *process.Process, bat *batch.Batch) (needSend bool, err error) {
	if bat.Length() == 0 {
		bat.Clean(proc.Mp())
		proc.SetInputBatch(batch.EmptyBatch)
		return false, nil
	}

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

	if s1 >= maxBatchSizeToSort {
		ctr.extraBat = bat

		return true, nil
	} else {
		ctr.batWaitForSort, err = ctr.batWaitForSort.Append(proc.Ctx, proc.Mp(), bat)
		if err != nil {
			return false, err
		}
	}

	return all >= maxBatchSizeToSort, nil
}

func (ctr *container) sortAndSend(proc *process.Process) (err error) {
	if ctr.batWaitForSort != nil {
		for i := range ctr.sortExprExecutor {
			ctr.sortVectors[i], err = ctr.sortExprExecutor[i].Eval(proc, []*batch.Batch{ctr.batWaitForSort})
			if err != nil {
				return err
			}
		}

		var strCol []string

		firstVec := ctr.sortVectors[0]
		if cap(ctr.resultOrderList) >= ctr.batWaitForSort.Length() {
			ctr.resultOrderList = ctr.resultOrderList[:ctr.batWaitForSort.Length()]
		} else {
			ctr.resultOrderList = make([]int64, ctr.batWaitForSort.Length())
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
				desc := ctr.desc[i]
				nullsLast := ctr.nullsLast[i]
				ps = partition.Partition(sels, ds, ps, ovec)
				vec := ctr.sortVectors[i]
				// skip sort for const vector
				if !vec.IsConst() {
					nullCnt := vec.GetNulls().Count()
					if nullCnt < vec.Length() {
						if vec.GetType().IsVarlen() {
							strCol = vector.MustStrCol(vec)
						} else {
							strCol = nil
						}
						for i, j := 0, len(ps); i < j; i++ {
							if i == j-1 {
								sort.Sort(desc, nullsLast, nullCnt > 0, sels[ps[i]:], vec, strCol)
							} else {
								sort.Sort(desc, nullsLast, nullCnt > 0, sels[ps[i]:ps[i+1]], vec, strCol)
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
	proc.SetInputBatch(ctr.batWaitForSort)
	ctr.batWaitForSort = ctr.extraBat

	return nil
}

func String(arg any, buf *bytes.Buffer) {
	ap := arg.(*Argument)
	buf.WriteString("τ([")
	for i, f := range ap.SortDirections {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	buf.WriteString("])")
}

func Prepare(proc *process.Process, arg any) (err error) {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ctr := ap.ctr
	{
		ctr.desc = make([]bool, len(ap.SortDirections))
		ctr.nullsLast = make([]bool, len(ap.SortDirections))
		ctr.sortVectors = make([]*vector.Vector, len(ap.SortDirections))
		for i, f := range ap.SortDirections {
			ctr.desc[i] = f.Flag&pbplan.OrderBySpec_DESC != 0
			if f.Flag&pbplan.OrderBySpec_NULLS_FIRST != 0 {
				ap.ctr.nullsLast[i] = false
			} else if f.Flag&pbplan.OrderBySpec_NULLS_LAST != 0 {
				ap.ctr.nullsLast[i] = true
			} else {
				ap.ctr.nullsLast[i] = ap.ctr.desc[i]
			}
		}
	}
	ctr.sortVectors = make([]*vector.Vector, len(ap.SortDirections))
	ctr.sortExprExecutor = make([]colexec.ExpressionExecutor, len(ap.SortDirections))
	for i := range ctr.sortVectors {
		ctr.sortExprExecutor[i], err = colexec.NewExpressionExecutor(proc, ap.SortDirections[i].Expr)
		if err != nil {
			return err
		}
	}

	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	ctr := arg.(*Argument).ctr

	bat := proc.InputBatch()
	if bat == nil {
		return true, ctr.sortAndSend(proc)
	}

	enoughToSend, err := ctr.appendBatch(proc, bat)
	if err != nil {
		return false, err
	}
	if enoughToSend {
		return false, ctr.sortAndSend(proc)
	}

	proc.SetInputBatch(batch.EmptyBatch)
	return false, nil
}
