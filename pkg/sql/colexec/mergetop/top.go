// Copyright 2021 - 2024 Matrix Origin
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

package mergetop

import (
	"bytes"
	"container/heap"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "merge_top"

func (mergeTop *MergeTop) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": mergetop([")
	for i, f := range mergeTop.Fs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	fmt.Fprintf(buf, "], %v)", mergeTop.Limit)
}

func (mergeTop *MergeTop) OpType() vm.OpType {
	return vm.MergeTop
}

func (mergeTop *MergeTop) Prepare(proc *process.Process) (err error) {
	if mergeTop.OpAnalyzer == nil {
		mergeTop.OpAnalyzer = process.NewAnalyzer(mergeTop.GetIdx(), mergeTop.IsFirst, mergeTop.IsLast, "mergetop")
	} else {
		mergeTop.OpAnalyzer.Reset()
	}

	// limit executor
	if mergeTop.ctr.limitExecutor == nil {
		mergeTop.ctr.limitExecutor, err = colexec.NewExpressionExecutor(proc, mergeTop.Limit)
		if err != nil {
			return err
		}
	}
	vec, err := mergeTop.ctr.limitExecutor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return err
	}
	mergeTop.ctr.limit = vector.MustFixedColWithTypeCheck[uint64](vec)[0]
	if mergeTop.ctr.limit > 1024 {
		mergeTop.ctr.sels = make([]int64, 0, 1024)
	} else {
		mergeTop.ctr.sels = make([]int64, 0, mergeTop.ctr.limit)
	}
	mergeTop.ctr.poses = make([]int32, 0, len(mergeTop.Fs))

	// executor for order list
	if len(mergeTop.ctr.executorsForOrderList) != len(mergeTop.Fs) {
		mergeTop.ctr.executorsForOrderList = make([]colexec.ExpressionExecutor, len(mergeTop.Fs))
		for i := range mergeTop.ctr.executorsForOrderList {
			mergeTop.ctr.executorsForOrderList[i], err = colexec.NewExpressionExecutor(proc, mergeTop.Fs[i].Expr)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (mergeTop *MergeTop) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	analyzer := mergeTop.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	result := vm.NewCallResult()
	if mergeTop.ctr.limit == 0 {
		result.Batch = nil
		result.Status = vm.ExecStop
		return result, nil
	}

	if end, err := mergeTop.ctr.build(mergeTop, proc, analyzer); err != nil {
		return result, err
	} else if end {
		result.Status = vm.ExecStop
		analyzer.Output(result.Batch)
		return result, nil
	}

	if mergeTop.ctr.bat == nil || mergeTop.ctr.bat.IsEmpty() {
		result.Batch = nil
		result.Status = vm.ExecStop
		return result, nil
	}
	err := mergeTop.ctr.eval(mergeTop.ctr.limit, proc, analyzer, &result)
	if err == nil {
		result.Status = vm.ExecStop
		analyzer.Output(result.Batch)
		return result, nil
	}
	analyzer.Output(result.Batch)
	return result, err
}

func (ctr *container) build(ap *MergeTop, proc *process.Process, analyzer process.Analyzer) (bool, error) {
	if ctr.bat != nil {
		ctr.bat.CleanOnlyData()
	}
	for {
		result, err := vm.ChildrenCall(ap.GetChildren(0), proc, analyzer)
		if err != nil {
			return true, err
		}
		if result.Batch == nil {
			return false, nil
		}

		bat, err := result.Batch.Dup(proc.GetMPool())
		if err != nil {
			return true, err
		}
		analyzer.Alloc(int64(bat.Size()))
		defer bat.Clean(proc.Mp())

		ctr.n = len(bat.Vecs)
		ctr.poses = ctr.poses[:0]
		for i := range ctr.executorsForOrderList {
			if ctr.executorsForOrderList[i].IsColumnExpr() {
				colIndex := ctr.executorsForOrderList[i].(*colexec.ColumnExpressionExecutor).GetColIndex()
				ctr.poses = append(ctr.poses, int32(colIndex))
			} else {
				vec, err := ctr.executorsForOrderList[i].EvalWithoutResultReusing(proc, []*batch.Batch{bat}, nil)
				if err != nil {
					return false, err
				}
				ctr.poses = append(ctr.poses, int32(len(bat.Vecs)))
				bat.Vecs = append(bat.Vecs, vec)
				analyzer.Alloc(int64(vec.Size()))
			}
		}

		if len(ctr.cmps) == 0 {
			mp := make(map[int]int, len(ctr.poses))
			for i, pos := range ctr.poses {
				mp[int(pos)] = i
			}

			if ctr.bat == nil {
				ctr.bat = batch.NewWithSize(len(bat.Vecs))
				for i, vec := range bat.Vecs {
					ctr.bat.Vecs[i] = vector.NewVec(*vec.GetType())
				}
			}

			for i := 0; i < len(bat.Vecs); i++ {
				var desc, nullsLast bool
				if pos, ok := mp[i]; ok {
					desc = ap.Fs[pos].Flag&plan.OrderBySpec_DESC != 0
					if ap.Fs[pos].Flag&plan.OrderBySpec_NULLS_FIRST != 0 {
						nullsLast = false
					} else if ap.Fs[pos].Flag&plan.OrderBySpec_NULLS_LAST != 0 {
						nullsLast = true
					} else {
						nullsLast = desc
					}
				}
				ctr.cmps = append(
					ctr.cmps,
					compare.New(*bat.Vecs[i].GetType(), desc, nullsLast),
				)
			}

		}

		if err := ctr.processBatch(ap.ctr.limit, bat, proc); err != nil {
			return false, err
		}
	}
}

func (ctr *container) processBatch(limit uint64, bat *batch.Batch, proc *process.Process) error {
	var start int64

	length := int64(bat.RowCount())
	if n := uint64(len(ctr.sels)); n < limit {
		start = int64(limit - n)
		if start > length {
			start = length
		}
		for i := int64(0); i < start; i++ {
			for j, vec := range ctr.bat.Vecs {
				if err := vec.UnionOne(bat.Vecs[j], i, proc.Mp()); err != nil {
					return err
				}
			}
			ctr.sels = append(ctr.sels, int64(n))
			n++
		}
		ctr.bat.AddRowCount(bat.RowCount())
		if n == limit {
			ctr.sort()
		}
	}
	if start == length {
		return nil
	}

	// bat is still have items
	for i, cmp := range ctr.cmps {
		cmp.Set(1, bat.Vecs[i])
	}
	for i, j := start, length; i < j; i++ {
		if ctr.compare(1, 0, i, ctr.sels[0]) < 0 {
			for _, cmp := range ctr.cmps {
				if err := cmp.Copy(1, 0, i, ctr.sels[0], proc); err != nil {
					return err
				}
			}
			heap.Fix(ctr, 0)
		}
	}
	return nil
}

func (ctr *container) eval(limit uint64, proc *process.Process, analyzer process.Analyzer, result *vm.CallResult) error {
	if uint64(len(ctr.sels)) < limit {
		ctr.sort()
	}
	for i, cmp := range ctr.cmps {
		ctr.bat.Vecs[i] = cmp.Vector()
	}
	sels := make([]int64, len(ctr.sels))
	for i, j := 0, len(ctr.sels); i < j; i++ {
		sels[len(sels)-1-i] = heap.Pop(ctr).(int64)
	}
	if err := ctr.bat.Shuffle(sels, proc.Mp()); err != nil {
		return err
	}
	for i := ctr.n; i < len(ctr.bat.Vecs); i++ {
		ctr.bat.Vecs[i].Free(proc.Mp())
	}
	ctr.bat.Vecs = ctr.bat.Vecs[:ctr.n]
	result.Batch = ctr.bat
	return nil
}

// do sort work for heap, and result order will be set in container.sels
func (ctr *container) sort() {
	for i, cmp := range ctr.cmps {
		cmp.Set(0, ctr.bat.Vecs[i])
	}
	heap.Init(ctr)
}
