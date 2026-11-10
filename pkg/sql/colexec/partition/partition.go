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

package partition

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/partitionhash"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "partition"

const cancellationCheckInterval = 1024

func checkCanceled(proc *process.Process, iteration int) error {
	if iteration&(cancellationCheckInterval-1) != 0 {
		return nil
	}
	if err, canceled := vm.CancelCheck(proc); canceled {
		return err
	}
	return nil
}

func (partition *Partition) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": partition([")
	for i, f := range partition.OrderBySpecs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	buf.WriteString("])")
}

func (partition *Partition) OpType() vm.OpType {
	return vm.Partition
}

func (partition *Partition) Prepare(proc *process.Process) (err error) {
	if partition.OpAnalyzer == nil {
		partition.OpAnalyzer = process.NewAnalyzer(partition.GetIdx(), partition.IsFirst, partition.IsLast, "partition")
	} else {
		partition.OpAnalyzer.Reset()
	}
	if partition.Limit != nil {
		return partition.prepareTopN(proc)
	}
	if partition.Algorithm == plan2.Node_PARTITION_ALGORITHM_HASH {
		return partition.prepareHash(proc)
	}

	if len(partition.ctr.executors) > 0 {
		return nil
	}

	ctr := &partition.ctr
	ctr.batchList = make([]*batch.Batch, 0, 16)
	ctr.orderCols = make([][]*vector.Vector, 0, 16)

	// Literal keys are equal across rows and batches; omit them from both
	// execution and comparison without changing the operator's plan specs.
	specs := nonLiteralOrderSpecs(partition.OrderBySpecs)
	partition.ctr.executors = make([]colexec.ExpressionExecutor, len(specs))
	for i := range partition.ctr.executors {
		partition.ctr.executors[i], err = colexec.NewExpressionExecutor(proc, specs[i].Expr)
		if err != nil {
			return err
		}
	}
	ctr.generateCompares(specs)
	return nil
}

// nonLiteralOrderSpecs keeps evaluator and comparator positions aligned.
// Literals are equal across batches; runtime constant columns need not be.
func nonLiteralOrderSpecs(specs []*plan2.OrderBySpec) []*plan2.OrderBySpec {
	keys := make([]*plan2.OrderBySpec, 0, len(specs))
	for _, spec := range specs {
		if spec.Expr.GetLit() == nil {
			keys = append(keys, spec)
		}
	}
	return keys
}

func (partition *Partition) Call(proc *process.Process) (vm.CallResult, error) {
	if partition.Limit != nil {
		return partition.callTopN(proc)
	}
	if partition.Algorithm == plan2.Node_PARTITION_ALGORITHM_HASH {
		return partition.callHash(proc)
	}
	analyzer := partition.OpAnalyzer

	ctr := &partition.ctr
	for {
		switch ctr.status {
		case receive:
			result, err := vm.ChildrenCall(partition.GetChildren(0), proc, analyzer)
			if err != nil {
				return result, err
			}
			if result.Status == vm.ExecYield {
				return result, nil
			}
			if result.Batch == nil {
				ctr.indexList = make([]int64, len(ctr.batchList))
				ctr.status = eval
			} else {
				if result.Batch.IsEmpty() {
					continue
				}
				if len(ctr.batchList) > ctr.i {
					if ctr.batchList[ctr.i] != nil {
						ctr.batchList[ctr.i].CleanOnlyData()
					}
					ctr.batchList[ctr.i], err = ctr.batchList[ctr.i].AppendWithCopy(proc.Ctx, proc.Mp(), result.Batch)
					if err != nil {
						return result, err
					}
				} else {
					appBat, err := result.Batch.Dup(proc.Mp())
					if err != nil {
						return result, err
					}
					analyzer.Alloc(int64(appBat.Size()))
					ctr.batchList = append(ctr.batchList, appBat)
				}

				if err = ctr.evaluateOrderColumn(proc, ctr.i); err != nil {
					return result, err
				}
				ctr.i++
			}

		case eval:
			result := vm.NewCallResult()
			if len(ctr.batchList) == 0 {
				result.Status = vm.ExecStop
				return result, nil
			}

			ok, err := ctr.pickAndSend(proc, &result)
			if ok {
				result.Status = vm.ExecStop
				return result, err
			}
			return result, err

		}
	}
}

func (ctr *container) evaluateOrderColumn(proc *process.Process, index int) error {
	inputs := []*batch.Batch{ctr.batchList[index]}

	if len(ctr.orderCols) < index+1 {
		ctr.orderCols = append(ctr.orderCols, make([]*vector.Vector, len(ctr.executors)))
	}
	for i := 0; i < len(ctr.executors); i++ {
		vec, err := ctr.executors[i].Eval(proc, inputs, nil)
		if err != nil {
			return err
		}
		if ctr.orderCols[index][i] != nil {
			ctr.orderCols[index][i].CleanOnlyData()
			if err = ctr.orderCols[index][i].UnionBatch(vec, 0, vec.Length(), nil, proc.Mp()); err != nil {
				return err
			}
		} else {
			ctr.orderCols[index][i], err = vec.Dup(proc.Mp())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (ctr *container) generateCompares(fs []*plan.OrderBySpec) {
	var desc, nullsLast bool

	ctr.compares = make([]compare.Compare, len(fs))
	// The legacy comparison for floats (notably NaN), JSON and arrays does
	// not provide the strict ordering needed by a heap. Keep their old path.
	ctr.useMergeHeap = true
	for i := range ctr.compares {
		desc = fs[i].Flag&plan2.OrderBySpec_DESC != 0
		if fs[i].Flag&plan2.OrderBySpec_NULLS_FIRST != 0 {
			nullsLast = false
		} else if fs[i].Flag&plan2.OrderBySpec_NULLS_LAST != 0 {
			nullsLast = true
		} else {
			nullsLast = desc
		}

		exprTyp := fs[i].Expr.Typ
		typ := types.MustTypeFromPlan(exprTyp)
		ctr.compares[i] = compare.New(typ, desc, nullsLast)
		ctr.useMergeHeap = ctr.useMergeHeap && partitionhash.Compatible(typ.Oid)
	}
}

func (ctr *container) pickAndSend(proc *process.Process, result *vm.CallResult) (sendOver bool, err error) {
	if ctr.useMergeHeap {
		return ctr.pickAndSendHeap(proc, result)
	}
	if ctr.buf != nil {
		ctr.buf.CleanOnlyData()
	} else {
		ctr.buf = batch.NewOffHeapWithSize(ctr.batchList[0].VectorCount())
		for i := range ctr.buf.Vecs {
			ctr.buf.Vecs[i] = vector.NewOffHeapVecWithType(*ctr.batchList[0].Vecs[i].GetType())
		}
	}
	mp := proc.Mp()

	wholeLength, choice := 0, 0
	hasSame := false
	var row int64
	var cols []*vector.Vector
	fromRemoveBatch := false
	for {
		if err = checkCanceled(proc, wholeLength); err != nil {
			return false, err
		}
		if wholeLength == 0 || fromRemoveBatch {
			choice = ctr.pickFirstRow()
		} else {
			if choice, hasSame = ctr.pickSameRow(row, cols, choice); !hasSame {
				break
			}
		}

		row = ctr.indexList[choice]
		cols = ctr.orderCols[choice]

		for j := range ctr.buf.Vecs {
			// here copy
			err = ctr.buf.Vecs[j].UnionOne(ctr.batchList[choice].Vecs[j], row, mp)
			if err != nil {
				return false, err
			}
		}
		wholeLength++

		ctr.indexList[choice]++
		removeHasSame := true
		if ctr.indexList[choice] == int64(ctr.batchList[choice].RowCount()) {
			removeHasSame = ctr.hasSameRow(row, cols, choice)
			ctr.removeBatch(proc, choice)
			fromRemoveBatch = true
		} else {
			fromRemoveBatch = false
		}

		if len(ctr.indexList) == 0 {
			sendOver = true
			break
		}
		if !removeHasSame {
			break
		}
	}
	ctr.buf.SetRowCount(wholeLength)
	result.Batch = ctr.buf
	return sendOver, nil
}

// compareRunRows deliberately uses the same comparison as the legacy path.
// The heap's stable run-ID tie breaker is separate from group equality.
func (ctr *container) compareRunRows(left int, leftRow int64, right int, rightRow int64) int {
	for k, cmp := range ctr.compares {
		cmp.Set(0, ctr.orderCols[left][k])
		cmp.Set(1, ctr.orderCols[right][k])
		if result := cmp.Compare(0, 1, leftRow, rightRow); result != 0 {
			return result
		}
	}
	return 0
}

func (ctr *container) lessRun(left, right int) bool {
	result := ctr.compareRunRows(left, ctr.indexList[left], right, ctr.indexList[right])
	return result < 0 || result == 0 && left < right
}

func (ctr *container) fixMergeHeap(root int) {
	for {
		child := 2*root + 1
		if child >= len(ctr.mergeHeap) {
			return
		}
		if child+1 < len(ctr.mergeHeap) && ctr.lessRun(ctr.mergeHeap[child+1], ctr.mergeHeap[child]) {
			child++
		}
		if !ctr.lessRun(ctr.mergeHeap[child], ctr.mergeHeap[root]) {
			return
		}
		ctr.mergeHeap[root], ctr.mergeHeap[child] = ctr.mergeHeap[child], ctr.mergeHeap[root]
		root = child
	}
}

func (ctr *container) initMergeHeap(proc *process.Process) error {
	ctr.mergeHeap = make([]int, 0, len(ctr.batchList))
	for i, bat := range ctr.batchList {
		if err := checkCanceled(proc, i); err != nil {
			return err
		}
		if bat == nil {
			continue
		}
		if ctr.indexList[i] >= int64(bat.RowCount()) {
			ctr.releaseRun(proc, i)
			continue
		}
		ctr.mergeHeap = append(ctr.mergeHeap, i)
	}
	for i := len(ctr.mergeHeap)/2 - 1; i >= 0; i-- {
		if err := checkCanceled(proc, len(ctr.mergeHeap)/2-1-i); err != nil {
			return err
		}
		ctr.fixMergeHeap(i)
	}
	return nil
}

// sameGroupRun bounds both comparison work and a single vector copy between
// cancellation checks. A logical group may span any number of these chunks.
func (ctr *container) sameGroupRun(run int, start int64) int {
	limit := min(int64(cancellationCheckInterval), int64(ctr.batchList[run].RowCount())-start)
	if len(ctr.compares) == 0 {
		return int(limit)
	}
	for k, cmp := range ctr.compares {
		cmp.Set(0, ctr.orderCols[run][k])
		cmp.Set(1, ctr.orderCols[run][k])
	}
	for n := int64(1); n < limit; n++ {
		for _, cmp := range ctr.compares {
			if cmp.Compare(0, 1, start, start+n) != 0 {
				return int(n)
			}
		}
	}
	return int(limit)
}

func (ctr *container) releaseRun(proc *process.Process, run int) {
	ctr.batchList[run].Clean(proc.Mp())
	ctr.batchList[run] = nil
	for _, col := range ctr.orderCols[run] {
		col.Free(proc.Mp())
	}
	ctr.orderCols[run] = nil
}

func (ctr *container) pickAndSendHeap(proc *process.Process, result *vm.CallResult) (bool, error) {
	if err := checkCanceled(proc, 0); err != nil {
		return false, err
	}
	if ctr.mergeHeap == nil {
		if err := ctr.initMergeHeap(proc); err != nil {
			return false, err
		}
	}
	if len(ctr.mergeHeap) == 0 {
		ctr.batchList = nil
		ctr.orderCols = nil
		ctr.indexList = nil
		ctr.mergeHeap = nil
		return true, nil
	}
	if ctr.buf == nil {
		first := ctr.batchList[ctr.mergeHeap[0]]
		ctr.buf = batch.NewOffHeapWithSize(first.VectorCount())
		for i := range ctr.buf.Vecs {
			ctr.buf.Vecs[i] = vector.NewOffHeapVecWithType(*first.Vecs[i].GetType())
		}
	} else {
		ctr.buf.CleanOnlyData()
	}
	rows := 0
	for {
		if err := checkCanceled(proc, 0); err != nil {
			return false, err
		}
		run := ctr.mergeHeap[0]
		start := ctr.indexList[run]
		count := ctr.sameGroupRun(run, start)
		for col := range ctr.buf.Vecs {
			if err := ctr.buf.Vecs[col].UnionBatch(ctr.batchList[run].Vecs[col], start, count, nil, proc.Mp()); err != nil {
				return false, err
			}
		}
		rows += count
		ctr.indexList[run] += int64(count)
		exhausted := ctr.indexList[run] == int64(ctr.batchList[run].RowCount())
		if exhausted {
			last := len(ctr.mergeHeap) - 1
			ctr.mergeHeap[0] = ctr.mergeHeap[last]
			ctr.mergeHeap = ctr.mergeHeap[:last]
		}
		ctr.fixMergeHeap(0)
		sameGroup := false
		if len(ctr.mergeHeap) > 0 {
			next := ctr.mergeHeap[0]
			sameGroup = ctr.compareRunRows(run, start, next, ctr.indexList[next]) == 0
		}
		// Keep the previous key alive until the next group boundary is decided.
		if exhausted {
			ctr.releaseRun(proc, run)
		}
		if !sameGroup {
			break
		}
	}
	ctr.buf.SetRowCount(rows)
	result.Batch = ctr.buf
	done := len(ctr.mergeHeap) == 0
	if done {
		ctr.batchList = nil
		ctr.orderCols = nil
		ctr.indexList = nil
		ctr.mergeHeap = nil
	}
	return done, nil
}

func (ctr *container) pickFirstRow() (batIndex int) {
	l := len(ctr.indexList)

	i, j := 0, 1
	for j < l {
		for k := 0; k < len(ctr.compares); k++ {
			ctr.compares[k].Set(0, ctr.orderCols[i][k])
			ctr.compares[k].Set(1, ctr.orderCols[j][k])
			result := ctr.compares[k].Compare(0, 1, ctr.indexList[i], ctr.indexList[j])
			if result < 0 {
				break
			} else if result > 0 {
				i = j
				break
			} else if k == len(ctr.compares)-1 {
				break
			}
		}
		j++
	}
	return i
}

func (ctr *container) pickSameRow(row int64, cols []*vector.Vector, first int) (batIndex int, hasSame bool) {
	l := len(ctr.indexList)

	// Only the previously selected batch advanced. Earlier heads were already
	// different from this group and cannot become equal without advancing.
	// A new group or removal restarts through pickFirstRow in pickAndSend.
	j := first
	for ; j < l; j++ {
		hasSame = true
		for k := 0; k < len(ctr.compares); k++ {
			ctr.compares[k].Set(0, cols[k])
			ctr.compares[k].Set(1, ctr.orderCols[j][k])
			result := ctr.compares[k].Compare(0, 1, row, ctr.indexList[j])
			if result != 0 {
				hasSame = false
				break
			}
		}
		if hasSame {
			break
		}
	}
	return j, hasSame
}

func (ctr *container) hasSameRow(row int64, cols []*vector.Vector, choice int) (hasSame bool) {
	l := len(ctr.indexList)

	for j := 0; j < l; j++ {
		if j == choice {
			continue
		}
		hasSame = true
		for k := 0; k < len(ctr.compares); k++ {
			ctr.compares[k].Set(0, cols[k])
			ctr.compares[k].Set(1, ctr.orderCols[j][k])
			result := ctr.compares[k].Compare(0, 1, row, ctr.indexList[j])
			if result != 0 {
				hasSame = false
				break
			}
		}
		if hasSame {
			break
		}
	}
	return hasSame
}

func (ctr *container) removeBatch(proc *process.Process, index int) {
	if ctr.batchList[index] != nil {
		ctr.batchList[index].Clean(proc.Mp())
	}
	ctr.batchList = append(ctr.batchList[:index], ctr.batchList[index+1:]...)
	ctr.indexList = append(ctr.indexList[:index], ctr.indexList[index+1:]...)
	for _, vec := range ctr.orderCols[index] {
		vec.Free(proc.Mp())
	}
	ctr.orderCols = append(ctr.orderCols[:index], ctr.orderCols[index+1:]...)
}
