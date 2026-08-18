// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package partition

import (
	"container/heap"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type topNContainer struct {
	state vm.CtrState
	limit uint64

	limitExecutor colexec.ExpressionExecutor
	partitionEval colexec.ExprEvalVector
	orderEval     colexec.ExprEvalVector
	hash          group.ResHashRelated
	isStrHash     bool
	keyNullable   bool

	retained     *batch.Batch
	retainedKeys *batch.Batch
	retainedDead []int
	keyDead      []int
	compares     []compare.Compare
	groups       []*partitionHeap
	output       *batch.Batch
	outputGroup  int
	outputSlots  []int64
	outputOffset int
}

const minTopNVarlenCompactBytes = 64 << 10

type partitionHeap struct {
	ctr   *topNContainer
	slots []int64
}

func (h partitionHeap) Len() int { return len(h.slots) }
func (h partitionHeap) Less(i, j int) bool {
	// The worst retained row is the root, so a better incoming row can replace it.
	return h.ctr.compareRetained(h.slots[i], h.slots[j]) > 0
}
func (h partitionHeap) Swap(i, j int)   { h.slots[i], h.slots[j] = h.slots[j], h.slots[i] }
func (h *partitionHeap) Push(value any) { h.slots = append(h.slots, value.(int64)) }
func (h *partitionHeap) Pop() any {
	last := len(h.slots) - 1
	value := h.slots[last]
	h.slots = h.slots[:last]
	return value
}

func (partition *Partition) prepareTopN(proc *process.Process) (err error) {
	if partition.PartitionByCount <= 0 || int(partition.PartitionByCount) >= len(partition.OrderBySpecs) {
		return moerr.NewInternalErrorNoCtx("invalid bounded partition key layout")
	}
	if partition.top == nil {
		partition.top = &topNContainer{}
	}
	ctr := partition.top
	ctr.state = vm.Build
	ctr.outputGroup = 0
	ctr.outputOffset = 0

	if ctr.limitExecutor == nil {
		ctr.limitExecutor, err = colexec.NewExpressionExecutor(proc, partition.Limit)
		if err != nil {
			return err
		}
	} else {
		ctr.limitExecutor.ResetForNextQuery()
	}
	vec, err := ctr.limitExecutor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return err
	}
	ctr.limit = vector.MustFixedColWithTypeCheck[uint64](vec)[0]

	partitionExprs := make([]*plan.Expr, partition.PartitionByCount)
	for i := range partitionExprs {
		partitionExprs[i] = partition.OrderBySpecs[i].Expr
	}
	orderSpecs := partition.OrderBySpecs[partition.PartitionByCount:]
	orderExprs := make([]*plan.Expr, len(orderSpecs))
	for i := range orderExprs {
		orderExprs[i] = orderSpecs[i].Expr
	}

	if len(ctr.partitionEval.Executor) == 0 {
		ctr.partitionEval, err = colexec.MakeEvalVector(proc, partitionExprs)
		if err != nil {
			return err
		}
	} else {
		ctr.partitionEval.ResetForNextQuery()
	}
	if len(ctr.orderEval.Executor) == 0 {
		ctr.orderEval, err = colexec.MakeEvalVector(proc, orderExprs)
		if err != nil {
			return err
		}
	} else {
		ctr.orderEval.ResetForNextQuery()
	}

	keyWidth := int32(0)
	for _, expr := range partitionExprs {
		if expr.Typ.Id == int32(types.T_tuple) {
			return moerr.NewInternalErrorNoCtx("tuple is not supported as partition key")
		}
		ctr.keyNullable = ctr.keyNullable || !expr.Typ.NotNullable
	}
	for _, expr := range partitionExprs {
		keyWidth += int32(group.GetKeyWidth(types.T(expr.Typ.Id), expr.Typ.Width, ctr.keyNullable))
	}
	ctr.isStrHash = keyWidth > 8
	if err = ctr.hash.BuildHashTable(
		proc.Ctx, proc.Mp(), false, ctr.isStrHash, ctr.keyNullable,
		false, 1024, nil, nil,
	); err != nil {
		return err
	}

	ctr.compares = make([]compare.Compare, len(orderSpecs))
	for i, spec := range orderSpecs {
		desc := spec.Flag&plan.OrderBySpec_DESC != 0
		nullsLast := desc
		if spec.Flag&plan.OrderBySpec_NULLS_FIRST != 0 {
			nullsLast = false
		} else if spec.Flag&plan.OrderBySpec_NULLS_LAST != 0 {
			nullsLast = true
		}
		typ := types.NewWithCharset(types.T(spec.Expr.Typ.Id), spec.Expr.Typ.Width, spec.Expr.Typ.Scale, uint8(spec.Expr.Typ.Charset))
		// Top-N order keys must use the same total order as the window sorter.
		// In particular, native float comparison is not a strict weak order for
		// NaNs and can make the heap discard rows from the SQL-order prefix.
		ctr.compares[i] = compare.NewOrder(typ, desc, nullsLast)
	}
	return nil
}

func (partition *Partition) callTopN(proc *process.Process) (vm.CallResult, error) {
	ctr := partition.top
	if ctr.state == vm.Build {
		for {
			result, err := vm.ChildrenCall(partition.GetChildren(0), proc, partition.OpAnalyzer)
			if err != nil {
				return result, err
			}
			if result.Batch == nil {
				if err, canceled := vm.CancelCheck(proc); canceled {
					return vm.CancelResult, err
				}
				ctr.state = vm.Eval
				if partition.PreReduce {
					ctr.preparePreReduceOutput()
				}
				break
			}
			if result.Batch.IsEmpty() {
				continue
			}
			if err = ctr.consume(proc, result.Batch); err != nil {
				return result, err
			}
		}
	}

	result := vm.NewCallResult()
	if ctr.state == vm.Eval {
		if partition.PreReduce {
			if ctr.outputOffset >= len(ctr.outputSlots) {
				ctr.state = vm.End
				result.Status = vm.ExecStop
				return result, nil
			}
			end := min(ctr.outputOffset+colexec.DefaultBatchSize, len(ctr.outputSlots))
			if err := ctr.emitRows(proc, &result, ctr.outputSlots[ctr.outputOffset:end]); err != nil {
				return result, err
			}
			ctr.outputOffset = end
			return result, nil
		}
		if ctr.outputGroup >= len(ctr.groups) {
			ctr.state = vm.End
			result.Status = vm.ExecStop
			return result, nil
		}
		if err := ctr.emitGroup(proc, &result); err != nil {
			return result, err
		}
		return result, nil
	}
	result.Status = vm.ExecStop
	return result, nil
}

func (ctr *topNContainer) consume(proc *process.Process, input *batch.Batch) error {
	inputs := []*batch.Batch{input}
	for i := range ctr.partitionEval.Executor {
		vec, err := ctr.partitionEval.Executor[i].Eval(proc, inputs, nil)
		if err != nil {
			return err
		}
		ctr.partitionEval.Vec[i] = vec
	}
	for i := range ctr.orderEval.Executor {
		vec, err := ctr.orderEval.Executor[i].Eval(proc, inputs, nil)
		if err != nil {
			return err
		}
		ctr.orderEval.Vec[i] = vec
	}
	// Keep expression-error behavior identical to the generic path even when
	// the proven upper bound is zero, but do not retain or hash any row.
	if ctr.limit == 0 {
		return nil
	}
	if ctr.retained == nil {
		ctr.retained = batch.NewWithSize(len(input.Vecs))
		for i, vec := range input.Vecs {
			ctr.retained.Vecs[i] = vector.NewVec(*vec.GetType())
		}
		ctr.retainedKeys = batch.NewWithSize(len(ctr.orderEval.Vec))
		for i, vec := range ctr.orderEval.Vec {
			ctr.retainedKeys.Vecs[i] = vector.NewVec(*vec.GetType())
			ctr.compares[i].Set(0, ctr.retainedKeys.Vecs[i])
		}
	}
	for i, cmp := range ctr.compares {
		cmp.Set(1, ctr.orderEval.Vec[i])
	}

	for start := 0; start < input.RowCount(); start += hashmap.UnitLimit {
		if err, canceled := vm.CancelCheck(proc); canceled {
			return err
		}
		count := min(hashmap.UnitLimit, input.RowCount()-start)
		groupIDs, _, err := ctr.hash.TxnItr.Insert(start, count, ctr.partitionEval.Vec)
		if err != nil {
			return err
		}
		for offset, groupID := range groupIDs[:count] {
			for len(ctr.groups) < int(groupID) {
				ctr.groups = append(ctr.groups, &partitionHeap{ctr: ctr})
			}
			row := int64(start + offset)
			groupHeap := ctr.groups[groupID-1]
			if uint64(groupHeap.Len()) < ctr.limit {
				slot, err := ctr.appendRow(proc, input, row)
				if err != nil {
					return err
				}
				heap.Push(groupHeap, slot)
			} else if ctr.compareInput(row, groupHeap.slots[0]) < 0 {
				if err := ctr.replaceRow(proc, input, row, groupHeap.slots[0]); err != nil {
					return err
				}
				heap.Fix(groupHeap, 0)
			}
		}
	}
	return nil
}

func (ctr *topNContainer) appendRow(proc *process.Process, input *batch.Batch, row int64) (int64, error) {
	slot := int64(ctr.retained.RowCount())
	for i := range ctr.retained.Vecs {
		if err := ctr.retained.Vecs[i].UnionOne(input.Vecs[i], row, proc.Mp()); err != nil {
			return 0, err
		}
	}
	for i := range ctr.retainedKeys.Vecs {
		if err := ctr.retainedKeys.Vecs[i].UnionOne(ctr.orderEval.Vec[i], row, proc.Mp()); err != nil {
			return 0, err
		}
		// Compare caches the fixed-column slice header. UnionOne may reallocate
		// the vector, so refresh the retained side before heap comparisons.
		ctr.compares[i].Set(0, ctr.retainedKeys.Vecs[i])
	}
	ctr.retained.AddRowCount(1)
	ctr.retainedKeys.AddRowCount(1)
	return slot, nil
}

func (ctr *topNContainer) replaceRow(proc *process.Process, input *batch.Batch, row, slot int64) error {
	for i := range ctr.retained.Vecs {
		dead := replacedVarlenBytes(ctr.retained.Vecs[i], slot)
		if err := ctr.retained.Vecs[i].Copy(input.Vecs[i], slot, row, proc.Mp()); err != nil {
			return err
		}
		ctr.retainedDead = growIntSlice(ctr.retainedDead, len(ctr.retained.Vecs))
		ctr.retainedDead[i] += dead
		if err := compactTopNVector(proc, ctr.retained, i, ctr.retainedDead); err != nil {
			return err
		}
	}
	for i := range ctr.retainedKeys.Vecs {
		dead := replacedVarlenBytes(ctr.retainedKeys.Vecs[i], slot)
		if err := ctr.retainedKeys.Vecs[i].Copy(ctr.orderEval.Vec[i], slot, row, proc.Mp()); err != nil {
			return err
		}
		ctr.keyDead = growIntSlice(ctr.keyDead, len(ctr.retainedKeys.Vecs))
		ctr.keyDead[i] += dead
		if err := compactTopNVector(proc, ctr.retainedKeys, i, ctr.keyDead); err != nil {
			return err
		}
		ctr.compares[i].Set(0, ctr.retainedKeys.Vecs[i])
	}
	return nil
}

func growIntSlice(values []int, length int) []int {
	if len(values) < length {
		values = append(values, make([]int, length-len(values))...)
	}
	return values
}

func replacedVarlenBytes(vec *vector.Vector, row int64) int {
	if !vec.GetType().IsVarlen() || vec.IsNull(uint64(row)) {
		return 0
	}
	length := len(vec.GetBytesAt(int(row)))
	if length <= types.VarlenaInlineSize {
		return 0
	}
	return length
}

func compactTopNVector(proc *process.Process, bat *batch.Batch, index int, deadBytes []int) error {
	vec := bat.Vecs[index]
	if deadBytes[index] < minTopNVarlenCompactBytes || deadBytes[index]*2 < len(vec.GetArea()) {
		return nil
	}
	compact, err := vec.CloneToFlatCompact(proc.Mp())
	if err != nil {
		return err
	}
	vec.Free(proc.Mp())
	bat.Vecs[index] = compact
	deadBytes[index] = 0
	return nil
}

func (ctr *topNContainer) compareInput(inputRow, retainedRow int64) int {
	for _, cmp := range ctr.compares {
		if result := cmp.Compare(1, 0, inputRow, retainedRow); result != 0 {
			return result
		}
	}
	return 0
}

func (ctr *topNContainer) compareRetained(left, right int64) int {
	for _, cmp := range ctr.compares {
		if result := cmp.Compare(0, 0, left, right); result != 0 {
			return result
		}
	}
	return 0
}

func (ctr *topNContainer) emitGroup(proc *process.Process, result *vm.CallResult) error {
	if err, canceled := vm.CancelCheck(proc); canceled {
		return err
	}
	slots := append([]int64(nil), ctr.groups[ctr.outputGroup].slots...)
	ctr.sortSlots(slots)
	ctr.outputGroup++
	return ctr.emitRows(proc, result, slots)
}

func (ctr *topNContainer) preparePreReduceOutput() {
	ctr.outputSlots = ctr.outputSlots[:0]
	for _, groupHeap := range ctr.groups {
		slots := append([]int64(nil), groupHeap.slots...)
		ctr.sortSlots(slots)
		ctr.outputSlots = append(ctr.outputSlots, slots...)
	}
}

func (ctr *topNContainer) sortSlots(slots []int64) {
	sort.Slice(slots, func(i, j int) bool {
		comparison := ctr.compareRetained(slots[i], slots[j])
		if comparison == 0 {
			return slots[i] < slots[j]
		}
		return comparison < 0
	})
}

func (ctr *topNContainer) emitRows(proc *process.Process, result *vm.CallResult, slots []int64) error {
	if err, canceled := vm.CancelCheck(proc); canceled {
		return err
	}
	if ctr.output == nil {
		ctr.output = batch.NewOffHeapWithSize(len(ctr.retained.Vecs))
		for i, vec := range ctr.retained.Vecs {
			ctr.output.Vecs[i] = vector.NewOffHeapVecWithType(*vec.GetType())
		}
	} else {
		ctr.output.CleanOnlyData()
	}

	for _, slot := range slots {
		for i := range ctr.output.Vecs {
			if err := ctr.output.Vecs[i].UnionOne(ctr.retained.Vecs[i], slot, proc.Mp()); err != nil {
				return err
			}
		}
	}
	ctr.output.SetRowCount(len(slots))
	result.Batch = ctr.output
	return nil
}

func (ctr *topNContainer) reset(proc *process.Process) {
	ctr.state = vm.Build
	ctr.outputGroup = 0
	ctr.outputOffset = 0
	ctr.outputSlots = nil
	ctr.limit = 0
	ctr.keyNullable = false
	ctr.isStrHash = false
	ctr.hash.Free0()
	ctr.groups = nil
	ctr.compares = nil
	ctr.retainedDead = nil
	ctr.keyDead = nil
	ctr.partitionEval.ResetForNextQuery()
	ctr.orderEval.ResetForNextQuery()
	if ctr.limitExecutor != nil {
		ctr.limitExecutor.ResetForNextQuery()
	}
	if ctr.retained != nil {
		ctr.retained.Clean(proc.Mp())
		ctr.retained = nil
	}
	if ctr.retainedKeys != nil {
		ctr.retainedKeys.Clean(proc.Mp())
		ctr.retainedKeys = nil
	}
	if ctr.output != nil {
		ctr.output.CleanOnlyData()
	}
}

func (ctr *topNContainer) free(proc *process.Process) {
	ctr.reset(proc)
	ctr.partitionEval.Free()
	ctr.orderEval.Free()
	if ctr.limitExecutor != nil {
		ctr.limitExecutor.Free()
		ctr.limitExecutor = nil
	}
	if ctr.output != nil {
		ctr.output.Clean(proc.Mp())
		ctr.output = nil
	}
}
