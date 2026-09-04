// Copyright 2026 Matrix Origin
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
	"math"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/partitionhash"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type hashContainer struct {
	state vm.CtrState

	partitionEval colexec.ExprEvalVector
	hash          group.ResHashRelated
	isStrHash     bool
	keyNullable   bool

	retained        *batch.Batch
	groupIDs        []uint64
	groupBoundaries []int64
	output          *batch.Batch
	outputGroup     int
	fallbackToSort  bool
	spillThreshold  int64
	observedMemory  int64
	// materializing is the replacement batch while a final selection is copied
	// in bounded chunks. It is never published until the whole permutation has
	// completed, so cancellation leaves retained in its original order.
	materializing *batch.Batch
	// scratchMemory is live mpool-owned finalization workspace that is not
	// retained by a batch. accountMemory records its overlapping peak together
	// with the retained and materializing batches.
	scratchMemory int64
}

func (partition *Partition) prepareHash(proc *process.Process) (err error) {
	if partition.hash == nil {
		partition.hash = &hashContainer{}
	}
	ctr := partition.hash
	ctr.state = vm.Build
	ctr.outputGroup = 0
	ctr.fallbackToSort = false
	ctr.spillThreshold = colexec.ResolveSpillThreshold(partition.SpillMem)
	ctr.keyNullable = false
	ctr.isStrHash = false

	exprs := make([]*plan.Expr, len(partition.OrderBySpecs))
	keyWidth := int32(0)
	for i, spec := range partition.OrderBySpecs {
		if spec == nil || spec.Expr == nil || spec.Expr.GetCol() == nil ||
			!partitionhash.Compatible(types.T(spec.Expr.Typ.Id)) ||
			types.T(spec.Expr.Typ.Id) == types.T_tuple {
			return moerr.NewInternalErrorNoCtx("invalid hash window partition key")
		}
		exprs[i] = spec.Expr
		ctr.keyNullable = ctr.keyNullable || !spec.Expr.Typ.NotNullable
	}
	if len(exprs) == 0 {
		return moerr.NewInternalErrorNoCtx("hash window partition requires a key")
	}
	for _, expr := range exprs {
		keyWidth += int32(group.GetKeyWidth(types.T(expr.Typ.Id), expr.Typ.Width, ctr.keyNullable))
	}
	ctr.isStrHash = keyWidth > 8

	if len(ctr.partitionEval.Executor) == 0 {
		ctr.partitionEval, err = colexec.MakeEvalVector(proc, exprs)
		if err != nil {
			return err
		}
	} else {
		ctr.partitionEval.ResetForNextQuery()
	}
	return ctr.hash.BuildHashTable(
		proc.Ctx, proc.Mp(), false, ctr.isStrHash, ctr.keyNullable,
		false, 1024, nil, nil,
	)
}

func (partition *Partition) callHash(proc *process.Process) (vm.CallResult, error) {
	ctr := partition.hash
	ctr.cleanOutput(proc)
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
				if err = ctr.finalize(proc, partition.OpAnalyzer, partition.OrderBySpecs); err != nil {
					return result, err
				}
				ctr.state = vm.Eval
				break
			}
			if result.Batch.IsEmpty() {
				continue
			}
			if err = ctr.consume(proc, partition.OpAnalyzer, result.Batch); err != nil {
				return result, err
			}
		}
	}

	result := vm.NewCallResult()
	if ctr.state != vm.Eval || ctr.outputGroup >= len(ctr.groupBoundaries) {
		ctr.state = vm.End
		result.Status = vm.ExecStop
		return result, nil
	}
	start := 0
	if ctr.outputGroup > 0 {
		start = int(ctr.groupBoundaries[ctr.outputGroup-1])
	}
	end := int(ctr.groupBoundaries[ctr.outputGroup])
	output, err := ctr.retained.Window(start, end)
	if err != nil {
		return result, err
	}
	ctr.output = output
	ctr.outputGroup++
	result.Batch = output
	return result, nil
}

func (ctr *hashContainer) consume(proc *process.Process, analyzer process.Analyzer, input *batch.Batch) error {
	var err error
	// A sort fallback still owns the buffered input and then needs selection and
	// materialization workspace.  Once the hash path has crossed its admission
	// threshold, accepting another over-budget batch would turn that one-way
	// fallback into an unbounded coordinator buffer.  Preserve the input already
	// accepted for the exact fallback, but fail the query before retaining any
	// additional batch that exceeds the same resource contract.
	if ctr.fallbackToSort && ctr.fallbackWouldExceedBudget(input) {
		return moerr.NewOOM(proc.Ctx)
	}
	ctr.retained, err = ctr.retained.AppendWithCopy(proc.Ctx, proc.Mp(), input)
	if err != nil {
		return err
	}
	ctr.accountMemory(analyzer)
	if ctr.fallbackToSort {
		return nil
	}

	inputs := []*batch.Batch{input}
	for i := range ctr.partitionEval.Executor {
		vec, err := ctr.partitionEval.Executor[i].Eval(proc, inputs, nil)
		if err != nil {
			return err
		}
		ctr.partitionEval.Vec[i] = vec
	}
	if !ctr.keyNullable && hashPartitionKeysHaveGrouping(ctr.partitionEval.Vec) {
		// GROUPING sentinels compare as NULL even when the declared key type is
		// non-nullable. A non-nullable hash table cannot encode that extra value
		// domain, so preserve the legacy comparator semantics via Sort.
		ctr.fallbackToSort = true
		ctr.hash.Free0()
		ctr.freeGroupIDs(proc.Mp())
		ctr.accountMemory(analyzer)
		return nil
	}
	hashKeys, normalizedKeys, err := normalizeHashPartitionKeys(
		ctr.partitionEval.Vec, input.RowCount(), proc.Mp(),
	)
	if err != nil {
		return err
	}
	defer freeNormalizedHashPartitionKeys(normalizedKeys, proc.Mp())
	for start := 0; start < input.RowCount(); start += hashmap.UnitLimit {
		if err, canceled := vm.CancelCheck(proc); canceled {
			return err
		}
		count := min(hashmap.UnitLimit, input.RowCount()-start)
		groupIDs, _, err := ctr.hash.TxnItr.Insert(start, count, hashKeys)
		if err != nil {
			return err
		}
		oldLength := len(ctr.groupIDs)
		nextGroupIDs, err := growHashPartitionSlice(ctr.groupIDs, oldLength+count, proc.Mp())
		if err != nil {
			return err
		}
		ctr.groupIDs = nextGroupIDs
		copy(ctr.groupIDs[oldLength:], groupIDs[:count])
		memory := int64(ctr.retained.Size()) + ctr.hash.Hash.Size() +
			int64(cap(ctr.groupIDs))*int64(unsafe.Sizeof(uint64(0))) +
			int64(ctr.retained.RowCount())*int64(unsafe.Sizeof(int64(0))) +
			int64(ctr.hash.Hash.GroupCount())*2*int64(unsafe.Sizeof(int64(0)))
		if colexec.ShouldSpill(memory, int64(ctr.retained.RowCount()), ctr.spillThreshold) {
			ctr.fallbackToSort = true
			ctr.hash.Free0()
			ctr.freeGroupIDs(proc.Mp())
			ctr.accountMemory(analyzer)
			break
		}
		ctr.accountMemory(analyzer)
	}
	return nil
}

func (ctr *hashContainer) fallbackWouldExceedBudget(input *batch.Batch) bool {
	retainedSize := int64(0)
	retainedRows := int64(0)
	if ctr.retained != nil {
		retainedSize = int64(ctr.retained.Size())
		retainedRows = int64(ctr.retained.RowCount())
	}
	inputSize := int64(input.Size())
	inputRows := int64(input.RowCount())
	if inputSize < 0 || retainedSize > math.MaxInt64-inputSize ||
		inputRows < 0 || retainedRows > math.MaxInt64-inputRows {
		return true
	}
	return colexec.ShouldSpill(retainedSize+inputSize, retainedRows+inputRows, ctr.spillThreshold)
}

func hashPartitionKeysHaveGrouping(keys []*vector.Vector) bool {
	for _, key := range keys {
		if key != nil && key.HasGrouping() {
			return true
		}
	}
	return false
}

func normalizeHashPartitionKeys(
	keys []*vector.Vector,
	rows int,
	mp *mpool.MPool,
) ([]*vector.Vector, []*vector.Vector, error) {
	var normalized []*vector.Vector
	owned := make([]*vector.Vector, 0, len(keys))
	for i, key := range keys {
		if key == nil || !key.HasGrouping() {
			continue
		}
		if normalized == nil {
			normalized = append([]*vector.Vector(nil), keys...)
		}
		view, err := key.WindowByLogicalRows(0, rows)
		if err != nil {
			freeNormalizedHashPartitionKeys(owned, mp)
			return nil, nil, err
		}
		view.GetNulls().Or(view.GetGrouping())
		view.SetGrouping(nil)
		normalized[i] = view
		owned = append(owned, view)
	}
	if normalized == nil {
		return keys, nil, nil
	}
	return normalized, owned, nil
}

func freeNormalizedHashPartitionKeys(keys []*vector.Vector, mp *mpool.MPool) {
	for _, key := range keys {
		key.Free(mp)
	}
}

func (ctr *hashContainer) finalize(proc *process.Process, analyzer process.Analyzer, specs []*plan.OrderBySpec) error {
	if ctr.retained == nil || ctr.retained.RowCount() == 0 {
		return nil
	}
	if ctr.fallbackToSort {
		return ctr.finalizeSortFallback(proc, analyzer, specs)
	}
	groupCount := int(ctr.hash.Hash.GroupCount())
	if groupCount == 0 || len(ctr.groupIDs) != ctr.retained.RowCount() {
		return moerr.NewInternalErrorNoCtx("invalid hash window partition cardinality")
	}
	positions, err := mpool.MakeSlice[int64](groupCount, proc.Mp(), true)
	if err != nil {
		return err
	}
	defer mpool.FreeSlice(proc.Mp(), positions)
	positionsMemory := int64(cap(positions)) * int64(unsafe.Sizeof(int64(0)))
	ctr.scratchMemory += positionsMemory
	defer func() { ctr.scratchMemory -= positionsMemory }()
	for i, groupID := range ctr.groupIDs {
		if err := checkCanceled(proc, i); err != nil {
			return err
		}
		if groupID == 0 || groupID > uint64(groupCount) {
			return moerr.NewInternalErrorNoCtx("invalid hash window partition group")
		}
		positions[groupID-1]++
	}
	ctr.groupBoundaries, err = mpool.MakeSlice[int64](groupCount, proc.Mp(), true)
	if err != nil {
		return err
	}
	ctr.accountMemory(analyzer)
	total := int64(0)
	for i, count := range positions {
		positions[i] = total
		total += count
		ctr.groupBoundaries[i] = total
	}
	selections, err := mpool.MakeSlice[int64](len(ctr.groupIDs), proc.Mp(), true)
	if err != nil {
		return err
	}
	defer mpool.FreeSlice(proc.Mp(), selections)
	selectionsMemory := int64(cap(selections)) * int64(unsafe.Sizeof(int64(0)))
	ctr.scratchMemory += selectionsMemory
	defer func() { ctr.scratchMemory -= selectionsMemory }()
	ctr.accountMemory(analyzer)
	for row, groupID := range ctr.groupIDs {
		if err := checkCanceled(proc, row); err != nil {
			return err
		}
		group := int(groupID - 1)
		selections[int(positions[group])] = int64(row)
		positions[group]++
	}
	if err := ctr.shuffleRetained(proc, analyzer, selections); err != nil {
		return err
	}
	ctr.freeGroupIDs(proc.Mp())
	ctr.hash.Free0()
	return nil
}

func (ctr *hashContainer) finalizeSortFallback(proc *process.Process, analyzer process.Analyzer, specs []*plan.OrderBySpec) error {
	inputs := []*batch.Batch{ctr.retained}
	for i := range ctr.partitionEval.Executor {
		vec, err := ctr.partitionEval.Executor[i].Eval(proc, inputs, nil)
		if err != nil {
			return err
		}
		ctr.partitionEval.Vec[i] = vec
	}
	compares := make([]compare.Compare, len(specs))
	for i, spec := range specs {
		desc := spec.Flag&plan.OrderBySpec_DESC != 0
		nullsLast := desc
		if spec.Flag&plan.OrderBySpec_NULLS_FIRST != 0 {
			nullsLast = false
		} else if spec.Flag&plan.OrderBySpec_NULLS_LAST != 0 {
			nullsLast = true
		}
		typ := types.NewWithCharset(types.T(spec.Expr.Typ.Id), spec.Expr.Typ.Width, spec.Expr.Typ.Scale, uint8(spec.Expr.Typ.Charset))
		compares[i] = compare.New(typ, desc, nullsLast)
		if compares[i] == nil {
			return moerr.NewInternalErrorNoCtx("unsupported sort fallback partition key")
		}
		compares[i].Set(0, ctr.partitionEval.Vec[i])
		compares[i].Set(1, ctr.partitionEval.Vec[i])
	}
	selections, err := mpool.MakeSlice[int64](ctr.retained.RowCount(), proc.Mp(), true)
	if err != nil {
		return err
	}
	defer mpool.FreeSlice(proc.Mp(), selections)
	selectionsMemory := int64(cap(selections)) * int64(unsafe.Sizeof(int64(0)))
	ctr.scratchMemory += selectionsMemory
	defer func() { ctr.scratchMemory -= selectionsMemory }()
	ctr.accountMemory(analyzer)
	for i := range selections {
		selections[i] = int64(i)
	}
	if err := stableSortPartitionSelections(proc, analyzer, selections, compares); err != nil {
		return err
	}
	groupCount := 1
	for i := 1; i < len(selections); i++ {
		if err := checkCanceled(proc, i); err != nil {
			return err
		}
		same := true
		for _, cmp := range compares {
			if cmp.Compare(0, 1, selections[i-1], selections[i]) != 0 {
				same = false
				break
			}
		}
		if !same {
			groupCount++
		}
	}
	ctr.groupBoundaries, err = mpool.MakeSlice[int64](groupCount, proc.Mp(), true)
	if err != nil {
		return err
	}
	ctr.accountMemory(analyzer)
	boundary := 0
	for i := 1; i < len(selections); i++ {
		if err := checkCanceled(proc, i); err != nil {
			return err
		}
		for _, cmp := range compares {
			if cmp.Compare(0, 1, selections[i-1], selections[i]) != 0 {
				ctr.groupBoundaries[boundary] = int64(i)
				boundary++
				break
			}
		}
	}
	ctr.groupBoundaries[boundary] = int64(len(selections))
	return ctr.shuffleRetained(proc, analyzer, selections)
}

// shuffleRetained materializes a selected copy in bounded units instead of
// calling Batch.Shuffle. Batch.Shuffle and Vector.Shuffle make a whole-vector
// selection copy and have no cancellation hook. This operator can retain a
// large, wide input, so cancellation must be observed during final copying too.
func (ctr *hashContainer) shuffleRetained(proc *process.Process, analyzer process.Analyzer, selections []int64) (err error) {
	attrs, attrTypes := ctr.retained.GetSchema()
	materializing := batch.NewWithSchema(ctr.retained.HasAllocationAccount(), attrs, attrTypes)
	if selection := ctr.retained.AllocationAccountSelection(); selection != nil {
		if err = materializing.SetAllocationAccount(selection); err != nil {
			materializing.Clean(proc.Mp())
			return err
		}
	} else {
		for i, vec := range ctr.retained.Vecs {
			if selection := vec.AllocationAccountSelection(); selection != nil {
				if err = materializing.Vecs[i].SetAllocationAccount(selection); err != nil {
					materializing.Clean(proc.Mp())
					return err
				}
			}
		}
	}
	ctr.materializing = materializing
	ctr.accountMemory(analyzer)
	defer func() {
		if err != nil {
			materializing.Clean(proc.Mp())
		}
		ctr.materializing = nil
	}()

	for start := 0; start < len(selections); start += cancellationCheckInterval {
		if err = checkCanceled(proc, start); err != nil {
			return err
		}
		end := min(start+cancellationCheckInterval, len(selections))
		if err = materializing.Union(ctr.retained, selections[start:end], proc.Mp()); err != nil {
			return err
		}
		ctr.accountMemory(analyzer)
	}

	retained := ctr.retained
	ctr.retained = materializing
	ctr.materializing = nil
	retained.Clean(proc.Mp())
	ctr.accountMemory(analyzer)
	return nil
}

// stableSortPartitionSelections keeps the fallback equivalent to sort.SliceStable
// while polling cancellation between bounded merge runs. A global fallback sort
// can be large precisely when estimates were wrong, so cancellation must not
// wait for the entire O(N log N) comparison phase to finish.
func stableSortPartitionSelections(proc *process.Process, analyzer process.Analyzer, selections []int64, compares []compare.Compare) error {
	if err, canceled := vm.CancelCheck(proc); canceled {
		return err
	}
	if len(selections) < 2 {
		return nil
	}
	scratch, err := mpool.MakeSlice[int64](len(selections), proc.Mp(), true)
	if err != nil {
		return err
	}
	defer mpool.FreeSlice(proc.Mp(), scratch)
	analyzer.Alloc(int64(cap(scratch)) * int64(unsafe.Sizeof(int64(0))))

	src, dst := selections, scratch
	sourceIsSelections := true
	for width := 1; width < len(selections); {
		for left := 0; left < len(selections); {
			if err := checkCanceled(proc, left); err != nil {
				return err
			}
			mid := left + width
			if mid >= len(selections) {
				if _, err := copyPartitionSelections(proc, dst[left:], src[left:], left); err != nil {
					return err
				}
				break
			}
			right := mid + width
			if right < mid || right > len(selections) {
				right = len(selections)
			}
			i, j, out := left, mid, left
			for i < mid && j < right {
				if err := checkCanceled(proc, out); err != nil {
					return err
				}
				if partitionSelectionLess(compares, src[j], src[i]) {
					dst[out] = src[j]
					j++
				} else {
					dst[out] = src[i]
					i++
				}
				out++
			}
			copied, err := copyPartitionSelections(proc, dst[out:], src[i:mid], out)
			if err != nil {
				return err
			}
			out += copied
			if _, err = copyPartitionSelections(proc, dst[out:], src[j:right], out); err != nil {
				return err
			}
			left = right
		}
		src, dst = dst, src
		sourceIsSelections = !sourceIsSelections
		if width > len(selections)/2 {
			width = len(selections)
		} else {
			width *= 2
		}
	}
	if !sourceIsSelections {
		if _, err := copyPartitionSelections(proc, selections, src, 0); err != nil {
			return err
		}
	}
	return nil
}

// copyPartitionSelections bounds the cancellation latency of the merge tail.
// A run can have an arbitrarily large remaining side after the other side is
// exhausted, so a single bulk copy would otherwise bypass the merge loop's
// cancellation checkpoints.
func copyPartitionSelections(proc *process.Process, dst, src []int64, iteration int) (int, error) {
	copied := 0
	for copied < len(src) {
		if err := checkCanceled(proc, iteration+copied); err != nil {
			return copied, err
		}
		next := min(copied+cancellationCheckInterval, len(src))
		copy(dst[copied:next], src[copied:next])
		copied = next
	}
	return copied, nil
}

func partitionSelectionLess(compares []compare.Compare, left, right int64) bool {
	for _, cmp := range compares {
		result := cmp.Compare(0, 1, left, right)
		if result != 0 {
			return result < 0
		}
	}
	return false
}

func (ctr *hashContainer) accountMemory(analyzer process.Analyzer) {
	current := int64(0)
	if ctr.retained != nil {
		current += int64(ctr.retained.Size())
	}
	if ctr.materializing != nil {
		current += int64(ctr.materializing.Size())
	}
	if ctr.hash.Hash != nil {
		current += ctr.hash.Hash.Size()
	}
	current += int64(cap(ctr.groupIDs)) * int64(unsafe.Sizeof(uint64(0)))
	current += int64(cap(ctr.groupBoundaries)) * int64(unsafe.Sizeof(int64(0)))
	current += ctr.scratchMemory
	if current > ctr.observedMemory {
		analyzer.Alloc(current - ctr.observedMemory)
	}
	ctr.observedMemory = current
}

func growHashPartitionSlice[T any](values []T, required int, mp *mpool.MPool) ([]T, error) {
	if required < 0 {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	if required <= cap(values) {
		return values[:required], nil
	}
	var value T
	elementSize := uint64(unsafe.Sizeof(value))
	if elementSize == 0 || uint64(required) > uint64(math.MaxInt64)/elementSize {
		return nil, mpool.ErrAllocationAllocatorLimit
	}
	oldBytes := int64(uint64(cap(values)) * elementSize)
	requiredBytes := int64(uint64(required) * elementSize)
	nextBytes, ok := mpool.GrowCapacity(oldBytes, requiredBytes)
	if !ok || nextBytes < requiredBytes {
		return nil, mpool.ErrAllocationAllocatorLimit
	}
	capacity := int((uint64(nextBytes) + elementSize - 1) / elementSize)
	next, err := mpool.MakeSlice[T](capacity, mp, true)
	if err != nil {
		return nil, err
	}
	copy(next, values)
	if cap(values) != 0 {
		mpool.FreeSlice(mp, values)
	}
	return next[:required], nil
}

func (ctr *hashContainer) freeGroupIDs(mp *mpool.MPool) {
	if cap(ctr.groupIDs) != 0 {
		mpool.FreeSlice(mp, ctr.groupIDs)
	}
	ctr.groupIDs = nil
}

func (ctr *hashContainer) freeGroupBoundaries(mp *mpool.MPool) {
	if cap(ctr.groupBoundaries) != 0 {
		mpool.FreeSlice(mp, ctr.groupBoundaries)
	}
	ctr.groupBoundaries = nil
}

func (ctr *hashContainer) cleanOutput(proc *process.Process) {
	if ctr.output != nil {
		ctr.output.Clean(proc.Mp())
		ctr.output = nil
	}
}

func (ctr *hashContainer) reset(proc *process.Process) {
	ctr.cleanOutput(proc)
	if ctr.retained != nil {
		ctr.retained.Clean(proc.Mp())
		ctr.retained = nil
	}
	if ctr.materializing != nil {
		ctr.materializing.Clean(proc.Mp())
		ctr.materializing = nil
	}
	ctr.hash.Free0()
	ctr.partitionEval.ResetForNextQuery()
	ctr.freeGroupIDs(proc.Mp())
	ctr.freeGroupBoundaries(proc.Mp())
	ctr.outputGroup = 0
	ctr.fallbackToSort = false
	ctr.observedMemory = 0
	ctr.scratchMemory = 0
	ctr.keyNullable = false
	ctr.isStrHash = false
	ctr.state = vm.Build
}

func (ctr *hashContainer) free(proc *process.Process) {
	ctr.reset(proc)
	ctr.partitionEval.Free()
}
