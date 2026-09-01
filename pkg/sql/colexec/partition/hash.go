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
	"sort"
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
				if err = ctr.finalize(proc, partition.OrderBySpecs); err != nil {
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
	before := 0
	if ctr.retained != nil {
		before = ctr.retained.Size()
	}
	var err error
	ctr.retained, err = ctr.retained.AppendWithCopy(proc.Ctx, proc.Mp(), input)
	if err != nil {
		return err
	}
	analyzer.Alloc(int64(ctr.retained.Size() - before))
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
			break
		}
	}
	return nil
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
	var owned []*vector.Vector
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

func (ctr *hashContainer) finalize(proc *process.Process, specs []*plan.OrderBySpec) error {
	if ctr.retained == nil || ctr.retained.RowCount() == 0 {
		return nil
	}
	if ctr.fallbackToSort {
		return ctr.finalizeSortFallback(proc, specs)
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
	for row, groupID := range ctr.groupIDs {
		if err := checkCanceled(proc, row); err != nil {
			return err
		}
		group := int(groupID - 1)
		selections[int(positions[group])] = int64(row)
		positions[group]++
	}
	if err := ctr.retained.Shuffle(selections, proc.Mp()); err != nil {
		return err
	}
	ctr.freeGroupIDs(proc.Mp())
	ctr.hash.Free0()
	return nil
}

func (ctr *hashContainer) finalizeSortFallback(proc *process.Process, specs []*plan.OrderBySpec) error {
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
	for i := range selections {
		selections[i] = int64(i)
	}
	sort.SliceStable(selections, func(i, j int) bool {
		for _, cmp := range compares {
			result := cmp.Compare(0, 1, selections[i], selections[j])
			if result != 0 {
				return result < 0
			}
		}
		return false
	})
	if err, canceled := vm.CancelCheck(proc); canceled {
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
	return ctr.retained.Shuffle(selections, proc.Mp())
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
	ctr.hash.Free0()
	ctr.partitionEval.ResetForNextQuery()
	ctr.freeGroupIDs(proc.Mp())
	ctr.freeGroupBoundaries(proc.Mp())
	ctr.outputGroup = 0
	ctr.fallbackToSort = false
	ctr.keyNullable = false
	ctr.isStrHash = false
	ctr.state = vm.Build
}

func (ctr *hashContainer) free(proc *process.Process) {
	ctr.reset(proc)
	ctr.partitionEval.Free()
}
