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

package group

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func runAccountedCountGroup(
	t *testing.T,
	keyType types.T,
	spillMem int64,
) (map[string]int64, int64) {
	t.Helper()
	proc := testutil.NewProcess(t)
	defer proc.Free()

	const groups = 1024
	input := batch.NewWithSize(1)
	switch keyType {
	case types.T_int32:
		keys := make([]int32, groups*2)
		for i := range groups {
			keys[i], keys[i+groups] = int32(i), int32(i)
		}
		input.Vecs[0] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
	case types.T_varchar:
		input.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		for pass := 0; pass < 2; pass++ {
			for i := range groups {
				value := []byte("key-" + strconv.Itoa(i) + "-varlen-payload")
				require.NoError(t, vector.AppendBytes(input.Vecs[0], value, false, proc.Mp()))
			}
		}
	default:
		t.Fatalf("unsupported test key type %s", keyType)
	}
	input.SetRowCount(groups * 2)

	g := newGroupOp(
		proc,
		[]*plan.Expr{colExpr(0, keyType)},
		[]aggexec.AggFuncExecExpression{countStarAgg()},
	)
	g.SpillMem = spillMem
	g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	allocation := installGroupTestAllocation(t, g, proc, 128<<20)
	require.NoError(t, g.Prepare(proc))

	got := make(map[string]int64, groups)
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		counts := vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[1])
		for row, count := range counts {
			var key string
			if keyType == types.T_int32 {
				key = strconv.FormatInt(int64(
					vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])[row]), 10)
			} else {
				key = string(result.Batch.Vecs[0].GetBytesAt(row))
			}
			got[key] = count
		}
	}
	records := g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"]
	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
	return got, records
}

func TestAccountedMedianAcceptsProspectiveGroupsFromGroupPreflight(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	input := batch.NewWithSize(2)
	input.Vecs[0] = testutil.MakeInt32Vector(
		[]int32{1, 1, 2, 2}, nil, proc.Mp())
	input.Vecs[1] = testutil.MakeInt64Vector(
		[]int64{1, 3, 5, 7}, nil, proc.Mp())
	input.SetRowCount(4)

	median := aggexec.MakeAggFunctionExpression(
		aggexec.AggIdOfMedian,
		false,
		[]*plan.Expr{colExpr(1, types.T_int64)},
		nil,
	)
	g := newGroupOp(
		proc,
		[]*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{median},
	)
	g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))

	got := make(map[int32]float64, 2)
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		keys := vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])
		medians := vector.MustFixedColNoTypeCheck[float64](result.Batch.Vecs[1])
		for row, key := range keys {
			got[key] = medians[row]
		}
	}
	require.Equal(t, map[int32]float64{1: 2, 2: 6}, got)

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
}

type groupTestAllocation struct {
	generation *process.ExecutionResourceGeneration
	registry   *mpool.AllocationAccountRegistry
	account    *mpool.AllocationAccount
}

type shortGroupSpillWriter struct{}

type rejectAfterReadAheadController struct {
	mu             sync.Mutex
	used           uint64
	armed          bool
	seenReadAhead  bool
	rejectNext     bool
	rejectedReload bool
}

type exactGroupScratchController struct {
	limit uint64
	used  uint64
	peak  uint64
}

type rejectNextGroupAllocationController struct {
	mu         sync.Mutex
	used       uint64
	armed      bool
	rejected   bool
	rejectWhen func() bool
}

func (c *rejectNextGroupAllocationController) arm() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.armed = true
}

func (c *rejectNextGroupAllocationController) AcquireAllocationCapacity(size uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.armed || c.rejectWhen != nil && c.rejectWhen() {
		c.armed = false
		c.rejected = true
		return mpool.ErrAllocationAccountCapacity
	}
	c.used += size
	return nil
}

func (c *rejectNextGroupAllocationController) ReleaseAllocationCapacity(size uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if size > c.used {
		panic("group test allocation capacity release underflow")
	}
	c.used -= size
}

func (c *rejectNextGroupAllocationController) snapshot() (uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used, c.rejected
}

func (c *exactGroupScratchController) AcquireAllocationCapacity(size uint64) error {
	if c.used > c.limit || size > c.limit-c.used {
		return mpool.ErrAllocationAccountCapacity
	}
	c.used += size
	c.peak = max(c.peak, c.used)
	return nil
}

func (c *exactGroupScratchController) ReleaseAllocationCapacity(size uint64) {
	if size > c.used {
		panic("group scratch capacity release underflow")
	}
	c.used -= size
}

func (c *rejectAfterReadAheadController) arm() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.armed = true
}

func (c *rejectAfterReadAheadController) AcquireAllocationCapacity(size uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rejectNext {
		c.rejectNext = false
		c.rejectedReload = true
		return mpool.ErrAllocationAccountCapacity
	}
	c.used += size
	if c.armed && !c.seenReadAhead && size == spillIOBufSize {
		c.seenReadAhead = true
		c.rejectNext = true
	}
	return nil
}

func (c *rejectAfterReadAheadController) ReleaseAllocationCapacity(size uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if size > c.used {
		panic("group test allocation capacity release underflow")
	}
	c.used -= size
}

func (c *rejectAfterReadAheadController) snapshot() (uint64, bool, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used, c.seenReadAhead, c.rejectedReload
}

func (shortGroupSpillWriter) Write(value []byte) (int, error) {
	if len(value) == 0 {
		return 0, nil
	}
	return len(value) - 1, nil
}

func installGroupTestAllocation(
	t testing.TB,
	op any,
	proc *process.Process,
	limit uint64,
) groupTestAllocation {
	t.Helper()
	proc.Base.Lim.Size = int64(limit)
	generation, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<16)
	require.NoError(t, err)
	account, err := registry.OpenWithController(limit, generation)
	require.NoError(t, err)
	switch op := op.(type) {
	case *Group:
		require.NoError(t, op.ctr.setAllocationAccount(account))
	case *MergeGroup:
		require.NoError(t, op.ctr.setAllocationAccount(account))
	default:
		t.Fatalf("unsupported accounted Group test operator %T", op)
	}
	return groupTestAllocation{
		generation: generation,
		registry:   registry,
		account:    account,
	}
}

func TestGroupAllocationBindingAndRecoveryBoundaryMatrix(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.Base.Lim.Size = 64 << 20
	generation, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(2, 64)
	require.NoError(t, err)
	first, err := registry.OpenWithController(64<<20, generation)
	require.NoError(t, err)
	second, err := registry.OpenWithController(64<<20, generation)
	require.NoError(t, err)

	var nilCtr *container
	require.ErrorIs(t, nilCtr.setAllocationAccount(first), mpool.ErrAllocationAccountInvalid)
	require.NoError(t, nilCtr.clearAllocationAccount(first))
	require.NoError(t, nilCtr.releaseRecoveryCapacity(first))
	require.NoError(t, nilCtr.clearRecoveryCapacity(first))
	require.NoError(t, nilCtr.releaseFinalRecoveryCapacity())

	ctr := &container{mp: proc.Mp()}
	require.ErrorIs(t, ctr.setAllocationAccount(first), mpool.ErrAllocationAccountInvariant)
	ctr.mp = nil
	require.NoError(t, ctr.setAllocationAccount(first))
	require.NoError(t, ctr.setAllocationAccount(first))
	require.ErrorIs(t, ctr.setAllocationAccount(second), mpool.ErrAllocationAccountMismatch)
	require.ErrorIs(t, ctr.installRecoveryCapacity(), mpool.ErrAllocationAccountInvalid)
	ctr.budget = generation
	require.NoError(t, ctr.installRecoveryCapacity())
	require.NoError(t, ctr.installRecoveryCapacity())
	require.ErrorIs(t, ctr.releaseRecoveryCapacity(second), mpool.ErrAllocationAccountInvariant)
	require.ErrorIs(t, ctr.clearAllocationAccount(second), mpool.ErrAllocationAccountMismatch)
	ctr.mp = proc.Mp()
	require.ErrorIs(t, ctr.clearAllocationAccount(first), mpool.ErrAllocationAccountInvariant)
	ctr.mp = nil
	require.NoError(t, ctr.releaseRecoveryCapacity(first))
	require.NoError(t, ctr.releaseRecoveryCapacity(first))
	require.NoError(t, ctr.clearAllocationAccount(first))

	_, _, err = registry.CompleteTerminal(first)
	require.NoError(t, err)
	_, _, err = registry.CompleteTerminal(second)
	require.NoError(t, err)
	require.Zero(t, generation.Snapshot().Used)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func finalizeGroupTestAllocation(
	t testing.TB,
	op any,
	state groupTestAllocation,
) {
	t.Helper()
	switch op := op.(type) {
	case *Group:
		require.False(t, op.HasPreparedProjection())
		require.NoError(t, op.ctr.clearAllocationAccount(state.account))
	case *MergeGroup:
		require.False(t, op.HasPreparedProjection())
		require.NoError(t, op.ctr.clearAllocationAccount(state.account))
	default:
		t.Fatalf("unsupported accounted Group test operator %T", op)
	}
	require.Zero(t, state.generation.Snapshot().Used)
	snapshot, first, err := state.registry.CompleteTerminal(state.account)
	require.NoError(t, err)
	require.True(t, first)
	require.Zero(t, snapshot.Used)
	for _, owner := range snapshot.Owners {
		require.Zero(t, owner.Current)
	}
}

func TestResizeGroupScratchFailurePreservesOwnedAllocation(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	g := newGroupOp(proc, nil, nil)
	allocation := installGroupTestAllocation(t, g, proc, 1<<20)
	require.NoError(t, g.Prepare(proc))

	owned, err := resizeGroupScratch[uint64](
		&g.ctr, nil, 1, GroupAllocationSiteSpillHashCodes)
	require.NoError(t, err)
	require.Len(t, owned, 1)
	used := allocation.account.Snapshot().Used
	require.Positive(t, used)

	retained, err := resizeGroupScratch(
		&g.ctr,
		owned,
		int(allocation.account.Snapshot().Limit/uint64(types.T_uint64.TypeLen()))+1,
		GroupAllocationSiteSpillHashCodes,
	)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Len(t, retained, len(owned))
	require.Equal(t, &owned[0], &retained[0])
	require.Equal(t, used, allocation.account.Snapshot().Used)

	freeGroupScratch(&g.ctr, retained)
	g.Free(proc, true, err)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
}

func TestDiscardableGroupScratchRetriesAtExactFinalCapacity(t *testing.T) {
	const rows = 257
	mp := mpool.MustNewZero()
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<16)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	controller := &exactGroupScratchController{
		limit: uint64(rows)*groupSpillRowIDBytes - 1,
	}
	class, err := account.RegisterCapacityController(controller)
	require.NoError(t, err)
	ctr := container{
		mp:                    mp,
		allocationAccount:     account,
		recoveryCapacityClass: class,
	}

	rowIDs, err := resizeDiscardableGroupScratch[int32](
		&ctr, nil, 1, GroupAllocationSiteSpillRows)
	require.NoError(t, err)

	// Growing disposable scratch releases the old borrower before acquiring
	// its replacement. The deliberately one-byte-short floor rejects the final
	// row-id allocation, not an old+new transient overlap.
	rowIDs, err = resizeDiscardableGroupScratch(
		&ctr, rowIDs, rows, GroupAllocationSiteSpillRows)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Nil(t, rowIDs)
	require.Zero(t, controller.used)

	// Retrying with the exact final physical capacity succeeds, and terminal
	// cleanup returns both the capacity class and allocation ledger to zero.
	controller.limit++
	rowIDs, err = resizeDiscardableGroupScratch(
		&ctr, rowIDs, rows, GroupAllocationSiteSpillRows)
	require.NoError(t, err)
	require.Equal(t, controller.limit, controller.used)
	require.Equal(t, controller.limit, controller.peak)

	freeGroupScratch(&ctr, rowIDs)
	require.Zero(t, controller.used)
	require.Zero(t, account.Snapshot().Used)
	require.NoError(t, account.UnregisterCapacityController(class, controller))
	snapshot, first, err := registry.CompleteTerminal(account)
	require.NoError(t, err)
	require.True(t, first)
	require.Zero(t, snapshot.Used)
	require.Zero(t, mp.CurrNB())
}

func TestRecoveryCapacityCoverCheckMatchesExactTarget(t *testing.T) {
	require.Equal(t, uint64(7), mustGroupRecoveryAdd(t, 3, 4))
	_, err := groupRecoveryAdd(math.MaxUint64, 1)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
	require.Equal(t, uint64(12), mustGroupRecoveryMul(t, 3, 4))
	_, err = groupRecoveryMul(math.MaxUint64, 2)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)

	var nilCtr *container
	require.NoError(t, nilCtr.ensureRecoveryCapacity(1, nil))
	require.True(t, nilCtr.recoveryCapacityCovers(1))
	withoutSlot := &container{}
	require.NoError(t, withoutSlot.ensureRecoveryCapacity(1, nil))
	require.True(t, withoutSlot.recoveryCapacityCovers(1))
	inactive := &container{recoveryCapacity: process.NewExecutionRecoveryCapacitySlot()}
	require.ErrorIs(t, inactive.ensureRecoveryCapacity(-1, nil),
		process.ErrExecutionResourceInvalid)
	require.Error(t, inactive.ensureRecoveryCapacity(1, nil))
	require.False(t, inactive.recoveryCapacityCovers(1))
	_, err = inactive.recoveryCapacityTarget(-1)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
	_, err = inactive.recoveryCapacityTarget(math.MaxInt)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
	require.ErrorIs(t, inactive.ensureRecoveryCapacity(math.MaxInt, nil),
		process.ErrExecutionResourceInvalid)

	tests := []struct {
		name     string
		current  uint64
		incoming int
		expected uint64
	}{
		{name: "empty", incoming: 0},
		{name: "within one aggregate chunk", current: 17, incoming: 239},
		{name: "row-id scratch capped at one chunk", current: 10_000, incoming: 256},
		{
			name:     "exact first chunk",
			incoming: aggBatchSize,
			expected: uint64(aggBatchSize) *
				(groupSpillHashBytes + groupSpillRowIDBytes),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctr := container{recoveryCapacity: process.NewExecutionRecoveryCapacitySlot()}
			if test.current != 0 {
				mp := mpool.MustNewZero()
				hash, err := hashmap.NewIntHashMap(false, mp)
				require.NoError(t, err)
				hash.AddGroups(test.current)
				ctr.hr.Hash = hash
				t.Cleanup(func() {
					hash.Free()
					require.Zero(t, mp.CurrNB())
				})
			}
			target, err := ctr.recoveryCapacityTarget(test.incoming)
			require.NoError(t, err)
			if test.expected != 0 {
				require.Equal(t, test.expected, target)
			}
			ctr.recoveryCapacityFloor = target
			if target == 0 {
				require.False(t, ctr.recoveryCapacityCovers(test.incoming))
				return
			}
			require.True(t, ctr.recoveryCapacityCovers(test.incoming))
			ctr.recoveryCapacityFloor = target - 1
			require.False(t, ctr.recoveryCapacityCovers(test.incoming))
		})
	}

	ctr := container{recoveryCapacity: process.NewExecutionRecoveryCapacitySlot()}
	ctr.recoveryCapacityFloor = 1
	require.False(t, ctr.recoveryCapacityCovers(-1))
	mp := mpool.MustNewZero()
	hash, err := hashmap.NewIntHashMap(false, mp)
	require.NoError(t, err)
	hash.AddGroups(math.MaxUint64)
	ctr.hr.Hash = hash
	require.False(t, ctr.recoveryCapacityCovers(1))
	hash.Free()
	require.Zero(t, mp.CurrNB())
}

func mustGroupRecoveryAdd(t *testing.T, left, right uint64) uint64 {
	t.Helper()
	value, err := groupRecoveryAdd(left, right)
	require.NoError(t, err)
	return value
}

func mustGroupRecoveryMul(t *testing.T, left, right uint64) uint64 {
	t.Helper()
	value, err := groupRecoveryMul(left, right)
	require.NoError(t, err)
	return value
}

func TestOptionalSpillBufferDoesNotBorrowRecoveryFloor(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	g := newGroupOp(proc, nil, nil)
	allocation := installGroupTestAllocation(t, g, proc, 8<<20)
	require.NoError(t, g.Prepare(proc))
	require.NoError(t, g.ctr.ensureRecoveryCapacity(hashmap.UnitLimit, g.OpAnalyzer))

	reserved, borrowed := g.ctr.recoveryCapacity.Snapshot()
	require.Equal(t,
		uint64(hashmap.UnitLimit)*(groupSpillHashBytes+groupSpillRowIDBytes),
		reserved,
	)
	require.Zero(t, borrowed)
	before := allocation.account.Snapshot().Used

	buffer, err := newGroupSpillBuffer(&g.ctr, GroupAllocationSiteSpillRead)
	require.NoError(t, err)
	require.NoError(t, buffer.Resize(spillIOBufSize))
	require.Greater(t, allocation.account.Snapshot().Used, before)
	afterReserved, afterBorrowed := g.ctr.recoveryCapacity.Snapshot()
	require.Equal(t, reserved, afterReserved)
	require.Zero(t, afterBorrowed)

	buffer.Free()
	require.Equal(t, before, allocation.account.Snapshot().Used)
	g.Free(proc, false, nil)
	finalizeGroupTestAllocation(t, g, allocation)
}

func TestGroupReleasesRecoveryFloorBeforeFinalFlush(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector(
		[]int32{1, 2, 3, 4}, nil, proc.Mp())
	input.SetRowCount(4)
	defer input.Clean(proc.Mp())

	g := newGroupOp(
		proc,
		[]*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{countStarAgg()},
	)
	allocation := installGroupTestAllocation(t, g, proc, 8<<20)
	require.NoError(t, g.Prepare(proc))
	_, err := g.buildOneBatch(proc, input)
	require.NoError(t, err)
	require.NotNil(t, g.ctr.recoveryCapacity)
	reserved, borrowed := g.ctr.recoveryCapacity.Snapshot()
	require.Positive(t, reserved)
	require.Zero(t, borrowed)
	before := allocation.generation.Snapshot().Used

	result, err := g.ctr.outputOneBatchFinal(proc, g.OpAnalyzer, g.Aggs)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.NotNil(t, g.ctr.recoveryCapacity)
	afterReserved, afterBorrowed := g.ctr.recoveryCapacity.Snapshot()
	require.Zero(t, afterReserved)
	require.Zero(t, afterBorrowed)
	after := allocation.generation.Snapshot().Used
	require.Equal(t, reserved, before-after,
		"dead spill recovery headroom must not overlap final result ownership")

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
}

func TestAccountedEmptyGroupingSetRowsReleaseAtOperatorFree(t *testing.T) {
	t.Run("group", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		defer proc.Free()
		child := colexec.NewMockOperator()
		g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)},
			[]aggexec.AggFuncExecExpression{countStarAgg()})
		g.GroupingFlag = []bool{false}
		g.AppendChild(child)
		allocation := installGroupTestAllocation(t, g, proc, 8<<20)
		require.NoError(t, g.Prepare(proc))
		require.Len(t, collectBatches(t, g, proc), 1)
		require.Positive(t, allocation.account.Snapshot().Used)

		g.Free(proc, false, nil)
		child.Free(proc, false, nil)
		require.Zero(t, allocation.account.Snapshot().Used)
		finalizeGroupTestAllocation(t, g, allocation)
	})

	t.Run("merge group", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		defer proc.Free()
		child := colexec.NewMockOperator()
		merge := newMergeGroupOp(
			[]aggexec.AggFuncExecExpression{countStarAgg()})
		merge.GroupingAware = true
		merge.EmptyGroupingSetIDs = []int64{1, 2}
		merge.GroupByTypes = []types.Type{
			types.T_int32.ToType(), types.T_int64.ToType(),
		}
		merge.AppendChild(child)
		allocation := installGroupTestAllocation(t, merge, proc, 8<<20)
		require.NoError(t, merge.Prepare(proc))
		require.Len(t, collectBatches(t, merge, proc), 1)
		require.Positive(t, allocation.account.Snapshot().Used)

		merge.Free(proc, false, nil)
		child.Free(proc, false, nil)
		require.Zero(t, allocation.account.Snapshot().Used)
		finalizeGroupTestAllocation(t, merge, allocation)
	})
}

func TestResetForSpillReleasesGroupingSentinel(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	g := newGroupOp(proc, nil, nil)
	allocation := installGroupTestAllocation(t, g, proc, 8<<20)
	require.NoError(t, g.Prepare(proc))

	before := allocation.account.Snapshot().Used
	rollup, err := vector.NewRollupConstWithAllocation(
		types.T_int32.ToType(),
		hashmap.UnitLimit,
		g.ctr.mp,
		g.ctr.expressionAllocation,
	)
	require.NoError(t, err)
	g.ctr.groupingRollup = []*vector.Vector{rollup}
	require.Greater(t, allocation.account.Snapshot().Used, before)

	g.ctr.resetForSpill()
	require.Nil(t, g.ctr.groupingRollup)
	require.Equal(t, before, allocation.account.Snapshot().Used)

	g.Free(proc, false, nil)
	finalizeGroupTestAllocation(t, g, allocation)
}

func TestAccountedGroupProjectionClosesBeforePreparedAttemptTerminal(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 1}, nil, proc.Mp())
	input.SetRowCount(3)

	g := newGroupOp(
		proc,
		[]*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{countStarAgg()},
	)
	g.ProjectList = []*plan.Expr{{
		Typ: plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_Sval{Sval: "accounted-projection-result-payload"},
		}},
	}}
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	g.AppendChild(child)
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))
	require.True(t, g.HasPreparedProjection())
	require.Positive(t, allocation.account.Snapshot().Used)

	result, err := vm.Exec(g, proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Len(t, result.Batch.Vecs, 1)
	require.Same(
		t,
		g.ctr.expressionAllocation,
		result.Batch.Vecs[0].AllocationAccountSelection(),
	)

	// Prepared execution performs Reset without Free before the statement
	// allocation attempt reaches its terminal boundary.
	g.Reset(proc, false, nil)
	require.False(t, g.HasPreparedProjection())
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	child.Free(proc, false, nil)
	input.Clean(proc.Mp())
}

func TestAccountedScalarGroupExactCapBoundary(t *testing.T) {
	run := func(limit uint64) (uint64, error) {
		proc := testutil.NewProcess(t)
		defer proc.Free()
		input := batch.NewWithSize(1)
		input.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3, 4}, nil, proc.Mp())
		input.SetRowCount(4)

		g := newGroupOp(proc, nil, []aggexec.AggFuncExecExpression{countStarAgg()})
		g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
		allocation := installGroupTestAllocation(t, g, proc, limit)
		err := g.Prepare(proc)
		if err == nil {
			for {
				var result vm.CallResult
				result, err = vm.Exec(g, proc)
				if err != nil || result.Status == vm.ExecStop {
					break
				}
			}
		}
		peak := allocation.generation.Snapshot().PeakUsed
		g.Free(proc, err != nil, err)
		require.Zero(t, allocation.account.Snapshot().Used)
		finalizeGroupTestAllocation(t, g, allocation)
		input.Clean(proc.Mp())
		return peak, err
	}

	peak, err := run(64 << 20)
	require.NoError(t, err)
	require.Positive(t, peak)

	exactPeak, err := run(peak)
	require.NoError(t, err)
	require.Equal(t, peak, exactPeak)

	_, err = run(peak - 1)
	require.Error(t, err)
	require.True(t, errors.Is(err, mpool.ErrAllocationAccountCapacity), err)
}

func TestConstVarlenaGroupKeyPreflightCopiesPayloadOnce(t *testing.T) {
	const rows = hashmap.UnitLimit
	payload := []byte(strings.Repeat("constant-group-key-", 64<<10))
	flags := make([]uint8, rows)
	for i := range flags {
		flags[i] = 1
	}

	run := func(limit uint64) (uint64, error) {
		mp := mpool.MustNewZero()
		registry, err := mpool.NewAllocationAccountRegistry(1, 512)
		require.NoError(t, err)
		account, err := registry.Open(limit)
		require.NoError(t, err)
		selection, err := vector.NewAllocationAccountSelection(
			account,
			mpool.AllocationOwnerGroup,
			GroupAllocationSiteKeyData,
			GroupAllocationSiteKeyArea,
			GroupAllocationSiteKeyNulls,
			GroupAllocationSiteKeyGrouping,
		)
		require.NoError(t, err)
		source, err := vector.NewConstBytes(
			types.T_text.ToType(), payload, rows, mp)
		require.NoError(t, err)
		destination := vector.NewOffHeapVecWithType(types.T_text.ToType())
		require.NoError(t, destination.SetAllocationAccount(selection))
		defer func() {
			destination.Free(mp)
			source.Free(mp)
			snapshot := account.Seal()
			require.Zero(t, snapshot.Used)
			_, err = registry.Finalize(account)
			require.NoError(t, err)
			require.Zero(t, mp.CurrNB())
		}()

		err = destination.PreExtendSelectedBatch(
			source, 0, rows, flags, rows, mp)
		admitted := account.Snapshot().Used
		if err == nil {
			err = destination.UnionBatchPreflighted(source, 0, rows, flags, mp)
			if err == nil {
				require.Equal(t, admitted, account.Snapshot().Used)
				require.Equal(t, len(payload), len(destination.GetArea()))
				require.Equal(t, rows, destination.Length())
			}
		}
		return account.Snapshot().Peak, err
	}

	peak, err := run(128 << 20)
	require.NoError(t, err)
	require.Positive(t, peak)
	require.Less(t, peak, uint64(4<<20),
		"a broadcast constant must not reserve one payload per selected row")
	exact, err := run(peak)
	require.NoError(t, err)
	require.Equal(t, peak, exact)
	_, err = run(peak - 1)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
}

func TestAccountedGroupForcedSpillReleasesMemoryDiskAndFD(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	const groups = 4096
	keys := make([]int32, groups*2)
	for i := range groups {
		keys[i] = int32(i)
		keys[i+groups] = int32(i)
	}
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
	input.SetRowCount(len(keys))

	g := newGroupOp(
		proc,
		[]*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{countStarAgg()},
	)
	// In test-only group-count mode, spill each 8K input wave while allowing a
	// normally distributed 1/32 partition to finish in memory after reload.
	g.SpillMem = 512
	g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))

	rows := 0
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		rows += result.Batch.RowCount()
		for _, count := range vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[1]) {
			require.Equal(t, int64(2), count)
		}
	}
	require.Equal(t, groups, rows)
	require.Positive(t, allocation.account.Snapshot().Peak)
	owner, ok := allocation.account.OwnerUsage(mpool.AllocationOwnerGroup)
	require.True(t, ok)
	require.Positive(t, owner.Peak)
	require.Positive(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])
	require.Positive(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecoveryReservedBytes"])

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	require.Zero(t, allocation.generation.Snapshot().SpillDiskUsed)
	require.Zero(t, allocation.generation.Snapshot().SpillFDUsed)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
}

func TestPreAllocateBuildChunkIncludesVectorBitmaps(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	const rows = 130
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector(make([]int32, rows), nil, proc.Mp())
	input.Vecs[0].GetNulls().Add(1)
	input.Vecs[0].GetGrouping().Add(2)
	input.SetRowCount(rows)

	g := newGroupOp(
		proc,
		[]*plan.Expr{colExpr(0, types.T_int32)},
		nil,
	)
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))
	require.NoError(t, g.ctr.buildHashTable(proc.Ctx, 0))
	inserted := append([]uint8(nil), hashmap.OneUInt8s[:rows]...)
	require.NoError(t, g.ctr.preflightBuildChunk(
		input.Vecs, 0, rows, inserted, rows))

	destination := g.ctr.groupByBatches[0].Vecs[0]
	requiredWords := (rows + 63) / 64
	require.GreaterOrEqual(t,
		destination.GetNulls().GetBitmap().ExternalStorageCapacity(),
		requiredWords,
	)
	require.GreaterOrEqual(t,
		destination.GetGrouping().GetBitmap().ExternalStorageCapacity(),
		requiredWords,
	)

	before := allocation.account.Snapshot().Used
	require.NoError(t, destination.UnionBatch(input.Vecs[0], 0, rows, nil, g.ctr.mp))
	require.Equal(t, before, allocation.account.Snapshot().Used)
	require.True(t, destination.GetNulls().Contains(1))
	require.True(t, destination.GetGrouping().Contains(2))

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
}

func TestGroupSelectedBinaryPreflightAllocatesNothingAfterHashCommit(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.T_text.ToType())
	require.NoError(t,
		vector.AppendBytes(input.Vecs[0], []byte("binary"), false, proc.Mp()))
	require.NoError(t,
		vector.AppendBytes(input.Vecs[0], []byte("text"), false, proc.Mp()))
	require.NoError(t, input.Vecs[0].SetBinaryStringRowsWithMP(
		[]bool{true, false}, proc.Mp()))
	input.SetRowCount(2)

	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_text)}, nil)
	generation, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<12)
	require.NoError(t, err)
	controller := &rejectNextGroupAllocationController{}
	account, err := registry.OpenWithController(64<<20, controller)
	require.NoError(t, err)
	require.NoError(t, g.ctr.setAllocationAccount(account))
	allocation := groupTestAllocation{
		generation: generation,
		registry:   registry,
		account:    account,
	}
	require.NoError(t, g.Prepare(proc))
	require.NoError(t, g.ctr.buildHashTable(proc.Ctx, 0))

	require.NoError(t, g.ctr.hr.TxnItr.PreviewInsert(
		0, input.RowCount(), g.ctr.hashKeyVectors(input.Vecs),
		g.ctr.hr.Hash.GroupCount(), &g.ctr.hr.insertPlan))
	preview := groupInsertPreview{
		values:    g.ctr.hr.insertPlan.Values(),
		inserted:  g.ctr.hr.insertPlan.Inserted(),
		newGroups: int(g.ctr.hr.insertPlan.NewGroups()),
	}
	require.Equal(t, 2, preview.newGroups)
	if !g.ctr.recoveryCapacityCovers(preview.newGroups) {
		require.NoError(t,
			g.ctr.ensureRecoveryCapacity(preview.newGroups, g.OpAnalyzer))
	}
	require.NoError(t, g.ctr.hr.Hash.PreAlloc(g.ctr.hr.insertPlan.NewGroups()))
	require.NoError(t, g.ctr.preflightBuildChunk(
		input.Vecs, 0, input.RowCount(), preview.inserted, preview.newGroups))
	admitted := account.Snapshot().Used

	controller.arm()
	values, added, err := g.ctr.commitGroupByChunk(
		input.Vecs, 0, input.RowCount(), preview)
	require.NoError(t, err)
	require.Equal(t, 2, added)
	require.GreaterOrEqual(t, len(values), 2)
	require.Equal(t, []uint64{1, 2}, values[:2])
	require.Equal(t, admitted, account.Snapshot().Used)
	_, rejected := controller.snapshot()
	require.False(t, rejected,
		"publication after hash commit must use only preflighted capacity")
	require.Equal(t, uint64(2), g.ctr.hr.Hash.GroupCount())
	require.Len(t, g.ctr.groupByBatches, 1)
	keys := g.ctr.groupByBatches[0].Vecs[0]
	require.True(t, keys.GetBinaryStringMetadataAt(0))
	require.False(t, keys.GetBinaryStringMetadataAt(1))

	g.Free(proc, false, nil)
	require.Zero(t, account.Snapshot().Used)
	used, _ := controller.snapshot()
	require.Zero(t, used)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
}

func TestCommitGroupByChunkClassifiesCommitPreviewErrorBeforePublication(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_text)}, nil)
	require.NoError(t, g.Prepare(proc))
	require.NoError(t, g.ctr.buildHashTable(proc.Ctx, 0))

	_, _, err := g.ctr.commitGroupByChunk(nil, 0, 0, groupInsertPreview{})
	require.Error(t, err)
	require.True(t, isGroupPrePublicationError(err))
	require.Zero(t, g.ctr.hr.Hash.GroupCount())

	g.Free(proc, false, nil)
}

func TestGroupKeySourceReservationSurvivesCompletePublication(t *testing.T) {
	tests := []struct {
		name               string
		initialGroups      int
		secondValues       []int64
		secondSources      []types.StringSource
		allowedAllocations int
	}{
		{
			name: "all-existing-reverse-order", initialGroups: 2,
			secondValues: []int64{0, 1},
			secondSources: []types.StringSource{
				types.StringSourceLiteral, types.StringSourceCOMStmt,
			},
			allowedAllocations: 1,
		},
		{
			name: "current-full-with-standby-new-group", initialGroups: aggBatchSize,
			secondValues: []int64{0, 1, aggBatchSize},
			secondSources: []types.StringSource{
				types.StringSourceLiteral,
				types.StringSourceCOMStmt,
				types.StringSourceLiteral,
			},
			allowedAllocations: 2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int64)}, nil)
			generation, err := proc.GetExecutionResourceBudget()
			require.NoError(t, err)
			registry, err := mpool.NewAllocationAccountRegistry(1, 1<<14)
			require.NoError(t, err)
			controller := &rejectNextGroupAllocationController{}
			account, err := registry.OpenWithController(128<<20, controller)
			require.NoError(t, err)
			require.NoError(t, g.ctr.setAllocationAccount(account))
			allocation := groupTestAllocation{
				generation: generation, registry: registry, account: account,
			}
			require.NoError(t, g.Prepare(proc))
			require.NoError(t, g.ctr.buildHashTable(proc.Ctx, 0))

			commit := func(input *batch.Batch) ([]uint64, int, error) {
				require.NoError(t, g.ctr.hr.TxnItr.PreviewInsert(
					0, input.RowCount(), g.ctr.hashKeyVectors(input.Vecs),
					g.ctr.hr.Hash.GroupCount(), &g.ctr.hr.insertPlan))
				preview := groupInsertPreview{
					values:    g.ctr.hr.insertPlan.Values(),
					inserted:  g.ctr.hr.insertPlan.Inserted(),
					newGroups: int(g.ctr.hr.insertPlan.NewGroups()),
				}
				if !g.ctr.recoveryCapacityCovers(preview.newGroups) {
					require.NoError(t,
						g.ctr.ensureRecoveryCapacity(preview.newGroups, g.OpAnalyzer))
				}
				require.NoError(t, g.ctr.hr.Hash.PreAlloc(g.ctr.hr.insertPlan.NewGroups()))
				require.NoError(t, g.ctr.preflightBuildChunk(
					input.Vecs, 0, input.RowCount(), preview.inserted, preview.newGroups))
				return g.ctr.commitGroupByChunk(input.Vecs, 0, input.RowCount(), preview)
			}

			initialBatches := make([]*batch.Batch, 0,
				(test.initialGroups+hashmap.UnitLimit-1)/hashmap.UnitLimit)
			initialAdded := 0
			for start := 0; start < test.initialGroups; start += hashmap.UnitLimit {
				count := min(hashmap.UnitLimit, test.initialGroups-start)
				initialValues := make([]int64, count)
				for i := range initialValues {
					initialValues[i] = int64(start + i)
				}
				initial := batch.NewWithSize(1)
				initial.Vecs[0] = testutil.MakeInt64Vector(initialValues, nil, proc.Mp())
				require.NoError(t,
					initial.Vecs[0].SetStringSource(types.StringSourceLiteral))
				initial.SetRowCount(len(initialValues))
				initialBatches = append(initialBatches, initial)
				_, added, err := commit(initial)
				require.NoError(t, err)
				initialAdded += added
			}
			require.Equal(t, test.initialGroups, initialAdded)

			second := batch.NewWithSize(1)
			second.Vecs[0] = testutil.MakeInt64Vector(test.secondValues, nil, proc.Mp())
			require.NoError(t,
				second.Vecs[0].SetStringSourcesWithMP(test.secondSources, proc.Mp()))
			second.SetRowCount(len(test.secondValues))
			require.NoError(t, g.ctr.hr.TxnItr.PreviewInsert(
				0, second.RowCount(), g.ctr.hashKeyVectors(second.Vecs),
				g.ctr.hr.Hash.GroupCount(), &g.ctr.hr.insertPlan))
			preview := groupInsertPreview{
				values:    g.ctr.hr.insertPlan.Values(),
				inserted:  g.ctr.hr.insertPlan.Inserted(),
				newGroups: int(g.ctr.hr.insertPlan.NewGroups()),
			}
			if !g.ctr.recoveryCapacityCovers(preview.newGroups) {
				require.NoError(t,
					g.ctr.ensureRecoveryCapacity(preview.newGroups, g.OpAnalyzer))
			}
			require.NoError(t, g.ctr.hr.Hash.PreAlloc(g.ctr.hr.insertPlan.NewGroups()))
			require.NoError(t, g.ctr.preflightBuildChunk(
				second.Vecs, 0, second.RowCount(), preview.inserted, preview.newGroups))
			remaining := test.allowedAllocations
			controller.rejectWhen = func() bool {
				if remaining > 0 {
					remaining--
					return false
				}
				return true
			}
			values, added, err := g.ctr.commitGroupByChunk(
				second.Vecs, 0, second.RowCount(), preview)
			require.NoError(t, err)
			require.Zero(t, remaining, "test must observe current/standby preflight allocations")
			_, rejected := controller.snapshot()
			require.False(t, rejected,
				"group-key publication must not allocate after retained preflight")
			require.Equal(t, test.initialGroups+preview.newGroups,
				int(g.ctr.hr.Hash.GroupCount()))
			require.Equal(t, preview.newGroups, added)
			require.Equal(t, preview.values, values)
			if test.initialGroups == aggBatchSize {
				require.Len(t, g.ctr.groupByBatches, 2)
			}
			keys := g.ctr.groupByBatches[0].Vecs[0]
			require.Equal(t, types.StringSourceLiteral, keys.GetStringSourceAt(0))
			require.Equal(t, types.StringSourceExpression, keys.GetStringSourceAt(1))

			controller.rejectWhen = nil
			g.Free(proc, false, nil)
			require.Zero(t, account.Snapshot().Used)
			finalizeGroupTestAllocation(t, g, allocation)
			for _, initial := range initialBatches {
				initial.Clean(proc.Mp())
			}
			second.Clean(proc.Mp())
		})
	}
}

func TestGroupSamePreviewDuplicateSourcePreflightsBeforeHashCommit(t *testing.T) {
	for _, rejectPreflight := range []bool{true, false} {
		t.Run(fmt.Sprintf("reject-preflight=%v", rejectPreflight), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			input := batch.NewWithSize(1)
			input.Vecs[0] = vector.NewVec(types.T_text.ToType())
			for _, value := range []string{"same", "same", "other"} {
				require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte(value), false, proc.Mp()))
			}
			require.NoError(t, input.Vecs[0].SetStringSourcesWithMP([]types.StringSource{
				types.StringSourceLiteral,
				types.StringSourceExpression,
				types.StringSourceLiteral,
			}, proc.Mp()))
			input.SetRowCount(3)
			inputSources := input.Vecs[0].GetStringSources()
			require.Len(t, inputSources, 3)
			inputSourceBacking := &inputSources[0]

			g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_text)}, nil)
			generation, err := proc.GetExecutionResourceBudget()
			require.NoError(t, err)
			registry, err := mpool.NewAllocationAccountRegistry(1, 1<<12)
			require.NoError(t, err)
			controller := &rejectNextGroupAllocationController{}
			account, err := registry.OpenWithController(64<<20, controller)
			require.NoError(t, err)
			require.NoError(t, g.ctr.setAllocationAccount(account))
			allocation := groupTestAllocation{
				generation: generation,
				registry:   registry,
				account:    account,
			}
			require.NoError(t, g.Prepare(proc))
			require.NoError(t, g.ctr.buildHashTable(proc.Ctx, 0))
			require.NoError(t, g.ctr.hr.TxnItr.PreviewInsert(
				0, input.RowCount(), g.ctr.hashKeyVectors(input.Vecs),
				g.ctr.hr.Hash.GroupCount(), &g.ctr.hr.insertPlan))
			preview := groupInsertPreview{
				values:    g.ctr.hr.insertPlan.Values(),
				inserted:  g.ctr.hr.insertPlan.Inserted(),
				newGroups: int(g.ctr.hr.insertPlan.NewGroups()),
			}
			require.Equal(t, []uint8{1, 0, 1}, preview.inserted)
			require.NoError(t, g.ctr.hr.Hash.PreAlloc(g.ctr.hr.insertPlan.NewGroups()))
			require.NoError(t, g.ctr.preflightBuildChunk(
				input.Vecs, 0, input.RowCount(), preview.inserted, preview.newGroups))

			if rejectPreflight {
				controller.arm()
			} else {
				controller.rejectWhen = func() bool {
					return g.ctr.hr.Hash.GroupCount() != 0
				}
			}
			values, added, commitErr := g.ctr.commitGroupByChunk(
				input.Vecs, 0, input.RowCount(), preview)
			require.Equal(t, []types.StringSource{
				types.StringSourceLiteral,
				types.StringSourceExpression,
				types.StringSourceLiteral,
			}, input.Vecs[0].GetStringSources())
			require.Same(t, inputSourceBacking, &input.Vecs[0].GetStringSources()[0],
				"group preview must not replace borrowed input sidecar ownership")
			if rejectPreflight {
				require.True(t, isGroupPrePublicationError(commitErr))
				require.Contains(t, commitErr.Error(), "allocation account capacity exceeded")
				require.ErrorIs(t, commitErr, mpool.ErrAllocationAccountCapacity)
				require.Zero(t, g.ctr.hr.Hash.GroupCount(),
					"source allocation rejection must precede hash publication")
				_, rejected := controller.snapshot()
				require.True(t, rejected)
				g.ctr.cancelGroupByPreflights()
			} else {
				require.NoError(t, commitErr)
				require.Equal(t, 2, added)
				require.Equal(t, []uint64{1, 1, 2}, values[:3])
				_, rejected := controller.snapshot()
				require.False(t, rejected,
					"hash commit and publication must use preflighted source capacity")
				controller.rejectWhen = nil
				require.Equal(t, types.StringSourceExpression,
					g.ctr.groupByBatches[0].Vecs[0].GetStringSourceAt(0))
				require.Equal(t, types.StringSourceLiteral,
					g.ctr.groupByBatches[0].Vecs[0].GetStringSourceAt(1))
			}

			g.Free(proc, false, nil)
			require.Zero(t, account.Snapshot().Used)
			finalizeGroupTestAllocation(t, g, allocation)
			input.Clean(proc.Mp())
		})
	}
}

func TestGroupExistingAndNewSourcesStayPreflightedThroughPublication(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	makeInput := func(values []string, sources []types.StringSource) *batch.Batch {
		input := batch.NewWithSize(1)
		input.Vecs[0] = vector.NewVec(types.T_text.ToType())
		for _, value := range values {
			require.NoError(t, vector.AppendBytes(
				input.Vecs[0], []byte(value), false, proc.Mp()))
		}
		require.NoError(t, input.Vecs[0].SetStringSourcesWithMP(sources, proc.Mp()))
		input.SetRowCount(len(values))
		return input
	}
	first := makeInput([]string{"a"}, []types.StringSource{types.StringSourceLiteral})
	second := makeInput(
		[]string{"a", "b"},
		[]types.StringSource{types.StringSourceExpression, types.StringSourceLiteral})

	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_text)}, nil)
	generation, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<12)
	require.NoError(t, err)
	controller := &rejectNextGroupAllocationController{}
	account, err := registry.OpenWithController(64<<20, controller)
	require.NoError(t, err)
	require.NoError(t, g.ctr.setAllocationAccount(account))
	allocation := groupTestAllocation{
		generation: generation, registry: registry, account: account,
	}
	require.NoError(t, g.Prepare(proc))
	require.NoError(t, g.ctr.buildHashTable(proc.Ctx, 0))

	commit := func(input *batch.Batch) error {
		require.NoError(t, g.ctr.hr.TxnItr.PreviewInsert(
			0, input.RowCount(), g.ctr.hashKeyVectors(input.Vecs),
			g.ctr.hr.Hash.GroupCount(), &g.ctr.hr.insertPlan))
		preview := groupInsertPreview{
			values: g.ctr.hr.insertPlan.Values(), inserted: g.ctr.hr.insertPlan.Inserted(),
			newGroups: int(g.ctr.hr.insertPlan.NewGroups()),
		}
		require.NoError(t, g.ctr.hr.Hash.PreAlloc(g.ctr.hr.insertPlan.NewGroups()))
		require.NoError(t, g.ctr.preflightBuildChunk(
			input.Vecs, 0, input.RowCount(), preview.inserted, preview.newGroups))
		_, _, err := g.ctr.commitGroupByChunk(
			input.Vecs, 0, input.RowCount(), preview)
		return err
	}
	require.NoError(t, commit(first))
	require.Equal(t, uint64(1), g.ctr.hr.Hash.GroupCount())
	controller.rejectWhen = func() bool {
		return g.ctr.hr.Hash.GroupCount() == 2
	}
	require.NoError(t, commit(second))
	_, rejected := controller.snapshot()
	require.False(t, rejected,
		"existing and new source publication must not allocate after hash commit")
	require.Equal(t, uint64(2), g.ctr.hr.Hash.GroupCount())
	require.Equal(t, []types.StringSource{
		types.StringSourceExpression, types.StringSourceLiteral,
	}, g.ctr.groupByBatches[0].Vecs[0].GetStringSources())

	controller.rejectWhen = nil
	g.Free(proc, false, nil)
	require.Zero(t, account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	first.Clean(proc.Mp())
	second.Clean(proc.Mp())
}

func TestPreAllocateBuildChunkIncludesSelectedVarlenaArea(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	values := []string{
		strings.Repeat("a", 100),
		strings.Repeat("b", 200),
		strings.Repeat("c", 300),
	}
	for _, value := range values {
		require.NoError(t, vector.AppendBytes(
			input.Vecs[0], []byte(value), false, proc.Mp()))
	}
	inputSources := []types.StringSource{
		types.StringSourceLiteral,
		types.StringSourceCOMStmt,
		types.StringSourceUserVariable,
	}
	require.NoError(t, input.Vecs[0].SetStringSourcesWithMP(inputSources, proc.Mp()))
	input.SetRowCount(len(values))

	g := newGroupOp(
		proc,
		[]*plan.Expr{colExpr(0, types.T_varchar)},
		nil,
	)
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))
	require.NoError(t, g.ctr.buildHashTable(proc.Ctx, 0))

	current, err := g.ctr.createNewGroupByBatch(input.Vecs, aggBatchSize)
	require.NoError(t, err)
	current.Vecs[0].SetLength(aggBatchSize - 1)
	current.SetRowCount(aggBatchSize - 1)
	g.ctr.groupByBatches = append(g.ctr.groupByBatches, current)

	insertedFlags := []uint8{1, 1, 1}
	require.NoError(t, g.ctr.preflightBuildChunk(
		input.Vecs, 0, input.RowCount(), insertedFlags, len(insertedFlags)))
	require.NotNil(t, g.ctr.groupByStandby)
	before := allocation.account.Snapshot().Used
	inserted, err := g.ctr.appendGroupByBatchWithStringSources(
		input.Vecs, 0, []uint8{1, 1, 1},
		[][]types.StringSource{inputSources}, 0)
	require.NoError(t, err)
	require.Equal(t, 3, inserted)
	require.Equal(t, before, allocation.account.Snapshot().Used)
	require.Len(t, g.ctr.groupByBatches, 2)
	require.Equal(t, values[0], string(
		g.ctr.groupByBatches[0].Vecs[0].GetBytesAt(aggBatchSize-1)))
	require.Equal(t, values[1], string(
		g.ctr.groupByBatches[1].Vecs[0].GetBytesAt(0)))
	require.Equal(t, values[2], string(
		g.ctr.groupByBatches[1].Vecs[0].GetBytesAt(1)))
	require.Equal(t, types.StringSourceLiteral,
		g.ctr.groupByBatches[0].Vecs[0].GetStringSourceAt(aggBatchSize-1))
	require.Equal(t, []types.StringSource{
		types.StringSourceCOMStmt,
		types.StringSourceUserVariable,
	}, g.ctr.groupByBatches[1].Vecs[0].GetStringSources())

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
}

func TestAppendDuplicateOnlyChunkDoesNotAllocateAfterHashCommit(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector([]int32{7}, nil, proc.Mp())
	input.SetRowCount(1)
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, nil)
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))

	current, err := g.ctr.createNewGroupByBatch(input.Vecs, aggBatchSize)
	require.NoError(t, err)
	current.Vecs[0].SetLength(aggBatchSize)
	current.SetRowCount(aggBatchSize)
	g.ctr.groupByBatches = append(g.ctr.groupByBatches, current)
	before := allocation.account.Snapshot().Used

	inserted, err := g.ctr.appendGroupByBatch(input.Vecs, 0, []uint8{0})
	require.NoError(t, err)
	require.Zero(t, inserted)
	require.Equal(t, before, allocation.account.Snapshot().Used)
	require.Len(t, g.ctr.groupByBatches, 1)
	require.Nil(t, g.ctr.groupByStandby)

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
}

func TestDuplicateOnlyPreflightDoesNotCreateGroupStorage(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector([]int32{7}, nil, proc.Mp())
	input.SetRowCount(1)
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, nil)
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))
	require.NoError(t, g.ctr.buildHashTable(proc.Ctx, 0))
	before := allocation.account.Snapshot().Used

	require.NoError(t, g.ctr.preflightBuildChunk(
		input.Vecs, 0, 1, []uint8{0}, 0))
	require.Equal(t, before, allocation.account.Snapshot().Used)
	require.Empty(t, g.ctr.groupByBatches)
	require.Nil(t, g.ctr.groupByStandby)

	g.Free(proc, false, nil)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
}

func TestSpillReloadRaisesRecoveryFloorForAccumulatedRecords(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	const waves = 4
	inputs := make([]*batch.Batch, 0, waves)
	for range waves {
		input := batch.NewWithSize(1)
		input.Vecs[0] = testutil.MakeInt32Vector([]int32{7}, nil, proc.Mp())
		input.SetRowCount(1)
		inputs = append(inputs, input)
	}

	g := newGroupOp(
		proc,
		[]*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{countStarAgg()},
	)
	g.SpillMem = 1 << 30
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))

	for _, input := range inputs {
		_, err := g.buildOneBatch(proc, input)
		require.NoError(t, err)
		_, rows, err := g.ctr.spillDataToDisk(proc, g.OpAnalyzer, nil)
		require.NoError(t, err)
		require.Equal(t, int64(1), rows)
		g.ctr.aggList, err = g.ctr.makeAggList(g.Aggs)
		require.NoError(t, err)
	}
	loaded, err := g.ctr.loadSpilledData(proc, g.OpAnalyzer, g.Aggs)
	require.NoError(t, err)
	require.True(t, loaded)
	require.GreaterOrEqual(t, g.ctr.recoveryCapacityFloor,
		uint64(2*(groupSpillHashBytes+groupSpillRowIDBytes)))
	result, err := g.ctr.getNextFinalResult(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, 1, result.Batch.RowCount())
	require.Equal(t, int64(waves),
		vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[1])[0])

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	for _, input := range inputs {
		input.Clean(proc.Mp())
	}
}

func TestSpillReloadRetriesWholeRecordAfterCapacityRejection(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	const groups = 4096
	keys := make([]int32, groups)
	for i := range keys {
		keys[i] = int32(i)
	}
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
	input.SetRowCount(groups)
	defer input.Clean(proc.Mp())

	g := newGroupOp(
		proc,
		[]*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{countStarAgg()},
	)
	g.SpillMem = 512
	proc.Base.Lim.Size = 128 << 20
	generation, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<16)
	require.NoError(t, err)
	controller := &rejectAfterReadAheadController{}
	account, err := registry.OpenWithController(128<<20, controller)
	require.NoError(t, err)
	require.NoError(t, g.ctr.setAllocationAccount(account))
	allocation := groupTestAllocation{
		generation: generation,
		registry:   registry,
		account:    account,
	}
	require.NoError(t, g.Prepare(proc))

	_, err = g.buildOneBatch(proc, input)
	require.NoError(t, err)
	_, spilledRows, err := g.ctr.spillDataToDisk(proc, g.OpAnalyzer, nil)
	require.NoError(t, err)
	require.Equal(t, int64(groups), spilledRows)
	require.Nil(t, g.ctr.spillHashCodes)
	require.Nil(t, g.ctr.spillFlagFlat)
	require.Nil(t, g.ctr.spillBucketRows)
	controller.arm()

	seen := make(map[int32]int64, groups)
	for {
		result, err := g.ctr.outputOneBatchFinal(proc, g.OpAnalyzer, g.Aggs)
		require.NoError(t, err)
		if result.Batch == nil {
			break
		}
		resultKeys := vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])
		resultCounts := vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[1])
		for row, key := range resultKeys {
			seen[key] = resultCounts[row]
		}
	}
	require.Len(t, seen, groups)
	for _, count := range seen {
		require.Equal(t, int64(1), count)
	}
	_, sawReadAhead, rejectedReload := controller.snapshot()
	require.True(t, sawReadAhead)
	require.True(t, rejectedReload)
	require.Positive(t,
		g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillReadAheadFallbacks"])
	require.Positive(t,
		g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillReloadRetries"])

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	used, _, _ := controller.snapshot()
	require.Zero(t, used)
	finalizeGroupTestAllocation(t, g, allocation)
}

func TestAccountedGroupResidentAndSpillMatchForH8AndHStr(t *testing.T) {
	for _, keyType := range []types.T{types.T_int32, types.T_varchar} {
		t.Run(keyType.String(), func(t *testing.T) {
			resident, residentRecords := runAccountedCountGroup(t, keyType, 1<<30)
			spilled, spillRecords := runAccountedCountGroup(t, keyType, 128)
			require.Equal(t, resident, spilled)
			require.Len(t, spilled, 1024)
			for _, count := range spilled {
				require.Equal(t, int64(2), count)
			}
			require.Zero(t, residentRecords)
			require.Positive(t, spillRecords)
		})
	}
}

func TestAccountedGroupSpillPreservesBinaryStringProvenance(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	const groups = 1024
	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.T_text.ToType())
	binaryRows := make([]bool, 0, groups*2)
	wantBinary := make(map[string]bool, groups)
	for pass := 0; pass < 2; pass++ {
		for group := 0; group < groups; group++ {
			key := "key-" + strconv.Itoa(group) + "-binary-provenance"
			require.NoError(t,
				vector.AppendBytes(input.Vecs[0], []byte(key), false, proc.Mp()))
			binary := group%2 == 0
			binaryRows = append(binaryRows, binary)
			wantBinary[key] = binary
		}
	}
	require.NoError(t,
		input.Vecs[0].SetBinaryStringRowsWithMP(binaryRows, proc.Mp()))
	input.SetRowCount(groups * 2)

	g := newGroupOp(
		proc,
		[]*plan.Expr{colExpr(0, types.T_text)},
		[]aggexec.AggFuncExecExpression{countStarAgg()},
	)
	g.SpillMem = 128
	g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	allocation := installGroupTestAllocation(t, g, proc, 128<<20)
	require.NoError(t, g.Prepare(proc))

	seen := 0
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		counts := vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[1])
		for row, count := range counts {
			key := string(result.Batch.Vecs[0].GetBytesAt(row))
			require.Equal(t, int64(2), count)
			require.Equal(t, wantBinary[key],
				result.Batch.Vecs[0].GetBinaryStringMetadataAt(row), "key=%s", key)
			seen++
		}
	}
	require.Equal(t, groups, seen)
	require.Positive(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
}

func testAllocationColumnExpr(pos int32, typ types.Type) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{
			Id:      int32(typ.Oid),
			Width:   typ.Width,
			Scale:   typ.Scale,
			Charset: uint32(typ.Charset),
		},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}},
	}
}

func makeSupportedAggregateSpillInput(
	t *testing.T,
	mp *mpool.MPool,
) (*batch.Batch, []types.Type) {
	t.Helper()
	const groups = 64
	const passes = 2
	rows := groups * passes
	decimalType := types.New(types.T_decimal64, 18, 2)
	columnTypes := []types.Type{
		types.T_int32.ToType(),
		types.T_int64.ToType(),
		types.T_varchar.ToType(),
		types.T_float64.ToType(),
		decimalType,
		types.T_int64.ToType(),
		types.T_int64.ToType(),
		types.T_char.ToType(),
	}
	keys := make([]int32, 0, rows)
	integers := make([]int64, 0, rows)
	floats := make([]float64, 0, rows)
	decimals := make([]types.Decimal64, 0, rows)
	orders := make([]int64, 0, rows)
	ties := make([]int64, 0, rows)
	texts := make([]string, 0, rows)
	for pass := range passes {
		for group := range groups {
			keys = append(keys, int32(group))
			integers = append(integers, int64(group%11+pass+1))
			floats = append(floats, float64(group%7)+float64(pass)+0.25)
			decimals = append(decimals, types.Decimal64(group*100+pass+1))
			orders = append(orders, int64(pass))
			ties = append(ties, int64(group))
			texts = append(texts, fmt.Sprintf("value-%02d-%d", group, pass))
		}
	}

	input := batch.NewWithSize(len(columnTypes))
	input.Vecs[0] = testutil.MakeInt32Vector(keys, nil, mp)
	input.Vecs[1] = vector.NewVec(columnTypes[1])
	require.NoError(t, vector.AppendFixedList(input.Vecs[1], integers, nil, mp))
	input.Vecs[2] = testutil.MakeVarcharVector(texts, nil, mp)
	input.Vecs[3] = vector.NewVec(columnTypes[3])
	require.NoError(t, vector.AppendFixedList(input.Vecs[3], floats, nil, mp))
	input.Vecs[4] = vector.NewVec(decimalType)
	require.NoError(t, vector.AppendFixedList(input.Vecs[4], decimals, nil, mp))
	input.Vecs[5] = vector.NewVec(columnTypes[5])
	require.NoError(t, vector.AppendFixedList(input.Vecs[5], orders, nil, mp))
	input.Vecs[6] = vector.NewVec(columnTypes[6])
	require.NoError(t, vector.AppendFixedList(input.Vecs[6], ties, nil, mp))
	input.Vecs[7] = vector.NewVec(columnTypes[7])
	for row, value := range integers {
		count := int64(row%3 + 1)
		sum := float64(value) * float64(count)
		payload := make([]byte, 16)
		copy(payload, types.EncodeFloat64(&sum))
		copy(payload[8:], types.EncodeInt64(&count))
		require.NoError(t, vector.AppendBytes(input.Vecs[7], payload, false, mp))
	}
	input.SetRowCount(rows)
	return input, columnTypes
}

func canonicalAggregateValue(vec *vector.Vector, row int) string {
	if vec.IsNull(uint64(row)) {
		return fmt.Sprintf("%d:null", vec.GetType().Oid)
	}
	value := vec.GetRawBytesAt(row)
	if vec.GetType().IsVarlen() {
		value = vec.GetBytesAt(row)
	}
	return fmt.Sprintf("%d:%x", vec.GetType().Oid, value)
}

func TestAccountedSupportedAggregateFamiliesResidentAndSpillMatch(t *testing.T) {
	type argument struct {
		column int32
		typ    types.Type
	}
	tests := []struct {
		name     string
		aggID    int64
		distinct bool
		args     []argument
	}{
		{name: "bit-and", aggID: aggexec.AggIdOfBitAnd, args: []argument{{1, types.T_int64.ToType()}}},
		{name: "bit-or", aggID: aggexec.AggIdOfBitOr, args: []argument{{1, types.T_int64.ToType()}}},
		{name: "bit-xor", aggID: aggexec.AggIdOfBitXor, args: []argument{{1, types.T_int64.ToType()}}},
		{name: "var-pop", aggID: aggexec.AggIdOfVarPop, args: []argument{{1, types.T_int64.ToType()}}},
		{name: "stddev-pop-distinct", aggID: aggexec.AggIdOfStdDevPop, distinct: true, args: []argument{{1, types.T_int64.ToType()}}},
		{name: "var-sample", aggID: aggexec.AggIdOfVarSample, args: []argument{{1, types.T_int64.ToType()}}},
		{name: "stddev-sample", aggID: aggexec.AggIdOfStdDevSample, args: []argument{{1, types.T_int64.ToType()}}},
		{name: "any", aggID: aggexec.AggIdOfAny, args: []argument{{2, types.T_varchar.ToType()}}},
		{name: "min", aggID: aggexec.AggIdOfMin, args: []argument{{2, types.T_varchar.ToType()}}},
		{name: "max", aggID: aggexec.AggIdOfMax, args: []argument{{1, types.T_int64.ToType()}}},
		{name: "max-by", aggID: aggexec.AggIdOfMaxBy, args: []argument{{2, types.T_varchar.ToType()}, {5, types.T_int64.ToType()}, {6, types.T_int64.ToType()}}},
		{name: "max-by-non-null", aggID: aggexec.AggIdOfMaxByNonNull, args: []argument{{2, types.T_varchar.ToType()}, {5, types.T_int64.ToType()}, {6, types.T_int64.ToType()}}},
		{name: "sum", aggID: aggexec.AggIdOfSum, args: []argument{{1, types.T_int64.ToType()}}},
		{name: "sum-distinct-decimal", aggID: aggexec.AggIdOfSum, distinct: true, args: []argument{{4, types.New(types.T_decimal64, 18, 2)}}},
		{name: "avg", aggID: aggexec.AggIdOfAvg, args: []argument{{1, types.T_int64.ToType()}}},
		{name: "avg-distinct", aggID: aggexec.AggIdOfAvg, distinct: true, args: []argument{{3, types.T_float64.ToType()}}},
		{name: "count-column", aggID: aggexec.AggIdOfCountColumn, args: []argument{{1, types.T_int64.ToType()}}},
		{name: "count-column-distinct", aggID: aggexec.AggIdOfCountColumn, distinct: true, args: []argument{{1, types.T_int64.ToType()}}},
		{name: "count-star", aggID: aggexec.AggIdOfCountStar},
		{name: "group-concat", aggID: aggexec.AggIdOfGroupConcat, args: []argument{{2, types.T_varchar.ToType()}}},
		{name: "avg-tw-cache", aggID: aggexec.AggIdOfAvgTwCache, args: []argument{{1, types.T_int64.ToType()}}},
		{name: "avg-tw-result", aggID: aggexec.AggIdOfAvgTwResult, args: []argument{{7, types.T_char.ToType()}}},
	}

	run := func(t *testing.T, tc struct {
		name     string
		aggID    int64
		distinct bool
		args     []argument
	}, spillMem int64) (map[int32]string, int64) {
		t.Helper()
		proc := testutil.NewProcess(t)
		defer proc.Free()
		input, columnTypes := makeSupportedAggregateSpillInput(t, proc.Mp())
		expressions := make([]*plan.Expr, len(tc.args))
		for i, arg := range tc.args {
			require.Equal(t, columnTypes[arg.column], arg.typ)
			expressions[i] = testAllocationColumnExpr(arg.column, arg.typ)
		}
		agg := aggexec.MakeAggFunctionExpression(
			tc.aggID, tc.distinct, expressions, nil)
		g := newGroupOp(
			proc,
			[]*plan.Expr{testAllocationColumnExpr(0, columnTypes[0])},
			[]aggexec.AggFuncExecExpression{agg},
		)
		g.SpillMem = spillMem
		g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
		allocation := installGroupTestAllocation(t, g, proc, 128<<20)
		require.NoError(t, g.Prepare(proc))

		got := make(map[int32]string, 64)
		for {
			result, err := vm.Exec(g, proc)
			require.NoError(t, err)
			if result.Status == vm.ExecStop || result.Batch == nil {
				break
			}
			keys := vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])
			for row, key := range keys {
				got[key] = canonicalAggregateValue(result.Batch.Vecs[1], row)
			}
		}
		records := g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"]
		g.Free(proc, false, nil)
		require.Zero(t, allocation.account.Snapshot().Used)
		finalizeGroupTestAllocation(t, g, allocation)
		input.Clean(proc.Mp())
		return got, records
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resident, residentRecords := run(t, tc, 1<<30)
			spilled, spillRecords := run(t, tc, 16)
			require.Len(t, resident, 64)
			require.Equal(t, resident, spilled)
			require.Zero(t, residentRecords)
			require.Positive(t, spillRecords)
		})
	}
}

func TestAccountedGroupConcatResidentAndSpillMatch(t *testing.T) {
	run := func(spillMem int64) (map[int32]string, int64) {
		proc := testutil.NewProcess(t)
		defer proc.Free()
		const groups = 64
		inputValues := []string{"first", "second"}
		keys := make([]int32, 0, groups*len(inputValues))
		values := make([]string, 0, groups*len(inputValues))
		for _, value := range inputValues {
			for group := range groups {
				keys = append(keys, int32(group))
				values = append(values, value)
			}
		}
		input := batch.NewWithSize(2)
		input.Vecs[0] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
		input.Vecs[1] = testutil.MakeVarcharVector(values, nil, proc.Mp())
		input.SetRowCount(len(keys))
		agg := aggexec.MakeAggFunctionExpression(
			aggexec.AggIdOfGroupConcat,
			false,
			[]*plan.Expr{colExpr(1, types.T_varchar)},
			nil,
		)
		g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)},
			[]aggexec.AggFuncExecExpression{agg})
		g.SpillMem = spillMem
		g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
		allocation := installGroupTestAllocation(t, g, proc, 128<<20)
		require.NoError(t, g.Prepare(proc))

		got := make(map[int32]string, groups)
		for {
			result, err := vm.Exec(g, proc)
			require.NoError(t, err)
			if result.Status == vm.ExecStop || result.Batch == nil {
				break
			}
			resultKeys := vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])
			for row, key := range resultKeys {
				got[key] = string(result.Batch.Vecs[1].GetBytesAt(row))
			}
		}
		records := g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"]
		g.Free(proc, false, nil)
		require.Zero(t, allocation.account.Snapshot().Used)
		finalizeGroupTestAllocation(t, g, allocation)
		input.Clean(proc.Mp())
		return got, records
	}

	resident, residentRecords := run(1 << 30)
	spilled, spillRecords := run(16)
	require.Equal(t, resident, spilled)
	require.Len(t, spilled, 64)
	require.Zero(t, residentRecords)
	require.Positive(t, spillRecords)
}

func TestAccountedDistinctAggregateSpillsAcrossInputWaves(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	const groups = 128
	makeInput := func() *batch.Batch {
		groupKeys := make([]int32, 0, groups*2)
		values := make([]int32, 0, groups*2)
		for group := range groups {
			groupKeys = append(groupKeys, int32(group), int32(group))
			values = append(values, 10, 20)
		}
		input := batch.NewWithSize(2)
		input.Vecs[0] = testutil.MakeInt32Vector(groupKeys, nil, proc.Mp())
		input.Vecs[1] = testutil.MakeInt32Vector(values, nil, proc.Mp())
		input.SetRowCount(len(groupKeys))
		return input
	}
	first, second := makeInput(), makeInput()
	distinctCount := aggexec.MakeAggFunctionExpression(
		aggexec.AggIdOfCountColumn,
		true,
		[]*plan.Expr{colExpr(1, types.T_int32)},
		nil,
	)
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{distinctCount})
	g.SpillMem = 64
	g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{first, second}))
	allocation := installGroupTestAllocation(t, g, proc, 128<<20)
	require.NoError(t, g.Prepare(proc))

	rows := 0
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		rows += result.Batch.RowCount()
		for _, count := range vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[1]) {
			require.Equal(t, int64(2), count)
		}
	}
	require.Equal(t, groups, rows)
	require.Positive(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	first.Clean(proc.Mp())
	second.Clean(proc.Mp())
}

func TestAccountedGroupMaxSpillDepthFinishesAdmittedLeafAndCleans(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector([]int32{7, 7}, nil, proc.Mp())
	input.SetRowCount(2)
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{countStarAgg()})
	// One resident group is deliberately still above this test-only threshold
	// after every repartition pass.
	g.SpillMem = 1
	g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))

	got := make(map[int32]int64)
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		keys := vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])
		counts := vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[1])
		for row, key := range keys {
			got[key] = counts[row]
		}
	}
	require.Equal(t, map[int32]int64{7: 2}, got)
	require.Equal(t, int64(spillMaxPass),
		g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillMaxLevel"])
	require.Positive(t,
		g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	require.Zero(t, allocation.generation.Snapshot().SpillDiskUsed)
	require.Zero(t, allocation.generation.Snapshot().SpillFDUsed)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
}

func TestAccountedGroupByteThresholdBelowResidentFloorFinishes(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	const groups = 4
	keys := make([]int64, 0, groups*2)
	for pass := 0; pass < 2; pass++ {
		for group := range groups {
			keys = append(keys, int64(group))
		}
	}
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt64Vector(keys, nil, proc.Mp())
	input.SetRowCount(len(keys))
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int64)},
		[]aggexec.AggFuncExecExpression{countStarAgg()})
	g.SpillMem = 64 << 10
	g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))

	got := make(map[int64]int64, groups)
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		resultKeys := vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[0])
		counts := vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[1])
		for row, key := range resultKeys {
			got[key] = counts[row]
		}
	}
	require.Len(t, got, groups)
	for _, count := range got {
		require.Equal(t, int64(2), count)
	}
	require.Equal(t, int64(spillMaxPass),
		g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillMaxLevel"])

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	require.Zero(t, allocation.generation.Snapshot().SpillDiskUsed)
	require.Zero(t, allocation.generation.Snapshot().SpillFDUsed)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
}

func TestAccountedGroupMaxSpillDepthPreservesCapacityError(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector([]int32{7}, nil, proc.Mp())
	input.SetRowCount(1)
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{countStarAgg()})
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))
	_, err := g.buildOneBatch(proc, input)
	require.NoError(t, err)

	retry, err := g.ctr.retrySpillReloadRecord(
		proc,
		g.OpAnalyzer,
		g.OpAnalyzer.GetOpStats(),
		&spillBucket{lv: spillMaxPass},
		&groupSpillReader{disabled: true},
		0,
		mpool.ErrAllocationAccountCapacity,
	)
	require.False(t, retry)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Nil(t, g.ctr.currentSpillBkt)

	g.Free(proc, true, err)
	require.Zero(t, allocation.account.Snapshot().Used)
	require.Zero(t, allocation.generation.Snapshot().SpillDiskUsed)
	require.Zero(t, allocation.generation.Snapshot().SpillFDUsed)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
}

func TestAccountedGroupSpillPreservesVarlenaNullAndPrepareKinds(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	const groups = 128
	input := batch.NewWithSize(2)
	input.Vecs[0] = vector.NewVec(types.T_text.ToType())
	input.Vecs[1] = vector.NewVec(types.T_text.ToType())
	for pass := 0; pass < 2; pass++ {
		for group := range groups {
			key := []byte("prepared-key-" + strconv.Itoa(group))
			require.NoError(t, vector.AppendBytes(input.Vecs[0], key, false, proc.Mp()))
			require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte("5"), false, proc.Mp()))
		}
		require.NoError(t, vector.AppendBytes(input.Vecs[0], nil, true, proc.Mp()))
		require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte("5"), false, proc.Mp()))
	}
	input.Vecs[0].SetPrepareParamKind(vector.PrepareParamDecimal)
	input.Vecs[1].SetPrepareParamKind(vector.PrepareParamFloat)
	input.SetRowCount(input.Vecs[0].Length())

	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_text)},
		[]aggexec.AggFuncExecExpression{minTextColumnAgg(1)})
	g.SpillMem = 32
	g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	allocation := installGroupTestAllocation(t, g, proc, 128<<20)
	require.NoError(t, g.Prepare(proc))

	rows, nulls := 0, 0
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		for row := 0; row < result.Batch.RowCount(); row++ {
			rows++
			if result.Batch.Vecs[0].IsNull(uint64(row)) {
				nulls++
				require.Equal(t, vector.PrepareParamNone,
					result.Batch.Vecs[0].GetPrepareParamKindAt(row))
			} else {
				require.Equal(t, vector.PrepareParamDecimal,
					result.Batch.Vecs[0].GetPrepareParamKindAt(row))
			}
			require.Equal(t, vector.PrepareParamFloat,
				result.Batch.Vecs[1].GetPrepareParamKindAt(row))
		}
	}
	require.Equal(t, groups+1, rows)
	require.Equal(t, 1, nulls)
	require.Positive(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
}

func TestAccountedGroupingSetSpillPreservesSentinelDomain(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	const groups = 128
	active := make([]int32, groups*2)
	inactivePayload := make([]int32, groups*2)
	for pass := 0; pass < 2; pass++ {
		for group := range groups {
			row := pass*groups + group
			active[row] = int32(group)
			// These ordinary values must not leak through the grouping sentinel.
			inactivePayload[row] = int32(group + 1000)
		}
	}
	input := batch.NewWithSize(2)
	input.Vecs[0] = testutil.MakeInt32Vector(active, nil, proc.Mp())
	input.Vecs[1] = testutil.MakeInt32Vector(inactivePayload, nil, proc.Mp())
	input.SetRowCount(len(active))

	g := newGroupOp(
		proc,
		[]*plan.Expr{colExpr(0, types.T_int32), colExpr(1, types.T_int32)},
		[]aggexec.AggFuncExecExpression{countStarAgg()},
	)
	g.GroupingFlag = []bool{true, false}
	g.SpillMem = 32
	g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	allocation := installGroupTestAllocation(t, g, proc, 128<<20)
	require.NoError(t, g.Prepare(proc))

	seen := make(map[int32]struct{}, groups)
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		counts := vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[2])
		keys := vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])
		for row := range result.Batch.RowCount() {
			require.True(t, result.Batch.Vecs[1].GetGrouping().Contains(uint64(row)))
			require.Equal(t, int64(2), counts[row])
			seen[keys[row]] = struct{}{}
		}
	}
	require.Len(t, seen, groups)
	require.Positive(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
}

func TestAccountedGroupSpillResourceAdmissionCleans(t *testing.T) {
	tests := []struct {
		name      string
		component process.ExecutionResourceComponent
		reserve   func(*process.ExecutionResourceGeneration) (func(), error)
	}{
		{
			name:      "disk",
			component: process.ExecutionResourceComponentSpillDisk,
			reserve: func(generation *process.ExecutionResourceGeneration) (func(), error) {
				token, err := generation.ReserveSpillDisk(generation.SpillDiskCap())
				return func() { token.Release() }, err
			},
		},
		{
			name:      "file-descriptor",
			component: process.ExecutionResourceComponentSpillFD,
			reserve: func(generation *process.ExecutionResourceGeneration) (func(), error) {
				token, err := generation.ReserveSpillFD(generation.SpillFDCap())
				return func() { token.Release() }, err
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			const groups = 128
			keys := make([]int32, groups)
			for i := range keys {
				keys[i] = int32(i)
			}
			input := batch.NewWithSize(1)
			input.Vecs[0] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
			input.SetRowCount(groups)

			g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)},
				[]aggexec.AggFuncExecExpression{countStarAgg()})
			g.SpillMem = 32
			g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
			allocation := installGroupTestAllocation(t, g, proc, 64<<20)
			releaseBlocker, err := tc.reserve(allocation.generation)
			require.NoError(t, err)
			released := false
			defer func() {
				if !released {
					releaseBlocker()
				}
			}()
			require.NoError(t, g.Prepare(proc))

			for err == nil {
				result, execErr := vm.Exec(g, proc)
				err = execErr
				if err == nil && result.Status == vm.ExecStop {
					t.Fatal("expected spill resource admission error")
				}
			}
			var resourceErr *process.ExecutionResourceError
			require.ErrorAs(t, err, &resourceErr)
			require.Equal(t, tc.component, resourceErr.Component)

			releaseBlocker()
			released = true
			g.Free(proc, true, err)
			require.Zero(t, allocation.account.Snapshot().Used)
			require.Zero(t, allocation.generation.Snapshot().SpillDiskUsed)
			require.Zero(t, allocation.generation.Snapshot().SpillFDUsed)
			finalizeGroupTestAllocation(t, g, allocation)
			input.Clean(proc.Mp())
		})
	}
}

func TestAccountedGroupCancellationResetAndReuse(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	baseContext := proc.Ctx
	ctx, cancel := context.WithCancel(baseContext)
	proc.Ctx = ctx

	first := batch.NewWithSize(1)
	first.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3, 4}, nil, proc.Mp())
	first.SetRowCount(4)
	firstChild := colexec.NewMockOperator().
		WithBatchs([]*batch.Batch{first}).
		WithBatchCallback(func(int) { cancel() })
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{countStarAgg()})
	g.SpillMem = 1
	g.AppendChild(firstChild)
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))

	result, err := vm.Exec(g, proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)
	g.Reset(proc, true, err)
	require.Zero(t, allocation.account.Snapshot().Used)
	firstChild.Free(proc, true, err)

	proc.Ctx = baseContext
	second := batch.NewWithSize(1)
	second.Vecs[0] = testutil.MakeInt32Vector([]int32{8, 5, 8, 6}, nil, proc.Mp())
	second.SetRowCount(4)
	secondChild := colexec.NewMockOperator().WithBatchs([]*batch.Batch{second})
	g.Children = nil
	g.SpillMem = 1 << 30
	g.AppendChild(secondChild)
	require.NoError(t, g.Prepare(proc))

	counts := make(map[int32]int64)
	for {
		result, execErr := vm.Exec(g, proc)
		require.NoError(t, execErr)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		keys := vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])
		values := vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[1])
		for row := range result.Batch.RowCount() {
			counts[keys[row]] = values[row]
		}
	}
	require.Equal(t, map[int32]int64{5: 1, 6: 1, 8: 2}, counts)

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	first.Clean(proc.Mp())
	second.Clean(proc.Mp())
}

func TestAccountedGroupShortSpillWriteCleans(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector([]int32{7}, nil, proc.Mp())
	input.SetRowCount(1)
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{countStarAgg()})
	g.SpillMem = 1
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))
	needSpill, err := g.buildOneBatch(proc, input)
	require.NoError(t, err)
	require.True(t, needSpill)

	hashes := make([]uint64, g.ctr.hr.Hash.GroupCount())
	hashes = g.ctr.hr.Hash.FillGroupHashes(hashes)
	g.ctr.computeBucketIndex(hashes, 1)
	require.Len(t, hashes, 1)
	bucket := int(hashes[0] & (spillNumBuckets - 1))
	g.ctr.currentSpillBkt = make([]*spillBucket, spillNumBuckets)
	for i := range g.ctr.currentSpillBkt {
		g.ctr.currentSpillBkt[i] = &spillBucket{lv: 1, name: "short-write-" + strconv.Itoa(i)}
	}
	file, err := os.CreateTemp(t.TempDir(), "group-short-write-*")
	require.NoError(t, err)
	g.ctr.currentSpillBkt[bucket].file = file
	g.ctr.currentSpillBkt[bucket].writer = shortGroupSpillWriter{}

	_, _, err = g.ctr.spillDataToDisk(proc, g.OpAnalyzer, nil)
	require.ErrorIs(t, err, io.ErrShortWrite)
	g.Free(proc, true, err)
	require.Zero(t, allocation.account.Snapshot().Used)
	require.Zero(t, allocation.generation.Snapshot().SpillDiskUsed)
	require.Zero(t, allocation.generation.Snapshot().SpillFDUsed)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
}

func TestAccountedGroupCorruptSpillRecordCleans(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	const groups = 128
	keys := make([]int32, groups)
	for i := range keys {
		keys[i] = int32(i)
	}
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
	input.SetRowCount(groups)
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{countStarAgg()})
	g.SpillMem = 32
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))
	needSpill, err := g.buildOneBatch(proc, input)
	require.NoError(t, err)
	require.True(t, needSpill)
	_, spilledRows, err := g.ctr.spillDataToDisk(proc, g.OpAnalyzer, nil)
	require.NoError(t, err)
	require.Equal(t, int64(groups), spilledRows)

	var corrupt *spillBucket
	for _, bucket := range g.ctr.currentSpillBkt {
		if bucket.cnt > 0 {
			corrupt = bucket
		}
	}
	require.NotNil(t, corrupt)
	require.NoError(t, corrupt.flushWriter())
	require.NoError(t, corrupt.file.Truncate(1))
	_, err = g.ctr.loadSpilledData(proc, g.OpAnalyzer, g.Aggs)
	require.Error(t, err)

	g.Free(proc, true, err)
	require.Zero(t, allocation.account.Snapshot().Used)
	require.Zero(t, allocation.generation.Snapshot().SpillDiskUsed)
	require.Zero(t, allocation.generation.Snapshot().SpillFDUsed)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
}

func TestAccountedGroupRetriesResidentStringSourcePreflight(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	makeInput := func(source types.StringSource) *batch.Batch {
		input := batch.NewWithSize(1)
		input.Vecs[0] = vector.NewVec(types.T_text.ToType())
		require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte("a"), false, proc.Mp()))
		require.NoError(t, input.Vecs[0].SetStringSource(source))
		input.SetRowCount(1)
		return input
	}
	first := makeInput(types.StringSourceLiteral)
	second := makeInput(types.StringSourceExpression)

	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_text)},
		[]aggexec.AggFuncExecExpression{countStarAgg()})
	g.SpillMem = 1 << 30
	generation, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<12)
	require.NoError(t, err)
	controller := &rejectNextGroupAllocationController{}
	account, err := registry.OpenWithController(64<<20, controller)
	require.NoError(t, err)
	require.NoError(t, g.ctr.setAllocationAccount(account))
	allocation := groupTestAllocation{generation: generation, registry: registry, account: account}
	g.AppendChild(colexec.NewMockOperator().
		WithBatchs([]*batch.Batch{first, second, batch.EmptyBatch}).
		WithBatchCallback(func(index int) {
			if index == 1 {
				require.Equal(t, uint64(1), g.ctr.hr.Hash.GroupCount())
				controller.arm()
			}
		}))
	require.NoError(t, g.Prepare(proc))

	var output *batch.Batch
	for {
		result, execErr := vm.Exec(g, proc)
		require.NoError(t, execErr)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		output = cloneBatch(t, proc, result.Batch)
	}
	require.NotNil(t, output)
	require.Equal(t, 1, output.RowCount())
	require.Equal(t, types.StringSourceExpression, output.Vecs[0].GetStringSourceAt(0))
	require.Equal(t, int64(2), vector.GetFixedAtNoTypeCheck[int64](output.Vecs[1], 0))
	_, rejected := controller.snapshot()
	require.True(t, rejected)
	require.Positive(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])

	output.Clean(proc.Mp())
	g.Free(proc, false, nil)
	require.Zero(t, account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	first.Clean(proc.Mp())
	second.Clean(proc.Mp())
}

func TestAccountedGroupRetriesAnyValueSourcePreflight(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	makeInput := func(keys []string, values []string, nulls []uint64, sources []types.StringSource) *batch.Batch {
		input := batch.NewWithSize(2)
		input.Vecs[0] = testutil.MakeVarcharVector(keys, nil, proc.Mp())
		input.Vecs[1] = testutil.MakeVarcharVector(values, nulls, proc.Mp())
		require.NoError(t, input.Vecs[1].SetStringSourcesWithMP(sources, proc.Mp()))
		input.SetRowCount(len(keys))
		return input
	}
	first := makeInput([]string{"a"}, []string{""}, []uint64{0},
		[]types.StringSource{types.StringSourceLiteral})
	second := makeInput([]string{"a", "b"}, []string{"winner-a", "winner-b"}, nil,
		[]types.StringSource{types.StringSourceLiteral, types.StringSourceCOMStmt})
	anyValue := aggexec.MakeAggFunctionExpression(
		aggexec.AggIdOfAny, false, []*plan.Expr{colExpr(1, types.T_varchar)}, nil)
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_varchar)},
		[]aggexec.AggFuncExecExpression{anyValue})
	g.SpillMem = 1 << 30
	generation, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<12)
	require.NoError(t, err)
	controller := &rejectNextGroupAllocationController{}
	account, err := registry.OpenWithController(64<<20, controller)
	require.NoError(t, err)
	require.NoError(t, g.ctr.setAllocationAccount(account))
	allocation := groupTestAllocation{generation: generation, registry: registry, account: account}
	g.AppendChild(colexec.NewMockOperator().
		WithBatchs([]*batch.Batch{first, second, batch.EmptyBatch}).
		WithBatchCallback(func(index int) {
			if index == 1 {
				require.Equal(t, uint64(1), g.ctr.hr.Hash.GroupCount())
				controller.arm()
			}
		}))
	require.NoError(t, g.Prepare(proc))
	seen := make(map[string]types.StringSource)
	for {
		result, execErr := vm.Exec(g, proc)
		require.NoError(t, execErr)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		for row := range result.Batch.RowCount() {
			seen[string(result.Batch.Vecs[0].GetBytesAt(row))] =
				result.Batch.Vecs[1].GetStringSourceAt(row)
		}
	}
	require.Equal(t, map[string]types.StringSource{
		"a": types.StringSourceLiteral, "b": types.StringSourceCOMStmt,
	}, seen)
	_, rejected := controller.snapshot()
	require.True(t, rejected)
	require.Positive(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])
	g.Free(proc, false, nil)
	require.Zero(t, account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	first.Clean(proc.Mp())
	second.Clean(proc.Mp())
}

func TestAccountedGroupCapacityPressureSpillsAndRetriesSameInput(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	const groupsPerWave = aggBatchSize
	makeInput := func(base int32) *batch.Batch {
		keys := make([]int32, groupsPerWave)
		for i := range keys {
			keys[i] = base + int32(i)
		}
		input := batch.NewWithSize(1)
		input.Vecs[0] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
		input.SetRowCount(len(keys))
		return input
	}
	first := makeInput(0)
	second := makeInput(groupsPerWave)

	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{countStarAgg()})
	g.SpillMem = 1 << 30
	allocation := installGroupTestAllocation(t, g, proc, 32<<20)
	var external uint64
	child := colexec.NewMockOperator().
		WithBatchs([]*batch.Batch{first, second, batch.EmptyBatch}).
		WithBatchCallback(func(index int) {
			switch index {
			case 1:
				snapshot := allocation.generation.Snapshot()
				require.Less(t, snapshot.Used, snapshot.Cap)
				external = snapshot.Cap - snapshot.Used - 1
				require.NoError(t, allocation.generation.AcquireAllocationCapacity(external))
			case 2:
				allocation.generation.ReleaseAllocationCapacity(external)
				external = 0
			}
		})
	g.AppendChild(child)
	require.NoError(t, g.Prepare(proc))
	defer func() {
		if external != 0 {
			allocation.generation.ReleaseAllocationCapacity(external)
		}
	}()

	rows := 0
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		rows += result.Batch.RowCount()
		for _, count := range vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[1]) {
			require.Equal(t, int64(1), count)
		}
	}
	require.Equal(t, groupsPerWave*2, rows)
	require.Positive(t, allocation.generation.Snapshot().RejectCount)
	require.Positive(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])

	g.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	first.Clean(proc.Mp())
	second.Clean(proc.Mp())
}

func TestAccountedGroupRetriesAggregateAreaPreflightBeforePublishingValues(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	const groups = hashmap.UnitLimit
	makeInput := func(withValues bool) *batch.Batch {
		keys := make([]int32, groups)
		values := make([]string, groups)
		nulls := make([]uint64, 0, groups)
		for row := range groups {
			keys[row] = int32(row)
			if withValues {
				values[row] = fmt.Sprintf("winner-%03d-%s", row, strings.Repeat("x", 64))
			} else {
				nulls = append(nulls, uint64(row))
			}
		}
		input := batch.NewWithSize(2)
		input.Vecs[0] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
		input.Vecs[1] = testutil.MakeVarcharVector(values, nulls, proc.Mp())
		input.SetRowCount(groups)
		return input
	}
	first, second := makeInput(false), makeInput(true)

	anyValue := aggexec.MakeAggFunctionExpression(
		aggexec.AggIdOfAny,
		false,
		[]*plan.Expr{colExpr(1, types.T_varchar)},
		nil,
	)
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{anyValue})
	g.SpillMem = 1 << 30
	generation, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<16)
	require.NoError(t, err)
	controller := &rejectNextGroupAllocationController{}
	account, err := registry.OpenWithController(128<<20, controller)
	require.NoError(t, err)
	require.NoError(t, g.ctr.setAllocationAccount(account))
	allocation := groupTestAllocation{
		generation: generation,
		registry:   registry,
		account:    account,
	}
	g.AppendChild(colexec.NewMockOperator().
		WithBatchs([]*batch.Batch{first, second, batch.EmptyBatch}).
		WithBatchCallback(func(index int) {
			if index == 1 {
				controller.arm()
			}
		}))
	require.NoError(t, g.Prepare(proc))

	seen := make(map[int32]string, groups)
	for {
		result, execErr := vm.Exec(g, proc)
		require.NoError(t, execErr)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		keys := vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])
		for row, key := range keys {
			require.False(t, result.Batch.Vecs[1].IsNull(uint64(row)))
			seen[key] = string(result.Batch.Vecs[1].GetBytesAt(row))
		}
	}
	require.Len(t, seen, groups)
	for key, value := range seen {
		require.Equal(t,
			fmt.Sprintf("winner-%03d-%s", key, strings.Repeat("x", 64)), value)
	}
	_, rejected := controller.snapshot()
	require.True(t, rejected)
	require.Positive(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])

	g.Free(proc, false, nil)
	require.Zero(t, account.Snapshot().Used)
	used, _ := controller.snapshot()
	require.Zero(t, used)
	finalizeGroupTestAllocation(t, g, allocation)
	first.Clean(proc.Mp())
	second.Clean(proc.Mp())
}

func runAccountedMergeGroupSpill(
	t *testing.T,
	groups int,
	spillMem int64,
) int64 {
	t.Helper()
	proc := testutil.NewProcess(t)
	defer proc.Free()
	makeSource := func() *batch.Batch {
		keys := make([]int32, groups)
		payloads := make([]int32, groups)
		for i := range groups {
			keys[i], payloads[i] = int32(i), int32(i*3)
		}
		source := batch.NewWithSize(2)
		source.Vecs[0] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
		source.Vecs[1] = testutil.MakeInt32Vector(payloads, nil, proc.Mp())
		source.SetRowCount(groups)
		return source
	}
	first, second := makeSource(), makeSource()
	partials := buildPartialGroupBatches(t, proc, []*batch.Batch{first, second}, false)
	first.Clean(proc.Mp())
	second.Clean(proc.Mp())

	merge := newMergeGroupOp([]aggexec.AggFuncExecExpression{countStarAgg()})
	merge.SpillMem = spillMem
	merge.AppendChild(colexec.NewMockOperator().WithBatchs(partials))
	allocation := installGroupTestAllocation(t, merge, proc, 128<<20)
	require.NoError(t, merge.Prepare(proc))
	rows := 0
	for {
		result, err := vm.Exec(merge, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		rows += result.Batch.RowCount()
		for _, count := range vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[2]) {
			require.Equal(t, int64(2), count)
		}
	}
	require.Equal(t, groups, rows)
	require.Positive(t, merge.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])
	maxLevel := merge.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillMaxLevel"]

	merge.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	require.Zero(t, allocation.generation.Snapshot().SpillDiskUsed)
	require.Zero(t, allocation.generation.Snapshot().SpillFDUsed)
	finalizeGroupTestAllocation(t, merge, allocation)
	for _, partial := range partials {
		partial.Clean(proc.Mp())
	}
	return maxLevel
}

func TestAccountedMergeGroupSpillsAndReleasesResources(t *testing.T) {
	runAccountedMergeGroupSpill(t, 128, 64)
}

func TestAccountedMergeGroupMaxSpillDepthFinishesAdmittedLeaves(t *testing.T) {
	require.Equal(t, int64(spillMaxPass),
		runAccountedMergeGroupSpill(t, 4, 1))
}

func TestAccountedMergeGroupRetriesResidentStringSourcePreflight(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	makePartial := func(source types.StringSource) *batch.Batch {
		input := batch.NewWithSize(1)
		input.Vecs[0] = vector.NewVec(types.T_text.ToType())
		require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte("a"), false, proc.Mp()))
		require.NoError(t, input.Vecs[0].SetStringSource(source))
		input.SetRowCount(1)
		partial := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_text)},
			[]aggexec.AggFuncExecExpression{countStarAgg()})
		partial.NeedEval = false
		partial.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
		require.NoError(t, partial.Prepare(proc))
		raw := collectBatches(t, partial, proc)
		require.Len(t, raw, 1)
		result := cloneBatch(t, proc, raw[0])
		partial.Free(proc, false, nil)
		input.Clean(proc.Mp())
		return result
	}
	first := makePartial(types.StringSourceLiteral)
	second := makePartial(types.StringSourceExpression)

	merge := newMergeGroupOp([]aggexec.AggFuncExecExpression{countStarAgg()})
	merge.SpillMem = 1 << 30
	generation, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<12)
	require.NoError(t, err)
	controller := &rejectNextGroupAllocationController{}
	account, err := registry.OpenWithController(64<<20, controller)
	require.NoError(t, err)
	require.NoError(t, merge.ctr.setAllocationAccount(account))
	allocation := groupTestAllocation{generation: generation, registry: registry, account: account}
	merge.AppendChild(colexec.NewMockOperator().
		WithBatchs([]*batch.Batch{first, second, batch.EmptyBatch}).
		WithBatchCallback(func(index int) {
			if index == 1 {
				require.Equal(t, uint64(1), merge.ctr.hr.Hash.GroupCount())
				controller.arm()
			}
		}))
	require.NoError(t, merge.Prepare(proc))

	var output *batch.Batch
	for {
		result, execErr := vm.Exec(merge, proc)
		require.NoError(t, execErr)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		output = cloneBatch(t, proc, result.Batch)
	}
	require.NotNil(t, output)
	require.Equal(t, 1, output.RowCount())
	require.Equal(t, types.StringSourceExpression, output.Vecs[0].GetStringSourceAt(0))
	require.Equal(t, int64(2), vector.GetFixedAtNoTypeCheck[int64](output.Vecs[1], 0))
	_, rejected := controller.snapshot()
	require.True(t, rejected)
	require.Positive(t, merge.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])

	output.Clean(proc.Mp())
	merge.Free(proc, false, nil)
	require.Zero(t, account.Snapshot().Used)
	finalizeGroupTestAllocation(t, merge, allocation)
	first.Clean(proc.Mp())
	second.Clean(proc.Mp())
}

func TestAccountedMergeGroupRetriesMinSourcePreflight(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	makePartial := func(keys, values []string, sources []types.StringSource) *batch.Batch {
		input := batch.NewWithSize(2)
		input.Vecs[0] = testutil.MakeVarcharVector(keys, nil, proc.Mp())
		input.Vecs[1] = testutil.MakeVarcharVector(values, nil, proc.Mp())
		require.NoError(t, input.Vecs[1].SetStringSourcesWithMP(sources, proc.Mp()))
		input.SetRowCount(len(keys))
		minValue := aggexec.MakeAggFunctionExpression(
			aggexec.AggIdOfMin, false, []*plan.Expr{colExpr(1, types.T_varchar)}, nil)
		partial := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_varchar)},
			[]aggexec.AggFuncExecExpression{minValue})
		partial.NeedEval = false
		partial.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
		require.NoError(t, partial.Prepare(proc))
		raw := collectBatches(t, partial, proc)
		require.Len(t, raw, 1)
		result := cloneBatch(t, proc, raw[0])
		partial.Free(proc, false, nil)
		input.Clean(proc.Mp())
		return result
	}
	first := makePartial([]string{"a"}, []string{"5"},
		[]types.StringSource{types.StringSourceLiteral})
	second := makePartial([]string{"a", "b"}, []string{"5", "5"},
		[]types.StringSource{types.StringSourceLiteral, types.StringSourceCOMStmt})
	minValue := aggexec.MakeAggFunctionExpression(
		aggexec.AggIdOfMin, false, []*plan.Expr{colExpr(1, types.T_varchar)}, nil)
	merge := newMergeGroupOp([]aggexec.AggFuncExecExpression{minValue})
	merge.SpillMem = 1 << 30
	generation, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<12)
	require.NoError(t, err)
	controller := &rejectNextGroupAllocationController{}
	account, err := registry.OpenWithController(64<<20, controller)
	require.NoError(t, err)
	require.NoError(t, merge.ctr.setAllocationAccount(account))
	allocation := groupTestAllocation{generation: generation, registry: registry, account: account}
	merge.AppendChild(colexec.NewMockOperator().
		WithBatchs([]*batch.Batch{first, second, batch.EmptyBatch}).
		WithBatchCallback(func(index int) {
			if index == 1 {
				require.Equal(t, uint64(1), merge.ctr.hr.Hash.GroupCount())
				controller.arm()
			}
		}))
	require.NoError(t, merge.Prepare(proc))
	seen := make(map[string]types.StringSource)
	for {
		result, execErr := vm.Exec(merge, proc)
		require.NoError(t, execErr)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		for row := range result.Batch.RowCount() {
			seen[string(result.Batch.Vecs[0].GetBytesAt(row))] =
				result.Batch.Vecs[1].GetStringSourceAt(row)
		}
	}
	require.Equal(t, map[string]types.StringSource{
		"a": types.StringSourceLiteral, "b": types.StringSourceCOMStmt,
	}, seen)
	_, rejected := controller.snapshot()
	require.True(t, rejected)
	require.Positive(t, merge.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])
	merge.Free(proc, false, nil)
	require.Zero(t, account.Snapshot().Used)
	finalizeGroupTestAllocation(t, merge, allocation)
	first.Clean(proc.Mp())
	second.Clean(proc.Mp())
}

func TestAccountedMergeGroupCapacityPressureSpillsAndRetriesPartial(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	const groupsPerWave = aggBatchSize
	makeSource := func(base int32) *batch.Batch {
		firstKey := make([]int32, groupsPerWave)
		secondKey := make([]int32, groupsPerWave)
		for i := range firstKey {
			firstKey[i] = base + int32(i)
			secondKey[i] = base + int32(i*3)
		}
		source := batch.NewWithSize(2)
		source.Vecs[0] = testutil.MakeInt32Vector(firstKey, nil, proc.Mp())
		source.Vecs[1] = testutil.MakeInt32Vector(secondKey, nil, proc.Mp())
		source.SetRowCount(groupsPerWave)
		return source
	}
	first, second := makeSource(0), makeSource(groupsPerWave)
	partials := buildPartialGroupBatches(t, proc, []*batch.Batch{first, second}, false)
	first.Clean(proc.Mp())
	second.Clean(proc.Mp())
	require.Len(t, partials, 2)

	merge := newMergeGroupOp([]aggexec.AggFuncExecExpression{countStarAgg()})
	merge.SpillMem = 1 << 30
	allocation := installGroupTestAllocation(t, merge, proc, 32<<20)
	var external uint64
	child := colexec.NewMockOperator().
		WithBatchs([]*batch.Batch{partials[0], partials[1], batch.EmptyBatch}).
		WithBatchCallback(func(index int) {
			switch index {
			case 1:
				snapshot := allocation.generation.Snapshot()
				require.Less(t, snapshot.Used, snapshot.Cap)
				external = snapshot.Cap - snapshot.Used - 1
				require.NoError(t, allocation.generation.AcquireAllocationCapacity(external))
			case 2:
				allocation.generation.ReleaseAllocationCapacity(external)
				external = 0
			}
		})
	merge.AppendChild(child)
	require.NoError(t, merge.Prepare(proc))
	defer func() {
		if external != 0 {
			allocation.generation.ReleaseAllocationCapacity(external)
		}
	}()

	rows := 0
	for {
		result, err := vm.Exec(merge, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		rows += result.Batch.RowCount()
		for _, count := range vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[2]) {
			require.Equal(t, int64(1), count)
		}
	}
	require.Equal(t, groupsPerWave*2, rows)
	require.Positive(t, allocation.generation.Snapshot().RejectCount)
	require.Positive(t, merge.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])

	merge.Free(proc, false, nil)
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, merge, allocation)
	for _, partial := range partials {
		partial.Clean(proc.Mp())
	}
}

func TestAccountedStreamingGroupOwnsPartialOutputBuffer(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 1, 3}, nil, proc.Mp())
	input.SetRowCount(4)

	g := newGroupOp(
		proc,
		[]*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{countStarAgg()},
	)
	g.NeedEval = false
	g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))

	result, err := vm.Exec(g, proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.NotEmpty(t, result.Batch.ExtraBuf)
	require.True(t, result.Batch.HasAccountedExtraBuffer())
	require.Positive(t, allocation.account.Snapshot().Used)
	payload := append([]byte(nil), result.Batch.ExtraBuf...)
	destination := batch.NewWithSize(0)
	destination.MoveExtraBufferFrom(result.Batch)
	require.False(t, result.Batch.HasAccountedExtraBuffer())

	g.Free(proc, false, nil)
	// A pipeline spool may outlive the Group's no-lock MPool. The moved
	// partial remains owned by the statement MPool until the consumer drops it.
	require.True(t, bytes.Equal(payload, destination.ExtraBuf))
	require.True(t, destination.HasAccountedExtraBuffer())
	require.Positive(t, allocation.account.Snapshot().Used)
	destination.Clean(proc.Mp())
	require.Zero(t, allocation.account.Snapshot().Used)
	finalizeGroupTestAllocation(t, g, allocation)
	input.Clean(proc.Mp())
}

func TestAccountedGroupAndMergeGroupAcceptOrderedGroupConcat(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{orderedGroupConcatAgg(false)})
	groupAllocation := installGroupTestAllocation(t, g, proc, 64<<20)
	require.NoError(t, g.Prepare(proc))
	g.Free(proc, false, nil)
	finalizeGroupTestAllocation(t, g, groupAllocation)

	merge := newMergeGroupOp([]aggexec.AggFuncExecExpression{
		orderedGroupConcatAgg(false),
	})
	allocation := installGroupTestAllocation(t, merge, proc, 64<<20)
	require.NoError(t, merge.Prepare(proc))
	merge.Free(proc, false, nil)
	finalizeGroupTestAllocation(t, merge, allocation)
}
