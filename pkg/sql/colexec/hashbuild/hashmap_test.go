// Copyright 2024 Matrix Origin
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

package hashbuild

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestObserveNullKeysUsesColumnLevelGroupingSentinel(t *testing.T) {
	mp := mpool.MustNewZero()
	newVec := func(nullRows, groupingRows []uint64) *vector.Vector {
		vec := vector.NewVec(types.T_int32.ToType())
		require.NoError(t, vector.AppendFixedList(
			vec,
			[]int32{1, 2},
			nil,
			mp,
		))
		for _, row := range nullRows {
			vec.GetNulls().Add(row)
		}
		for _, row := range groupingRows {
			vec.GetGrouping().Add(row)
		}
		return vec
	}

	tests := []struct {
		name         string
		nullRows     []uint64
		groupingRows []uint64
		want         bool
	}{
		{
			name:         "grouping sentinel masks null in same row",
			nullRows:     []uint64{0},
			groupingRows: []uint64{0},
			want:         false,
		},
		{
			name:         "full grouping is sentinel",
			nullRows:     []uint64{0, 1},
			groupingRows: []uint64{0, 1},
			want:         false,
		},
		{
			name:         "partial grouping without null",
			groupingRows: []uint64{0},
			want:         false,
		},
		{
			name:         "null outside partial grouping is retained",
			nullRows:     []uint64{1},
			groupingRows: []uint64{0},
			want:         true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			vec := newVec(test.nullRows, test.groupingRows)
			defer vec.Free(mp)
			builder := HashmapBuilder{TrackNullKeys: true}
			builder.observeNullKeys([]*vector.Vector{vec})
			require.Equal(t, test.want, builder.HasNullKey)
		})
	}
}

func TestBuildHashmapPreservesRowwiseGroupingAcrossCopiedBatchMerge(t *testing.T) {
	for _, test := range []struct {
		name       string
		groupFirst bool
		columns    int
	}{
		{name: "grouping then ordinary", groupFirst: true, columns: 1},
		{name: "ordinary then grouping", groupFirst: false, columns: 1},
		{name: "multi-column grouping pattern", groupFirst: true, columns: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			builder := newTestHashmapBuilder(t)
			defer builder.Free(proc)

			exprs := make([]*plan.Expr, test.columns)
			for column := range exprs {
				exprs[column] = newExpr(int32(column), types.T_int32.ToType())
			}
			require.NoError(t, builder.Prepare(exprs, -1, -1, nil, proc))

			appendInput := func(grouping bool, groupingColumn int) {
				input := batch.NewWithSize(test.columns)
				for column := 0; column < test.columns; column++ {
					if grouping && column == groupingColumn {
						input.Vecs[column] = vector.NewRollupConst(
							types.T_int32.ToType(), 1, proc.Mp(),
						)
					} else {
						input.Vecs[column] = vector.NewVec(types.T_int32.ToType())
						require.NoError(t, vector.AppendFixed(
							input.Vecs[column], int32(column), false, proc.Mp(),
						))
					}
				}
				input.SetRowCount(1)
				require.NoError(t, builder.CopyBuildBatch(input, proc))
				builder.InputBatchRowCount++
				input.Clean(proc.Mp())
			}

			appendInput(test.groupFirst, 0)
			if test.columns == 2 {
				// A different grouping column in the later row proves that the
				// per-column bit pattern survives tail coalescing.
				appendInput(true, 1)
			} else {
				appendInput(!test.groupFirst, 0)
			}

			require.Len(t, builder.Batches.Buf, 1)
			require.Equal(t, 2, builder.Batches.Buf[0].RowCount())
			require.False(t, builder.Batches.Buf[0].Vecs[0].IsGrouping())
			require.True(t, builder.Batches.Buf[0].Vecs[0].HasGrouping())

			require.NoError(t, builder.BuildHashmap(false, false, false, proc))
			require.Nil(t, builder.IntHashMap)
			require.NotNil(t, builder.StrHashMap)
			require.Equal(t, uint64(2), builder.StrHashMap.GroupCount())
		})
	}
}

func TestBuildHashmapDetectsGroupingForOriginalBuildRelation(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	builder := newTestHashmapBuilder(t)
	defer builder.Free(proc)

	expr := newExpr(0, types.T_int32.ToType())
	expr.GetCol().RelPos = 1
	require.NoError(t, builder.Prepare([]*plan.Expr{expr}, -1, -1, nil, proc))

	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixedList(
		input.Vecs[0], []int32{0, 0}, nil, proc.Mp(),
	))
	input.Vecs[0].GetGrouping().Add(1)
	input.SetRowCount(2)
	require.NoError(t, builder.CopyBuildBatch(input, proc))
	builder.InputBatchRowCount = input.RowCount()
	input.Clean(proc.Mp())

	require.NoError(t, builder.BuildHashmap(false, false, false, proc))
	require.Nil(t, builder.IntHashMap)
	require.NotNil(t, builder.StrHashMap)
	require.Equal(t, uint64(2), builder.StrHashMap.GroupCount())
}

func TestBuildHashMap(t *testing.T) {
	hb := newTestHashmapBuilder(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	err := hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc)
	require.NoError(t, err)

	inputBatch := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, int(100000), proc.Mp())
	err = hb.CopyBuildBatch(inputBatch, proc)
	hb.InputBatchRowCount = inputBatch.RowCount()
	inputBatch.Clean(proc.Mp())
	require.NoError(t, err)

	err = hb.BuildHashmap(false, true, true, proc)
	require.NoError(t, err)
	require.Less(t, int64(0), hb.GetSize())
	require.Less(t, uint64(0), hb.GetGroupCount())
	hb.Reset(proc, true)
	hb.Free(proc)
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestHashmapBuilderPhysicalAllocationsChargeOnce(t *testing.T) {
	const budgetCap = uint64(16 << 20)
	budget, err := process.NewExecutionResourceBudget(budgetCap, budgetCap)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.OpenWithController(budgetCap, generation)
	require.NoError(t, err)

	var op HashBuild
	op.NeedHashMap = true
	require.NoError(t, op.SetAllocationAccount(account))
	hb := &op.ctr.hashmapBuilder
	hb.setBudget(generation)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	require.NoError(t, hb.Prepare(
		[]*plan.Expr{newExpr(0, types.T_int32.ToType())},
		-1,
		-1,
		nil,
		proc,
	))
	input := testutil.NewBatch(
		[]types.Type{types.T_int32.ToType()},
		true,
		10_000,
		proc.Mp(),
	)
	require.NoError(t, hb.copyBuildBatch(input, proc))
	require.NotEmpty(t, hb.Batches.Buf)
	for _, copied := range hb.Batches.Buf {
		require.Same(t, hb.batchAllocation, copied.AllocationAccountSelection())
	}
	hb.InputBatchRowCount = input.RowCount()
	input.Clean(proc.Mp())

	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.Positive(t, account.Snapshot().Used)
	require.Equal(t, account.Snapshot().Used, generation.Used())

	jm := hb.GetJoinMap(proc.Mp())
	require.NotNil(t, jm)
	jm.IncRef(2)
	hb.Reset(proc, false)
	beforeResize := account.Snapshot().Used
	require.NoError(t, jm.PreAlloc(100_000))
	require.Greater(t, account.Snapshot().Used, beforeResize)
	beforeFirstConsumer := account.Snapshot().Used
	jm.Free()
	require.Equal(t, beforeFirstConsumer, account.Snapshot().Used)
	jm.Free()
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, generation.Used())

	terminal, first, err := registry.CompleteTerminal(account)
	require.NoError(t, err)
	require.True(t, first)
	require.Equal(t, mpool.AllocationAccountTerminalValid, terminal.State)
}

func TestHashmapBuilderAccountedBatchCopyOneByteShortRollsBack(t *testing.T) {
	const budgetCap = uint64(64 << 20)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	input := testutil.NewBatch(
		[]types.Type{types.T_int32.ToType(), types.T_varchar.ToType()},
		true,
		10_000,
		proc.Mp(),
	)
	defer input.Clean(proc.Mp())

	measure := func(limit uint64, metadataSlots uint64) (
		mpool.AllocationAccountSnapshot,
		uint64,
		error,
	) {
		budget := process.MustNewExecutionResourceBudget(budgetCap, budgetCap)
		generation, err := budget.OpenGeneration(1)
		require.NoError(t, err)
		registry, err := mpool.NewAllocationAccountRegistry(1, metadataSlots)
		require.NoError(t, err)
		account, err := registry.OpenWithController(limit, generation)
		require.NoError(t, err)
		var op HashBuild
		op.NeedHashMap = true
		require.NoError(t, op.SetAllocationAccount(account))
		hb := &op.ctr.hashmapBuilder
		hb.setBudget(generation)

		copyErr := hb.copyBuildBatch(input, proc)
		snapshot := account.Snapshot()
		metadataPeak := registry.PeakAllocationMetadata()
		if copyErr == nil {
			hb.cleanBatches(proc)
		}
		require.Empty(t, hb.Batches.Buf)
		require.Zero(t, account.Snapshot().Used)
		require.Zero(t, generation.Used())
		require.NoError(t, op.ClearAllocationAccount(account))
		terminal, first, terminalErr := registry.CompleteTerminal(account)
		require.NoError(t, terminalErr)
		require.True(t, first)
		require.Equal(t, mpool.AllocationAccountTerminalValid, terminal.State)
		return snapshot, metadataPeak, copyErr
	}

	probe, metadataPeak, err := measure(budgetCap, 128)
	require.NoError(t, err)
	require.Positive(t, probe.Peak)
	require.Positive(t, metadataPeak)
	rejected, _, err := measure(probe.Peak-1, 128)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Zero(t, rejected.Used)
	rejected, _, err = measure(budgetCap, metadataPeak-1)
	require.ErrorIs(t, err, mpool.ErrAllocationMetadataSlots)
	require.Zero(t, rejected.Used)
}

func TestAccountedJoinMapTransfersBatchesAndGroupSelsToLastConsumer(t *testing.T) {
	const budgetCap = uint64(16 << 20)
	budget := process.MustNewExecutionResourceBudget(budgetCap, budgetCap)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.OpenWithController(budgetCap, generation)
	require.NoError(t, err)
	var op HashBuild
	op.NeedHashMap = true
	require.NoError(t, op.SetAllocationAccount(account))
	hb := &op.ctr.hashmapBuilder
	hb.setBudget(generation)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	require.NoError(t, hb.Prepare(
		[]*plan.Expr{newExpr(0, types.T_int32.ToType())},
		-1,
		-1,
		nil,
		proc,
	))
	input := makeIntKeyValueBatch(
		proc,
		[]int32{1, 1, 2, 2},
		[]int32{10, 20, 30, 40},
	)
	require.NoError(t, hb.copyBuildBatch(input, proc))
	hb.InputBatchRowCount = input.RowCount()
	input.Clean(proc.Mp())
	require.NoError(t, hb.BuildHashmap(false, true, false, proc))
	require.Positive(t, hb.Sels.Size())

	jm := hb.GetJoinMap(proc.Mp())
	require.NotNil(t, jm)
	jm.IncRef(2)
	hb.Reset(proc, false)
	require.Equal(t, []int32{0, 1}, jm.GetSels(0))
	require.Equal(t, []int32{2, 3}, jm.GetSels(1))
	live := account.Snapshot().Used
	require.Positive(t, live)
	jm.Free()
	require.Equal(t, live, account.Snapshot().Used)
	jm.Free()
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, generation.Used())

	terminal, first, err := registry.CompleteTerminal(account)
	require.NoError(t, err)
	require.True(t, first)
	require.Equal(t, mpool.AllocationAccountTerminalValid, terminal.State)
}

func TestAccountedEmptyJoinMapUsesPhysicalAllocationAsSoleCharge(t *testing.T) {
	for _, tc := range []struct {
		name         string
		keyWidth     int
		initialBytes uint64
	}{
		{name: "int", keyWidth: 4, initialBytes: hashtable.Int64HashMapInitialAllocationBytes()},
		{name: "string", keyWidth: 128, initialBytes: hashtable.StringHashMapInitialAllocationBytes()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const capBytes = uint64(64 << 20)
			budget := process.MustNewExecutionResourceBudget(capBytes, capBytes)
			generation, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			registry, err := mpool.NewAllocationAccountRegistry(1, 64)
			require.NoError(t, err)
			account, err := registry.OpenWithController(capBytes, generation)
			require.NoError(t, err)
			mp := mpool.MustNewZero()

			jm, err := NewAccountedEmptyJoinMap(tc.keyWidth, account, mp)
			require.NoError(t, err)
			descriptorBytes := hashtable.HashMapBlockDescriptorBytes()
			expectedInitial := tc.initialBytes + descriptorBytes
			require.Equal(t, expectedInitial, account.Snapshot().Used)
			require.Equal(t, expectedInitial, generation.Used())

			require.NoError(t, jm.PreAlloc(10_000))
			require.Equal(t, account.Snapshot().Used, generation.Used())
			require.Equal(t, account.Snapshot().Used, generation.Used())
			jm.Free()
			require.Zero(t, account.Snapshot().Used)
			require.Zero(t, generation.Used())
			terminal, first, err := registry.CompleteTerminal(account)
			require.NoError(t, err)
			require.True(t, first)
			require.Equal(t, mpool.AllocationAccountTerminalValid, terminal.State)
		})
	}
}

func TestAccountedEmptyJoinMapInitialFailureRollsBackController(t *testing.T) {
	initial := hashtable.Int64HashMapInitialAllocationBytes() +
		hashtable.HashMapBlockDescriptorBytes()
	budget := process.MustNewExecutionResourceBudget(initial, initial)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 2)
	require.NoError(t, err)
	account, err := registry.OpenWithController(initial-1, generation)
	require.NoError(t, err)
	mp := mpool.MustNewZero()

	jm, err := NewAccountedEmptyJoinMap(4, account, mp)
	require.Nil(t, jm)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, generation.Used())
	require.Zero(t, registry.LiveAllocationMetadata())
	require.Zero(t, mp.CurrNB())
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
}

func TestAccountedJoinMapLateFreeKeepsOriginalGeneration(t *testing.T) {
	const capBytes = uint64(64 << 20)
	budget := process.MustNewExecutionResourceBudget(capBytes, capBytes)
	firstGeneration, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	secondGeneration, err := budget.OpenGeneration(2)
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(2, 16)
	require.NoError(t, err)
	firstAccount, err := registry.OpenWithController(capBytes, firstGeneration)
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	jm, err := NewAccountedEmptyJoinMap(4, firstAccount, mp)
	require.NoError(t, err)
	firstUsed := firstGeneration.Used()
	require.Positive(t, firstUsed)

	secondAccount, err := registry.OpenWithController(capBytes, secondGeneration)
	require.NoError(t, err)
	require.Zero(t, secondGeneration.Used())
	jm.Free()
	require.Zero(t, firstGeneration.Used())
	require.Zero(t, firstAccount.Snapshot().Used)
	require.Zero(t, secondGeneration.Used())

	_, _, err = registry.CompleteTerminal(firstAccount)
	require.NoError(t, err)
	_, _, err = registry.CompleteTerminal(secondAccount)
	require.NoError(t, err)
}

func TestAccountedRuntimeFilterUniqueKeysDegradeWithoutFailingHashBuild(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	values := make([]string, 1_024)
	for i := range values {
		values[i] = strconv.Itoa(i) + strings.Repeat(string(rune('a'+i%26)), 4<<10)
	}
	input := testutil.NewBatchWithVectors([]*vector.Vector{
		testutil.MakeVarcharVector(values, nil, proc.Mp()),
	}, nil)
	defer input.Clean(proc.Mp())
	exprs := []*plan.Expr{newExpr(0, types.T_varchar.ToType())}

	run := func(limit uint64, needUnique bool) (mpool.AllocationAccountSnapshot, bool) {
		budget := process.MustNewExecutionResourceBudget(64<<20, 64<<20)
		generation, err := budget.OpenGeneration(1)
		require.NoError(t, err)
		registry, err := mpool.NewAllocationAccountRegistry(1, 128)
		require.NoError(t, err)
		account, err := registry.OpenWithController(limit, generation)
		require.NoError(t, err)
		builder := &HashmapBuilder{}
		builder.SetBudget(generation)
		require.NoError(t, builder.SetAllocationAccount(account))
		require.NoError(t, builder.Prepare(exprs, -1, -1, nil, proc))
		require.NoError(t, builder.CopyBuildBatch(input, proc))
		builder.InputBatchRowCount = input.RowCount()
		require.NoError(t, builder.BuildHashmap(false, false, needUnique, proc))
		snapshot := account.Snapshot()
		fallback, _ := builder.runtimeFilterFallbackState()
		if needUnique {
			require.True(t, fallback)
			require.Empty(t, builder.UniqueJoinKeys)
			require.NotNil(t, builder.StrHashMap)
			require.Equal(t, uint64(input.RowCount()), builder.StrHashMap.GroupCount())
		}
		builder.Free(proc)
		require.Zero(t, account.Snapshot().Used)
		require.Zero(t, generation.Used())
		_, _, err = registry.CompleteTerminal(account)
		require.NoError(t, err)
		return snapshot, fallback
	}

	baseline, fallback := run(64<<20, false)
	require.False(t, fallback)
	require.Positive(t, baseline.Peak)
	// The exact baseline peak is sufficient for the required hash build, but
	// not for a second, optional copy of the 4 MiB runtime-filter key payload.
	constrained, fallback := run(baseline.Peak, true)
	require.True(t, fallback)
	require.LessOrEqual(t, constrained.Peak, baseline.Peak)
}

func TestSpillExpressionStorageUsesRetainedAccount(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget := process.MustNewExecutionResourceBudget(16<<20, 16<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.OpenWithController(16<<20, generation)
	require.NoError(t, err)
	var op HashBuild
	op.NeedHashMap = true
	require.NoError(t, op.SetAllocationAccount(account))
	ctr := &op.ctr
	ctr.hashmapBuilder.setBudget(generation)
	expr := makeIssue26454ConcatKey(t, proc)
	executors, err := ctr.initSpillExprExecs(proc, []*plan.Expr{expr})
	require.NoError(t, err)
	constructorUsed := account.Snapshot().Used
	input := batch.NewWithSize(2)
	input.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
	input.Vecs[1] = testutil.MakeInt32Vector([]int32{3, 4}, nil, proc.Mp())
	input.SetRowCount(2)
	defer input.Clean(proc.Mp())
	result, err := executors[0].Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"1-3", "2-4"}, vector.InefficientMustStrCol(result))
	require.Greater(t, account.Snapshot().Used, constructorUsed)
	require.Equal(t, account.Snapshot().Used, generation.Used())

	ctr.freeSpillExprExecs()
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, generation.Used())
	require.NoError(t, op.ClearAllocationAccount(account))
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
}

func TestSpillExpressionStorageHonorsAccountCapacity(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	run := func(limit uint64) (uint64, error) {
		budget := process.MustNewExecutionResourceBudget(16<<20, 16<<20)
		generation, err := budget.OpenGeneration(1)
		require.NoError(t, err)
		registry, err := mpool.NewAllocationAccountRegistry(1, 64)
		require.NoError(t, err)
		account, err := registry.OpenWithController(limit, generation)
		require.NoError(t, err)
		var op HashBuild
		op.NeedHashMap = true
		require.NoError(t, op.SetAllocationAccount(account))
		op.ctr.hashmapBuilder.setBudget(generation)
		executors, evalErr := op.ctr.initSpillExprExecs(
			proc,
			[]*plan.Expr{makeIssue26454ConcatKey(t, proc)},
		)
		if evalErr == nil {
			input := testutil.NewBatch(
				[]types.Type{types.T_int32.ToType(), types.T_int32.ToType()},
				true,
				10_000,
				proc.Mp(),
			)
			_, evalErr = executors[0].Eval(proc, []*batch.Batch{input}, nil)
			input.Clean(proc.Mp())
		}
		peak := account.Snapshot().Peak
		op.ctr.freeSpillExprExecs()
		require.Zero(t, account.Snapshot().Used)
		require.Zero(t, generation.Used())
		require.NoError(t, op.ClearAllocationAccount(account))
		_, _, terminalErr := registry.CompleteTerminal(account)
		require.NoError(t, terminalErr)
		return peak, evalErr
	}

	peak, err := run(16 << 20)
	require.NoError(t, err)
	require.Greater(t, peak, uint64(1))
	_, err = run(peak - 1)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
}

func TestIssue26454ExpressionKeyBuildUsesActualCapacity(t *testing.T) {
	const capBytes = uint64(16 << 20)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	for _, tc := range []struct {
		name  string
		expr  *plan.Expr
		input func() *batch.Batch
	}{
		{
			name: "concat cast key",
			expr: makeIssue26454ConcatKey(t, proc),
			input: func() *batch.Batch {
				return testutil.NewBatch(
					[]types.Type{types.T_int32.ToType(), types.T_int32.ToType()},
					true,
					10_000,
					proc.Mp(),
				)
			},
		},
		{
			name: "case equality key",
			expr: makeIssue26454CaseKey(t, proc),
			input: func() *batch.Batch {
				values := make([]string, 10_000)
				for i := range values {
					if i%2 == 0 {
						values[i] = "ATM_CON"
					} else {
						values[i] = "OTHER"
					}
				}
				bat := batch.NewWithSize(1)
				bat.Vecs[0] = testutil.MakeVarcharVector(values, nil, proc.Mp())
				bat.SetRowCount(len(values))
				return bat
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			budget := process.MustNewExecutionResourceBudget(capBytes, capBytes)
			generation, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			registry, err := mpool.NewAllocationAccountRegistry(1, 128)
			require.NoError(t, err)
			account, err := registry.OpenWithController(capBytes, generation)
			require.NoError(t, err)
			var op HashBuild
			op.NeedHashMap = true
			require.NoError(t, op.SetAllocationAccount(account))
			hb := &op.ctr.hashmapBuilder
			hb.setBudget(generation)
			require.NoError(t, hb.Prepare([]*plan.Expr{tc.expr}, -1, -1, nil, proc))
			input := tc.input()
			require.NoError(t, hb.copyBuildBatch(input, proc))
			hb.InputBatchRowCount = input.RowCount()
			input.Clean(proc.Mp())
			require.NoError(t, hb.BuildHashmap(false, false, false, proc))
			require.LessOrEqual(t, generation.Used(), capBytes)

			jm := hb.GetJoinMap(proc.Mp())
			require.NotNil(t, jm)
			jm.IncRef(1)
			hb.Reset(proc, false)
			jm.Free()
			require.Zero(t, account.Snapshot().Used)
			require.Zero(t, generation.Used())
			terminal, first, err := registry.CompleteTerminal(account)
			require.NoError(t, err)
			require.True(t, first)
			require.Equal(t, mpool.AllocationAccountTerminalValid, terminal.State)
		})
	}
}

func TestPreparedParamExpressionExecutorRemainsConst(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value []byte
		null  bool
	}{
		{name: "non-null", value: []byte("prepared")},
		{name: "null", null: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			params := vector.NewVec(types.T_text.ToType())
			defer params.Free(proc.Mp())
			require.NoError(t, vector.AppendBytes(params, tc.value, tc.null, proc.Mp()))
			proc.SetPrepareParams(params)

			expr := &plan.Expr{
				Typ:  plan.Type{Id: int32(types.T_text)},
				Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
			}
			executor, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer executor.Free()
			input := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, colexec.DefaultBatchSize, proc.Mp())
			defer input.Clean(proc.Mp())

			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.True(t, result.IsConst())
			require.Equal(t, 1, result.Length())
		})
	}
}

func TestGetJoinMapTransfersGroupSels(t *testing.T) {
	hb := newTestHashmapBuilder(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	input := makeIntKeyValueBatch(proc, []int32{1, 1}, []int32{10, 20})
	require.NoError(t, hb.CopyBuildBatch(input, proc))
	hb.InputBatchRowCount = input.RowCount()
	input.Clean(proc.Mp())

	require.NoError(t, hb.BuildHashmap(false, true, false, proc))
	require.Greater(t, hb.Sels.Size(), int64(0))

	jm := hb.GetJoinMap(proc.Mp())
	require.NotNil(t, jm)
	jm.IncRef(1)
	require.Zero(t, hb.Sels.Size(), "builder must relinquish GroupSels ownership")
	require.Equal(t, []int32{0, 1}, jm.GetSels(0))

	// A subsequent empty build cleanup must not release the previous JoinMap's sels.
	hb.Reset(proc, false)
	hb.Reset(proc, false)
	require.Equal(t, []int32{0, 1}, jm.GetSels(0))

	hb.Free(proc)
	jm.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestDedupUpdateBuildGroupsNullKeysSeparately(t *testing.T) {
	hb := newTestHashmapBuilder(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	hb.IsDedup = true
	hb.OnDuplicateAction = plan.Node_UPDATE
	defer func() {
		hb.Reset(proc, true)
		hb.Free(proc)
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))

	rows := hashmap.UnitLimit * 2
	keys := make([]int32, rows)
	nulls := make([]uint64, hashmap.UnitLimit)
	for i := 0; i < hashmap.UnitLimit; i++ {
		keys[i] = int32(i + 1)
		nulls[i] = uint64(hashmap.UnitLimit + i)
	}
	keyVec := testutil.MakeInt32Vector(keys, nulls, proc.Mp())
	bat := batch.New([]string{"id"})
	bat.SetVector(0, keyVec)
	bat.SetRowCount(rows)
	require.NoError(t, hb.CopyBuildBatch(bat, proc))
	hb.InputBatchRowCount = bat.RowCount()
	bat.Clean(proc.Mp())

	require.NoError(t, hb.BuildHashmap(false, true, false, proc))
	require.Equal(t, uint64(hashmap.UnitLimit), hb.GetGroupCount())

	nullRows := hb.Sels.Get(0)
	require.Len(t, nullRows, hashmap.UnitLimit)
	for i, row := range nullRows {
		require.Equal(t, int32(hashmap.UnitLimit+i), row)
	}
	for group := 1; group <= hashmap.UnitLimit; group++ {
		require.Equal(t, []int32{int32(group - 1)}, hb.Sels.Get(int32(group)))
	}
}

func TestHashMapAllocAndFree(t *testing.T) {
	mp := mpool.MustNewZero()

	hb := newTestHashmapBuilder(t)
	var err error
	hb.IntHashMap, err = hashmap.NewIntHashMap(false, mp)
	require.NoError(t, err)
	err = hb.IntHashMap.PreAlloc(100)
	require.NoError(t, err)
	hb.IntHashMap.Free()
	hb.IntHashMap, err = hashmap.NewIntHashMap(false, mp)
	require.NoError(t, err)
	err = hb.IntHashMap.PreAlloc(10000)
	require.NoError(t, err)
	hb.IntHashMap.Free()
	hb.IntHashMap, err = hashmap.NewIntHashMap(false, mp)
	require.NoError(t, err)
	err = hb.IntHashMap.PreAlloc(1000000)
	require.NoError(t, err)
	hb.IntHashMap.Free()
	hb.IntHashMap, err = hashmap.NewIntHashMap(false, mp)
	require.NoError(t, err)
	err = hb.IntHashMap.PreAlloc(100000000)
	require.NoError(t, err)
	hb.IntHashMap.Free()
}

func TestIteratorReuseAcrossBuilds(t *testing.T) {
	hb := newTestHashmapBuilder(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))

	b := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, 16, proc.Mp())
	defer b.Clean(proc.Mp())

	hb.InputBatchRowCount = b.RowCount()
	require.NoError(t, hb.CopyBuildBatch(b, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedIntIterator)
	itr1 := hb.cachedIntIterator

	// Reset should detach owner but keep iterator for reuse.
	hb.Reset(proc, true)
	require.NotNil(t, hb.cachedIntIterator)
	require.Same(t, itr1, hb.cachedIntIterator)

	// Next build should reuse the same iterator instance.
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	hb.InputBatchRowCount = b.RowCount()
	require.NoError(t, hb.CopyBuildBatch(b, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.Same(t, itr1, hb.cachedIntIterator)
}

func TestStrIteratorCapacityPrune(t *testing.T) {
	hb := newTestHashmapBuilder(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_varchar.ToType())}, -1, -1, nil, proc))

	// Build with an oversized string to inflate iterator buffers beyond threshold.
	vec := vector.NewVec(types.T_varchar.ToType())
	large := strings.Repeat("x", hashmap.MaxStrIteratorCapacity+4096)
	require.NoError(t, vector.AppendBytes(vec, []byte(large), false, proc.Mp()))
	bat := batch.New([]string{"col"})
	bat.SetVector(0, vec)
	bat.SetRowCount(1)
	hb.InputBatchRowCount = bat.RowCount()
	require.NoError(t, hb.CopyBuildBatch(bat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedStrIterator)
	require.Greater(t, hashmap.StrIteratorCapacity(hb.cachedStrIterator), hashmap.MaxStrIteratorCapacity)

	hb.detachAndPruneCachedIterators()
	require.Nil(t, hb.cachedStrIterator)
}

func TestStrIteratorBelowThresholdIsKept(t *testing.T) {
	hb := newTestHashmapBuilder(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_varchar.ToType())}, -1, -1, nil, proc))

	// Build with small strings so iterator capacity stays below threshold.
	vec := vector.NewVec(types.T_varchar.ToType())
	for i := 0; i < 4; i++ {
		require.NoError(t, vector.AppendBytes(vec, []byte("small"), false, proc.Mp()))
	}
	bat := batch.New([]string{"col"})
	bat.SetVector(0, vec)
	bat.SetRowCount(vec.Length())
	hb.InputBatchRowCount = bat.RowCount()
	require.NoError(t, hb.CopyBuildBatch(bat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedStrIterator)
	require.Less(t, hashmap.StrIteratorCapacity(hb.cachedStrIterator), hashmap.MaxStrIteratorCapacity)

	hb.detachAndPruneCachedIterators()
	require.NotNil(t, hb.cachedStrIterator, "iterator below threshold should be kept")
}

func TestResetWithHashTableSentKeepsCache(t *testing.T) {
	hb := newTestHashmapBuilder(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))

	// Build once to populate cachedIntIterator.
	b := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, 8, proc.Mp())
	defer b.Clean(proc.Mp())
	hb.InputBatchRowCount = b.RowCount()
	require.NoError(t, hb.CopyBuildBatch(b, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedIntIterator)

	// hashTableHasNotSent=false should skip FreeHashMapAndBatches and keep cached iterator.
	hb.Reset(proc, false)
	require.NotNil(t, hb.cachedIntIterator)
}

func TestAlternateIntStrBuildsReuseIndependently(t *testing.T) {
	hb := newTestHashmapBuilder(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	// First int build
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	bInt := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, 4, proc.Mp())
	defer bInt.Clean(proc.Mp())
	hb.InputBatchRowCount = bInt.RowCount()
	require.NoError(t, hb.CopyBuildBatch(bInt, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedIntIterator)
	require.Nil(t, hb.cachedStrIterator)

	hb.Reset(proc, true)
	// Simulate a new plan with different key types.
	hb.executors = nil

	// Then str build
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_varchar.ToType())}, -1, -1, nil, proc))
	vec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(vec, []byte("a"), false, proc.Mp()))
	bat := batch.New([]string{"col"})
	bat.SetVector(0, vec)
	bat.SetRowCount(1)
	hb.InputBatchRowCount = bat.RowCount()
	require.NoError(t, hb.CopyBuildBatch(bat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedStrIterator)

	hb.Reset(proc, true)
	// Simulate switching back to int keys in a new plan.
	hb.executors = nil

	// Build int again and ensure int cache still exists (reuse if retained).
	prevIntItr := hb.cachedIntIterator
	// Use a fresh int batch to avoid zero-row short-circuit.
	bInt2 := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, 4, proc.Mp())
	defer bInt2.Clean(proc.Mp())
	hb.InputBatchRowCount = bInt2.RowCount()
	require.NoError(t, hb.CopyBuildBatch(bInt2, proc))
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedIntIterator)
	if prevIntItr != nil {
		require.Same(t, prevIntItr, hb.cachedIntIterator)
	}
}

func TestBuildHashmapWithZeroInputKeepsCachesUntouched(t *testing.T) {
	hb := newTestHashmapBuilder(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))

	// No rows added.
	hb.InputBatchRowCount = 0
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))

	require.Nil(t, hb.cachedIntIterator)
	require.Nil(t, hb.cachedStrIterator)
}

func TestDedupBuildDuplicateKeyStillFailsByDefault(t *testing.T) {
	hb := newTestHashmapBuilder(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	hb.IsDedup = true
	hb.OnDuplicateAction = plan.Node_FAIL
	hb.DedupColName = "id"
	hb.DedupColTypes = []plan.Type{newExpr(0, types.T_int32.ToType()).Typ}
	defer func() {
		hb.Reset(proc, true)
		hb.Free(proc)
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	bat := makeIntKeyValueBatch(proc, []int32{1, 1}, []int32{10, 20})
	require.NoError(t, hb.CopyBuildBatch(bat, proc))
	hb.InputBatchRowCount = bat.RowCount()
	bat.Clean(proc.Mp())

	err := hb.BuildHashmap(false, false, false, proc)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
}

func TestDedupBuildKeepLastForReplace(t *testing.T) {
	hb := newTestHashmapBuilder(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	hb.IsDedup = true
	hb.DedupBuildKeepLast = true
	hb.OnDuplicateAction = plan.Node_FAIL
	hb.DedupColName = "id"
	hb.DedupColTypes = []plan.Type{newExpr(0, types.T_int32.ToType()).Typ}
	defer func() {
		hb.Reset(proc, true)
		hb.Free(proc)
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	bat := makeIntKeyValueBatch(proc, []int32{1, 1, 2}, []int32{10, 20, 30})
	require.NoError(t, hb.CopyBuildBatch(bat, proc))
	hb.InputBatchRowCount = bat.RowCount()
	bat.Clean(proc.Mp())

	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.Equal(t, 2, hb.InputBatchRowCount)
	require.Equal(t, 2, hb.Batches.RowCount())
	require.Equal(t, uint64(2), hb.GetGroupCount())

	out := hb.Batches.Buf[0]
	require.Equal(t, 2, out.RowCount())
	keys := vector.MustFixedColNoTypeCheck[int32](out.Vecs[0])[:out.RowCount()]
	values := vector.MustFixedColNoTypeCheck[int32](out.Vecs[1])[:out.RowCount()]
	require.Equal(t, []int32{1, 2}, keys)
	require.Equal(t, []int32{20, 30}, values)
}

func TestDedupBuildKeepLastPreservesDeleteOnlyRows(t *testing.T) {
	hb := newTestHashmapBuilder(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	hb.IsDedup = true
	hb.DedupBuildKeepLast = true
	hb.OnDuplicateAction = plan.Node_FAIL
	hb.DedupColName = "id"
	hb.DedupColTypes = []plan.Type{newExpr(0, types.T_int32.ToType()).Typ}
	defer func() {
		hb.Reset(proc, true)
		hb.Free(proc)
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, 2, []int32{2}, proc))
	bat := makeIntKeyValueBatchWithMarker(
		proc,
		[]int32{1, 1, 2},
		[]int32{10, 20, 30},
		[]int32{100, 0, 0},
		[]uint64{1, 2},
	)
	require.NoError(t, hb.CopyBuildBatch(bat, proc))
	hb.InputBatchRowCount = bat.RowCount()
	bat.Clean(proc.Mp())

	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.Equal(t, 3, hb.InputBatchRowCount)
	require.Equal(t, 3, hb.Batches.RowCount())
	require.Equal(t, uint64(2), hb.GetGroupCount())
	require.NotNil(t, hb.DelRows)
	require.True(t, hb.DelRows.Contains(2))

	out := hb.Batches.Buf[0]
	require.Equal(t, 3, out.RowCount())
	require.True(t, out.Vecs[0].IsNull(2))
	require.True(t, out.Vecs[1].IsNull(2))
	require.Falsef(t, out.Vecs[2].IsNull(2), "nulls=%v", out.Vecs[2].GetNulls().GetBitmap().String())
	markers := vector.MustFixedColNoTypeCheck[int32](out.Vecs[2])[:out.RowCount()]
	require.Equal(t, int32(100), markers[2])
}

func TestAccountedDedupScratchAndDeleteBitmapFollowJoinMapLifetime(t *testing.T) {
	const capBytes = uint64(64 << 20)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget := process.MustNewExecutionResourceBudget(capBytes, capBytes)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 256)
	require.NoError(t, err)
	account, err := registry.OpenWithController(capBytes, generation)
	require.NoError(t, err)

	var op HashBuild
	op.NeedHashMap = true
	require.NoError(t, op.SetAllocationAccount(account))
	hb := &op.ctr.hashmapBuilder
	hb.setBudget(generation)
	hb.IsDedup = true
	hb.DedupBuildKeepLast = true
	hb.OnDuplicateAction = plan.Node_FAIL
	hb.DedupColName = "id"
	hb.DedupColTypes = []plan.Type{newExpr(0, types.T_int32.ToType()).Typ}
	require.NoError(t, hb.Prepare(
		[]*plan.Expr{newExpr(0, types.T_int32.ToType())},
		-1,
		2,
		[]int32{2},
		proc,
	))
	input := makeIntKeyValueBatchWithMarker(
		proc,
		[]int32{1, 1, 2},
		[]int32{10, 20, 30},
		[]int32{100, 0, 0},
		[]uint64{1, 2},
	)
	require.NoError(t, hb.copyBuildBatch(input, proc))
	hb.InputBatchRowCount = input.RowCount()
	input.Clean(proc.Mp())

	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.DelRows)
	require.True(t, hb.DelRows.HasExternalStorage())
	require.True(t, hb.DelRows.Contains(2))
	require.Equal(t, account.Snapshot().Used, generation.Used())
	require.Positive(t, account.Snapshot().Used)

	jm := hb.GetJoinMap(proc.Mp())
	require.NotNil(t, jm)
	jm.IncRef(1)
	hb.Reset(proc, false)
	// DelRows remains physically owned by the consumer together with the map.
	require.Positive(t, account.Snapshot().Used)
	require.True(t, jm.IsDeleted(2))
	jm.Free()
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, generation.Used())
	require.NoError(t, op.ClearAllocationAccount(account))
	terminal, first, err := registry.CompleteTerminal(account)
	require.NoError(t, err)
	require.True(t, first)
	require.Equal(t, mpool.AllocationAccountTerminalValid, terminal.State)
}

func TestAccountedDedupBitmapExactBoundaryRollsBack(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	for _, tc := range []struct {
		name    string
		cap     uint64
		wantErr bool
	}{
		{name: "one byte short", cap: 7, wantErr: true},
		{name: "exact", cap: 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			budget := process.MustNewExecutionResourceBudget(tc.cap, tc.cap)
			generation, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			registry, err := mpool.NewAllocationAccountRegistry(1, 1)
			require.NoError(t, err)
			account, err := registry.OpenWithController(tc.cap, generation)
			require.NoError(t, err)
			hb := &HashmapBuilder{mapAllocationAccount: account}
			bm, err := hb.newDedupBitmap(
				64,
				proc.Mp(),
				HashBuildAllocationSiteDedupIgnoreBitmap,
			)
			if tc.wantErr {
				require.Error(t, err)
				require.True(t, IsRetryableMemoryCapacity(err))
				require.Nil(t, bm)
				require.Zero(t, account.Snapshot().Used)
			} else {
				require.NoError(t, err)
				require.Equal(t, uint64(8), account.Snapshot().Used)
				releaseDedupBitmap(bm, proc.Mp())
				require.Zero(t, account.Snapshot().Used)
			}
			terminal, _, err := registry.CompleteTerminal(account)
			require.NoError(t, err)
			require.Equal(t, mpool.AllocationAccountTerminalValid, terminal.State)
		})
	}
}

// TestDedupBuildKeepLastMarksConflictBucketForDiscardedFanout reproduces the
// REPLACE multi-UK fan-out case (issue #24428) at the hashbuild layer: one new
// row (same new PK) fans out to several build rows that carry DIFFERENT old
// PKs. keep-last keeps one and turns the others into delete-only rows. The
// surviving bucket must still be marked deleted (DelRows) when a discarded
// row's old PK equals the surviving row's new key, otherwise the dedup-join
// probe side raises a false DuplicateEntry for the existing row REPLACE removes.
func TestDedupBuildKeepLastMarksConflictBucketForDiscardedFanout(t *testing.T) {
	hb := newTestHashmapBuilder(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	hb.IsDedup = true
	hb.DedupBuildKeepLast = true
	hb.OnDuplicateAction = plan.Node_FAIL
	hb.DedupColName = "id"
	hb.DedupColTypes = []plan.Type{newExpr(0, types.T_int32.ToType()).Typ}
	defer func() {
		hb.Reset(proc, true)
		hb.Free(proc)
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	// keyCols = col0 (new PK); delColIdx = col1 (old PK); marker = col2 (old
	// row id). Empty keep-col list so BuildHashmap also preserves the old-PK
	// column on the delete-only rows.
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, 1, 2, nil, proc))
	// Three fan-out copies of new PK=1 that matched old rows with PK 1, 2, 3.
	bat := makeIntKeyValueBatchWithMarker(
		proc,
		[]int32{1, 1, 1},
		[]int32{1, 2, 3},
		[]int32{100, 200, 300},
		nil,
	)
	require.NoError(t, hb.CopyBuildBatch(bat, proc))
	hb.InputBatchRowCount = bat.RowCount()
	bat.Clean(proc.Mp())

	require.NoError(t, hb.BuildHashmap(false, false, false, proc))

	// One surviving row (index 0) plus two delete-only rows (index 1, 2).
	require.Equal(t, 3, hb.Batches.RowCount())
	require.NotNil(t, hb.DelRows)
	require.True(t, hb.DelRows.Contains(1), "delete-only row should be marked")
	require.True(t, hb.DelRows.Contains(2), "delete-only row should be marked")
	// The fix: the surviving bucket (index 0, new key = 1) is marked deleted
	// because a discarded fan-out row carried old PK = 1.
	require.True(t, hb.DelRows.Contains(0),
		"surviving bucket must be marked deleted via a discarded row's old PK")

	// Delete-only rows keep the old-PK column (col1) that drives that marking,
	// while their new-PK column (col0) is nulled so they only delete.
	out := hb.Batches.Buf[0]
	require.True(t, out.Vecs[0].IsNull(1))
	require.True(t, out.Vecs[0].IsNull(2))
	require.False(t, out.Vecs[1].IsNull(1))
	require.False(t, out.Vecs[1].IsNull(2))
	oldPks := vector.MustFixedColNoTypeCheck[int32](out.Vecs[1])[:out.RowCount()]
	require.ElementsMatch(t, []int32{1, 2}, []int32{oldPks[1], oldPks[2]})
}

func TestDedupBuildIgnoreOnlyMarksCandidateOwnOldKey(t *testing.T) {
	tests := []struct {
		name    string
		newKeys []int32
		oldKeys []int32
	}{
		{
			name:    "another candidate old key is not released",
			newKeys: []int32{4, 1},
			oldKeys: []int32{1, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hb := newTestHashmapBuilder(t)
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			hb.IsDedup = true
			hb.OnDuplicateAction = plan.Node_IGNORE
			defer func() {
				hb.Reset(proc, true)
				hb.Free(proc)
				require.Equal(t, int64(0), proc.Mp().CurrNB())
			}()

			require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, 1, -1, nil, proc))
			bat := makeIntKeyValueBatch(proc, tt.newKeys, tt.oldKeys)
			require.NoError(t, hb.CopyBuildBatch(bat, proc))
			hb.InputBatchRowCount = bat.RowCount()
			bat.Clean(proc.Mp())

			require.NoError(t, hb.BuildHashmap(false, false, false, proc))
			require.NotNil(t, hb.DelRows)
			require.Zero(t, hb.DelRows.Count())
		})
	}
}

func TestDedupBuildIgnorePrefersOriginalKeyOwner(t *testing.T) {
	for _, oldKeys := range [][]int32{{1, 2}, {2, 1}} {
		t.Run(strings.Join([]string{strconv.Itoa(int(oldKeys[0])), strconv.Itoa(int(oldKeys[1]))}, "_"), func(t *testing.T) {
			hb := newTestHashmapBuilder(t)
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			hb.IsDedup = true
			hb.OnDuplicateAction = plan.Node_IGNORE
			defer func() {
				hb.Reset(proc, true)
				hb.Free(proc)
				require.Equal(t, int64(0), proc.Mp().CurrNB())
			}()

			require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, 1, -1, nil, proc))
			bat := makeIntKeyValueBatch(proc, []int32{2, 2}, oldKeys)
			require.NoError(t, hb.CopyBuildBatch(bat, proc))
			hb.InputBatchRowCount = bat.RowCount()
			bat.Clean(proc.Mp())

			require.NoError(t, hb.BuildHashmap(false, false, false, proc))
			require.Equal(t, 1, hb.Batches.RowCount())
			keptOldKeys := vector.MustFixedColNoTypeCheck[int32](hb.Batches.Buf[0].Vecs[1])
			require.Equal(t, []int32{2}, keptOldKeys[:hb.Batches.RowCount()])
			require.NotNil(t, hb.DelRows)
			require.True(t, hb.DelRows.Contains(0), "delRows=%s count=%d", hb.DelRows.String(), hb.DelRows.Count())
		})
	}
}

func TestDedupBuildIgnoreRebuildsAfterOwnerReplacement(t *testing.T) {
	hb := newTestHashmapBuilder(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	hb.IsDedup = true
	hb.OnDuplicateAction = plan.Node_IGNORE
	defer func() {
		hb.Reset(proc, true)
		hb.Free(proc)
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, 1, -1, nil, proc))
	bat := makeIntKeyValueBatch(proc, []int32{2, 1, 2}, []int32{1, 3, 2})
	require.NoError(t, hb.CopyBuildBatch(bat, proc))
	hb.InputBatchRowCount = bat.RowCount()
	bat.Clean(proc.Mp())

	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.Equal(t, 2, hb.Batches.RowCount())
	keys := vector.MustFixedColNoTypeCheck[int32](hb.Batches.Buf[0].Vecs[0])
	oldKeys := vector.MustFixedColNoTypeCheck[int32](hb.Batches.Buf[0].Vecs[1])
	require.Equal(t, []int32{1, 2}, keys[:hb.Batches.RowCount()])
	require.Equal(t, []int32{3, 2}, oldKeys[:hb.Batches.RowCount()])
	require.NotNil(t, hb.DelRows)
	require.False(t, hb.DelRows.Contains(0))
	require.True(t, hb.DelRows.Contains(1), "delRows=%s count=%d", hb.DelRows.String(), hb.DelRows.Count())
}

func TestBuildHashmapErrorDoesNotLeakIterators(t *testing.T) {
	hb := newTestHashmapBuilder(t)
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)

	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))

	// Inject a failing executor to trigger an error during evalJoinCondition.
	hb.executors = []colexec.ExpressionExecutor{failingExecutor{}}
	hb.InputBatchRowCount = 1
	hb.Batches.Buf = []*batch.Batch{batch.NewWithSize(1)}
	hb.Batches.Buf[0].Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	require.Error(t, hb.BuildHashmap(false, false, false, proc))
	require.Nil(t, hb.cachedIntIterator)
	require.Nil(t, hb.cachedStrIterator)

	// After failure, a normal path should still succeed and populate cache.
	hb.executors = nil
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	hb.InputBatchRowCount = 1
	hb.Batches.Reset()
	hb.Batches.Buf = nil
	intVec := testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	intBat := batch.New([]string{"col"})
	intBat.SetVector(0, intVec)
	intBat.SetRowCount(1)
	require.NoError(t, hb.CopyBuildBatch(intBat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedIntIterator)
}

func TestBuildHashmapReuseUniqueSelsBuffer(t *testing.T) {
	hb := newTestHashmapBuilder(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))

	bat := makeIntBatch(t, 4, proc)
	defer bat.Clean(proc.Mp())

	// First build: should allocate uniqueSels
	hb.InputBatchRowCount = bat.RowCount()
	require.NoError(t, hb.CopyBuildBatch(bat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, true, proc))
	require.NotNil(t, hb.uniqueSels)
	require.Greater(t, cap(hb.uniqueSels), 0)
	require.Greater(t, len(hb.uniqueSels), 0)
	firstPtr := &hb.uniqueSels[0]

	hb.Reset(proc, true)
	hb.executors = nil

	// Second build: reuse same buffer
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	hb.InputBatchRowCount = bat.RowCount()
	hb.Batches.Reset()
	hb.Batches.Buf = nil
	require.NoError(t, hb.CopyBuildBatch(bat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, true, proc))
	require.NotNil(t, hb.uniqueSels)
	require.Greater(t, len(hb.uniqueSels), 0)
	require.Equal(t, firstPtr, &hb.uniqueSels[0])
}

func TestBuildHashmapDoesNotCreateUniqueSelsWhenNotNeeded(t *testing.T) {
	hb := newTestHashmapBuilder(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))

	bat := makeIntBatch(t, 2, proc)
	defer bat.Clean(proc.Mp())

	hb.InputBatchRowCount = bat.RowCount()
	require.NoError(t, hb.CopyBuildBatch(bat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.Nil(t, hb.uniqueSels, "should not allocate uniqueSels when needUniqueVec is false")
}

func TestCachedStrIteratorOwnerClearedBeforeReuse(t *testing.T) {
	hb := newTestHashmapBuilder(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	// Build once to create cached str iterator.
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_varchar.ToType())}, -1, -1, nil, proc))
	bat := makeStrBatch(t, 4, proc)
	defer bat.Clean(proc.Mp())
	hb.InputBatchRowCount = bat.RowCount()
	require.NoError(t, hb.CopyBuildBatch(bat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedStrIterator)

	// Bind to a stale map.
	staleMap, err := hashmap.NewStrHashMap(false, proc.Mp())
	require.NoError(t, err)
	hashmap.IteratorChangeOwner(hb.cachedStrIterator, staleMap)

	// Next build should clear stale owner.
	hb.Reset(proc, true)
	hb.executors = nil
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_varchar.ToType())}, -1, -1, nil, proc))
	hb.InputBatchRowCount = bat.RowCount()
	hb.Batches.Reset()
	hb.Batches.Buf = nil
	require.NoError(t, hb.CopyBuildBatch(bat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))

	rv := reflect.ValueOf(hb.cachedStrIterator).Elem()
	mpField := rv.FieldByName("mp")
	require.False(t, mpField.IsNil())
	require.NotEqual(t, reflect.ValueOf(staleMap).Pointer(), mpField.Pointer())
}

func TestSwitchKeyTypeCreatesCorrectIterator(t *testing.T) {
	hb := newTestHashmapBuilder(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	// Build int first.
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	intBat := makeIntBatch(t, 2, proc)
	defer intBat.Clean(proc.Mp())
	hb.InputBatchRowCount = intBat.RowCount()
	require.NoError(t, hb.CopyBuildBatch(intBat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedIntIterator)

	// Switch to varchar keys; cached str should be created and usable.
	hb.Reset(proc, true)
	hb.executors = nil
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_varchar.ToType())}, -1, -1, nil, proc))
	strBat := makeStrBatch(t, 2, proc)
	defer strBat.Clean(proc.Mp())
	hb.InputBatchRowCount = strBat.RowCount()
	hb.Batches.Reset()
	hb.Batches.Buf = nil
	require.NoError(t, hb.CopyBuildBatch(strBat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedStrIterator)
}

func TestCachedIteratorOwnerClearedBeforeReuse(t *testing.T) {
	hb := newTestHashmapBuilder(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	// Build once to create cached int iterator and bind to map A.
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	bat := makeIntBatch(t, 4, proc)
	defer bat.Clean(proc.Mp())
	hb.InputBatchRowCount = bat.RowCount()
	require.NoError(t, hb.CopyBuildBatch(bat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedIntIterator)

	// Manually bind iterator to a different map to simulate stale owner.
	staleMap, err := hashmap.NewIntHashMap(false, proc.Mp())
	require.NoError(t, err)
	hashmap.IteratorChangeOwner(hb.cachedIntIterator, staleMap)

	// Next build should clear stale owner and rebind to fresh map, not panic.
	hb.Reset(proc, true)
	hb.executors = nil
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	hb.InputBatchRowCount = bat.RowCount()
	hb.Batches.Reset()
	hb.Batches.Buf = nil
	require.NoError(t, hb.CopyBuildBatch(bat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))

	// Owner should now be non-nil and point to the new map (i.e., not staleMap).
	rv := reflect.ValueOf(hb.cachedIntIterator).Elem()
	mpField := rv.FieldByName("mp")
	require.False(t, mpField.IsNil())
	require.NotEqual(t, reflect.ValueOf(staleMap).Pointer(), mpField.Pointer())
}

func TestFreeThenBuildRepopulatesCache(t *testing.T) {
	hb := newTestHashmapBuilder(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	// First build to populate cache.
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	intVec := testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
	intBat := batch.New([]string{"col"})
	intBat.SetVector(0, intVec)
	intBat.SetRowCount(2)
	hb.InputBatchRowCount = intBat.RowCount()
	require.NoError(t, hb.CopyBuildBatch(intBat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedIntIterator)

	// Free should clear cache.
	hb.Free(proc)
	require.Nil(t, hb.cachedIntIterator)
	require.Nil(t, hb.cachedStrIterator)

	// Build again after Free should succeed and repopulate cache.
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	hb.InputBatchRowCount = intBat.RowCount()
	require.NoError(t, hb.CopyBuildBatch(intBat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedIntIterator)
}

// failingExecutor always returns an error; used to simulate BuildHashmap failure paths.
type failingExecutor struct{}

func (f failingExecutor) Eval(*process.Process, []*batch.Batch, []bool) (*vector.Vector, error) {
	return nil, moerr.NewInternalErrorNoCtx("exec failed")
}
func (f failingExecutor) EvalWithoutResultReusing(*process.Process, []*batch.Batch, []bool) (*vector.Vector, error) {
	return nil, moerr.NewInternalErrorNoCtx("exec failed")
}
func (f failingExecutor) IsColumnExpr() bool { return false }
func (f failingExecutor) TypeName() string   { return "failingExecutor" }
func (f failingExecutor) Free()              {}
func (f failingExecutor) ResetForNextQuery() {}

// Benchmarks: cached vs new iterator paths for int/str.
func BenchmarkBuildHashmapCachedInt(b *testing.B) {
	proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
	hb := newTestHashmapBuilder(b)
	require.NoError(b, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	data := makeIntBatch(b, 1024, proc)
	defer data.Clean(proc.Mp())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hb.InputBatchRowCount = data.RowCount()
		require.NoError(b, hb.CopyBuildBatch(data, proc))
		require.NoError(b, hb.BuildHashmap(false, false, false, proc))
		hb.Reset(proc, true)
	}
}

func BenchmarkBuildHashmapCachedStr(b *testing.B) {
	proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
	hb := newTestHashmapBuilder(b)
	require.NoError(b, hb.Prepare([]*plan.Expr{newExpr(0, types.T_varchar.ToType())}, -1, -1, nil, proc))
	data := makeStrBatch(b, 1024, proc)
	defer data.Clean(proc.Mp())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hb.InputBatchRowCount = data.RowCount()
		require.NoError(b, hb.CopyBuildBatch(data, proc))
		require.NoError(b, hb.BuildHashmap(false, false, false, proc))
		hb.Reset(proc, true)
	}
}

func makeIntBatch(tb testing.TB, n int, proc *process.Process) *batch.Batch {
	ints := make([]int32, n)
	for i := 0; i < n; i++ {
		ints[i] = int32(i)
	}
	vec := testutil.MakeInt32Vector(ints, nil, proc.Mp())
	bat := batch.New([]string{"col"})
	bat.SetVector(0, vec)
	bat.SetRowCount(n)
	return bat
}

func makeStrBatch(tb testing.TB, n int, proc *process.Process) *batch.Batch {
	vec := vector.NewVec(types.T_varchar.ToType())
	for i := 0; i < n; i++ {
		require.NoError(tb, vector.AppendBytes(vec, []byte("v"+strconv.Itoa(i)), false, proc.Mp()))
	}
	bat := batch.New([]string{"col"})
	bat.SetVector(0, vec)
	bat.SetRowCount(n)
	return bat
}

func makeIntKeyValueBatch(proc *process.Process, keys []int32, values []int32) *batch.Batch {
	keyVec := testutil.MakeInt32Vector(keys, nil, proc.Mp())
	valueVec := testutil.MakeInt32Vector(values, nil, proc.Mp())
	bat := batch.New([]string{"id", "v"})
	bat.SetVector(0, keyVec)
	bat.SetVector(1, valueVec)
	bat.SetRowCount(len(keys))
	return bat
}

func makeIntKeyValueBatchWithMarker(
	proc *process.Process,
	keys []int32,
	values []int32,
	markers []int32,
	markerNulls []uint64,
) *batch.Batch {
	keyVec := testutil.MakeInt32Vector(keys, nil, proc.Mp())
	valueVec := testutil.MakeInt32Vector(values, nil, proc.Mp())
	markerVec := testutil.MakeInt32Vector(markers, markerNulls, proc.Mp())
	bat := batch.New([]string{"id", "v", "old_row_id"})
	bat.SetVector(0, keyVec)
	bat.SetVector(1, valueVec)
	bat.SetVector(2, markerVec)
	bat.SetRowCount(len(keys))
	return bat
}

// Cold path benchmarks: recreate builder each iteration (no cached iterator reuse).
func BenchmarkBuildHashmapColdInt(b *testing.B) {
	proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
	data := makeIntBatch(b, 1024, proc)
	defer data.Clean(proc.Mp())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hb := newTestHashmapBuilder(b)
		require.NoError(b, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
		hb.InputBatchRowCount = data.RowCount()
		require.NoError(b, hb.CopyBuildBatch(data, proc))
		require.NoError(b, hb.BuildHashmap(false, false, false, proc))
		hb.Free(proc)
	}
}

func BenchmarkBuildHashmapColdStr(b *testing.B) {
	proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
	data := makeStrBatch(b, 1024, proc)
	defer data.Clean(proc.Mp())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hb := newTestHashmapBuilder(b)
		require.NoError(b, hb.Prepare([]*plan.Expr{newExpr(0, types.T_varchar.ToType())}, -1, -1, nil, proc))
		hb.InputBatchRowCount = data.RowCount()
		require.NoError(b, hb.CopyBuildBatch(data, proc))
		require.NoError(b, hb.BuildHashmap(false, false, false, proc))
		hb.Free(proc)
	}
}

func BenchmarkCopyBuildBatchAccounting(b *testing.B) {
	const capBytes = uint64(256 << 20)
	proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
	defer proc.Free()
	input := testutil.NewBatch(
		[]types.Type{types.T_int32.ToType(), types.T_varchar.ToType()},
		true,
		colexec.DefaultBatchSize,
		proc.Mp(),
	)
	defer input.Clean(proc.Mp())
	budget := process.MustNewExecutionResourceBudget(capBytes, capBytes)
	generation, err := budget.OpenGeneration(1)
	if err != nil {
		b.Fatal(err)
	}
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	if err != nil {
		b.Fatal(err)
	}
	account, err := registry.OpenWithController(capBytes, generation)
	if err != nil {
		b.Fatal(err)
	}
	hb := &HashmapBuilder{}
	if err = hb.SetAllocationAccount(account); err != nil {
		b.Fatal(err)
	}
	hb.setBudget(generation)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err = hb.copyBuildBatch(input, proc); err != nil {
			b.Fatal(err)
		}
		hb.cleanBatches(proc)
	}
	b.StopTimer()
	if generation.Used() != 0 {
		b.Fatalf("generation used = %d", generation.Used())
	}
	if err = hb.ClearAllocationAccount(account); err != nil {
		b.Fatal(err)
	}
	if _, _, err = registry.CompleteTerminal(account); err != nil {
		b.Fatal(err)
	}
}

// BenchmarkResidentHashBuildAccounting compares a local physical account with
// the production shared budget controller across the complete resident owner
// closure: copied batches, key expression, hash cells/descriptors, and terminal
// release all run on every iteration. The builder intentionally has no
// unaccounted mode.
// The 32-row case models high-frequency TP statements; 8,192 rows exercises a
// full physical batch without entering spill.
func BenchmarkResidentHashBuildAccounting(b *testing.B) {
	const capBytes = uint64(512 << 20)
	for _, rows := range []int{32, colexec.DefaultBatchSize} {
		for _, stringKey := range []bool{false, true} {
			kind := "int"
			if stringKey {
				kind = "varchar"
			}
			for _, controlled := range []bool{false, true} {
				mode := "local-account"
				if controlled {
					mode = "budget-controlled"
				}
				b.Run(fmt.Sprintf("%s/rows-%d/%s", kind, rows, mode), func(b *testing.B) {
					proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
					defer proc.Free()
					var input *batch.Batch
					var keyType types.Type
					if stringKey {
						input = makeStrBatch(b, rows, proc)
						keyType = types.T_varchar.ToType()
					} else {
						input = makeIntBatch(b, rows, proc)
						keyType = types.T_int32.ToType()
					}
					defer input.Clean(proc.Mp())

					var generation *process.ExecutionResourceGeneration
					registry, err := mpool.NewAllocationAccountRegistry(1, 4_096)
					if err != nil {
						b.Fatal(err)
					}
					var account *mpool.AllocationAccount
					if controlled {
						budget := process.MustNewExecutionResourceBudget(capBytes, capBytes)
						generation, err = budget.OpenGeneration(1)
						if err != nil {
							b.Fatal(err)
						}
						account, err = registry.OpenWithController(capBytes, generation)
						if err != nil {
							b.Fatal(err)
						}
					} else {
						account, err = registry.Open(capBytes)
						if err != nil {
							b.Fatal(err)
						}
					}

					b.ReportAllocs()
					b.SetBytes(int64(input.Size()))
					b.ResetTimer()
					for range b.N {
						hb := &HashmapBuilder{}
						if controlled {
							hb.SetBudget(generation)
						}
						if err := hb.SetAllocationAccount(account); err != nil {
							b.Fatal(err)
						}
						if err := hb.Prepare(
							[]*plan.Expr{newExpr(0, keyType)},
							-1,
							-1,
							nil,
							proc,
						); err != nil {
							b.Fatal(err)
						}
						hb.InputBatchRowCount = input.RowCount()
						if err := hb.CopyBuildBatch(input, proc); err != nil {
							b.Fatal(err)
						}
						if err := hb.BuildHashmap(false, false, false, proc); err != nil {
							b.Fatal(err)
						}
						hb.Free(proc)
					}
					b.StopTimer()
					if account.Snapshot().Used != 0 {
						b.Fatalf("account used = %d", account.Snapshot().Used)
					}
					if _, _, err := registry.CompleteTerminal(account); err != nil {
						b.Fatal(err)
					}
					if controlled {
						if generation.Used() != 0 {
							b.Fatalf("generation used = %d", generation.Used())
						}
						generation.Close()
					}
				})
			}
		}
	}
}

func TestExtractRestoreCachedIterators(t *testing.T) {
	hb := newTestHashmapBuilder(t)
	mp := mpool.MustNewZero()

	intMap, err := hashmap.NewIntHashMap(false, mp)
	require.NoError(t, err)
	strMap, err := hashmap.NewStrHashMap(false, mp)
	require.NoError(t, err)

	hb.cachedIntIterator = intMap.NewIterator()
	hb.cachedStrIterator = strMap.NewIterator()

	intItr, strItr := hb.ExtractCachedIteratorsForReuse()
	require.Nil(t, hb.cachedIntIterator)
	require.Nil(t, hb.cachedStrIterator)

	// Owners should be cleared after extraction.
	rvInt := reflect.ValueOf(intItr).Elem()
	require.True(t, rvInt.FieldByName("mp").IsNil())
	rvStr := reflect.ValueOf(strItr).Elem()
	require.True(t, rvStr.FieldByName("mp").IsNil())

	hb.RestoreCachedIterators(intItr, strItr)
	require.Same(t, intItr, hb.cachedIntIterator)
	require.Same(t, strItr, hb.cachedStrIterator)
}

func TestStrIteratorLargeStringTriggersPrune(t *testing.T) {
	hb := newTestHashmapBuilder(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_varchar.ToType())}, -1, -1, nil, proc))

	// Build a batch with one very large string to bloat iterator buffers.
	vec := vector.NewVec(types.T_varchar.ToType())
	large := strings.Repeat("x", hashmap.MaxStrIteratorCapacity+2048)
	require.NoError(t, vector.AppendBytes(vec, []byte(large), false, proc.Mp()))
	bat := batch.New([]string{"col"})
	bat.SetVector(0, vec)
	bat.SetRowCount(1)

	hb.InputBatchRowCount = bat.RowCount()
	require.NoError(t, hb.CopyBuildBatch(bat, proc))

	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedStrIterator)
	require.Greater(t, hashmap.StrIteratorCapacity(hb.cachedStrIterator), hashmap.MaxStrIteratorCapacity)

	hb.Reset(proc, true)
	require.Nil(t, hb.cachedStrIterator, "iterator with oversized buffers should be dropped")
}

// TestResetWithNilPointers tests that Reset() handles nil pointers gracefully
// This is a regression test for the panic fix where Reset() would crash when
// curVecs or UniqueJoinKeys contained nil pointers.
func TestResetWithNilPointers(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	hb := newTestHashmapBuilder(t)

	// Test case 1: curVecs with nil pointers and needDupVec = true
	hb.needDupVec = true
	hb.curVecs = make([]*vector.Vector, 2)
	hb.curVecs[0] = nil
	hb.curVecs[1] = nil

	// Test case 2: UniqueJoinKeys with nil pointers
	hb.UniqueJoinKeys = make([]*vector.Vector, 3)
	hb.UniqueJoinKeys[0] = nil
	hb.UniqueJoinKeys[1] = nil
	hb.UniqueJoinKeys[2] = nil

	// Reset should not panic
	hb.Reset(proc, true)
	require.Nil(t, hb.curVecs)
	require.Nil(t, hb.UniqueJoinKeys)
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestResetWithMixedNilAndValidPointers tests Reset() with a mix of nil and valid vectors
func TestResetWithMixedNilAndValidPointers(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	hb := newTestHashmapBuilder(t)

	// Create some valid vectors
	vec1 := testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
	vec2 := testutil.MakeInt32Vector([]int32{4, 5, 6}, nil, proc.Mp())

	// Test case: curVecs with mix of nil and valid vectors
	hb.needDupVec = true
	hb.curVecs = []*vector.Vector{vec1, nil, vec2}

	// Test case: UniqueJoinKeys with mix of nil and valid vectors
	hb.UniqueJoinKeys = []*vector.Vector{nil}

	// Reset should free valid vectors and not panic on nil
	hb.Reset(proc, true)
	require.Nil(t, hb.curVecs)
	require.Nil(t, hb.UniqueJoinKeys)
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestFreeWithNilPointers tests that Free() handles nil pointers gracefully
func TestFreeWithNilPointers(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	hb := newTestHashmapBuilder(t)

	// Test case: UniqueJoinKeys with nil pointers
	hb.UniqueJoinKeys = make([]*vector.Vector, 3)
	hb.UniqueJoinKeys[0] = nil
	hb.UniqueJoinKeys[1] = nil
	hb.UniqueJoinKeys[2] = nil

	// Free should not panic
	hb.Free(proc)
	require.Nil(t, hb.UniqueJoinKeys)
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestFreeWithMixedNilAndValidPointers tests Free() with a mix of nil and valid vectors
func TestFreeWithMixedNilAndValidPointers(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	hb := newTestHashmapBuilder(t)

	// Create some valid vectors
	vec1 := testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
	vec2 := testutil.MakeInt32Vector([]int32{4, 5, 6}, nil, proc.Mp())

	// Test case: UniqueJoinKeys with mix of nil and valid vectors
	hb.UniqueJoinKeys = []*vector.Vector{vec1, nil, vec2}

	// Free should free valid vectors and not panic on nil
	hb.Free(proc)
	require.Nil(t, hb.UniqueJoinKeys)
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}
