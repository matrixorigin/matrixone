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

package hashbuild

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bufferlease"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type spillTestHarness struct {
	op         *HashBuild
	proc       *process.Process
	budget     *process.ExecutionResourceBudget
	generation *process.ExecutionResourceGeneration
	registry   *mpool.AllocationAccountRegistry
	account    *mpool.AllocationAccount
	files      []*os.File
}

func newSpillTestHarness(t *testing.T, limit uint64) *spillTestHarness {
	t.Helper()
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	budget := process.MustNewExecutionResourceBudget(limit, limit)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 256)
	require.NoError(t, err)
	account, err := registry.OpenWithController(limit, generation)
	require.NoError(t, err)
	op := &HashBuild{NeedHashMap: true}
	require.NoError(t, op.SetAllocationAccount(account))
	op.ctr.hashmapBuilder.setBudget(generation)
	op.ctr.spillUUID = strings.ReplaceAll(t.Name(), "/", "_")
	return &spillTestHarness{
		op:         op,
		proc:       proc,
		budget:     budget,
		generation: generation,
		registry:   registry,
		account:    account,
		files:      make([]*os.File, spillNumBuckets),
	}
}

func TestDirectSpillKeepsAdmittedRecoveryUntilBuildEnds(t *testing.T) {
	for _, cancelAtEnd := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%t", cancelAtEnd), func(t *testing.T) {
			h := newSpillTestHarness(t, 8<<20)
			t.Cleanup(func() {
				h.op.ctr.hashmapBuilder.Free(h.proc)
				h.op.ctr.dropSpillScratchBuffers()
				require.NoError(t, h.op.releaseRecoveryCapacity(h.account, true))
				h.close(t)
				require.Zero(t, h.proc.Mp().CurrNB())
			})
			h.op.IsShuffle = true
			h.op.JoinMapRefCnt = 1
			h.op.Conditions = []*plan.Expr{newExpr(0, types.T_int64.ToType())}
			h.op.ctr.setSpillThreshold(0)
			require.NoError(t, h.op.installRecoveryCapacity(h.generation))
			require.NoError(t, h.op.ctr.hashmapBuilder.Prepare(h.op.Conditions, 0, 0, nil, h.proc))
			ctx, cancel := context.WithCancelCause(h.proc.Ctx)
			h.proc.Ctx = ctx
			t.Cleanup(func() { cancel(context.Canceled) })
			child := colexec.NewMockOperator()
			t.Cleanup(func() { child.Free(h.proc, false, nil) })
			for _, value := range []int64{1, 2} {
				input := batch.NewWithSize(1)
				child.WithBatchs([]*batch.Batch{input})
				input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
				require.NoError(t, vector.AppendFixed(input.Vecs[0], value, false, h.proc.Mp()))
				input.SetRowCount(1)
			}
			var reserved uint64
			child.WithBatchCallback(func(index int) {
				if index == 1 {
					require.Len(t, h.op.ctr.hashmapBuilder.Batches.Buf, 1)
					reserved, _ = h.op.ctr.recoveryCapacity.Snapshot()
					require.Positive(t, reserved)
					// New allocations are denied; borrowing already-admitted
					// recovery bytes must still drain both retained and direct input.
					require.NoError(t, h.budget.UpdateAggregateCap(1))
				}
			})
			ended := false
			child.WithEndOfDataCallback(func() {
				ended = true
				require.NotNil(t, h.op.ctr.recoveryCapacity)
				capacity, _ := h.op.ctr.recoveryCapacity.Snapshot()
				require.Equal(t, reserved, capacity, "continuation must reuse, not grow, its admitted floor")
				if cancelAtEnd {
					cancel(context.Canceled)
				}
			})
			h.op.SetChildren([]vm.Operator{child})
			err := h.op.build(h.proc, process.NewAnalyzer(0, false, false, "hash build"))
			if cancelAtEnd {
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, h.op.ctr.spillBundle)
				require.Zero(t, h.generation.SpillDiskUsed())
			} else {
				require.NoError(t, err)
				require.Equal(t, int64(2), spillFileRows(t, h))
			}
			require.True(t, ended)
			require.Nil(t, h.op.ctr.recoveryCapacity)
			require.Zero(t, h.account.Snapshot().Used)
			require.Zero(t, h.generation.Used())
		})
	}
}

func (h *spillTestHarness) close(t *testing.T) {
	t.Helper()
	h.op.ctr.dropSpillScratchBuffers()
	h.op.ctr.freeSpillExprExecs()
	for _, file := range h.files {
		if file != nil {
			require.NoError(t, file.Close())
		}
	}
	if h.op.ctr.spillBundle != nil {
		h.op.ctr.spillBundle.release()
		h.op.ctr.spillBundle = nil
	}
	require.Zero(t, h.account.Snapshot().Used)
	require.Zero(t, h.generation.Used())
	require.NoError(t, h.op.ClearAllocationAccount(h.account))
	terminal, first, err := h.registry.CompleteTerminal(h.account)
	require.NoError(t, err)
	require.True(t, first)
	require.Equal(t, mpool.AllocationAccountTerminalValid, terminal.State)
	h.proc.Free()
}

func spillFileRows(t *testing.T, h *spillTestHarness) int64 {
	t.Helper()
	require.NotNil(t, h.op.ctr.spillBundle)
	h.op.ctr.spillBundle.mu.Lock()
	entries := make([]*spillFileEntry, 0, len(h.op.ctr.spillBundle.entries))
	for _, entry := range h.op.ctr.spillBundle.entries {
		entries = append(entries, entry)
	}
	h.op.ctr.spillBundle.mu.Unlock()
	var total int64
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		file, err := entry.fs.OpenFile(context.Background(), entry.name)
		require.NoError(t, err)
		defer func() { require.NoError(t, file.Close()) }()
		_, err = file.Seek(0, io.SeekStart)
		require.NoError(t, err)
		reader := bufio.NewReader(file)
		for {
			var header [16]byte
			_, err = io.ReadFull(reader, header[:])
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			rows := types.DecodeInt64(header[:8])
			payload := types.DecodeInt64(header[8:])
			require.GreaterOrEqual(t, rows, int64(0))
			require.GreaterOrEqual(t, payload, int64(0))
			_, err = io.CopyN(io.Discard, reader, payload)
			require.NoError(t, err)
			var magic [8]byte
			_, err = io.ReadFull(reader, magic[:])
			require.NoError(t, err)
			require.Equal(t, uint64(spillMagic), types.DecodeUint64(magic[:]))
			total += rows
		}
	}
	return total
}

func TestComputeXXHashBuild(t *testing.T) {
	mp := mpool.MustNewZero()
	first := testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, mp)
	second := testutil.MakeVarcharVector([]string{"a", "b", "c"}, nil, mp)
	defer first.Free(mp)
	defer second.Free(mp)
	hashes := make([]uint64, 3)
	computeXXHash([]*vector.Vector{first, second}, hashes)
	require.NotEqual(t, hashes[0], hashes[1])

	constant := testutil.MakeInt32Vector([]int32{5}, nil, mp)
	defer constant.Free(mp)
	constant.SetClass(vector.CONSTANT)
	computeXXHash([]*vector.Vector{constant}, hashes)
	require.Equal(t, hashes[0], hashes[1])
	require.Equal(t, hashes[1], hashes[2])
}

func TestShouldSpillBatches(t *testing.T) {
	bat := batch.NewWithSize(0)
	bat.SetRowCount(2)
	op := &HashBuild{IsShuffle: true, NeedHashMap: true}
	op.ctr.setSpillThreshold(1)
	op.ctr.hashmapBuilder.Batches.Buf = []*batch.Batch{bat}
	op.ctr.hashmapBuilder.InputBatchRowCount = bat.RowCount()
	require.True(t, op.shouldSpillBatches())
	op.IsShuffle = false
	require.False(t, op.shouldSpillBatches())
	op.IsShuffle = true
	op.NeedHashMap = false
	require.False(t, op.shouldSpillBatches())
}

func TestAutoSpillUsesLiveExecutionHeadroom(t *testing.T) {
	budget := process.MustNewExecutionResourceBudget(512*mpool.MB, 512*mpool.MB)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	first, err := generation.RegisterMemoryGrowthParticipant()
	require.NoError(t, err)
	second, err := generation.RegisterMemoryGrowthParticipant()
	require.NoError(t, err)
	defer first.Release()
	defer second.Release()

	op := &HashBuild{IsShuffle: true, NeedHashMap: true}
	op.ctr.setSpillThreshold(0)
	op.ctr.memoryGrowthParticipant = first
	spill, err := op.shouldSpillBeforeRetain(64*mpool.MB, nil)
	require.NoError(t, err)
	require.False(t, spill)

	retained, err := generation.ReserveTransientMemory(300 * mpool.MB)
	require.NoError(t, err)
	defer retained.Release()
	spill, err = op.shouldSpillBeforeRetain(140*mpool.MB, nil)
	require.NoError(t, err)
	require.True(t, spill)
	require.True(t, op.ctr.autoSpillTriggered)
	require.Equal(t, uint64(106*mpool.MB), op.ctr.autoSpillLimitAtTrigger)
}

func TestEstimatedResidentBuildBytesUsesRowCountUpperBound(t *testing.T) {
	builder := &HashmapBuilder{keyWidth: 8}
	got, err := builder.EstimatedHashMapBytes(513)
	require.NoError(t, err)
	require.Equal(t, hashtable.EstimateInt64HashMapSize(513), got)

	builder.keyWidth = 9
	got, err = builder.EstimatedHashMapBytes(513)
	require.NoError(t, err)
	require.Equal(t, hashtable.EstimateStringHashMapSize(513), got)

	got, err = builder.EstimatedResidentBuildBytes(math.MaxUint64, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(math.MaxUint64), got)

	_, err = builder.EstimatedHashMapBytes(-1)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
	var nilBuilder *HashmapBuilder
	_, err = nilBuilder.EstimatedHashMapBytes(1)
	require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)
}

func BenchmarkAutoSpillRetainedProjection(b *testing.B) {
	proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
	defer proc.Free()
	input := testutil.NewBatch([]types.Type{types.T_int64.ToType()}, true, colexec.DefaultBatchSize, proc.Mp())
	defer input.Clean(proc.Mp())
	executor, err := colexec.NewExpressionExecutor(proc, newExpr(0, types.T_int64.ToType()))
	require.NoError(b, err)
	defer executor.Free()
	for _, retainedBatches := range []int{128, 1024, 8192} {
		b.Run(fmt.Sprintf("batches=%d", retainedBatches), func(b *testing.B) {
			budget := process.MustNewExecutionResourceBudget(1<<40, 1<<40)
			generation, err := budget.OpenGeneration(1)
			require.NoError(b, err)
			participant, err := generation.RegisterMemoryGrowthParticipant()
			require.NoError(b, err)
			defer participant.Release()
			op := &HashBuild{IsShuffle: true, NeedHashMap: true}
			op.ctr.setSpillThreshold(0)
			op.ctr.memoryGrowthParticipant = participant
			hb := &op.ctr.hashmapBuilder
			hb.keyWidth = 8
			hb.executors = []colexec.ExpressionExecutor{executor}
			hb.Batches.Buf = make([]*batch.Batch, retainedBatches)
			for i := range hb.Batches.Buf {
				hb.Batches.Buf[i] = input
			}
			hb.Batches.MemSize = int64(input.Size()) * int64(retainedBatches)
			hb.InputBatchRowCount = input.RowCount() * (retainedBatches + 1)
			inputBytes := int64(input.Size())
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := op.shouldSpillBeforeRetain(inputBytes, input); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestAutoSpillProjectsIncomingGroupingKeys(t *testing.T) {
	for _, tc := range []struct {
		name     string
		column   int
		row      uint64
		want     bool
		borrowed bool
	}{
		{name: "first key sentinel", column: 0, row: 0, want: true},
		{name: "non-key sentinel", column: 1, row: 0},
		{name: "out-of-range key bit", column: 0, row: 513},
		{name: "borrowed key sentinel", column: 0, want: true, borrowed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			input := testutil.NewBatch([]types.Type{types.T_int64.ToType(), types.T_int64.ToType()}, true, 513, proc.Mp())
			defer input.Clean(proc.Mp())
			if tc.borrowed {
				validity := make([]byte, (input.RowCount()+7)/8)
				for i := range validity {
					validity[i] = 0xff
				}
				validity[0] &^= 1
				lease, err := bufferlease.NewRefCounted(validity, int64(len(validity)), nil)
				require.NoError(t, err)
				defer lease.Release()
				require.NoError(t, input.Vecs[0].GetGrouping().InstallBorrowedValidity(validity, 0, input.RowCount(), 1, lease))
			} else {
				input.Vecs[tc.column].GetGrouping().Add(tc.row)
			}
			expr := newExpr(0, types.T_int64.ToType())
			expr.GetCol().RelPos = 1 // Direct keys still resolve against the single build batch.
			executor, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer executor.Free()
			inputBytes := int64(input.Size())
			intBytes := hashtable.EstimateInt64HashMapSize(513)
			strBytes := hashtable.EstimateStringHashMapSize(513)
			require.Greater(t, strBytes, intBytes)
			limit := uint64(inputBytes) + (intBytes+strBytes)/2
			budget := process.MustNewExecutionResourceBudget(limit, limit)
			generation, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			participant, err := generation.RegisterMemoryGrowthParticipant()
			require.NoError(t, err)
			defer participant.Release()
			op := &HashBuild{IsShuffle: true, NeedHashMap: true}
			op.ctr.setSpillThreshold(0)
			op.ctr.memoryGrowthParticipant = participant
			op.ctr.hashmapBuilder.keyWidth = 8
			op.ctr.hashmapBuilder.InputBatchRowCount = 513
			op.ctr.hashmapBuilder.executors = []colexec.ExpressionExecutor{executor}
			spill, err := op.shouldSpillBeforeRetain(inputBytes, input)
			require.NoError(t, err)
			require.Equal(t, tc.want, spill)
			require.Equal(t, tc.want, op.ctr.autoSpillHasGrouping)
			if tc.borrowed {
				require.True(t, input.Vecs[0].GetGrouping().HasBorrowedValidity(), "projection must not mutate upstream-owned metadata")
			}

			// A sentinel first appearing in a later input changes the projected
			// map kind before that input is copied into the retained relation.
			input.Vecs[0].GetGrouping().Add(0)
			spill, err = op.shouldSpillBeforeRetain(inputBytes, input)
			require.NoError(t, err)
			require.True(t, spill)
			// Reusing the upstream buffer cannot erase the retained fact.
			input.Vecs[0].GetGrouping().Clear()
			spill, err = op.shouldSpillBeforeRetain(inputBytes, input)
			require.NoError(t, err)
			require.True(t, spill)
		})
	}
}

func TestAccountedSpillAdaptsAndPreservesRows(t *testing.T) {
	h := newSpillTestHarness(t, 80<<10)
	defer h.close(t)
	values := make([]int64, colexec.DefaultBatchSize)
	for i := range values {
		values[i] = int64(i)
	}
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt64Vector(values, nil, h.proc.Mp())
	input.SetRowCount(len(values))
	defer input.Clean(h.proc.Mp())
	executors, err := h.op.ctr.initSpillExprExecs(
		h.proc,
		[]*plan.Expr{newExpr(0, types.T_int64.ToType())},
	)
	require.NoError(t, err)
	analyzer := process.NewAnalyzer(0, false, false, "test")
	require.NoError(t, h.op.ctr.spillBatchWithPressure(
		h.proc, input, h.files, executors, analyzer, false,
	))
	require.Positive(t, analyzer.GetOpStats().ExtraStats["HashBuildSpillInputReductions"])
	require.NoError(t, h.op.ctr.flushSpillBuffers(h.proc, h.files, analyzer))
	require.Equal(t, int64(len(values)), spillFileRows(t, h))
}

func TestAccountedSpillBroadcastsPreparedParamKey(t *testing.T) {
	h := newSpillTestHarness(t, 80<<10)
	defer h.close(t)
	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, []byte("prepared"), false, h.proc.Mp()))
	defer func() {
		h.op.ctr.freeSpillExprExecs()
		params.Free(h.proc.Mp())
	}()
	h.proc.SetPrepareParams(params)

	values := make([]int64, colexec.DefaultBatchSize)
	for i := range values {
		values[i] = int64(i)
	}
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt64Vector(values, nil, h.proc.Mp())
	input.SetRowCount(len(values))
	defer input.Clean(h.proc.Mp())
	paramExpr := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_text)},
		Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
	}
	executors, err := h.op.ctr.initSpillExprExecs(h.proc, []*plan.Expr{
		paramExpr,
		newExpr(0, types.T_int64.ToType()),
	})
	require.NoError(t, err)
	analyzer := process.NewAnalyzer(0, false, false, "test")
	require.NoError(t, h.op.ctr.spillBatchWithPressure(
		h.proc, input, h.files, executors, analyzer, false,
	))
	require.Positive(t,
		analyzer.GetOpStats().ExtraStats["HashBuildSpillInputReductions"])
	require.NoError(t, h.op.ctr.flushSpillBuffers(h.proc, h.files, analyzer))
	require.Equal(t, int64(len(values)), spillFileRows(t, h))
}

func TestAccountedSpillCoalescesWithoutDuplicateOwnership(t *testing.T) {
	h := newSpillTestHarness(t, 8<<20)
	defer h.close(t)
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 1, 1}, nil, h.proc.Mp())
	input.SetRowCount(3)
	defer input.Clean(h.proc.Mp())
	executors, err := h.op.ctr.initSpillExprExecs(
		h.proc,
		[]*plan.Expr{newExpr(0, types.T_int32.ToType())},
	)
	require.NoError(t, err)
	analyzer := process.NewAnalyzer(0, false, false, "test")
	for range 2 {
		require.NoError(t, h.op.ctr.spillBatchWithPressure(
			h.proc, input, h.files, executors, analyzer, false,
		))
	}
	var pending int
	for _, buffer := range h.op.ctr.spillAccountedBuckets {
		if buffer != nil {
			pending += buffer.Len()
		}
	}
	require.Positive(t, pending)
	require.NoError(t, h.op.ctr.flushSpillBuffers(h.proc, h.files, analyzer))
	require.Equal(t, int64(6), spillFileRows(t, h))
	require.Equal(t, h.account.Snapshot().Used, h.generation.Used())
}

func TestSpillWithoutAllocationAccountFailsClosed(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	bat := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, 1, proc.Mp())
	defer bat.Clean(proc.Mp())
	err := (&container{}).spillBatchBounded(
		proc,
		bat,
		make([]*os.File, spillNumBuckets),
		nil,
		process.NewAnalyzer(0, false, false, "test"),
		false,
	)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
}

func TestSpillMinimumUnitPressureIsControlled(t *testing.T) {
	h := newSpillTestHarness(t, 1<<10)
	defer h.close(t)
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeVarcharVector(
		[]string{strings.Repeat("x", 64<<10)}, nil, h.proc.Mp(),
	)
	input.SetRowCount(1)
	defer input.Clean(h.proc.Mp())
	executors, err := h.op.ctr.initSpillExprExecs(
		h.proc,
		[]*plan.Expr{newExpr(0, types.T_varchar.ToType())},
	)
	require.NoError(t, err)
	err = h.op.ctr.spillBatchWithPressure(
		h.proc,
		input,
		h.files,
		executors,
		process.NewAnalyzer(0, false, false, "test"),
		false,
	)
	var minimum *MinimumAllocationPressureError
	require.True(t, errors.As(err, &minimum), "unexpected error: %v", err)
	require.Contains(t, err.Error(), "last capacity refusal:")
	require.Equal(t, MemoryPressureMinimumUnit, MemoryPressureReasonOf(err))
}

func TestWriteSpillPayloadCancellationStopsBeforeIO(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	ctx, cancel := context.WithCancelCause(proc.Ctx)
	process.ReplacePipelineCtx(proc, ctx, cancel)
	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	file, err := spillfs.CreateFile(context.Background(), t.Name())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, file.Close())
		require.NoError(t, spillfs.RemoveFile(context.Background(), t.Name()))
	}()
	proc.Cancel(context.Canceled)
	err = (&container{}).writeOpenSpillPayload(
		proc,
		file,
		0,
		[]byte("stale"),
		1,
		process.NewAnalyzer(0, false, false, "test"),
	)
	require.ErrorIs(t, err, context.Canceled)
	info, statErr := file.Stat()
	require.NoError(t, statErr)
	require.Zero(t, info.Size())
}
