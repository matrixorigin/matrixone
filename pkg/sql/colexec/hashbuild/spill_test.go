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
	"io"
	"os"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type spillTestHarness struct {
	op         *HashBuild
	proc       *process.Process
	generation *process.HashBuildBudgetGeneration
	registry   *mpool.AllocationAccountRegistry
	account    *mpool.AllocationAccount
	files      []*os.File
}

func newSpillTestHarness(t *testing.T, limit uint64) *spillTestHarness {
	t.Helper()
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	budget := process.MustNewHashBuildBudget(limit, limit)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 256)
	require.NoError(t, err)
	account, err := registry.OpenWithController(limit, generation)
	require.NoError(t, err)
	op := &HashBuild{NeedHashMap: true}
	require.NoError(t, op.SetAllocationAccount(account))
	op.ctr.hashmapBuilder.setBudget(generation)
	op.ctr.spillUUID = t.Name()
	return &spillTestHarness{
		op:         op,
		proc:       proc,
		generation: generation,
		registry:   registry,
		account:    account,
		files:      make([]*os.File, spillNumBuckets),
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

func spillFileRows(t *testing.T, files []*os.File) int64 {
	t.Helper()
	var total int64
	for _, file := range files {
		if file == nil {
			continue
		}
		_, err := file.Seek(0, io.SeekStart)
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
	require.Equal(t, int64(len(values)), spillFileRows(t, h.files))
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
	require.Equal(t, int64(len(values)), spillFileRows(t, h.files))
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
	require.Equal(t, int64(6), spillFileRows(t, h.files))
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
	err = (&container{}).writeSpillPayload(
		proc,
		file,
		[]byte("stale"),
		1,
		process.NewAnalyzer(0, false, false, "test"),
	)
	require.ErrorIs(t, err, context.Canceled)
	info, statErr := file.Stat()
	require.NoError(t, statErr)
	require.Zero(t, info.Size())
}
