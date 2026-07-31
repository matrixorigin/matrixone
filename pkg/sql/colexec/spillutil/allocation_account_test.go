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

package spillutil

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type testSpillAllocationAccount struct {
	registry   *mpool.AllocationAccountRegistry
	account    *mpool.AllocationAccount
	allocation *SpillAllocationAccount
}

func newTestSpillAllocationAccount(
	t testing.TB,
	limit uint64,
	metadataSlots uint64,
) testSpillAllocationAccount {
	t.Helper()
	registry, err := mpool.NewAllocationAccountRegistry(1, metadataSlots)
	require.NoError(t, err)
	account, err := registry.Open(limit)
	require.NoError(t, err)
	allocation, err := NewSpillAllocationAccount(account, 2)
	require.NoError(t, err)
	return testSpillAllocationAccount{
		registry:   registry,
		account:    account,
		allocation: allocation,
	}
}

func finalizeTestSpillAllocationAccount(
	t testing.TB,
	state testSpillAllocationAccount,
) {
	t.Helper()
	snapshot := state.account.Seal()
	require.Zero(t, snapshot.Used)
	require.Zero(t, state.registry.LiveAllocationMetadata())
	_, err := state.registry.Finalize(state.account)
	require.NoError(t, err)
}

func writeSpillAllocationTestFile(
	t testing.TB,
	bat *batch.Batch,
	truncate int,
) *os.File {
	t.Helper()
	var encoded bytes.Buffer
	require.NoError(t, marshalSpillRecord(bat, &encoded))
	payload := encoded.Bytes()
	if truncate > 0 {
		payload = payload[:len(payload)-truncate]
	}
	path := filepath.Join(t.TempDir(), "spill.bin")
	require.NoError(t, os.WriteFile(path, payload, 0o600))
	file, err := os.Open(path)
	require.NoError(t, err)
	return file
}

func writeSpillAllocationTestRecords(
	t testing.TB,
	batches ...*batch.Batch,
) *os.File {
	t.Helper()
	var payload bytes.Buffer
	for _, bat := range batches {
		var encoded bytes.Buffer
		require.NoError(t, marshalSpillRecord(bat, &encoded))
		_, err := payload.Write(encoded.Bytes())
		require.NoError(t, err)
	}
	path := filepath.Join(t.TempDir(), "spill-records.bin")
	require.NoError(t, os.WriteFile(path, payload.Bytes(), 0o600))
	file, err := os.Open(path)
	require.NoError(t, err)
	return file
}

func TestSpillAllocationAccountDecodedBatchLifecycle(t *testing.T) {
	proc := testutil.NewProcessWithMPool(
		t,
		"",
		mpool.MustNew("spill-allocation-decoded"),
	)
	defer proc.Free()
	state := newTestSpillAllocationAccount(t, 8<<20, 16)
	source := testutil.NewBatchWithVectors([]*vector.Vector{
		testutil.NewVector(
			4,
			types.T_int64.ToType(),
			proc.Mp(),
			false,
			[]int64{1, 2, 3, 4},
		),
		testutil.NewVector(
			4,
			types.T_varchar.ToType(),
			proc.Mp(),
			false,
			[]string{"a", "payload-longer-than-inline", "c", "d"},
		),
	}, nil)
	defer source.Clean(proc.Mp())

	reader := BucketReader{
		fd:         writeSpillAllocationTestFile(t, source, 0),
		allocation: state.allocation,
	}
	reuse := batch.NewOffHeapWithSize(0)
	decoded, err := reader.ReadBatch(proc, reuse)
	require.NoError(t, err)
	require.Same(t, reuse, decoded)
	require.NotNil(t, decoded.AllocationAccountSelection())
	for _, vec := range decoded.Vecs {
		require.NotNil(t, vec.AllocationAccountSelection())
	}
	require.Positive(t, state.account.Snapshot().Used)

	reader.Close()
	reuse.Clean(proc.Mp())
	require.Zero(t, state.account.Snapshot().Used)

	reader = BucketReader{
		fd:           writeSpillAllocationTestRecords(t, source, source),
		mergeRecords: true,
		allocation:   state.allocation,
	}
	reuse = batch.NewOffHeapWithSize(0)
	decoded, err = reader.ReadBatch(proc, reuse)
	require.NoError(t, err)
	require.Equal(t, 2*source.RowCount(), decoded.RowCount())
	require.Positive(t, state.account.Snapshot().Used)
	reader.Close()
	reuse.Clean(proc.Mp())
	require.Zero(t, state.account.Snapshot().Used)

	reader = BucketReader{
		fd:         writeSpillAllocationTestFile(t, source, 1),
		allocation: state.allocation,
	}
	reuse = batch.NewOffHeapWithSize(0)
	_, err = reader.ReadBatch(proc, reuse)
	require.Error(t, err)
	require.Zero(t, state.account.Snapshot().Used)
	require.Zero(t, state.registry.LiveAllocationMetadata())
	reader.Close()
	finalizeTestSpillAllocationAccount(t, state)
}

func TestSpillAllocationAccountDecodedReuseRetriesFromCleanRecord(t *testing.T) {
	proc := testutil.NewProcessWithMPool(
		t,
		"",
		mpool.MustNew("spill-allocation-decoded-retry"),
	)
	defer proc.Free()
	makeSource := func(width int) *batch.Batch {
		values := make([]string, 1_024)
		for i := range values {
			values[i] = strings.Repeat("x", width)
		}
		return testutil.NewBatchWithVectors([]*vector.Vector{
			testutil.NewVector(
				len(values),
				types.T_varchar.ToType(),
				proc.Mp(),
				false,
				values,
			),
		}, nil)
	}
	first := makeSource(512)
	second := makeSource(2_048)
	defer first.Clean(proc.Mp())
	defer second.Clean(proc.Mp())

	measure := newTestSpillAllocationAccount(t, 64<<20, 128)
	reader := BucketReader{
		fd:         writeSpillAllocationTestRecords(t, first, second),
		allocation: measure.allocation,
	}
	reuse := batch.NewOffHeapWithSize(0)
	_, err := reader.ReadBatch(proc, reuse)
	require.NoError(t, err)
	firstUsed := measure.account.Snapshot().Used
	require.Positive(t, firstUsed)
	reuse.Clean(proc.Mp())
	require.NoError(t, reuse.SetAllocationAccount(measure.allocation.decoded))
	_, err = reader.ReadBatch(proc, reuse)
	require.NoError(t, err)
	secondUsed := measure.account.Snapshot().Used
	require.Positive(t, secondUsed)
	reuse.Clean(proc.Mp())
	reader.Close()
	finalizeTestSpillAllocationAccount(t, measure)

	limit := max(firstUsed, secondUsed) + 128<<10
	require.Less(t, limit, firstUsed+secondUsed)
	budget := process.MustNewHashBuildBudget(limit, limit)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 128)
	require.NoError(t, err)
	account, err := registry.OpenWithController(limit, generation)
	require.NoError(t, err)
	allocation, err := NewSpillAllocationAccount(account, 2)
	require.NoError(t, err)
	reader = BucketReader{allocation: allocation}
	require.NoError(t, reader.EnsureBuffer(generation))
	reader.ResetForFd(writeSpillAllocationTestRecords(t, first, second))
	reuse = batch.NewOffHeapWithSize(0)
	_, err = reader.ReadBatch(proc, reuse)
	require.NoError(t, err)
	rejects := generation.RejectCount()
	_, err = reader.ReadBatch(proc, reuse)
	require.NoError(t, err)
	require.Equal(t, rejects, generation.RejectCount(),
		"the local account rejects the overlap before the shared controller")
	require.Equal(t, uint64(1), reader.cleanRetries,
		"replacement overlap must exercise the clean-record retry")
	require.Equal(t,
		generation.Snapshot().AllocationUsed+uint64(64<<10),
		generation.Used(),
		"decoded payloads have no duplicate hard reservation",
	)
	reuse.Clean(proc.Mp())
	reader.Close()
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, generation.Used())
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
}

func TestSpillAllocationAccountScatterScratchLifecycle(t *testing.T) {
	proc := testutil.NewProcessWithMPool(
		t,
		"",
		mpool.MustNew("spill-allocation-scatter"),
	)
	defer proc.Free()
	state := newTestSpillAllocationAccount(t, 1<<20, 64)
	engine, err := NewSpillEngineWithAllocation(
		SpillEngineConfig{},
		state.allocation,
	)
	require.NoError(t, err)

	source := testutil.NewBatchWithVectors([]*vector.Vector{
		testutil.NewVector(
			8,
			types.T_int64.ToType(),
			proc.Mp(),
			false,
			[]int64{1, 2, 3, 4, 5, 6, 7, 8},
		),
	}, nil)
	defer source.Clean(proc.Mp())
	writers := MakeBucketWriters("spill_allocation_scatter")
	defer func() {
		for i := range writers {
			writers[i].Close()
		}
	}()
	analyzer := process.NewAnalyzer(0, false, false, "test")
	require.NoError(t, engine.scatterBatchBounded(
		proc,
		source,
		source.Vecs,
		writers,
		0,
		false,
		analyzer,
	))
	require.Len(t, engine.scatterHashValues, source.RowCount())
	require.Len(t, engine.scatterBucketRowIds, source.RowCount())
	snapshot := state.account.Snapshot()
	require.Greater(
		t,
		snapshot.Used,
		uint64(source.RowCount()*(8+4)),
	)
	require.Greater(t, snapshot.Peak, snapshot.Used)
	require.NoError(t, engine.flushScatterBuffers(proc, writers, analyzer))
	var writtenRows int64
	for i := range writers {
		writtenRows += writers[i].Rows
	}
	require.Equal(t, int64(source.RowCount()), writtenRows)
	require.Equal(t, snapshot.Used, state.account.Snapshot().Used)

	engine.releaseScatterScratch()
	require.Zero(t, state.account.Snapshot().Used)
	engine.Cleanup(proc)
	finalizeTestSpillAllocationAccount(t, state)
}

func TestSpillAllocationAccountScatterChargesOnlyExternalSource(t *testing.T) {
	proc := testutil.NewProcessWithMPool(
		t,
		"",
		mpool.MustNew("spill-allocation-scatter-source"),
	)
	defer proc.Free()
	const limit = uint64(8 << 20)
	budget := process.MustNewHashBuildBudget(limit, limit)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.OpenWithController(limit, generation)
	require.NoError(t, err)
	allocation, err := NewSpillAllocationAccount(account, 2)
	require.NoError(t, err)
	engine, err := NewSpillEngineWithAllocation(
		SpillEngineConfig{Budget: generation},
		allocation,
	)
	require.NoError(t, err)
	source := testutil.NewBatchWithVectors([]*vector.Vector{
		testutil.NewVector(
			8,
			types.T_int64.ToType(),
			proc.Mp(),
			false,
			[]int64{1, 2, 3, 4, 5, 6, 7, 8},
		),
	}, nil)
	defer source.Clean(proc.Mp())
	writers := MakeBucketWriters("spill_allocation_scatter_source")
	defer func() {
		for i := range writers {
			writers[i].Close()
		}
	}()
	require.NoError(t, engine.scatterBatchBounded(
		proc,
		source,
		source.Vecs,
		writers,
		0,
		false,
		process.NewAnalyzer(0, false, false, "test"),
	))
	snapshot := generation.Snapshot()
	require.Nil(t, engine.scatterScratchReservation)
	require.Equal(t, snapshot.AllocationUsed, snapshot.Used,
		"the upstream source token is transient and private spill bytes are exact")
	require.Positive(t, snapshot.ReserveCount,
		"the unaccounted upstream source remains part of the peak")
	require.Greater(t, snapshot.PeakUsed, snapshot.Used)

	engine.releaseScatterScratch()
	engine.Cleanup(proc)
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, generation.Used())
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
}

func TestSpillAllocationAccountMarshalBufferLifecycle(t *testing.T) {
	proc := testutil.NewProcessWithMPool(
		t,
		"",
		mpool.MustNew("spill-allocation-marshal"),
	)
	defer proc.Free()
	state := newTestSpillAllocationAccount(t, 1<<20, 8)
	source := testutil.NewBatchWithVectors([]*vector.Vector{
		testutil.NewVector(
			4,
			types.T_int64.ToType(),
			proc.Mp(),
			false,
			[]int64{1, 2, 3, 4},
		),
	}, nil)
	defer source.Clean(proc.Mp())

	accounted, err := state.allocation.newBuffer(
		proc.Mp(),
		SpillAllocationSiteMarshalBuffer,
	)
	require.NoError(t, err)
	require.NoError(t, marshalSpillRecordTo(source, accounted))
	var legacy bytes.Buffer
	require.NoError(t, marshalSpillRecord(source, &legacy))
	require.Equal(t, legacy.Bytes(), accounted.Bytes())
	used := state.account.Snapshot().Used
	require.Positive(t, used)

	accounted.Reset()
	require.Zero(t, accounted.Len())
	require.Equal(t, used, state.account.Snapshot().Used)
	require.NoError(t, marshalSpillRecordTo(source, accounted))
	require.Equal(t, used, state.account.Snapshot().Used)

	accounted.Free()
	require.Zero(t, state.account.Snapshot().Used)
	finalizeTestSpillAllocationAccount(t, state)
}

func TestSpillAllocationAccountCoalesceAdmissionFallback(t *testing.T) {
	proc := testutil.NewProcessWithMPool(
		t,
		"",
		mpool.MustNew("spill-allocation-coalesce-fallback"),
	)
	defer proc.Free()
	// The record buffer consumes the only metadata slot. Coalescing is an
	// optional optimization, so its admission failure must fall back to one
	// direct write instead of failing the scatter.
	state := newTestSpillAllocationAccount(t, 1<<20, 1)
	engine, err := NewSpillEngineWithAllocation(
		SpillEngineConfig{},
		state.allocation,
	)
	require.NoError(t, err)
	source := testutil.NewBatchWithVectors([]*vector.Vector{
		testutil.NewVector(
			2,
			types.T_int64.ToType(),
			proc.Mp(),
			false,
			[]int64{1, 2},
		),
	}, nil)
	defer source.Clean(proc.Mp())
	writers := MakeBucketWriters("spill_allocation_coalesce_fallback")
	defer func() {
		for i := range writers {
			writers[i].Close()
		}
	}()

	require.NoError(t, engine.appendScatterRecord(
		proc,
		source,
		&writers[0],
		0,
		process.NewAnalyzer(0, false, false, "test"),
	))
	require.Equal(t, int64(source.RowCount()), writers[0].Rows)
	require.Nil(t, engine.scatterAccountedWriteBuffers[0].Bytes())
	require.Equal(t, uint64(1), state.registry.PeakAllocationMetadata())

	engine.releaseScatterScratch()
	engine.Cleanup(proc)
	finalizeTestSpillAllocationAccount(t, state)
}

func TestSpillAllocationAccountScatterFailureCleanup(t *testing.T) {
	proc := testutil.NewProcessWithMPool(
		t,
		"",
		mpool.MustNew("spill-allocation-scatter-failure"),
	)
	defer proc.Free()
	const rows = 8
	state := newTestSpillAllocationAccount(t, rows*(8+4), 8)
	engine, err := NewSpillEngineWithAllocation(
		SpillEngineConfig{},
		state.allocation,
	)
	require.NoError(t, err)

	source := testutil.NewBatchWithVectors([]*vector.Vector{
		testutil.NewVector(
			rows,
			types.T_int64.ToType(),
			proc.Mp(),
			false,
			[]int64{1, 2, 3, 4, 5, 6, 7, 8},
		),
	}, nil)
	defer source.Clean(proc.Mp())
	writers := MakeBucketWriters("spill_allocation_scatter_failure")
	defer func() {
		for i := range writers {
			writers[i].Close()
		}
	}()
	err = engine.scatterBatchBounded(
		proc,
		source,
		source.Vecs,
		writers,
		0,
		false,
		process.NewAnalyzer(0, false, false, "test"),
	)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Equal(t, uint64(rows*(8+4)), state.account.Snapshot().Used)

	engine.releaseScatterScratch()
	require.Zero(t, state.account.Snapshot().Used)
	engine.Cleanup(proc)
	finalizeTestSpillAllocationAccount(t, state)
}

func TestSpillAllocationAccountRebuildAndRecursiveSpillLifecycle(t *testing.T) {
	proc := testutil.NewProcessWithMPool(
		t,
		"",
		mpool.MustNew("spill-allocation-rebuild"),
	)
	defer proc.Free()
	const limit = uint64(64 << 20)
	budget := process.MustNewHashBuildBudget(limit, limit)
	generation, err := budget.OpenGenerationWithSpillCaps(
		1,
		limit,
		1<<30,
		4_096,
	)
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 4_096)
	require.NoError(t, err)
	account, err := registry.OpenWithController(limit, generation)
	require.NoError(t, err)
	engine, err := NewSpillEngineForAccount(
		SpillEngineConfig{
			BuildKeyExprs:           makeTestKeyExpr(),
			Budget:                  generation,
			SpillThreshold:          100,
			NeedsBuildForEmptyProbe: true,
		},
		account,
		hashbuild.HashBuildAllocationOwner,
	)
	require.NoError(t, err)

	values := make([]int32, 5_000)
	for i := range values {
		values[i] = int32(i)
	}
	source := makeInt32Batch(proc, values)
	fd := writeBuildFile(proc, "accounted_recursive_build", source)
	source.Clean(proc.Mp())
	engine.InitFromSpilledMap([]*os.File{fd})
	analyzer := process.NewAnalyzer(0, false, false, "test")

	respills := 0
	ready := 0
	for steps := 0; engine.HasMoreBuckets(); steps++ {
		require.Less(t, steps, 4_096, "recursive spill queue made no progress")
		jm, result, rebuildErr := engine.RebuildHashmap(proc, analyzer)
		require.NoError(t, rebuildErr)
		switch result {
		case BucketReSpilled:
			respills++
		case BucketReady:
			ready++
			require.NotNil(t, jm)
			jm.Free()
		case BucketSkip, BucketEmptyBuild:
			require.Nil(t, jm)
		default:
			require.NotEqual(t, BucketQueueEmpty, result)
		}
	}
	require.Positive(t, respills)
	require.Positive(t, ready)
	require.Positive(t, account.Snapshot().Peak)

	engine.Cleanup(proc)
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, generation.Used())
	require.Zero(t, generation.SpillDiskUsed())
	require.Zero(t, generation.SpillFDUsed())
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
}
