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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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

func TestSpillAllocationAccountScatterScratchLifecycle(t *testing.T) {
	proc := testutil.NewProcessWithMPool(
		t,
		"",
		mpool.MustNew("spill-allocation-scatter"),
	)
	defer proc.Free()
	state := newTestSpillAllocationAccount(t, 1<<20, 8)
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
	require.NoError(t, engine.scatterBatchBounded(
		proc,
		source,
		source.Vecs,
		writers,
		0,
		false,
		process.NewAnalyzer(0, false, false, "test"),
	))
	require.Len(t, engine.scatterHashValues, source.RowCount())
	require.Len(t, engine.scatterBucketRowIds, source.RowCount())
	snapshot := state.account.Snapshot()
	require.Equal(t, uint64(source.RowCount()*(8+4)), snapshot.Used)
	require.Greater(t, snapshot.Peak, snapshot.Used)

	engine.releaseScatterScratch()
	require.Zero(t, state.account.Snapshot().Used)
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
