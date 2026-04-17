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

package ioutil

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------- pipelineResult tests ----------

func TestPipelineResult_ErrorMethods(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r := &pipelineResult{ctx: ctx, cancel: cancel}

	// initially no error
	require.False(t, r.hasError())
	require.NoError(t, r.getError())

	// set first error
	err1 := fmt.Errorf("first")
	r.setError(err1)
	require.True(t, r.hasError())
	require.ErrorIs(t, r.getError(), err1)

	// second error is ignored (first wins)
	err2 := fmt.Errorf("second")
	r.setError(err2)
	require.ErrorIs(t, r.getError(), err1)
}

func TestPipelineResult_SetErrorCancelsCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r := &pipelineResult{ctx: ctx, cancel: cancel}

	r.setError(fmt.Errorf("boom"))
	select {
	case <-r.ctx.Done():
	default:
		t.Fatal("expected context to be cancelled after setError")
	}
}

// ---------- freeBatches tests ----------

func TestFreeBatches_NilEntries(t *testing.T) {
	// should not panic with nil entries
	freeBatches(nil, nil)
	freeBatches([]*batch.Batch{nil, nil}, nil)
}

func TestFreeBatches_WithBatches(t *testing.T) {
	mp, err := mpool.NewMPool("test_free_batches", 0, mpool.NoFixed)
	require.NoError(t, err)

	typs := []types.Type{types.T_int32.ToType()}
	bat := containers.MockBatch(typs, 10, 0, nil)
	cnBat := containers.ToCNBatch(bat)

	freeBatches([]*batch.Batch{cnBat}, mp)
}

// ---------- SinkPool lifecycle tests ----------

func TestNewSinkPool_CreateAndClose(t *testing.T) {
	pool := NewSinkPool(2, 1)
	require.NotNil(t, pool)
	pool.Close()
}

func TestGetDefaultSinkPool_ReturnsSameInstance(t *testing.T) {
	p1 := GetDefaultSinkPool()
	p2 := GetDefaultSinkPool()
	require.Same(t, p1, p2)
}

// ---------- mockFileSinker for unit testing ----------

type mockFileSinker struct {
	sinkedRows int
	sinkErr    error
	syncErr    error
	stats      objectio.ObjectStats
	closed     atomic.Bool
}

func (m *mockFileSinker) Sink(_ context.Context, bat *batch.Batch) error {
	if m.sinkErr != nil {
		return m.sinkErr
	}
	m.sinkedRows += bat.RowCount()
	return nil
}

func (m *mockFileSinker) Sync(_ context.Context) (*objectio.ObjectStats, error) {
	if m.syncErr != nil {
		return nil, m.syncErr
	}
	return &m.stats, nil
}

func (m *mockFileSinker) Reset() {}

func (m *mockFileSinker) Close() error {
	m.closed.Store(true)
	return nil
}

func mockFactory(sinkErr, syncErr error) FileSinkerFactory {
	return func(mp *mpool.MPool, fs fileservice.FileService) FileSinker {
		return &mockFileSinker{sinkErr: sinkErr, syncErr: syncErr}
	}
}

// ---------- SinkPool.Submit tests ----------

func TestSinkPool_SubmitSuccess(t *testing.T) {
	pool := NewSinkPool(2, 1)
	defer pool.Close()

	mp, err := mpool.NewMPool("test_submit", 0, mpool.NoFixed)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	r := &pipelineResult{ctx: ctx, cancel: cancel}

	typs := []types.Type{types.T_int32.ToType()}
	bat := containers.MockBatch(typs, 10, 0, nil)
	cnBat := containers.ToCNBatch(bat)

	err = pool.Submit(&poolSinkJob{
		data:    []*batch.Batch{cnBat},
		factory: mockFactory(nil, nil),
		mp:      mp,
		fs:      nil,
		result:  r,
	})
	require.NoError(t, err)

	r.pending.Wait()
	require.NoError(t, r.getError())
	require.Len(t, r.persisted, 1)
}

func TestSinkPool_SubmitMultipleJobs(t *testing.T) {
	pool := NewSinkPool(3, 2)
	defer pool.Close()

	mp, err := mpool.NewMPool("test_multi", 0, mpool.NoFixed)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	r := &pipelineResult{ctx: ctx, cancel: cancel}

	const numJobs = 5
	typs := []types.Type{types.T_int32.ToType()}

	for i := 0; i < numJobs; i++ {
		bat := containers.MockBatch(typs, 10, 0, nil)
		cnBat := containers.ToCNBatch(bat)

		err = pool.Submit(&poolSinkJob{
			data:    []*batch.Batch{cnBat},
			factory: mockFactory(nil, nil),
			mp:      mp,
			fs:      nil,
			result:  r,
		})
		require.NoError(t, err)
	}

	r.pending.Wait()
	require.NoError(t, r.getError())
	require.Len(t, r.persisted, numJobs)
}

func TestSinkPool_SubmitSinkError(t *testing.T) {
	pool := NewSinkPool(2, 1)
	defer pool.Close()

	mp, err := mpool.NewMPool("test_sink_err", 0, mpool.NoFixed)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	r := &pipelineResult{ctx: ctx, cancel: cancel}

	typs := []types.Type{types.T_int32.ToType()}
	bat := containers.MockBatch(typs, 10, 0, nil)
	cnBat := containers.ToCNBatch(bat)

	sinkErr := fmt.Errorf("sink failure")
	err = pool.Submit(&poolSinkJob{
		data:    []*batch.Batch{cnBat},
		factory: mockFactory(sinkErr, nil),
		mp:      mp,
		fs:      nil,
		result:  r,
	})
	require.NoError(t, err)

	r.pending.Wait()
	require.ErrorContains(t, r.getError(), "sink failure")
}

func TestSinkPool_SubmitSyncError(t *testing.T) {
	pool := NewSinkPool(2, 1)
	defer pool.Close()

	mp, err := mpool.NewMPool("test_sync_err", 0, mpool.NoFixed)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	r := &pipelineResult{ctx: ctx, cancel: cancel}

	typs := []types.Type{types.T_int32.ToType()}
	bat := containers.MockBatch(typs, 10, 0, nil)
	cnBat := containers.ToCNBatch(bat)

	syncErr := fmt.Errorf("sync failure")
	err = pool.Submit(&poolSinkJob{
		data:    []*batch.Batch{cnBat},
		factory: mockFactory(nil, syncErr),
		mp:      mp,
		fs:      nil,
		result:  r,
	})
	require.NoError(t, err)

	r.pending.Wait()
	require.ErrorContains(t, r.getError(), "sync failure")
}

func TestSinkPool_SubmitAfterError(t *testing.T) {
	pool := NewSinkPool(2, 1)
	defer pool.Close()

	mp, err := mpool.NewMPool("test_after_err", 0, mpool.NoFixed)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	r := &pipelineResult{ctx: ctx, cancel: cancel}

	// pre-set an error
	r.setError(fmt.Errorf("pre-existing error"))

	typs := []types.Type{types.T_int32.ToType()}
	bat := containers.MockBatch(typs, 10, 0, nil)
	cnBat := containers.ToCNBatch(bat)

	err = pool.Submit(&poolSinkJob{
		data:    []*batch.Batch{cnBat},
		factory: mockFactory(nil, nil),
		mp:      mp,
		fs:      nil,
		result:  r,
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "pre-existing error")
}

func TestSinkPool_SubmitContextCancelled(t *testing.T) {
	pool := NewSinkPool(1, 1)
	defer pool.Close()

	mp, err := mpool.NewMPool("test_ctx_cancel", 0, mpool.NoFixed)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	r := &pipelineResult{ctx: ctx, cancel: cancel}

	typs := []types.Type{types.T_int32.ToType()}

	bat := containers.MockBatch(typs, 10, 0, nil)
	cnBat := containers.ToCNBatch(bat)

	err = pool.Submit(&poolSinkJob{
		data:    []*batch.Batch{cnBat},
		factory: mockFactory(nil, nil),
		mp:      mp,
		fs:      nil,
		result:  r,
	})
	require.Error(t, err)

	r.pending.Wait()
}

// ---------- Sink worker skips job on error ----------

func TestSinkPool_WorkerSkipsOnError(t *testing.T) {
	pool := NewSinkPool(1, 1)
	defer pool.Close()

	mp, err := mpool.NewMPool("test_skip_err", 0, mpool.NoFixed)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	r := &pipelineResult{ctx: ctx, cancel: cancel}

	// first job sets error
	typs := []types.Type{types.T_int32.ToType()}

	bat1 := containers.MockBatch(typs, 10, 0, nil)
	cnBat1 := containers.ToCNBatch(bat1)
	err = pool.Submit(&poolSinkJob{
		data:    []*batch.Batch{cnBat1},
		factory: mockFactory(fmt.Errorf("fail"), nil),
		mp:      mp,
		fs:      nil,
		result:  r,
	})
	require.NoError(t, err)
	r.pending.Wait()

	// second job should be skipped because result has error
	bat2 := containers.MockBatch(typs, 10, 0, nil)
	cnBat2 := containers.ToCNBatch(bat2)
	err = pool.Submit(&poolSinkJob{
		data:    []*batch.Batch{cnBat2},
		factory: mockFactory(nil, nil),
		mp:      mp,
		fs:      nil,
		result:  r,
	})
	// Submit returns pre-existing error directly
	require.Error(t, err)
}

// ---------- SyncWorker skips on error ----------

func TestSinkPool_SyncWorkerSkipsOnError(t *testing.T) {
	pool := NewSinkPool(2, 1)
	defer pool.Close()

	mp, err := mpool.NewMPool("test_sync_skip", 0, mpool.NoFixed)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	r := &pipelineResult{ctx: ctx, cancel: cancel}

	typs := []types.Type{types.T_int32.ToType()}

	// Submit 2 jobs: first has sync error, second should be skipped in sync worker
	bat1 := containers.MockBatch(typs, 10, 0, nil)
	cnBat1 := containers.ToCNBatch(bat1)
	err = pool.Submit(&poolSinkJob{
		data:    []*batch.Batch{cnBat1},
		factory: mockFactory(nil, fmt.Errorf("sync error")),
		mp:      mp,
		fs:      nil,
		result:  r,
	})
	require.NoError(t, err)

	bat2 := containers.MockBatch(typs, 10, 0, nil)
	cnBat2 := containers.ToCNBatch(bat2)
	err = pool.Submit(&poolSinkJob{
		data:    []*batch.Batch{cnBat2},
		factory: mockFactory(nil, nil),
		mp:      mp,
		fs:      nil,
		result:  r,
	})
	require.NoError(t, err)

	r.pending.Wait()
	require.Error(t, r.getError())
}

// ---------- Concurrent stress test ----------

func TestSinkPool_ConcurrentSubmit(t *testing.T) {
	pool := NewSinkPool(4, 2)
	defer pool.Close()

	mp, err := mpool.NewMPool("test_concurrent", 0, mpool.NoFixed)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	r := &pipelineResult{ctx: ctx, cancel: cancel}

	typs := []types.Type{types.T_int32.ToType()}
	var wg sync.WaitGroup
	const goroutines = 8
	const jobsPerGoroutine = 3

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < jobsPerGoroutine; j++ {
				bat := containers.MockBatch(typs, 5, 0, nil)
				cnBat := containers.ToCNBatch(bat)
				_ = pool.Submit(&poolSinkJob{
					data:    []*batch.Batch{cnBat},
					factory: mockFactory(nil, nil),
					mp:      mp,
					fs:      nil,
					result:  r,
				})
			}
		}()
	}

	wg.Wait()
	r.pending.Wait()
	require.NoError(t, r.getError())
	require.Len(t, r.persisted, goroutines*jobsPerGoroutine)
}

// ---------- Integration: Sinker with WithPipelineFlush ----------

func TestSinker_WithPipelineFlush_NoSortKey(t *testing.T) {
	proc := testutil.NewProc(t)
	fs, err := fileservice.Get[fileservice.FileService](
		proc.GetFileService(), defines.SharedFileServiceName)
	require.NoError(t, err)

	attrs, typs, seqnums := mockSchema(3, -1)

	factory := NewFSinkerImplFactory(
		seqnums,
		-1,    // no sort key
		false, // not primary key
		false,
		0,
	)

	sinker := NewSinker(
		-1,
		attrs,
		typs,
		factory,
		proc.Mp(),
		fs,
		WithMemorySizeThreshold(1), // force spill
		WithPipelineFlush(),
	)

	require.True(t, sinker.pipe.enabled)

	// write enough data to trigger spill
	for i := 0; i < 3; i++ {
		bat := containers.MockBatch(typs, 8192, -1, nil)
		err = sinker.Write(context.Background(), containers.ToCNBatch(bat))
		assert.NoError(t, err)
	}

	err = sinker.Sync(context.Background())
	assert.NoError(t, err)

	rows := 0
	for j := 0; j < len(sinker.result.persisted); j++ {
		rows += int(sinker.result.persisted[j].Rows())
	}
	for j := 0; j < len(sinker.result.tail); j++ {
		rows += int(sinker.result.tail[j].RowCount())
	}
	require.Equal(t, 8192*3, rows)

	require.NoError(t, sinker.Close())
}

func TestSinker_WithPipelineFlush_WithSortKey(t *testing.T) {
	proc := testutil.NewProc(t)
	fs, err := fileservice.Get[fileservice.FileService](
		proc.GetFileService(), defines.SharedFileServiceName)
	require.NoError(t, err)

	attrs, typs, seqnums := mockSchema(3, 2)

	factory := NewFSinkerImplFactory(
		seqnums,
		2,
		true,
		false,
		0,
	)

	sinker := NewSinker(
		2,
		attrs,
		typs,
		factory,
		proc.Mp(),
		fs,
		WithMemorySizeThreshold(1), // force spill
		WithAllMergeSorted(),
		WithPipelineFlush(),
	)

	// Write only 1 batch so we get exactly 1 persisted object
	// (multiple persisted objects with merge sort is not yet implemented)
	bat := containers.MockBatch(typs, 8192, 2, nil)
	err = sinker.Write(context.Background(), containers.ToCNBatch(bat))
	assert.NoError(t, err)

	err = sinker.Sync(context.Background())
	assert.NoError(t, err)

	rows := 0
	for j := 0; j < len(sinker.result.persisted); j++ {
		rows += int(sinker.result.persisted[j].Rows())
	}
	for j := 0; j < len(sinker.result.tail); j++ {
		rows += int(sinker.result.tail[j].RowCount())
	}
	require.Equal(t, 8192, rows)

	require.NoError(t, sinker.Close())
}

func TestSinker_WithPipelineFlush_Reset(t *testing.T) {
	proc := testutil.NewProc(t)
	fs, err := fileservice.Get[fileservice.FileService](
		proc.GetFileService(), defines.SharedFileServiceName)
	require.NoError(t, err)

	attrs, typs, seqnums := mockSchema(3, -1)

	factory := NewFSinkerImplFactory(
		seqnums,
		-1,
		false,
		false,
		0,
	)

	sinker := NewSinker(
		-1,
		attrs,
		typs,
		factory,
		proc.Mp(),
		fs,
		WithMemorySizeThreshold(1),
		WithPipelineFlush(),
	)

	// first round
	for i := 0; i < 2; i++ {
		bat := containers.MockBatch(typs, 8192, -1, nil)
		err = sinker.Write(context.Background(), containers.ToCNBatch(bat))
		assert.NoError(t, err)
	}
	err = sinker.Sync(context.Background())
	assert.NoError(t, err)

	rows := 0
	for j := 0; j < len(sinker.result.persisted); j++ {
		rows += int(sinker.result.persisted[j].Rows())
	}
	for j := 0; j < len(sinker.result.tail); j++ {
		rows += int(sinker.result.tail[j].RowCount())
	}
	require.Equal(t, 8192*2, rows)

	// reset and do a second round
	sinker.Reset()
	require.Nil(t, sinker.pipe.result)

	for i := 0; i < 2; i++ {
		bat := containers.MockBatch(typs, 8192, -1, nil)
		err = sinker.Write(context.Background(), containers.ToCNBatch(bat))
		assert.NoError(t, err)
	}
	err = sinker.Sync(context.Background())
	assert.NoError(t, err)

	rows = 0
	for j := 0; j < len(sinker.result.persisted); j++ {
		rows += int(sinker.result.persisted[j].Rows())
	}
	for j := 0; j < len(sinker.result.tail); j++ {
		rows += int(sinker.result.tail[j].RowCount())
	}
	require.Equal(t, 8192*2, rows)

	require.NoError(t, sinker.Close())
}

func TestSinkPool_SubmitCloseRace(t *testing.T) {
	mp, err := mpool.NewMPool("test_race", 0, mpool.NoFixed)
	require.NoError(t, err)

	typs := []types.Type{types.T_int32.ToType()}

	// Run multiple iterations to increase chance of exposing races.
	for iter := 0; iter < 50; iter++ {
		pool := NewSinkPool(2, 1)

		ctx, cancel := context.WithCancel(context.Background())
		r := &pipelineResult{ctx: ctx, cancel: cancel}

		var submitWg sync.WaitGroup
		const goroutines = 4

		for g := 0; g < goroutines; g++ {
			submitWg.Add(1)
			go func() {
				defer submitWg.Done()
				for i := 0; i < 10; i++ {
					bat := containers.MockBatch(typs, 5, 0, nil)
					cnBat := containers.ToCNBatch(bat)
					_ = pool.Submit(&poolSinkJob{
						data:    []*batch.Batch{cnBat},
						factory: mockFactory(nil, nil),
						mp:      mp,
						fs:      nil,
						result:  r,
					})
				}
			}()
		}

		// Close concurrently with submitters
		pool.Close()
		submitWg.Wait()
		r.pending.Wait()
		cancel()
	}
}

func TestSinker_PipelineFlushKey(t *testing.T) {
	ctx := context.Background()

	// key not set
	val := ctx.Value(PipelineFlushKey)
	require.Nil(t, val)

	// key set to true
	ctx = context.WithValue(ctx, PipelineFlushKey, true)
	require.Equal(t, true, ctx.Value(PipelineFlushKey))
}
