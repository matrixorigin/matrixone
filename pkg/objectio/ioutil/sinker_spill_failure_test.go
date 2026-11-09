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

package ioutil

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

var spillInjectedError = errors.New("injected sink failure")

type spillFailureSink struct {
	mode     string
	received *batch.Batch
}

func (s *spillFailureSink) Sink(_ context.Context, b *batch.Batch) error {
	s.received = b
	switch s.mode {
	case "error":
		return spillInjectedError
	case "panic":
		panic(spillInjectedError)
	}
	return nil
}
func (*spillFailureSink) Sync(context.Context) (*objectio.ObjectStats, error) {
	return &objectio.ObjectStats{}, nil
}
func (*spillFailureSink) Reset()       {}
func (*spillFailureSink) Close() error { return nil }

func TestStreamingSpillFailureBufferOwnership(t *testing.T) {
	for _, offHeap := range []bool{false, true} {
		for _, rows := range []int{1, int(objectio.BlockMaxRows)} {
			for _, mode := range []string{"success", "error", "panic"} {
				t.Run(fmt.Sprintf("offheap=%t/rows=%d/%s", offHeap, rows, mode), func(t *testing.T) {
					proc := testutil.NewProc(t)
					fs, err := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
					require.NoError(t, err)
					attrs, typs, _ := mockSchema(3, 2)
					file := &spillFailureSink{mode: mode}
					pool := &ownershipBuffer{IBatchBuffer: containers.NewOneSchemaBatchBuffer(mpool.MB, attrs, typs, offHeap), active: make(map[*batch.Batch]bool), t: t}
					sinker := NewSinker(2, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, proc.Mp(), fs, WithMemorySizeThreshold(1), WithBuffer(pool, true))
					t.Cleanup(func() {
						if sinker.buf.buffers != nil {
							_ = sinker.Close()
						}
					})
					data := containers.MockBatch(typs, rows, 2, nil)
					defer data.Close()
					before := proc.Mp().CurrNB()
					var gotErr error
					var gotPanic any
					func() {
						defer func() { gotPanic = recover() }()
						gotErr = sinker.Write(context.Background(), containers.ToCNBatch(data))
					}()
					require.NoError(t, sinker.Close())
					leaked := proc.Mp().CurrNB() - before
					require.NotNil(t, file.received)
					switch mode {
					case "success":
						require.NoError(t, gotErr)
						require.Nil(t, gotPanic)
					case "error":
						require.ErrorIs(t, gotErr, spillInjectedError)
						require.Nil(t, gotPanic)
					case "panic":
						require.Equal(t, spillInjectedError, gotPanic)
					}
					require.Zero(t, leaked, "mpool bytes retained after Close")
				})
			}
		}
	}
}

// ownershipBuffer detects double returns even when Close would hide them.
type ownershipBuffer struct {
	containers.IBatchBuffer
	active map[*batch.Batch]bool
	t      *testing.T
}

func (b *ownershipBuffer) FetchWithSchema(attrs []string, typs []types.Type) *batch.Batch {
	bat := b.IBatchBuffer.FetchWithSchema(attrs, typs)
	require.False(b.t, b.active[bat], "batch fetched while active")
	b.active[bat] = true
	return bat
}
func (b *ownershipBuffer) Putback(bat *batch.Batch, mp *mpool.MPool) {
	require.True(b.t, b.active[bat], "batch returned twice or without ownership")
	delete(b.active, bat)
	b.IBatchBuffer.Putback(bat, mp)
}

func TestFetchBufferPartialPreExtendFailure(t *testing.T) {
	attrs, typs, _ := mockSchema(3, 2)
	mp, err := mpool.NewMPool("partial-preextend", mpool.MB, mpool.NoFixed)
	require.NoError(t, err)
	reserve, err := mp.Alloc(mpool.MB-40000, true)
	require.NoError(t, err)
	defer mp.Free(reserve)
	baseline := mp.CurrNB()
	pool := &ownershipBuffer{IBatchBuffer: containers.NewOneSchemaBatchBuffer(mpool.MB, attrs, typs, true), active: make(map[*batch.Batch]bool), t: t}
	sinker := NewSinker(2, attrs, typs, nil, mp, nil, WithBuffer(pool, true))
	t.Cleanup(func() {
		if sinker.buf.buffers != nil {
			_ = sinker.Close()
		}
	})
	bat, err := sinker.fetchBuffer()
	require.Error(t, err)
	require.Nil(t, bat)
	require.Empty(t, pool.active)
	require.Zero(t, pool.Len(), "a cleaned partial batch must not enter the cache")
	require.Equal(t, baseline, mp.CurrNB(), "partial allocations must be released immediately")
	require.NoError(t, sinker.Close())
	require.Equal(t, baseline, mp.CurrNB())
}

func TestWriteOwnedTransfer(t *testing.T) {
	for _, mode := range []string{"success", "error", "partial"} {
		t.Run(mode, func(t *testing.T) {
			proc := testutil.NewProc(t)
			attrs, typs, _ := mockSchema(3, 2)
			file := &spillFailureSink{mode: mode}
			pool := &ownershipBuffer{IBatchBuffer: containers.NewOneSchemaBatchBuffer(mpool.MB, attrs, typs, false), active: make(map[*batch.Batch]bool), t: t}
			sinker := NewSinker(2, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, proc.Mp(), nil, WithMemorySizeThreshold(1), WithBuffer(pool, true))
			t.Cleanup(func() {
				if sinker.buf.buffers != nil {
					_ = sinker.Close()
				}
			})
			rows := int(objectio.BlockMaxRows)
			if mode == "partial" {
				rows = 1
			}
			source := containers.MockBatch(typs, rows, 2, nil)
			defer source.Close()
			input, err := containers.ToCNBatch(source).Dup(proc.Mp())
			require.NoError(t, err)
			if mode != "partial" {
				pool.active[input] = true
			}
			owned, err := sinker.WriteOwned(context.Background(), input)
			if mode == "error" {
				require.ErrorIs(t, err, spillInjectedError)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, mode != "partial", owned)
			if !owned {
				require.Equal(t, rows, input.RowCount())
				input.Clean(proc.Mp())
			}
			require.NoError(t, sinker.Close())
			require.Empty(t, pool.active)
		})
	}
}

func TestWriteRestagesFullBatchOnFailure(t *testing.T) {
	proc := testutil.NewProc(t)
	attrs, typs, _ := mockSchema(3, 2)
	file := &spillFailureSink{mode: "error"}
	pool := &ownershipBuffer{IBatchBuffer: containers.NewOneSchemaBatchBuffer(mpool.MB, attrs, typs, false), active: make(map[*batch.Batch]bool), t: t}
	sinker := NewSinker(2, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, proc.Mp(), nil, WithMemorySizeThreshold(mpool.MB), WithBuffer(pool, true))
	t.Cleanup(func() {
		if sinker.buf.buffers != nil {
			_ = sinker.Close()
		}
	})
	source := containers.MockBatch(typs, int(objectio.BlockMaxRows), 2, nil)
	defer source.Close()
	require.NoError(t, sinker.Write(context.Background(), containers.ToCNBatch(source)))
	sinker.staged.memorySizeThreshold = 1
	require.ErrorIs(t, sinker.Write(context.Background(), containers.ToCNBatch(source)), spillInjectedError)
	require.NoError(t, sinker.Close())
	require.Empty(t, pool.active)
}

func TestWriteOwnedPartialStagedSpillsBeforeDecision(t *testing.T) {
	proc := testutil.NewProc(t)
	attrs, typs, _ := mockSchema(3, 2)
	file := &spillFailureSink{}
	pool := &ownershipBuffer{IBatchBuffer: containers.NewOneSchemaBatchBuffer(mpool.MB, attrs, typs, false), active: make(map[*batch.Batch]bool), t: t}
	sinker := NewSinker(2, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, proc.Mp(), nil, WithMemorySizeThreshold(mpool.MB), WithBuffer(pool, true))
	t.Cleanup(func() {
		if sinker.buf.buffers != nil {
			_ = sinker.Close()
		}
	})
	partial := containers.MockBatch(typs, 1, 2, nil)
	defer partial.Close()
	full := containers.MockBatch(typs, int(objectio.BlockMaxRows), 2, nil)
	defer full.Close()
	require.NoError(t, sinker.Write(context.Background(), containers.ToCNBatch(partial)))
	sinker.staged.memorySizeThreshold = 1
	input := containers.ToCNBatch(full)
	owned, err := sinker.WriteOwned(context.Background(), input)
	require.NoError(t, err)
	require.False(t, owned)
	require.Equal(t, int(objectio.BlockMaxRows), input.RowCount())
	require.NoError(t, sinker.Close())
	require.Empty(t, pool.active)
}

func TestWriteNoSortFailureThenReset(t *testing.T) {
	proc := testutil.NewProc(t)
	attrs, typs, _ := mockSchema(3, 2)
	file := &spillFailureSink{mode: "error"}
	pool := &ownershipBuffer{IBatchBuffer: containers.NewOneSchemaBatchBuffer(mpool.MB, attrs, typs, false), active: make(map[*batch.Batch]bool), t: t}
	sinker := NewSinker(-1, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, proc.Mp(), nil, WithMemorySizeThreshold(1), WithBuffer(pool, true))
	t.Cleanup(func() {
		if sinker.buf.buffers != nil {
			_ = sinker.Close()
		}
	})
	source := containers.MockBatch(typs, int(objectio.BlockMaxRows), 2, nil)
	defer source.Close()
	input := containers.ToCNBatch(source)
	require.ErrorIs(t, sinker.Write(context.Background(), input), spillInjectedError)
	require.Empty(t, pool.active)
	sinker.Reset()
	file.mode = "success"
	require.NoError(t, sinker.Write(context.Background(), input))
	require.Empty(t, pool.active)
	require.Equal(t, int(objectio.BlockMaxRows), input.RowCount())
	require.NoError(t, sinker.Close())
}

type panicFetchBuffer struct {
	*ownershipBuffer
	remaining int
}

func (b *panicFetchBuffer) FetchWithSchema(attrs []string, typs []types.Type) *batch.Batch {
	if b.remaining == 0 {
		panic(spillInjectedError)
	}
	b.remaining--
	return b.ownershipBuffer.FetchWithSchema(attrs, typs)
}

func TestMergeReplacementPanicOwnership(t *testing.T) {
	for _, pipeline := range []bool{false, true} {
		t.Run(fmt.Sprintf("pipeline=%t", pipeline), func(t *testing.T) {
			proc := testutil.NewProc(t)
			attrs, typs, _ := mockSchema(3, 2)
			base := &ownershipBuffer{IBatchBuffer: containers.NewOneSchemaBatchBuffer(mpool.MB, attrs, typs, true), active: make(map[*batch.Batch]bool), t: t}
			pool := &panicFetchBuffer{ownershipBuffer: base, remaining: 100}
			options := []SinkerOption{WithMemorySizeThreshold(100 * mpool.MB), WithBuffer(pool, true)}
			if pipeline {
				options = append(options, WithPipelineFlush())
			} else {
				options = append(options, WithDedupAll())
			}
			file := &spillFailureSink{}
			sinker := NewSinker(2, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, proc.Mp(), nil, options...)
			t.Cleanup(func() {
				if sinker.buf.buffers != nil {
					_ = sinker.Close()
				}
			})
			before := proc.Mp().CurrNB()
			source := containers.MockBatch(typs, 2*int(objectio.BlockMaxRows), 2, nil)
			defer source.Close()
			require.NoError(t, sinker.Write(context.Background(), containers.ToCNBatch(source)))
			pool.remaining = 2 // panic fetching the second replacement, after two transfers
			require.PanicsWithValue(t, spillInjectedError, func() { _ = sinker.trySpill(context.Background()) })
			require.Empty(t, base.active)
			require.NoError(t, sinker.Close())
			require.Equal(t, before, proc.Mp().CurrNB())
		})
	}
}

type lifecycleSink struct {
	spillFailureSink
	syncMode string
	resets   int
}

func (s *lifecycleSink) Sync(context.Context) (*objectio.ObjectStats, error) {
	if s.syncMode == "panic" {
		panic(spillInjectedError)
	}
	if s.syncMode == "error" {
		return nil, spillInjectedError
	}
	return &objectio.ObjectStats{}, nil
}
func (s *lifecycleSink) Reset() { s.resets++ }

func TestSyncExecutorResetOnFailure(t *testing.T) {
	for _, path := range []string{"streaming", "dedup", "unsorted"} {
		for _, stage := range []string{"sink", "sync"} {
			for _, mode := range []string{"error", "panic"} {
				t.Run(path+"/"+stage+"/"+mode, func(t *testing.T) {
					proc := testutil.NewProc(t)
					attrs, typs, _ := mockSchema(3, 2)
					file := &lifecycleSink{}
					if stage == "sink" {
						file.mode = mode
					} else {
						file.syncMode = mode
					}
					key := 2
					options := []SinkerOption{WithMemorySizeThreshold(1), WithOffHeap()}
					if path == "unsorted" {
						key = -1
					}
					if path == "dedup" {
						options = append(options, WithDedupAll())
					}
					sinker := NewSinker(key, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, proc.Mp(), nil, options...)
					t.Cleanup(func() {
						if sinker.buf.buffers != nil {
							_ = sinker.Close()
						}
					})
					before := proc.Mp().CurrNB()
					source := containers.MockBatch(typs, int(objectio.BlockMaxRows), 2, nil)
					defer source.Close()
					if mode == "panic" {
						require.PanicsWithValue(t, spillInjectedError, func() { _ = sinker.Write(context.Background(), containers.ToCNBatch(source)) })
					} else {
						require.ErrorIs(t, sinker.Write(context.Background(), containers.ToCNBatch(source)), spillInjectedError)
					}
					require.Equal(t, 1, file.resets)
					require.NoError(t, sinker.Close())
					require.Equal(t, before, proc.Mp().CurrNB())
				})
			}
		}
	}
}

func TestPipelineRejectedSubmissionReleasesBatches(t *testing.T) {
	proc := testutil.NewProc(t)
	attrs, typs, _ := mockSchema(3, 2)
	sinker := NewSinker(2, attrs, typs, nil, proc.Mp(), nil, WithMemorySizeThreshold(100*mpool.MB), WithOffHeap(), WithPipelineFlush())
	t.Cleanup(func() {
		if sinker.buf.buffers != nil {
			_ = sinker.Close()
		}
	})
	before := proc.Mp().CurrNB()
	source := containers.MockBatch(typs, int(objectio.BlockMaxRows), 2, nil)
	defer source.Close()
	require.NoError(t, sinker.Write(context.Background(), containers.ToCNBatch(source)))
	sinker.staged.memorySizeThreshold = 1
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, sinker.trySpillMergeSortPipeline(ctx), context.Canceled)
	sinker.cleanupInMemoryStaged()
	sinker.Reset()
	require.NoError(t, sinker.Close())
	require.Equal(t, before, proc.Mp().CurrNB())
}

type contentCheckingSink struct {
	calls   int
	failAt  int
	panicAt int
	rows    [][]int32
}

func (s *contentCheckingSink) Sink(_ context.Context, b *batch.Batch) error {
	s.calls++
	for i := 0; i < b.RowCount(); i++ {
		row := make([]int32, len(b.Vecs))
		for col := range b.Vecs {
			row[col] = vector.MustFixedColNoTypeCheck[int32](b.Vecs[col])[i]
		}
		s.rows = append(s.rows, row)
	}
	if s.calls == s.panicAt {
		panic(spillInjectedError)
	}
	if s.calls == s.failAt {
		return spillInjectedError
	}
	return nil
}
func (*contentCheckingSink) Sync(context.Context) (*objectio.ObjectStats, error) {
	return &objectio.ObjectStats{}, nil
}
func (*contentCheckingSink) Reset()       {}
func (*contentCheckingSink) Close() error { return nil }

func TestStreamingMultiBlockContentAndReset(t *testing.T) {
	for _, offHeap := range []bool{false, true} {
		for _, mode := range []string{"success", "error", "panic"} {
			for _, at := range []int{1, 2, 3} {
				if mode == "success" && at != 1 {
					continue
				}
				t.Run(fmt.Sprintf("offheap=%t/%s/%d", offHeap, mode, at), func(t *testing.T) {
					proc := testutil.NewProc(t)
					attrs, typs, _ := mockSchema(3, 2)
					pool := &ownershipBuffer{IBatchBuffer: containers.NewOneSchemaBatchBuffer(mpool.MB, attrs, typs, offHeap), active: make(map[*batch.Batch]bool), t: t}
					file := &contentCheckingSink{}
					if mode == "error" {
						file.failAt = at
					}
					if mode == "panic" {
						file.panicAt = at
					}
					s := NewSinker(2, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, proc.Mp(), nil, WithMemorySizeThreshold(100*mpool.MB), WithBuffer(pool, true))
					t.Cleanup(func() {
						if s.buf.buffers != nil {
							_ = s.Close()
						}
					})
					before := proc.Mp().CurrNB()
					n := 2*int(objectio.BlockMaxRows) + 1
					source := containers.MockBatch(typs, n, 2, nil)
					defer source.Close()
					input := containers.ToCNBatch(source)
					for col := range input.Vecs {
						values := vector.MustFixedColNoTypeCheck[int32](input.Vecs[col])
						for i := range values {
							values[i] = int32((n-1-i)*10 + col)
						}
					}
					for generation := 0; generation < 2; generation++ {
						if generation == 1 {
							s.Reset()
							file.rows = nil
							file.calls = 0
							file.failAt = 0
							file.panicAt = 0
						}
						require.NoError(t, s.Write(context.Background(), input))
						var err error
						var gotPanic any
						func() { defer func() { gotPanic = recover() }(); err = s.trySpill(context.Background()) }()
						blocks := 3
						if generation == 0 && mode != "success" {
							blocks = at
						}
						if generation == 0 && mode == "error" {
							require.ErrorIs(t, err, spillInjectedError)
						} else {
							require.NoError(t, err)
						}
						if generation == 0 && mode == "panic" {
							require.Equal(t, spillInjectedError, gotPanic)
						} else {
							require.Nil(t, gotPanic)
						}
						require.Equal(t, blocks, file.calls)
						count := blocks * int(objectio.BlockMaxRows)
						if count > n {
							count = n
						}
						require.Len(t, file.rows, count)
						for i, row := range file.rows {
							for col, value := range row {
								require.Equal(t, int32(i*10+col), value)
							}
						}
						require.Empty(t, pool.active)
					}
					require.NoError(t, s.Close())
					require.Equal(t, before, proc.Mp().CurrNB())
				})
			}
		}
	}
}

type compressRejectSink struct{ arena *objectio.WriteArena }

func (s *compressRejectSink) Sink(context.Context, *batch.Batch) error {
	s.arena.CompressBuf(int(mpool.MaxAllocationSize()) + 1)
	return nil
}
func (*compressRejectSink) Sync(context.Context) (*objectio.ObjectStats, error) {
	return &objectio.ObjectStats{}, nil
}
func (*compressRejectSink) Reset()         {}
func (s *compressRejectSink) Close() error { s.arena.FreeBuffers(); return nil }

func TestStreamingRealCompressBufRejection(t *testing.T) {
	proc := testutil.NewProc(t)
	attrs, typs, _ := mockSchema(3, 2)
	file := &compressRejectSink{arena: objectio.NewArena(0)}
	pool := &ownershipBuffer{IBatchBuffer: containers.NewOneSchemaBatchBuffer(mpool.MB, attrs, typs, true), active: make(map[*batch.Batch]bool), t: t}
	s := NewSinker(2, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, proc.Mp(), nil, WithMemorySizeThreshold(1), WithBuffer(pool, true))
	t.Cleanup(func() {
		if s.buf.buffers != nil {
			_ = s.Close()
		}
	})
	before := proc.Mp().CurrNB()
	source := containers.MockBatch(typs, int(objectio.BlockMaxRows), 2, nil)
	defer source.Close()
	require.Panics(t, func() { _ = s.Write(context.Background(), containers.ToCNBatch(source)) })
	require.Empty(t, pool.active)
	require.NoError(t, s.Close())
	require.Equal(t, before, proc.Mp().CurrNB())
}

type generationSink struct {
	entered  chan *batch.Batch
	release  chan struct{}
	fail     bool
	observed chan int32
}

func (s *generationSink) Sink(_ context.Context, b *batch.Batch) error {
	if s.entered != nil {
		s.entered <- b
		<-s.release
	}
	s.observed <- vector.MustFixedColNoTypeCheck[int32](b.Vecs[2])[0]
	if s.fail {
		return spillInjectedError
	}
	return nil
}
func (*generationSink) Sync(context.Context) (*objectio.ObjectStats, error) {
	return &objectio.ObjectStats{}, nil
}
func (*generationSink) Reset()       {}
func (*generationSink) Close() error { return nil }

func TestPipelineAcceptedFailureResetGeneration(t *testing.T) {
	proc := testutil.NewProc(t)
	attrs, typs, _ := mockSchema(3, 2)
	entered := make(chan *batch.Batch, 1)
	release := make(chan struct{})
	observed := make(chan int32, 2)
	var factories atomic.Int32
	s := NewSinker(2, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker {
		if factories.Add(1) == 1 {
			return &generationSink{entered: entered, release: release, fail: true, observed: observed}
		}
		return &generationSink{observed: observed}
	}, proc.Mp(), nil, WithMemorySizeThreshold(100*mpool.MB), WithOffHeap(), WithPipelineFlush())
	t.Cleanup(func() {
		if s.buf.buffers != nil {
			_ = s.Close()
		}
	})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	before := proc.Mp().CurrNB()
	source := containers.MockBatch(typs, int(objectio.BlockMaxRows), 2, nil)
	defer source.Close()
	input := containers.ToCNBatch(source)
	for i := range vector.MustFixedColNoTypeCheck[int32](input.Vecs[2]) {
		vector.MustFixedColNoTypeCheck[int32](input.Vecs[2])[i] = 7
	}
	require.NoError(t, s.Write(context.Background(), input))
	require.NoError(t, s.trySpill(context.Background()))
	old := <-entered
	oldResult := s.pipe.result
	resetStarted, resetDone := make(chan struct{}), make(chan struct{})
	go func() { close(resetStarted); s.Reset(); close(resetDone) }()
	<-resetStarted
	early := false
	select {
	case <-resetDone:
		early = true
	default:
	}
	unblock()
	<-resetDone
	require.False(t, early, "Reset returned before accepted job finished")
	require.Equal(t, int32(7), <-observed)
	require.ErrorIs(t, oldResult.getError(), spillInjectedError)
	require.Nil(t, s.pipe.result)
	for i := range vector.MustFixedColNoTypeCheck[int32](input.Vecs[2]) {
		vector.MustFixedColNoTypeCheck[int32](input.Vecs[2])[i] = 19
	}
	require.NoError(t, s.Write(context.Background(), input))
	require.NoError(t, s.trySpill(context.Background()))
	require.NotSame(t, oldResult, s.pipe.result)
	require.NoError(t, s.drainPipeline())
	require.Equal(t, int32(19), <-observed)
	require.NotNil(t, old) // worker held this batch until its first generation finished
	require.NoError(t, s.Close())
	require.Equal(t, before, proc.Mp().CurrNB())
}

func TestAccumulatedAndPipelineMultiBlockContentReset(t *testing.T) {
	for _, pipeline := range []bool{false, true} {
		t.Run(fmt.Sprintf("pipeline=%t", pipeline), func(t *testing.T) {
			proc := testutil.NewProc(t)
			attrs, typs, _ := mockSchema(3, 2)
			file := &contentCheckingSink{}
			opts := []SinkerOption{WithMemorySizeThreshold(100 * mpool.MB), WithTailSizeCap(1), WithOffHeap()}
			if pipeline {
				opts = append(opts, WithPipelineFlush())
			} else {
				opts = append(opts, WithDedupAll())
			}
			s := NewSinker(2, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, proc.Mp(), nil, opts...)
			t.Cleanup(func() {
				if s.buf.buffers != nil {
					_ = s.Close()
				}
			})
			before := proc.Mp().CurrNB()
			n := 2*int(objectio.BlockMaxRows) + 1
			source := containers.MockBatch(typs, n, 2, nil)
			defer source.Close()
			input := containers.ToCNBatch(source)
			for gen := 0; gen < 2; gen++ {
				for col := range input.Vecs {
					values := vector.MustFixedColNoTypeCheck[int32](input.Vecs[col])
					for i := range values {
						values[i] = int32((n-1-i)*10 + col + gen)
					}
				}
				require.NoError(t, s.Write(context.Background(), input))
				require.NoError(t, s.Sync(context.Background()))
				require.Len(t, file.rows, n)
				for i, row := range file.rows {
					for col, value := range row {
						require.Equal(t, int32(i*10+col+gen), value)
					}
				}
				s.Reset()
				file.rows = nil
				file.calls = 0
			}
			require.NoError(t, s.Close())
			require.Equal(t, before, proc.Mp().CurrNB())
		})
	}
}

type replacementFailBuffer struct {
	containers.IBatchBuffer
	mp       *mpool.MPool
	armed    bool
	fetches  int
	reserve  []byte
	fetched  []*batch.Batch
	returned []*batch.Batch
}

func (b *replacementFailBuffer) FetchWithSchema(attrs []string, typs []types.Type) *batch.Batch {
	if b.armed {
		b.fetches++
	}
	var bat *batch.Batch
	if b.armed && b.fetches == 2 {
		// Force a fresh replacement and leave room for only one column.
		bat = batch.NewWithSchema(true, attrs, typs)
		var err error
		b.reserve, err = b.mp.Alloc(int(b.mp.Cap()-b.mp.CurrNB()-40000), true)
		if err != nil {
			panic(err)
		}
	} else {
		bat = b.IBatchBuffer.FetchWithSchema(attrs, typs)
	}
	if b.armed {
		b.fetched = append(b.fetched, bat)
	}
	return bat
}
func (b *replacementFailBuffer) Putback(bat *batch.Batch, mp *mpool.MPool) {
	if b.armed {
		b.returned = append(b.returned, bat)
	}
	b.IBatchBuffer.Putback(bat, mp)
}

func TestReplacementPreExtendFailureAfterTransfer(t *testing.T) {
	for _, pipeline := range []bool{false, true} {
		t.Run(fmt.Sprintf("pipeline=%t", pipeline), func(t *testing.T) {
			mp, err := mpool.NewMPool("replacement-failure", 8*mpool.MB, mpool.NoFixed)
			require.NoError(t, err)
			attrs, typs, _ := mockSchema(3, 2)
			pool := &replacementFailBuffer{IBatchBuffer: containers.NewOneSchemaBatchBuffer(mpool.MB, attrs, typs, true), mp: mp}
			entered, release, observed := make(chan *batch.Batch, 1), make(chan struct{}), make(chan int32, 1)
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			file := &generationSink{entered: entered, release: release, observed: observed}
			opts := []SinkerOption{WithMemorySizeThreshold(100 * mpool.MB), WithBuffer(pool, true)}
			if pipeline {
				opts = append(opts, WithPipelineFlush())
			} else {
				opts = append(opts, WithDedupAll())
			}
			s := NewSinker(2, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, mp, nil, opts...)
			t.Cleanup(func() {
				unblock()
				if pool.reserve != nil {
					mp.Free(pool.reserve)
					pool.reserve = nil
				}
				if s.buf.buffers != nil {
					_ = s.Close()
				}
			})
			source := containers.MockBatch(typs, 2*int(objectio.BlockMaxRows), 2, nil)
			defer source.Close()
			input := containers.ToCNBatch(source)
			for i := range vector.MustFixedColNoTypeCheck[int32](input.Vecs[2]) {
				vector.MustFixedColNoTypeCheck[int32](input.Vecs[2])[i] = 7
			}
			require.NoError(t, s.Write(context.Background(), input))
			pool.armed = true
			if pipeline {
				s.staged.memorySizeThreshold = 1
			}
			err = s.trySpill(context.Background())
			require.Error(t, err)
			require.Len(t, pool.fetched, 2)
			returns := func(bat *batch.Batch) int {
				count := 0
				for _, returned := range pool.returned {
					if returned == bat {
						count++
					}
				}
				return count
			}
			require.Equal(t, 1, returns(pool.fetched[1]))
			if pipeline {
				workerBatch := <-entered
				require.Same(t, pool.fetched[0], workerBatch)
				require.NotContains(t, pool.returned, workerBatch)
				unblock()
				require.NoError(t, s.drainPipeline())
				require.Equal(t, int32(7), <-observed)
			} else {
				require.Equal(t, 1, returns(pool.fetched[0]))
			}
			mp.Free(pool.reserve)
			pool.reserve = nil
			s.Reset()
			require.NoError(t, s.Close())
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestRealSinkerReadbackReset(t *testing.T) {
	proc := testutil.NewProc(t)
	fs, err := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	attrs, typs, seq := mockSchema(3, 2)
	s := NewSinker(2, attrs, typs, NewFSinkerImplFactory(seq, 2, true, false, 0), proc.Mp(), fs,
		WithMemorySizeThreshold(100*mpool.MB), WithTailSizeCap(1), WithOffHeap(), WithDedupAll())
	t.Cleanup(func() {
		if s.buf.buffers != nil {
			_ = s.Close()
		}
	})
	before := proc.Mp().CurrNB()
	n := 2*int(objectio.BlockMaxRows) + 1
	source := containers.MockBatch(typs, n, 2, nil)
	defer source.Close()
	input := containers.ToCNBatch(source)
	for gen := 0; gen < 2; gen++ {
		for col := range input.Vecs {
			values := vector.MustFixedColNoTypeCheck[int32](input.Vecs[col])
			for i := range values {
				values[i] = int32((n-1-i)*10 + col + gen)
			}
		}
		require.NoError(t, s.Write(context.Background(), input))
		require.NoError(t, s.Sync(context.Background()))
		require.NotEmpty(t, s.result.persisted)
		row := 0
		for _, stats := range s.result.persisted {
			reader, err := NewObjectReader(fs, stats.ObjectLocation())
			require.NoError(t, err)
			for block := uint16(0); uint32(block) < stats.BlkCnt(); block++ {
				data, release, err := reader.LoadColumns(context.Background(), seq, typs, block, proc.Mp())
				require.NoError(t, err)
				for i := 0; i < data.RowCount(); i++ {
					for col := range data.Vecs {
						require.Equal(t, int32(row*10+col+gen), vector.MustFixedColNoTypeCheck[int32](data.Vecs[col])[i])
					}
					row++
				}
				release()
			}
		}
		require.Equal(t, n, row)
		s.Reset()
	}
	require.NoError(t, s.Close())
	require.Equal(t, before, proc.Mp().CurrNB())
}

// corruptOnReplacement injects a malformed non-key input only after the
// replacement was fetched. Its PreExtend must still succeed before UnionOne.
type corruptOnReplacement struct {
	containers.IBatchBuffer
	armed             bool
	fetched, returned []*batch.Batch
	priorReturns      []int
	corrupt           func()
}

func (b *corruptOnReplacement) FetchWithSchema(attrs []string, typs []types.Type) *batch.Batch {
	bat := b.IBatchBuffer.FetchWithSchema(attrs, typs)
	if b.armed {
		b.fetched = append(b.fetched, bat)
		prior := 0
		for _, returned := range b.returned {
			if returned == bat {
				prior++
			}
		}
		b.priorReturns = append(b.priorReturns, prior)
		if len(b.fetched) == 2 {
			b.corrupt()
		}
	}
	return bat
}
func (b *corruptOnReplacement) Putback(bat *batch.Batch, mp *mpool.MPool) {
	if b.armed {
		b.returned = append(b.returned, bat)
	}
	b.IBatchBuffer.Putback(bat, mp)
}

func TestReplacementMergePanicOwnershipAndReset(t *testing.T) {
	for _, pipeline := range []bool{false, true} {
		t.Run(fmt.Sprintf("pipeline=%t", pipeline), func(t *testing.T) {
			proc := testutil.NewProc(t)
			mp := proc.Mp()
			before := mp.CurrNB()
			attrs, typs, _ := mockSchema(3, 2)
			pool := &corruptOnReplacement{IBatchBuffer: containers.NewOneSchemaBatchBuffer(mpool.MB, attrs, typs, true)}
			entered, release, observed := make(chan *batch.Batch, 1), make(chan struct{}), make(chan int32, 2)
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			var factories atomic.Int32
			opts := []SinkerOption{WithMemorySizeThreshold(100 * mpool.MB), WithBuffer(pool, true)}
			if pipeline {
				opts = append(opts, WithPipelineFlush())
			} else {
				opts = append(opts, WithDedupAll())
			}
			s := NewSinker(2, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker {
				if pipeline && factories.Add(1) == 1 {
					return &generationSink{entered: entered, release: release, observed: observed}
				}
				return &generationSink{observed: observed}
			}, mp, nil, opts...)
			t.Cleanup(func() {
				unblock()
				if s.buf.buffers != nil {
					_ = s.Close()
				}
			})
			n := 2 * int(objectio.BlockMaxRows)
			source := containers.MockBatch(typs, n, 2, nil)
			defer source.Close()
			input := containers.ToCNBatch(source)
			for col := range input.Vecs {
				values := vector.MustFixedColNoTypeCheck[int32](input.Vecs[col])
				for i := range values {
					values[i] = int32(i)
				}
			}
			require.NoError(t, s.Write(context.Background(), input))
			pool.corrupt = func() { s.staged.inMemory[1].Vecs[0].Free(mp) }
			pool.armed = true
			if pipeline {
				s.staged.memorySizeThreshold = 1
			}
			var panicStack []byte
			func() {
				defer func() {
					if recover() != nil {
						panicStack = debug.Stack()
					}
				}()
				_ = s.trySpill(context.Background())
			}()
			require.Contains(t, string(panicStack), "vector.(*Vector).UnionOne")
			require.Len(t, pool.fetched, 2)
			count := func(bat *batch.Batch) int {
				n := 0
				for _, b := range pool.returned {
					if b == bat {
						n++
					}
				}
				return n
			}
			require.Equal(t, 1, count(pool.fetched[1])-pool.priorReturns[1], "active replacement must return exactly once")
			if pipeline {
				worker := <-entered
				require.Same(t, pool.fetched[0], worker)
				require.Zero(t, count(worker)-pool.priorReturns[0], "worker owns submitted output")
				unblock()
				require.NoError(t, s.drainPipeline())
				require.Equal(t, int32(0), <-observed)
			} else {
				require.Equal(t, 1, count(pool.fetched[0])-pool.priorReturns[0])
			}
			pool.armed = false
			s.Reset()
			for col := range input.Vecs {
				values := vector.MustFixedColNoTypeCheck[int32](input.Vecs[col])
				for i := range values {
					values[i] = int32(i + 19)
				}
			}
			require.NoError(t, s.Write(context.Background(), input))
			require.NoError(t, s.Sync(context.Background()))
			require.Equal(t, int32(19), <-observed)
			require.NoError(t, s.Close())
			require.Equal(t, before, mp.CurrNB())
		})
	}
}
