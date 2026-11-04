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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
	require.Equal(t, 1, pool.Len())
	require.Greater(t, mp.CurrNB(), baseline, "first column should have allocated before failure")
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
