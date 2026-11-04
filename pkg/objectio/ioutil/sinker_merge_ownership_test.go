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
	"sync"
	"testing"
	"time"

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

type panicFetchBuffer struct {
	*containers.BatchFreeList
	calls, panicAt int
	panicPutAfter  int
	panicValue     any
	beforePanic    func()
	beforeFetch    func(int)
	freshAt        int
}

func (b *panicFetchBuffer) FetchWithSchema(attrs []string, typs []types.Type) *batch.Batch {
	b.calls++
	if b.beforeFetch != nil {
		b.beforeFetch(b.calls)
	}
	if b.calls == b.panicAt {
		if b.beforePanic != nil {
			b.beforePanic()
		}
		panic(b.panicValue)
	}
	if b.calls == b.freshAt {
		return batch.NewWithSchema(true, attrs, typs)
	}
	return b.BatchFreeList.FetchWithSchema(attrs, typs)
}

func TestMergeSpillReplacementFetchError(t *testing.T) {
	for _, mode := range []string{"accumulate", "pipeline"} {
		t.Run(mode, func(t *testing.T) {
			const capBytes = 8 << 20
			mp, err := mpool.NewMPool("replacement-fetch-error", capBytes, mpool.NoFixed)
			require.NoError(t, err)
			defer mpool.DeleteMPool(mp)
			fs, err := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
			require.NoError(t, err)
			attrs := []string{"a", "b", "c"}
			typs := []types.Type{types.T_int32.ToType(), types.T_int32.ToType(), types.T_int32.ToType()}
			buf := &panicFetchBuffer{BatchFreeList: containers.NewBatchFreeList(attrs, typs, true)}
			opts := []SinkerOption{WithBuffer(buf, true), WithOffHeap(), WithMemorySizeThreshold(1 << 30)}
			if mode == "accumulate" {
				opts = append(opts, WithDedupAll())
			} else {
				opts = append(opts, WithPipelineFlush())
			}
			sinker := NewSinker(2, attrs, typs, mockFactory(nil, nil), mp, fs, opts...)
			defer sinker.Close()
			data := containers.MockBatch(typs, 100, 2, nil)
			defer data.Close()
			baseline := mp.CurrNB()
			require.NoError(t, sinker.Write(context.Background(), containers.ToCNBatch(data)))
			target := buf.calls + 2
			buf.freshAt = target
			var filler []byte
			defer func() {
				if filler != nil {
					mp.Free(filler)
				}
			}()
			buf.beforeFetch = func(call int) {
				if call == target {
					// Leave insufficient room for a block vector during PreExtend.
					filler, err = mp.Alloc(int(capBytes-mp.CurrNB()-1024), true)
					require.NoError(t, err)
				}
			}
			err = sinker.trySpill(context.Background())
			require.Error(t, err)
			require.Equal(t, target, buf.calls)
			require.NotNil(t, filler)
			mp.Free(filler)
			filler = nil
			require.NoError(t, sinker.Close())
			require.Equal(t, baseline, mp.CurrNB())
		})
	}
}

type blockingOwnershipSinker struct {
	mockFileSinker
	started chan struct{}
	release chan struct{}
	rows    chan int
}

type keyRecordingSinker struct {
	mockFileSinker
	keys []int64
}

func (s *keyRecordingSinker) Sink(_ context.Context, bat *batch.Batch) error {
	s.keys = append(s.keys, vector.MustFixedColNoTypeCheck[int64](bat.Vecs[0])...)
	return nil
}

func TestMergeSpillResetKeepsNewData(t *testing.T) {
	for _, mode := range []string{"accumulate", "pipeline"} {
		t.Run(mode, func(t *testing.T) {
			proc := testutil.NewProc(t)
			fs, err := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
			require.NoError(t, err)
			attrs := []string{"key"}
			typs := []types.Type{types.T_int64.ToType()}
			buf := &panicFetchBuffer{BatchFreeList: containers.NewBatchFreeList(attrs, typs, true)}
			file := &keyRecordingSinker{}
			opts := []SinkerOption{WithBuffer(buf, true), WithMemorySizeThreshold(1 << 30)}
			if mode == "accumulate" {
				opts = append(opts, WithDedupAll())
			} else {
				opts = append(opts, WithPipelineFlush())
			}
			sinker := NewSinker(0, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, proc.Mp(), fs, opts...)
			defer sinker.Close()
			input := batch.NewWithSchema(false, attrs, typs)
			defer input.Clean(proc.Mp())
			for _, key := range []int64{8, 7} {
				require.NoError(t, vector.AppendFixed(input.Vecs[0], key, false, proc.Mp()))
			}
			input.SetRowCount(2)
			require.NoError(t, sinker.Write(context.Background(), input))
			want := errors.New("old generation panic")
			buf.panicValue, buf.panicAt = want, buf.calls+2
			require.PanicsWithValue(t, want, func() { _ = sinker.trySpill(context.Background()) })
			sinker.Reset()
			buf.panicAt = 0
			input.CleanOnlyData()
			for _, key := range []int64{3, 1, 3, 2, 1} {
				require.NoError(t, vector.AppendFixed(input.Vecs[0], key, false, proc.Mp()))
			}
			input.SetRowCount(5)
			require.NoError(t, sinker.Write(context.Background(), input))
			require.NoError(t, sinker.trySpill(context.Background()))
			require.NoError(t, sinker.drainPipeline())
			if mode == "accumulate" {
				require.Equal(t, []int64{1, 2, 3}, file.keys)
			} else {
				require.Equal(t, []int64{1, 1, 2, 3, 3}, file.keys)
			}
		})
	}
}

func TestMergeSpillFullBlockFetchPanicByRange(t *testing.T) {
	for _, mode := range []string{"accumulate", "pipeline"} {
		for _, disjoint := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/disjoint=%t", mode, disjoint), func(t *testing.T) {
				proc := testutil.NewProc(t)
				fs, err := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
				require.NoError(t, err)
				attrs := []string{"key"}
				typs := []types.Type{types.T_int64.ToType()}
				buf := &panicFetchBuffer{BatchFreeList: containers.NewBatchFreeList(attrs, typs, true)}
				opts := []SinkerOption{WithBuffer(buf, true), WithMemorySizeThreshold(1 << 30)}
				if mode == "accumulate" {
					opts = append(opts, WithDedupAll())
				} else {
					opts = append(opts, WithPipelineFlush())
				}
				sinker := NewSinker(0, attrs, typs, mockFactory(nil, nil), proc.Mp(), fs, opts...)
				defer sinker.Close()
				input := batch.NewWithSchema(false, attrs, typs)
				defer input.Clean(proc.Mp())
				for part := range 2 {
					for i := range int(objectio.BlockMaxRows) {
						key := int64(i*2 + part)
						if disjoint {
							key = int64(part*int(objectio.BlockMaxRows) + i)
						}
						require.NoError(t, vector.AppendFixed(input.Vecs[0], key, false, proc.Mp()))
					}
				}
				input.SetRowCount(2 * int(objectio.BlockMaxRows))
				bytes, objects := proc.Mp().OnHeapOutstanding()
				offHeap := proc.Mp().CurrNB()
				require.NoError(t, sinker.Write(context.Background(), input))
				require.Len(t, sinker.staged.inMemory, 2)
				first := vector.MustFixedColNoTypeCheck[int64](sinker.staged.inMemory[0].Vecs[0])
				second := vector.MustFixedColNoTypeCheck[int64](sinker.staged.inMemory[1].Vecs[0])
				if disjoint {
					require.Less(t, first[len(first)-1], second[0])
				} else {
					require.Less(t, second[0], first[len(first)-1])
				}
				want := errors.New("full output replacement panic")
				buf.panicValue, buf.panicAt = want, buf.calls+2
				require.PanicsWithValue(t, want, func() { _ = sinker.trySpill(context.Background()) })
				require.Equal(t, buf.panicAt, buf.calls)
				require.NoError(t, sinker.Close())
				gotBytes, gotObjects := proc.Mp().OnHeapOutstanding()
				require.Equal(t, bytes, gotBytes)
				require.Equal(t, objects, gotObjects)
				require.Equal(t, offHeap, proc.Mp().CurrNB())
			})
		}
	}
}

func (s *blockingOwnershipSinker) Sink(_ context.Context, bat *batch.Batch) error {
	close(s.started)
	<-s.release
	s.rows <- bat.RowCount()
	return nil
}

func TestPipelineProducerPanicAfterWorkerTakesBatch(t *testing.T) {
	proc := testutil.NewProc(t)
	fs, err := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	attrs, typs, _ := mockSchema(3, 2)
	buf := &panicFetchBuffer{BatchFreeList: containers.NewBatchFreeList(attrs, typs, true)}
	worker := &blockingOwnershipSinker{started: make(chan struct{}), release: make(chan struct{}), rows: make(chan int, 1)}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(worker.release) }) }
	factory := func(*mpool.MPool, fileservice.FileService) FileSinker { return worker }
	sinker := NewSinker(2, attrs, typs, factory, proc.Mp(), fs, WithBuffer(buf, true), WithMemorySizeThreshold(1<<30), WithPipelineFlush())
	t.Cleanup(func() { release(); _ = sinker.Close() })
	data := containers.MockBatch(typs, 100, 2, nil)
	defer data.Close()
	bytes, objects := proc.Mp().OnHeapOutstanding()
	offHeap := proc.Mp().CurrNB()
	require.NoError(t, sinker.Write(context.Background(), containers.ToCNBatch(data)))
	sinker.staged.memorySizeThreshold = 1
	want := errors.New("producer panic after submit")
	buf.panicValue = want
	buf.panicAt = buf.calls + 2
	buf.beforePanic = func() {
		select {
		case <-worker.started:
		case <-time.After(5 * time.Second):
			panic("worker did not take submitted batch")
		}
	}
	require.PanicsWithValue(t, want, func() { _ = sinker.trySpill(context.Background()) })
	require.Equal(t, buf.panicAt, buf.calls)
	// The worker still owns the batch when the producer unwinds.
	release()
	require.NoError(t, sinker.drainPipeline())
	require.Equal(t, 100, <-worker.rows)
	select {
	case rows := <-worker.rows:
		t.Fatalf("worker consumed an extra batch: %d rows", rows)
	default:
	}
	require.NoError(t, sinker.Close())
	gotBytes, gotObjects := proc.Mp().OnHeapOutstanding()
	require.Equal(t, bytes, gotBytes)
	require.Equal(t, objects, gotObjects)
	require.Equal(t, offHeap, proc.Mp().CurrNB())
}

type panicAfterRealSink struct {
	*FSinkerImpl
	want   error
	closes int
}

func (s *panicAfterRealSink) Sink(ctx context.Context, bat *batch.Batch) error {
	if err := s.FSinkerImpl.Sink(ctx, bat); err != nil {
		return err
	}
	panic(s.want)
}

func (s *panicAfterRealSink) Close() error {
	s.closes++
	return s.FSinkerImpl.Close()
}

func TestStreamingPanicAfterRealSinkClosesWriter(t *testing.T) {
	proc := testutil.NewProc(t)
	fs, err := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	attrs, typs, _ := mockSchema(3, 2)
	want := errors.New("panic after real sink")
	base := NewFSinkerImplFactory([]uint16{0, 1, 2}, 2, false, false, 0)(proc.Mp(), fs).(*FSinkerImpl)
	file := &panicAfterRealSink{FSinkerImpl: base, want: want}
	sinker := NewSinker(2, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, proc.Mp(), fs, WithMemorySizeThreshold(1))
	t.Cleanup(func() { _ = sinker.Close() })
	data := containers.MockBatch(typs, 100, 2, nil)
	defer data.Close()
	bytes, objects := proc.Mp().OnHeapOutstanding()
	offHeap := proc.Mp().CurrNB()
	require.PanicsWithValue(t, want, func() { _ = sinker.Write(context.Background(), containers.ToCNBatch(data)) })
	require.NotNil(t, file.arena)
	require.NotNil(t, file.writer)
	require.NoError(t, sinker.Close())
	require.Equal(t, 1, file.closes)
	require.Nil(t, file.arena)
	require.Nil(t, file.writer)
	gotBytes, gotObjects := proc.Mp().OnHeapOutstanding()
	require.Equal(t, bytes, gotBytes)
	require.Equal(t, objects, gotObjects)
	require.Equal(t, offHeap, proc.Mp().CurrNB())
}

func (b *panicFetchBuffer) Putback(bat *batch.Batch, mp *mpool.MPool) {
	b.BatchFreeList.Putback(bat, mp)
	if b.panicPutAfter > 0 && b.calls >= b.panicPutAfter {
		b.panicPutAfter = 0
		panic(b.panicValue)
	}
}

func TestMergeSpillReplacementFetchPanic(t *testing.T) {
	for _, mode := range []string{"accumulate", "pipeline"} {
		t.Run(mode, func(t *testing.T) {
			proc := testutil.NewProc(t)
			fs, err := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
			require.NoError(t, err)
			attrs, typs, _ := mockSchema(3, 2)
			buf := &panicFetchBuffer{BatchFreeList: containers.NewBatchFreeList(attrs, typs, true)}
			opts := []SinkerOption{WithBuffer(buf, true), WithMemorySizeThreshold(1 << 30)}
			if mode == "accumulate" {
				opts = append(opts, WithDedupAll())
			} else {
				opts = append(opts, WithPipelineFlush())
			}
			sinker := NewSinker(2, attrs, typs, mockFactory(nil, nil), proc.Mp(), fs, opts...)
			t.Cleanup(func() { _ = sinker.Close() })
			data := containers.MockBatch(typs, 100, 2, nil)
			defer data.Close()
			bytes, objects := proc.Mp().OnHeapOutstanding()
			offHeap := proc.Mp().CurrNB()
			require.NoError(t, sinker.Write(context.Background(), containers.ToCNBatch(data)))
			want := errors.New("replacement fetch panic")
			buf.panicValue = want
			buf.panicAt = buf.calls + 2 // initial merge buffer succeeds, replacement panics.
			require.PanicsWithValue(t, want, func() { _ = sinker.trySpill(context.Background()) })
			require.Equal(t, buf.panicAt, buf.calls)
			// Each returned batch must occur once in the freelist.
			count := buf.Len()
			batches := make([]*batch.Batch, 0, count)
			seen := make(map[*batch.Batch]bool)
			for i := 0; i < count; i++ {
				b := buf.FetchWithSchema(attrs, typs)
				batches = append(batches, b)
				require.False(t, seen[b], "batch returned more than once")
				seen[b] = true
			}
			for _, b := range batches {
				sinker.putBackBuffer(b)
			}
			sinker.Reset()
			buf.panicAt = 0
			require.NoError(t, sinker.Write(context.Background(), containers.ToCNBatch(data)))
			require.NoError(t, sinker.trySpill(context.Background()))
			require.NoError(t, sinker.drainPipeline())
			require.NoError(t, sinker.Close())
			gotBytes, gotObjects := proc.Mp().OnHeapOutstanding()
			require.Equal(t, bytes, gotBytes)
			require.Equal(t, objects, gotObjects)
			require.Equal(t, offHeap, proc.Mp().CurrNB())
		})
	}
}

// Two overlapping full blocks force another source return after the first
// output and its replacement have been handed off.
func TestMergeSpillReplacementOwnedOnPanic(t *testing.T) {
	for _, mode := range []string{"accumulate", "pipeline"} {
		t.Run(mode, func(t *testing.T) {
			proc := testutil.NewProc(t)
			fs, err := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
			require.NoError(t, err)
			attrs, typs, _ := mockSchema(3, 2)
			buf := &panicFetchBuffer{BatchFreeList: containers.NewBatchFreeList(attrs, typs, true)}
			opts := []SinkerOption{WithBuffer(buf, true), WithMemorySizeThreshold(1 << 30)}
			if mode == "accumulate" {
				opts = append(opts, WithDedupAll())
			} else {
				opts = append(opts, WithPipelineFlush())
			}
			sinker := NewSinker(2, attrs, typs, mockFactory(nil, nil), proc.Mp(), fs, opts...)
			t.Cleanup(func() { _ = sinker.Close() })
			data := containers.MockBatch(typs, int(objectio.BlockMaxRows), 2, nil)
			defer data.Close()
			bytes, objects := proc.Mp().OnHeapOutstanding()
			offHeap := proc.Mp().CurrNB()
			require.NoError(t, sinker.Write(context.Background(), containers.ToCNBatch(data)))
			require.NoError(t, sinker.Write(context.Background(), containers.ToCNBatch(data)))
			want := errors.New("source returned after replacement")
			buf.panicValue = want
			buf.panicPutAfter = buf.calls + 2
			require.PanicsWithValue(t, want, func() { _ = sinker.trySpill(context.Background()) })
			require.Zero(t, buf.panicPutAfter)
			require.NoError(t, sinker.Close())
			gotBytes, gotObjects := proc.Mp().OnHeapOutstanding()
			require.Equal(t, bytes, gotBytes)
			require.Equal(t, objects, gotObjects)
			require.Equal(t, offHeap, proc.Mp().CurrNB())
		})
	}
}
