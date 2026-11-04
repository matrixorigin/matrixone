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

type panicFetchBuffer struct {
	*containers.BatchFreeList
	calls, panicAt int
	panicPutAfter  int
	panicValue     any
}

func (b *panicFetchBuffer) FetchWithSchema(attrs []string, typs []types.Type) *batch.Batch {
	b.calls++
	if b.calls == b.panicAt {
		panic(b.panicValue)
	}
	return b.BatchFreeList.FetchWithSchema(attrs, typs)
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
