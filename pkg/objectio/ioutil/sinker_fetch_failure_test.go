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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

type fetchTrackingBuffer struct {
	*containers.BatchFreeList
	last *batch.Batch
}

func (b *fetchTrackingBuffer) FetchWithSchema(attrs []string, typs []types.Type) *batch.Batch {
	b.last = b.BatchFreeList.FetchWithSchema(attrs, typs)
	return b.last
}

func TestFetchBufferPreExtendFailureReleasesBatch(t *testing.T) {
	for _, tc := range []struct{ reuse, throughWrite bool }{
		{}, {reuse: true}, {throughWrite: true},
	} {
		t.Run(fmt.Sprintf("reuse=%t/write=%t", tc.reuse, tc.throughWrite), func(t *testing.T) {
			const capBytes = 1 << 20
			mp, err := mpool.NewMPool("fetch-buffer-failure", capBytes, mpool.NoFixed)
			require.NoError(t, err)
			defer mpool.DeleteMPool(mp)
			// Leave room for exactly two int32 block vectors, so the third
			// allocation fails after earlier columns have acquired memory.
			fillerSize := capBytes - 2*int(objectio.BlockMaxRows)*4
			filler, err := mp.Alloc(fillerSize, true)
			require.NoError(t, err)
			defer mp.Free(filler)
			attrs := []string{"a", "b", "c"}
			typs := []types.Type{types.T_int32.ToType(), types.T_int32.ToType(), types.T_int32.ToType()}
			buf := &fetchTrackingBuffer{BatchFreeList: containers.NewBatchFreeList(attrs, typs, true)}
			if tc.reuse {
				bat := buf.FetchWithSchema(attrs, typs)
				require.NoError(t, bat.Vecs[0].PreExtend(objectio.BlockMaxRows, mp))
				buf.Putback(bat, mp)
			}
			sinker := NewSinker(-1, attrs, typs, nil, mp, nil, WithOffHeap(), WithBuffer(buf, true))
			defer sinker.Close()
			if tc.throughWrite {
				input := containers.MockBatch(typs, 100, -1, nil)
				defer input.Close()
				err = sinker.Write(context.Background(), containers.ToCNBatch(input))
			} else {
				_, err = sinker.fetchBuffer()
			}
			require.Error(t, err)
			require.NotNil(t, buf.last)
			got := mp.CurrNB()
			if got != int64(fillerSize) {
				// Avoid carrying the leaked baseline allocation into other tests.
				buf.last.Clean(mp)
			}
			require.Equal(t, int64(fillerSize), got, "failed pre-extension must release all fetched vector allocations")
		})
	}
}
