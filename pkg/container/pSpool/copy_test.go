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

package pSpool

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func TestOneBatchMemoryCacheSelection(t *testing.T) {
	for _, test := range []struct {
		name               string
		buffers            []int
		dataSize, areaSize int
		dataCap, areaCap   int
		remaining          int
	}{
		{"data_first_exact", []int{64, 128, 256}, 128, 64, 128, 64, 1},
		{"area_first_exact", []int{64, 128, 256}, 64, 128, 64, 128, 1},
		{"data_only_exact", []int{64, 128}, 64, 0, 64, 0, 1},
		{"area_only_exact", []int{64, 128}, 0, 64, 0, 64, 1},
		{"equal_requests", []int{64, 64, 128}, 64, 64, 64, 64, 1},
		{"larger_best_fit", []int{64, 128, 256}, 100, 200, 128, 256, 1},
		{"undersized_fallback", []int{64, 128}, 256, 0, 128, 0, 1},
		{"zero_requests", []int{64, 128}, 0, 0, 0, 0, 2},
		{"empty_cache", nil, 64, 128, 0, 0, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() {
				defer mpool.DeleteMPool(mp)
				require.Zero(t, mp.CurrNB())
			})
			cache := oneBatchMemoryCache{}
			t.Cleanup(func() {
				for i := range cache.buffers {
					cache.buffers[i].Free(mp)
				}
			})
			for _, size := range test.buffers {
				func() {
					owner := vector.NewOffHeapVecWithType(types.T_int8.ToType())
					defer owner.Free(mp)
					require.NoError(t, owner.PreExtend(size, mp))
					cache.buffers = append(cache.buffers, vector.DetachVectorData(owner))
				}()
			}
			vec := vector.NewOffHeapVecWithType(types.T_text.ToType())
			t.Cleanup(func() { vec.Free(mp) })
			require.NoError(t, cache.setSuitableBuffers([]batchMemoryRequest{
				{vec, test.dataSize, vector.DetachedDataBuffer},
				{vec, test.areaSize, vector.DetachedAreaBuffer},
			}))
			require.Equal(t, test.dataCap, cap(vec.GetData()))
			require.Equal(t, test.areaCap, cap(vec.GetArea()))
			require.Len(t, cache.buffers, test.remaining)
		})
	}
}

func TestCachedBatchExactFitReuse(t *testing.T) {
	for _, test := range []struct {
		name         string
		payloadBytes int
	}{
		{"exact", 4096},
		{"spare_capacity", 4097},
	} {
		t.Run(test.name, func(t *testing.T) {
			sourceMP := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() {
				defer mpool.DeleteMPool(sourceMP)
				require.Zero(t, sourceMP.CurrNB())
			})
			source := batch.NewWithSize(1)
			t.Cleanup(func() { source.Clean(sourceMP) })
			source.Vecs[0] = vector.NewOffHeapVecWithType(types.T_text.ToType())
			payload := bytes.Repeat([]byte("x"), test.payloadBytes)
			require.NoError(t, vector.AppendBytes(source.Vecs[0], payload, false, sourceMP))
			source.SetRowCount(1)

			mp := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() {
				defer mpool.DeleteMPool(mp)
				require.Zero(t, mp.CurrNB())
			})
			cache := initCachedBatch(mp, 1)
			t.Cleanup(cache.free)
			copyAndReturn := func() {
				copied, useCache, cacheID, err := cache.GetCopiedBatch(source)
				require.NoError(t, err)
				defer cache.CacheBatch(useCache, cacheID, copied)
				require.Equal(t, 1, copied.RowCount())
				require.False(t, copied.Vecs[0].IsNull(0))
				require.Equal(t, payload, copied.Vecs[0].GetBytesAt(0))
			}
			copyAndReturn()
			allocated, retained := mp.Stats().NumAllocBytes.Load(), mp.CurrNB()
			for range 2 {
				copyAndReturn()
				require.Equal(t, allocated, mp.Stats().NumAllocBytes.Load(),
					"an identical batch must not allocate replacement backing after warmup")
				require.Equal(t, retained, mp.CurrNB(),
					"an identical batch must not inflate retained backing after warmup")
			}
		})
	}
}
