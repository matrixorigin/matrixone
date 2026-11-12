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
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestSpoolConcurrentCopiesPreserveSource(t *testing.T) {
	for _, tc := range []struct {
		name  string
		extra []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"aggregate_metadata", []byte("aggregate state")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZeroNoFixed()
			var src *batch.Batch
			const readers = 8
			caches := make([]*cachedBatch, readers)
			t.Cleanup(func() {
				defer mpool.DeleteMPool(mp)
				for _, cache := range caches {
					if cache != nil {
						cache.free()
					}
				}
				if src != nil {
					src.Clean(mp)
				}
				require.Zero(t, mp.CurrNB())
			})
			src = newSpoolTestBatch(t, mp, 1)
			src.ExtraBuf = tc.extra
			for i := range caches {
				caches[i] = initCachedBatch(mp, 1)
			}
			// Reuse each destination cache with nonempty -> nil -> empty metadata.
			for _, extra := range [][]byte{tc.extra, nil, {}} {
				func() {
					src.ExtraBuf = extra
					type copied struct {
						batch    *batch.Batch
						cacheID  uint32
						useCache bool
						err      error
					}
					results := make([]copied, readers)
					start := make(chan struct{})
					var workers sync.WaitGroup
					for i, cache := range caches {
						workers.Add(1)
						go func() {
							defer workers.Done()
							<-start
							r := &results[i]
							r.batch, r.useCache, r.cacheID, r.err = cache.GetCopiedBatch(src)
						}()
					}
					close(start)
					workers.Wait()
					defer func() {
						for i, result := range results {
							if result.err == nil && result.batch != nil {
								caches[i].CacheBatch(result.useCache, result.cacheID, result.batch)
							}
						}
					}()
					require.Equal(t, extra, src.ExtraBuf, "copy must not transfer metadata from the source")
					for i, result := range results {
						require.NoError(t, result.err)
						require.True(t, result.useCache)
						require.Equal(t, extra, result.batch.ExtraBuf)
						require.Equal(t, src.RowCount(), result.batch.RowCount())
						require.Equal(t, int64(1), vector.MustFixedColNoTypeCheck[int64](result.batch.Vecs[0])[0])
						if len(extra) > 0 {
							require.NotSame(t, &src.ExtraBuf[0], &result.batch.ExtraBuf[0])
							for j := 0; j < i; j++ {
								require.NotSame(t, &results[j].batch.ExtraBuf[0], &result.batch.ExtraBuf[0])
							}
						}
					}
					if len(extra) > 0 {
						results[0].batch.ExtraBuf[0] ^= 0xff
						require.Equal(t, extra, src.ExtraBuf)
						for _, result := range results[1:] {
							require.Equal(t, extra, result.batch.ExtraBuf)
						}
					}
				}()
			}
		})
	}
}
