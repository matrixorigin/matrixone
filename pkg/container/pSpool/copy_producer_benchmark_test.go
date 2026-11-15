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
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// Independent producers model separate spools. Each producer owns its source:
// GetCopiedBatch transfers ExtraBuf and must not share a mutable source batch.
func BenchmarkCachedBatchProducerShapes(b *testing.B) {
	for _, tc := range []struct {
		name                string
		columns, rows, size int
		ragged, fixed       bool
	}{
		{"fixed_8192", 1, 8192, 0, false, true},
		{"tiny_exact_2", 2, 1, 64, false, false},
		{"tiny_slack_2", 2, 1, 65, false, false},
		{"ragged_2", 2, 1, 65, true, false},
		{"dense_exact_17", 17, 1, 64, false, false},
		{"dense_exact_20", 20, 1, 64, false, false},
		{"dense_exact_128", 128, 1, 64, false, false},
		{"ragged_20", 20, 1, 65, true, false},
		{"bulk_varlen_2", 2, 256, 65, true, false},
	} {
		b.Run(tc.name, func(b *testing.B) {
			type producer struct {
				cache   *cachedBatch
				sources [2]*batch.Batch
			}
			producers := make([]producer, runtime.GOMAXPROCS(0))
			for worker := range producers {
				sm := mpool.MustNewZeroNoFixed()
				b.Cleanup(func() {
					defer mpool.DeleteMPool(sm)
					if sm.CurrNB() != 0 {
						b.Error("source leak")
					}
				})
				for version := range 2 {
					source := batch.NewWithSize(tc.columns)
					b.Cleanup(func() { source.Clean(sm) })
					for column := range tc.columns {
						if tc.fixed {
							source.Vecs[column] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
							if e := vector.AppendFixedList(source.Vecs[column], make([]int64, tc.rows), nil, sm); e != nil {
								b.Fatal(e)
							}
						} else {
							source.Vecs[column] = vector.NewOffHeapVecWithType(types.T_text.ToType())
							size := tc.size
							if tc.ragged {
								size += column*17 + version*(64+column*14)
							}
							value := bytes.Repeat([]byte{byte(column + 1)}, size)
							for range tc.rows {
								if e := vector.AppendBytes(source.Vecs[column], value, false, sm); e != nil {
									b.Fatal(e)
								}
							}
						}
					}
					source.SetRowCount(tc.rows)
					producers[worker].sources[version] = source
				}
				mp := mpool.MustNewZeroNoFixed()
				b.Cleanup(func() {
					defer mpool.DeleteMPool(mp)
					if mp.CurrNB() != 0 {
						b.Error("target leak")
					}
				})
				producers[worker].cache = initCachedBatch(mp, 1)
				b.Cleanup(producers[worker].cache.free)
				for warm := range 4 {
					got, use, id, e := producers[worker].cache.GetCopiedBatch(producers[worker].sources[warm%2])
					if e != nil {
						b.Fatal(e)
					}
					producers[worker].cache.CacheBatch(use, id, got)
				}
			}
			var next atomic.Int64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				p := &producers[next.Add(1)-1]
				iteration := 0
				for pb.Next() {
					got, use, id, e := p.cache.GetCopiedBatch(p.sources[iteration%2])
					if e != nil {
						b.Error(e)
						return
					}
					p.cache.CacheBatch(use, id, got)
					iteration++
				}
			})
		})
	}
}
