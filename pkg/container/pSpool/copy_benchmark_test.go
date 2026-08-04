// Copyright 2026 Matrix Origin
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

package pSpool

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func BenchmarkCachedBatchReuse(b *testing.B) {
	mp := mpool.MustNewZero()
	source := batch.NewWithSize(1)
	source.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	values := make([]int64, 8192)
	for i := range values {
		values[i] = int64(i)
	}
	if err := vector.AppendFixedList(
		source.Vecs[0],
		values,
		nil,
		mp,
	); err != nil {
		b.Fatal(err)
	}
	source.SetRowCount(len(values))
	cache := initCachedBatch(mp, 1)
	b.Cleanup(func() {
		source.Clean(mp)
		cache.free()
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copied, useCache, cacheID, err := cache.GetCopiedBatch(source)
		if err != nil {
			b.Fatal(err)
		}
		cache.CacheBatch(useCache, cacheID, copied)
	}
}
