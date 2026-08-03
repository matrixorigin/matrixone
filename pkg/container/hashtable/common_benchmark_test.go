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

package hashtable

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

func BenchmarkInt64HashMapSegmentedFind(b *testing.B) {
	const entryCount = 100_000

	mp := mpool.MustNewNoLock(b.Name())
	ht := new(Int64HashMap)
	if err := ht.Init(mp); err != nil {
		b.Fatal(err)
	}

	hashes := make([]uint64, entryCount)
	values := make([]uint64, entryCount)
	for i := range hashes {
		hashes[i] = uint64(i+1) * 0x9e3779b97f4a7c15
	}
	if err := ht.InsertBatch(len(hashes), hashes, nil, values); err != nil {
		b.Fatal(err)
	}
	target := maxElemCnt(maxIntCellCntPerBlock*8, intCellSize)
	if err := ht.ResizeOnDemand(int(target - ht.elemCnt)); err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() {
		ht.Free()
		mpool.DeleteMPool(mp)
	})
	b.SetBytes(int64(len(hashes) * 8))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		ht.FindBatch(len(hashes), hashes, nil, values)
	}
}
