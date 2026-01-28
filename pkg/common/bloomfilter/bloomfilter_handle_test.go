// Copyright 2021 - 2024 Matrix Origin
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

package bloomfilter

import (
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestBloomFilter_HandleAndRefCount(t *testing.T) {
	bf := New(1000, 0.0001)
	require.NotNil(t, bf.shared)
	require.Equal(t, int32(1), bf.shared.refCount)

	// Test NewHandle
	h1 := bf.NewHandle()
	require.NotNil(t, h1)
	require.Equal(t, bf.shared, h1.shared)
	require.Equal(t, int32(2), bf.shared.refCount)

	h2 := h1.NewHandle()
	require.NotNil(t, h2)
	require.Equal(t, bf.shared, h2.shared)
	require.Equal(t, int32(3), bf.shared.refCount)

	// Test Free
	h2.Free()
	require.Equal(t, int32(2), bf.shared.refCount)
	require.Nil(t, h2.shared) // h2 should be cleared

	bf.Free()
	require.Equal(t, int32(1), h1.shared.refCount)
	require.Nil(t, bf.shared)

	h1.Free()
	// After last Free, Clean should have been called, but we can't easily check internal state
	// of a nil shared, but we can check that it doesn't crash.
}

func TestBloomFilter_ConcurrentHandles(t *testing.T) {
	mp := mpool.MustNewZero()
	bf := New(1000, 0.0001)
	defer bf.Free()

	vec := vector.NewVec(types.T_int64.ToType())
	vector.AppendFixed(vec, int64(1), false, mp)
	vector.AppendFixed(vec, int64(2), false, mp)
	vector.AppendFixed(vec, int64(3), false, mp)
	defer vec.Free(mp)
	bf.Add(vec)

	const numGoroutines = 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			h := bf.NewHandle()
			defer h.Free()

			// Each handle should be able to test concurrently without interference
			require.True(t, h.TestRow(vec, 0)) // value 1
			require.True(t, h.TestRow(vec, 1)) // value 2
			require.True(t, h.TestRow(vec, 2)) // value 3

			h.Test(vec, func(exist bool, row int) {
				if row < 3 {
					require.True(t, exist)
				}
			})
		}()
	}
	wg.Wait()
}

func TestBloomFilter_TestRow(t *testing.T) {
	mp := mpool.MustNewZero()
	bf := New(1000, 0.0001)
	defer bf.Free()

	vec := vector.NewVec(types.T_int32.ToType())
	vector.AppendFixed(vec, int32(10), false, mp)
	vector.AppendFixed(vec, int32(20), false, mp)
	vector.AppendFixed(vec, int32(30), false, mp)
	vector.AppendFixed(vec, int32(40), false, mp)
	vector.AppendFixed(vec, int32(50), false, mp)
	defer vec.Free(mp)
	bf.Add(vec)

	require.True(t, bf.TestRow(vec, 0))
	require.True(t, bf.TestRow(vec, 2))
	require.True(t, bf.TestRow(vec, 4))

	// Test a value not in filter
	missingVec := vector.NewVec(types.T_int32.ToType())
	vector.AppendFixed(missingVec, int32(99), false, mp)
	defer missingVec.Free(mp)
	require.False(t, bf.TestRow(missingVec, 0))

	// Test row out of range
	require.False(t, bf.TestRow(vec, -1))
	require.False(t, bf.TestRow(vec, 100))
}

func TestBloomFilter_FreeCleanup(t *testing.T) {
	bf := New(100, 0.01)
	shared := bf.shared
	require.NotNil(t, shared.bitmap.Ptr())

	bf.Free()
	require.Nil(t, shared.bitmap.Ptr())
	require.Nil(t, shared.hashSeed)
}

func TestBloomFilter_MarshalUnmarshalHandle(t *testing.T) {
	mp := mpool.MustNewZero()
	bf := New(1000, 0.0001)

	vec := vector.NewVec(types.T_int64.ToType())
	vector.AppendFixed(vec, int64(12345), false, mp)
	defer vec.Free(mp)
	bf.Add(vec)

	data, err := bf.Marshal()
	require.NoError(t, err)

	bf2 := BloomFilter{}
	err = bf2.Unmarshal(data)
	require.NoError(t, err)
	defer bf2.Free()

	require.Equal(t, int32(1), bf2.shared.refCount)
	require.True(t, bf2.TestRow(vec, 0))

	bf.Free() // original is gone, bf2 should still work
	require.True(t, bf2.TestRow(vec, 0))
}

func TestBloomFilter_LargeVector(t *testing.T) {
	mp := mpool.MustNewZero()
	bf := New(10000, 0.001)
	defer bf.Free()

	// 1000 rows, more than hashmap.UnitLimit (usually 64 or 256)
	count := 1000
	vec := vector.NewVec(types.T_int64.ToType())
	for i := 0; i < count; i++ {
		vector.AppendFixed(vec, int64(i), false, mp)
	}
	defer vec.Free(mp)
	
	bf.Add(vec)

	allPass := true
	bf.Test(vec, func(exist bool, _ int) {
if !exist {
allPass = false
}
})
	require.True(t, allPass)
}
