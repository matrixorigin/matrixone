// Copyright 2021 Matrix Origin
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
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	cTestCount = 200000
	cTestRate  = 0.00001
	cVecCount  = 10
)

func TestCBloomFilter(t *testing.T) {
	bf := NewCBloomFilter(1000, 3)
	assert.NotNil(t, bf)
	defer bf.Free()

	key1 := []byte("hello")
	key2 := []byte("world")
	key3 := []byte("matrixone")

	bf.Add(key1)
	bf.Add(key2)

	assert.True(t, bf.Test(key1))
	assert.True(t, bf.Test(key2))
	// key3 might be a false positive, but with 1000 bits and 3 keys it's unlikely
	assert.False(t, bf.Test(key3))

	// Test Marshal/Unmarshal
	data, err := bf.Marshal()
	assert.NoError(t, err)
	assert.NotNil(t, data)

	bf2 := &CBloomFilter{}
	err = bf2.Unmarshal(data)
	assert.NoError(t, err)
	assert.NotNil(t, bf2)
	defer bf2.Free()

	assert.True(t, bf2.Test(key1))
	assert.True(t, bf2.Test(key2))
	assert.False(t, bf2.Test(key3))

	// Test TestAndAdd
	key4 := []byte("new_key")
	assert.False(t, bf2.TestAndAdd(key4))
	assert.True(t, bf2.Test(key4))
	assert.True(t, bf2.TestAndAdd(key4))
}

func TestCBloomFilterWithVector(t *testing.T) {
	mp := mpool.MustNewZero()
	vecs := make([]*vector.Vector, cVecCount)
	for i := 0; i < cVecCount; i++ {
		vecs[i] = newVector(cTestCount/cVecCount, types.New(types.T_int64, 0, 0), mp)
	}

	boom := NewCBloomFilterWithProbability(cTestCount, cTestRate)
	defer boom.Free()

	for j := 0; j < cVecCount; j++ {
		boom.TestAndAddVector(vecs[j], func(_ bool, _ bool, _ int) {})
	}
	for j := 0; j < cVecCount; j++ {
		vecs[j].Free(mp)
	}

	testVec := newVector(cTestCount/cVecCount, types.New(types.T_int64, 0, 0), mp)
	defer testVec.Free(mp)

	allAdd := true
	boom.TestVector(testVec, func(exits bool, _ bool, _ int) {
		allAdd = allAdd && exits
	})
	require.Equal(t, allAdd, true)

	testVec2 := newVector(int(cTestCount*1.2), types.New(types.T_int64, 0, 0), mp)
	defer testVec2.Free(mp)

	allAdd = true
	boom.TestVector(testVec2, func(exits bool, _ bool, _ int) {
		allAdd = allAdd && exits
	})
	require.Equal(t, allAdd, false)
}

func TestCBloomFilterWithSeed(t *testing.T) {
	nbits := uint64(1000)
	k := uint32(3)
	seed := uint64(12345)

	bf1 := NewCBloomFilterWithSeed(nbits, k, seed)
	require.NotNil(t, bf1)
	defer bf1.Free()

	bf2 := NewCBloomFilterWithSeed(nbits, k, seed)
	require.NotNil(t, bf2)
	defer bf2.Free()

	bf3 := NewCBloomFilterWithSeed(nbits, k, seed+1)
	require.NotNil(t, bf3)
	defer bf3.Free()

	key := []byte("test_key")

	bf1.Add(key)
	bf2.Add(key)
	bf3.Add(key)

	assert.True(t, bf1.Test(key))
	assert.True(t, bf2.Test(key))
	assert.True(t, bf3.Test(key))

	// Marshal and compare bitmaps
	data1, _ := bf1.Marshal()
	data2, _ := bf2.Marshal()
	data3, _ := bf3.Marshal()

	assert.Equal(t, data1, data2, "Filters with same seed should have identical bitmaps")
	// bf3 might have same bitmap by chance, but unlikely
	assert.NotEqual(t, data1, data3, "Filters with different seeds should likely have different bitmaps")
}

func TestCBloomFilter_Free(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := newVector(int(cTestCount*1.2), types.New(types.T_int64, 0, 0), mp)
	defer vec.Free(mp)

	boom := NewCBloomFilterWithProbability(cTestCount, cTestRate)
	boom.TestAndAddVector(vec, func(_ bool, _ bool, _ int) {})

	allAdd := true
	boom.TestVector(vec, func(exits bool, _ bool, _ int) {
		allAdd = allAdd && exits
	})
	require.Equal(t, allAdd, true)

	boom.Free()
	// after free, ptr is nil, it should not panic
	boom.AddVector(vec)
	boom.TestVector(vec, nil)
	boom.TestAndAddVector(vec, nil)

	// a new bloom filter
	boom = NewCBloomFilterWithProbability(cTestCount, cTestRate)
	defer boom.Free()

	findOne := false
	boom.TestVector(vec, func(exits bool, _ bool, _ int) {
		findOne = findOne || exits
	})
	require.Equal(t, findOne, false)
}

func BenchmarkCBloomFiltrerAddVector(b *testing.B) {
	mp := mpool.MustNewZero()
	vecs := make([]*vector.Vector, cVecCount)
	for i := 0; i < cVecCount; i++ {
		vecs[i] = newVector(cTestCount/cVecCount, types.New(types.T_int64, 0, 0), mp)
	}
	defer func() {
		for i := range vecs {
			vecs[i].Free(mp)
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		boom := NewCBloomFilterWithProbability(cTestCount, cTestRate)
		for j := 0; j < cVecCount; j++ {
			boom.AddVector(vecs[j])
		}
		boom.Free()
	}
}

func BenchmarkCBloomFiltrerTestVector(b *testing.B) {
	mp := mpool.MustNewZero()
	vecs := make([]*vector.Vector, cVecCount)
	for i := 0; i < cVecCount; i++ {
		vecs[i] = newVector(cTestCount/cVecCount, types.New(types.T_int64, 0, 0), mp)
	}
	defer func() {
		for i := range vecs {
			vecs[i].Free(mp)
		}
	}()

	boom := NewCBloomFilterWithProbability(cTestCount, cTestRate)
	for j := 0; j < cVecCount; j++ {
		boom.AddVector(vecs[j])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < cVecCount; j++ {
			boom.TestVector(vecs[j], nil)
		}
	}
}

func BenchmarkCBloomFiltrerTestAndAddVector(b *testing.B) {
	mp := mpool.MustNewZero()
	vecs := make([]*vector.Vector, cVecCount)
	for i := 0; i < cVecCount; i++ {
		vecs[i] = newVector(cTestCount/cVecCount, types.New(types.T_int64, 0, 0), mp)
	}
	defer func() {
		for i := range vecs {
			vecs[i].Free(mp)
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		boom := NewCBloomFilterWithProbability(cTestCount, cTestRate)
		for j := 0; j < cVecCount; j++ {
			boom.TestAndAddVector(vecs[j], func(_ bool, _ bool, _ int) {})
		}
		boom.Free()
	}
}

func BenchmarkCBloomFiltrerAddVarlenaVector(b *testing.B) {
	mp := mpool.MustNewZero()
	vecs := make([]*vector.Vector, cVecCount)
	for i := 0; i < cVecCount; i++ {
		vecs[i] = newVector(cTestCount/cVecCount, types.New(types.T_varchar, 0, 0), mp)
	}
	defer func() {
		for i := range vecs {
			vecs[i].Free(mp)
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		boom := NewCBloomFilterWithProbability(cTestCount, cTestRate)
		for j := 0; j < cVecCount; j++ {
			boom.AddVector(vecs[j])
		}
		boom.Free()
	}
}

func BenchmarkCBloomFiltrerTestVarlenaVector(b *testing.B) {
	mp := mpool.MustNewZero()
	vecs := make([]*vector.Vector, cVecCount)
	for i := 0; i < cVecCount; i++ {
		vecs[i] = newVector(cTestCount/cVecCount, types.New(types.T_varchar, 0, 0), mp)
	}
	defer func() {
		for i := range vecs {
			vecs[i].Free(mp)
		}
	}()

	boom := NewCBloomFilterWithProbability(cTestCount, cTestRate)
	for j := 0; j < cVecCount; j++ {
		boom.AddVector(vecs[j])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < cVecCount; j++ {
			boom.TestVector(vecs[j], nil)
		}
	}
}

func BenchmarkCBloomFiltrerTestAndAddVarlenaVector(b *testing.B) {
	mp := mpool.MustNewZero()
	vecs := make([]*vector.Vector, cVecCount)
	for i := 0; i < cVecCount; i++ {
		vecs[i] = newVector(cTestCount/cVecCount, types.New(types.T_varchar, 0, 0), mp)
	}
	defer func() {
		for i := range vecs {
			vecs[i].Free(mp)
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		boom := NewCBloomFilterWithProbability(cTestCount, cTestRate)
		for j := 0; j < cVecCount; j++ {
			boom.TestAndAddVector(vecs[j], func(_ bool, _ bool, _ int) {})
		}
		boom.Free()
	}
}

func TestCBloomFilter_MarshalUnmarshalWithVector(t *testing.T) {
	mp := mpool.MustNewZero()
	vecs := make([]*vector.Vector, cVecCount)
	for i := 0; i < cVecCount; i++ {
		vecs[i] = newVector(cTestCount/cVecCount, types.New(types.T_int64, 0, 0), mp)
	}
	defer func() {
		for i := 0; i < cVecCount; i++ {
			vecs[i].Free(mp)
		}
	}()

	// Create and populate original filter
	bf1 := NewCBloomFilterWithProbability(cTestCount, cTestRate)
	defer bf1.Free()
	for j := 0; j < cVecCount; j++ {
		bf1.AddVector(vecs[j])
	}

	// Marshal
	data, err := bf1.Marshal()
	require.NoError(t, err)
	require.NotNil(t, data)

	// Unmarshal into new filter
	bf2 := CBloomFilter{}
	err = bf2.Unmarshal(data)
	require.NoError(t, err)
	defer bf2.Free()

	// Verify both filters behave the same
	testVec := newVector(cTestCount/cVecCount, types.New(types.T_int64, 0, 0), mp)
	defer testVec.Free(mp)

	// Test original filter
	allFound1 := true
	bf1.TestVector(testVec, func(exists bool, _ bool, _ int) {
		allFound1 = allFound1 && exists
	})

	// Test unmarshaled filter
	allFound2 := true
	bf2.TestVector(testVec, func(exists bool, _ bool, _ int) {
		allFound2 = allFound2 && exists
	})

	require.Equal(t, allFound1, allFound2, "original and unmarshaled filters should behave the same")

	// Test with new data that wasn't added
	newVec := newVector(cTestCount*2, types.New(types.T_int64, 0, 0), mp)
	defer newVec.Free(mp)

	allFoundNew1 := true
	bf1.TestVector(newVec, func(exists bool, _ bool, _ int) {
		allFoundNew1 = allFoundNew1 && exists
	})

	allFoundNew2 := true
	bf2.TestVector(newVec, func(exists bool, _ bool, _ int) {
		allFoundNew2 = allFoundNew2 && exists
	})

	require.Equal(t, allFoundNew1, allFoundNew2, "original and unmarshaled filters should behave the same for new data")
}

func TestCBloomFilter_UnmarshalError(t *testing.T) {
	bf := &CBloomFilter{}
	// C function will return null and go code will panic
	// malformed data, first 8 bytes is nbits, next 4 bytes is k, but data is too short
	err := bf.Unmarshal([]byte{1, 2, 3})
	require.Error(t, err)
}

func TestCBloomFilter_AddVectorWithNulls(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.New(types.T_int32, 0, 0))
	defer vec.Free(mp)

	count := 8192
	for i := 0; i < count; i++ {
		if i%2 != 0 {
			err := vector.AppendFixed(vec, int32(0), true, mp)
			require.NoError(t, err)
		} else {
			err := vector.AppendFixed(vec, int32(i), false, mp)
			require.NoError(t, err)
		}
	}

	bf := NewCBloomFilterWithProbability(int64(count), 0.00001)
	defer bf.Free()

	bf.AddVector(vec)

	callCount := 0
	bf.TestVector(vec, func(exists bool, isNull bool, idx int) {
		callCount++
		if idx%2 != 0 {
			require.True(t, isNull, "idx %d should be null", idx)
			require.False(t, exists, "idx %d should not exist", idx)
		} else {
			require.False(t, isNull, "idx %d should not be null", idx)
			require.True(t, exists, "idx %d should exist", idx)
		}
	})
	require.Equal(t, count, callCount)

	b := make([]byte, 4)
	for i := 0; i < count; i++ {
		if i%2 != 0 {
			continue
		}
		binary.LittleEndian.PutUint32(b, uint32(i))
		require.True(t, bf.Test(b))
	}
}

func TestCBloomFilter_TestAndAddVectorWithNulls(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.New(types.T_int32, 0, 0))
	defer vec.Free(mp)

	count := 8192
	for i := 0; i < count; i++ {
		if i%2 != 0 {
			err := vector.AppendFixed(vec, int32(0), true, mp)
			require.NoError(t, err)
		} else {
			err := vector.AppendFixed(vec, int32(i), false, mp)
			require.NoError(t, err)
		}
	}

	bf := NewCBloomFilterWithProbability(int64(count), 0.00001)
	defer bf.Free()

	// First time: nulls should not exist. Non-nulls might exist due to collisions (false positives).
	callCount1 := 0
	bf.TestAndAddVector(vec, func(exists bool, isNull bool, idx int) {
		callCount1++
		if idx%2 != 0 {
			require.True(t, isNull, "idx %d should be null", idx)
			require.False(t, exists, "idx %d should not exist (null)", idx)
		} else {
			require.False(t, isNull, "idx %d should not be null", idx)
		}
	})
	require.Equal(t, count, callCount1)

	// Second time: even indices (non-null) should exist
	callCount2 := 0
	bf.TestAndAddVector(vec, func(exists bool, isNull bool, idx int) {
		callCount2++
		if idx%2 != 0 {
			require.True(t, isNull, "idx %d should be null", idx)
			require.False(t, exists, "idx %d (null) should still not exist", idx)
		} else {
			require.False(t, isNull, "idx %d should not be null", idx)
			require.True(t, exists, "idx %d (non-null) should exist now", idx)
		}
	})
	require.Equal(t, count, callCount2)
}

func TestCBloomFilter_AddVarlenaVectorWithNulls(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.New(types.T_varchar, 0, 0))
	defer vec.Free(mp)

	count := 8192
	for i := 0; i < count; i++ {
		if i%2 != 0 {
			err := vector.AppendBytes(vec, []byte{}, true, mp)
			require.NoError(t, err)
		} else {
			err := vector.AppendBytes(vec, []byte(fmt.Sprintf("%d", i)), false, mp)
			require.NoError(t, err)
		}
	}

	bf := NewCBloomFilterWithProbability(int64(count), 0.00001)
	defer bf.Free()

	bf.AddVector(vec)

	callCount := 0
	bf.TestVector(vec, func(exists bool, isNull bool, idx int) {
		callCount++
		if idx%2 != 0 {
			require.True(t, isNull, "idx %d should be null", idx)
			require.False(t, exists, "idx %d should not exist", idx)
		} else {
			require.False(t, isNull, "idx %d should not be null", idx)
			require.True(t, exists, "idx %d should exist", idx)
		}
	})
	require.Equal(t, count, callCount)

	for i := 0; i < count; i++ {
		if i%2 != 0 {
			continue
		}
		require.True(t, bf.Test([]byte(fmt.Sprintf("%d", i))))
	}
}

func TestCBloomFilter_TestAndAddVarlenaVectorWithNulls(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.New(types.T_varchar, 0, 0))
	defer vec.Free(mp)

	count := 8192
	for i := 0; i < count; i++ {
		if i%2 != 0 {
			err := vector.AppendBytes(vec, []byte{}, true, mp)
			require.NoError(t, err)
		} else {
			err := vector.AppendBytes(vec, []byte(fmt.Sprintf("%d", i)), false, mp)
			require.NoError(t, err)
		}
	}

	bf := NewCBloomFilterWithProbability(int64(count), 0.00001)
	defer bf.Free()

	// First time: nulls should not exist. Non-nulls might exist due to collisions (false positives).
	callCount1 := 0
	bf.TestAndAddVector(vec, func(exists bool, isNull bool, idx int) {
		callCount1++
		if idx%2 != 0 {
			require.True(t, isNull, "idx %d should be null", idx)
			require.False(t, exists, "idx %d should not exist (null)", idx)
		} else {
			require.False(t, isNull, "idx %d should not be null", idx)
		}
	})
	require.Equal(t, count, callCount1)

	// Second time: even indices (non-null) should exist
	callCount2 := 0
	bf.TestAndAddVector(vec, func(exists bool, isNull bool, idx int) {
		callCount2++
		if idx%2 != 0 {
			require.True(t, isNull, "idx %d should be null", idx)
			require.False(t, exists, "idx %d (null) should still not exist", idx)
		} else {
			require.False(t, isNull, "idx %d should not be null", idx)
			require.True(t, exists, "idx %d (non-null) should exist now", idx)
		}
	})
	require.Equal(t, count, callCount2)
}

func TestCBloomFilter_TestZeroRowCount(t *testing.T) {
	bf := NewCBloomFilterWithProbability(0, 0.0001)
	require.NotNil(t, bf)
	defer bf.Free()
	bf2 := NewCBloomFilterWithProbability(5000, 0.001)
	require.NotNil(t, bf)
	defer bf2.Free()
}

func TestCBloomFilter_SharePointer(t *testing.T) {
	bf := NewCBloomFilter(1000, 3)
	require.NotNil(t, bf)

	// Initial refcnt should be 1
	assert.Equal(t, int32(1), bf.refcnt)
	assert.NotNil(t, bf.ptr)

	// SharePointer should increment refcnt
	sharedBf1 := bf.SharePointer()
	assert.Equal(t, int32(2), bf.refcnt)
	assert.Equal(t, bf.ptr, sharedBf1.ptr)

	sharedBf2 := bf.SharePointer()
	assert.Equal(t, int32(3), bf.refcnt)
	assert.Equal(t, bf.ptr, sharedBf2.ptr)

	// Free should decrement refcnt, but not free C memory until 0
	sharedBf1.Free()
	assert.Equal(t, int32(2), bf.refcnt)
	assert.NotNil(t, bf.ptr) // C memory still allocated

	sharedBf2.Free()
	assert.Equal(t, int32(1), bf.refcnt)
	assert.NotNil(t, bf.ptr) // C memory still allocated

	// Last Free should free C memory
	bf.Free()
	assert.Equal(t, int32(0), bf.refcnt)
	assert.Nil(t, bf.ptr) // C memory should be freed
}

func TestCBloomFilter_Getters(t *testing.T) {
	nbits := uint64(2048) // next_pow2_64 will make this 2048
	k := uint32(5)
	seed := uint64(888)

	bf := NewCBloomFilterWithSeed(nbits, k, seed)
	require.NotNil(t, bf)
	defer bf.Free()

	// nbits is adjusted to the next power of 2
	assert.Equal(t, nbits, bf.GetNbits())
	assert.Equal(t, k, bf.GetK())
	assert.Equal(t, seed, bf.GetSeed())

	// Test with NewCBloomFilter where seed is random
	bf2 := NewCBloomFilter(1000, 3)
	require.NotNil(t, bf2)
	defer bf2.Free()

	// nbits should be 1024
	assert.Equal(t, uint64(1024), bf2.GetNbits())
	assert.Equal(t, uint32(3), bf2.GetK())
	// We can't know the exact seed, but it should be non-zero
	assert.NotEqual(t, uint64(0), bf2.GetSeed())
}

func TestCBloomFilter_Merge(t *testing.T) {
	nbits := uint64(1024)
	k := uint32(3)
	seed := uint64(42)

	bf1 := NewCBloomFilterWithSeed(nbits, k, seed)
	require.NotNil(t, bf1)
	defer bf1.Free()

	bf2 := NewCBloomFilterWithSeed(nbits, k, seed)
	require.NotNil(t, bf2)
	defer bf2.Free()

	key1 := []byte("key1")
	key2 := []byte("key2")
	key3 := []byte("key3")

	bf1.Add(key1)
	bf2.Add(key2)

	err := bf1.Merge(bf2)
	require.NoError(t, err)

	assert.True(t, bf1.Test(key1))
	assert.True(t, bf1.Test(key2))
	assert.False(t, bf1.Test(key3))

	// Test with different seed
	bf3 := NewCBloomFilterWithSeed(nbits, k, seed+1)
	require.NotNil(t, bf3)
	defer bf3.Free()
	err = bf1.Merge(bf3)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "error code: 2")

	// Test with different k
	bf4 := NewCBloomFilterWithSeed(nbits, k+1, seed)
	require.NotNil(t, bf4)
	defer bf4.Free()
	err = bf1.Merge(bf4)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "error code: 3")

	// Test with different nbits
	bf5 := NewCBloomFilterWithSeed(nbits*2, k, seed)
	require.NotNil(t, bf5)
	defer bf5.Free()
	err = bf1.Merge(bf5)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "error code: 1")
}

// TestCBloomFilterConstVector covers #25621: constant vectors must test/add the single physical
// value and broadcast the result to all logical rows; const-null must return all-zero and never
// panic. Pre-fix: non-null constants returned [1,0,0] (false negatives) and const-null panicked.
func TestCBloomFilterConstVector(t *testing.T) {
	mp := mpool.MustNewZero()
	int64Typ := types.New(types.T_int64, 0, 0)
	varTyp := types.New(types.T_varchar, 64, 0)

	// TestVector, also capturing per-row isNull via the callback.
	testVec := func(bf *CBloomFilter, v *vector.Vector) (res []uint8, nulls []bool) {
		nulls = make([]bool, v.Length())
		res = bf.TestVector(v, func(exist bool, isNull bool, row int) { nulls[row] = isNull })
		return
	}

	t.Run("const_int64_nonnull", func(t *testing.T) {
		bf := NewCBloomFilter(1000, 3)
		defer bf.Free()
		v, err := vector.NewConstFixed[int64](int64Typ, 42, 3, mp)
		require.NoError(t, err)
		defer v.Free(mp)

		bf.AddVector(v)
		res, nulls := testVec(bf, v)
		require.Equal(t, []uint8{1, 1, 1}, res)
		require.Equal(t, []bool{false, false, false}, nulls)
	})

	t.Run("const_varchar_nonnull", func(t *testing.T) {
		bf := NewCBloomFilter(1000, 3)
		defer bf.Free()
		v, err := vector.NewConstBytes(varTyp, []byte("key"), 3, mp)
		require.NoError(t, err)
		defer v.Free(mp)

		bf.AddVector(v)
		res, nulls := testVec(bf, v)
		require.Equal(t, []uint8{1, 1, 1}, res)
		require.Equal(t, []bool{false, false, false}, nulls)
	})

	t.Run("const_null_int64", func(t *testing.T) {
		bf := NewCBloomFilter(1000, 3)
		defer bf.Free()
		v := vector.NewConstNull(int64Typ, 3, mp)
		defer v.Free(mp)

		require.NotPanics(t, func() { bf.AddVector(v) })
		var res []uint8
		var nulls []bool
		require.NotPanics(t, func() { res, nulls = testVec(bf, v) })
		require.Equal(t, []uint8{0, 0, 0}, res)
		require.Equal(t, []bool{true, true, true}, nulls)
	})

	t.Run("const_null_varchar", func(t *testing.T) {
		bf := NewCBloomFilter(1000, 3)
		defer bf.Free()
		v := vector.NewConstNull(varTyp, 3, mp)
		defer v.Free(mp)

		require.NotPanics(t, func() { bf.AddVector(v) })
		var res []uint8
		var nulls []bool
		require.NotPanics(t, func() { res, nulls = testVec(bf, v) })
		require.Equal(t, []uint8{0, 0, 0}, res)
		require.Equal(t, []bool{true, true, true}, nulls)
	})

	// testAndAddVec captures the per-row "exists" state that TestAndAddVector reports.
	testAndAddVec := func(bf *CBloomFilter, v *vector.Vector) []bool {
		out := make([]bool, v.Length())
		bf.TestAndAddVector(v, func(exist bool, isNull bool, row int) { out[row] = exist })
		return out
	}

	// TestAndAdd is STATEFUL and must preserve logical per-row semantics for a
	// constant: row 0 inserts the value, so rows 1..n-1 observe it (no false
	// negatives). Pre-fix this broadcast row 0's pre-insert result to every row
	// ([false,false,false]), which is wrong — and made constant and flat
	// representations of the same rows observably different.
	t.Run("testAndAdd_const_int64", func(t *testing.T) {
		bf := NewCBloomFilter(1000, 3)
		defer bf.Free()
		v, err := vector.NewConstFixed[int64](int64Typ, 99, 3, mp)
		require.NoError(t, err)
		defer v.Free(mp)

		require.Equal(t, []bool{false, true, true}, testAndAddVec(bf, v))

		// now present in every logical row (no false negatives).
		res, _ := testVec(bf, v)
		require.Equal(t, []uint8{1, 1, 1}, res)
	})

	// Equivalence: a constant vector and a flat vector holding the same repeated
	// value must report identical TestAndAdd per-row states.
	t.Run("testAndAdd_const_eq_flat_int64", func(t *testing.T) {
		constV, err := vector.NewConstFixed[int64](int64Typ, 99, 3, mp)
		require.NoError(t, err)
		defer constV.Free(mp)

		flatV := vector.NewVec(int64Typ)
		defer flatV.Free(mp)
		for i := 0; i < 3; i++ {
			require.NoError(t, vector.AppendFixed[int64](flatV, 99, false, mp))
		}

		bfConst := NewCBloomFilter(1000, 3)
		defer bfConst.Free()
		bfFlat := NewCBloomFilter(1000, 3)
		defer bfFlat.Free()

		flatStates := testAndAddVec(bfFlat, flatV)
		constStates := testAndAddVec(bfConst, constV)
		require.Equal(t, []bool{false, true, true}, flatStates)
		require.Equal(t, flatStates, constStates)
	})

	t.Run("testAndAdd_const_eq_flat_varchar", func(t *testing.T) {
		constV, err := vector.NewConstBytes(varTyp, []byte("key"), 3, mp)
		require.NoError(t, err)
		defer constV.Free(mp)

		flatV := vector.NewVec(varTyp)
		defer flatV.Free(mp)
		for i := 0; i < 3; i++ {
			require.NoError(t, vector.AppendBytes(flatV, []byte("key"), false, mp))
		}

		bfConst := NewCBloomFilter(1000, 3)
		defer bfConst.Free()
		bfFlat := NewCBloomFilter(1000, 3)
		defer bfFlat.Free()

		flatStates := testAndAddVec(bfFlat, flatV)
		constStates := testAndAddVec(bfConst, constV)
		require.Equal(t, []bool{false, true, true}, flatStates)
		require.Equal(t, flatStates, constStates)
	})
}

// TestCBloomFilterEmptyFixedVector covers #25618: the fixed-width vector APIs (Test/Add/
// TestAndAdd) must not panic on a zero-length vector — they used to unconditionally index
// &fixedData[0]/&results[0]. This is fixed by the same nitem==0 guards as the constant-vector
// work (#25621): an empty vector has physical count 0. TestVector returns empty; AddVector and
// TestAndAddVector are no-ops that never invoke the callback.
func TestCBloomFilterEmptyFixedVector(t *testing.T) {
	bf := NewCBloomFilterWithProbability(1, 0.01)
	defer bf.Free()

	// fixed-width (the issue's repro) plus variable-length, for completeness.
	for _, typ := range []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()} {
		vec := vector.NewVec(typ)
		called := false
		cb := func(bool, bool, int) { called = true }

		require.NotPanics(t, func() { require.Empty(t, bf.TestVector(vec, cb)) })
		require.NotPanics(t, func() { bf.AddVector(vec) })
		require.NotPanics(t, func() { bf.TestAndAddVector(vec, cb) })
		require.False(t, called, "callback must not fire for an empty vector (%s)", typ.String())
	}
}
