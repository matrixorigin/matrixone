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
	"github.com/matrixorigin/matrixone/pkg/testutil"
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
		vecs[i] = testutil.NewVector(cTestCount/cVecCount, types.New(types.T_int64, 0, 0), mp, false, nil)
	}

	boom := NewCBloomFilterWithProbability(cTestCount, cTestRate)
	defer boom.Free()

	for j := 0; j < cVecCount; j++ {
		boom.TestAndAddVector(vecs[j], func(_ bool, _ bool, _ int) {})
	}
	for j := 0; j < cVecCount; j++ {
		vecs[j].Free(mp)
	}

	testVec := testutil.NewVector(cTestCount/cVecCount, types.New(types.T_int64, 0, 0), mp, false, nil)
	defer testVec.Free(mp)

	allAdd := true
	boom.TestVector(testVec, func(exits bool, _ bool, _ int) {
		allAdd = allAdd && exits
	})
	require.Equal(t, allAdd, true)

	testVec2 := testutil.NewVector(int(cTestCount*1.2), types.New(types.T_int64, 0, 0), mp, false, nil)
	defer testVec2.Free(mp)

	allAdd = true
	boom.TestVector(testVec2, func(exits bool, _ bool, _ int) {
		allAdd = allAdd && exits
	})
	require.Equal(t, allAdd, false)
}

func TestCBloomFilter_Free(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := testutil.NewVector(int(cTestCount*1.2), types.New(types.T_int64, 0, 0), mp, false, nil)
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
		vecs[i] = testutil.NewVector(cTestCount/cVecCount, types.New(types.T_int64, 0, 0), mp, false, nil)
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
		vecs[i] = testutil.NewVector(cTestCount/cVecCount, types.New(types.T_int64, 0, 0), mp, false, nil)
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
		vecs[i] = testutil.NewVector(cTestCount/cVecCount, types.New(types.T_int64, 0, 0), mp, false, nil)
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
		vecs[i] = testutil.NewVector(cTestCount/cVecCount, types.New(types.T_varchar, 0, 0), mp, false, nil)
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
		vecs[i] = testutil.NewVector(cTestCount/cVecCount, types.New(types.T_varchar, 0, 0), mp, false, nil)
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
		vecs[i] = testutil.NewVector(cTestCount/cVecCount, types.New(types.T_varchar, 0, 0), mp, false, nil)
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
		vecs[i] = testutil.NewVector(cTestCount/cVecCount, types.New(types.T_int64, 0, 0), mp, false, nil)
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
	testVec := testutil.NewVector(cTestCount/cVecCount, types.New(types.T_int64, 0, 0), mp, false, nil)
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
	newVec := testutil.NewVector(cTestCount*2, types.New(types.T_int64, 0, 0), mp, false, nil)
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
	assert.Panics(t, func() {
		// malformed data, first 8 bytes is nbits, next 4 bytes is k, but data is too short
		_ = bf.Unmarshal([]byte{1, 2, 3})
	})
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
