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
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	cTestCount = 20000
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

	boom := NewCBloomFilterWithProbaility(cTestCount, cTestRate)
	defer boom.Free()

	for j := 0; j < cVecCount; j++ {
		boom.TestAndAddVector(vecs[j], func(_ bool, _ int) {})
	}
	for j := 0; j < cVecCount; j++ {
		vecs[j].Free(mp)
	}

	testVec := testutil.NewVector(cTestCount/cVecCount, types.New(types.T_int64, 0, 0), mp, false, nil)
	defer testVec.Free(mp)

	var allAdd atomic.Bool
	allAdd.Store(true)
	boom.TestVector(testVec, func(exits bool, _ int) {
		allAdd.Store(allAdd.Load() && exits)
	})
	require.Equal(t, allAdd.Load(), true)

	testVec2 := testutil.NewVector(int(cTestCount*1.2), types.New(types.T_int64, 0, 0), mp, false, nil)
	defer testVec2.Free(mp)

	allAdd.Store(true)
	boom.TestVector(testVec2, func(exits bool, _ int) {
		allAdd.Store(allAdd.Load() && exits)
	})
	require.Equal(t, allAdd.Load(), false)
}

func TestCBloomFilter_Free(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := testutil.NewVector(int(cTestCount*1.2), types.New(types.T_int64, 0, 0), mp, false, nil)
	defer vec.Free(mp)

	boom := NewCBloomFilterWithProbaility(cTestCount, cTestRate)
	boom.TestAndAddVector(vec, func(_ bool, _ int) {})

	var allAdd atomic.Bool
	allAdd.Store(true)
	boom.TestVector(vec, func(exits bool, _ int) {
		allAdd.Store(allAdd.Load() && exits)
	})
	require.Equal(t, allAdd.Load(), true)

	boom.Free()
	// after free, ptr is nil, it should not panic
	boom.AddVector(vec)
	boom.TestVector(vec, nil)
	boom.TestAndAddVector(vec, nil)

	// a new bloom filter
	boom = NewCBloomFilterWithProbaility(cTestCount, cTestRate)
	defer boom.Free()

	var findOne atomic.Bool
	findOne.Store(false)
	boom.TestVector(vec, func(exits bool, _ int) {
		findOne.Store(findOne.Load() || exits)
	})
	require.Equal(t, findOne.Load(), false)
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
		boom := NewCBloomFilterWithProbaility(cTestCount, cTestRate)
		for j := 0; j < cVecCount; j++ {
			boom.AddVector(vecs[j])
		}
		boom.Free()
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
		boom := NewCBloomFilterWithProbaility(cTestCount, cTestRate)
		for j := 0; j < cVecCount; j++ {
			boom.TestAndAddVector(vecs[j], func(_ bool, _ int) {})
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
	bf1 := NewCBloomFilterWithProbaility(cTestCount, cTestRate)
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
	var allFound1 atomic.Bool
	allFound1.Store(true)
	bf1.TestVector(testVec, func(exists bool, _ int) {
		allFound1.Store(allFound1.Load() && exists)
	})

	// Test unmarshaled filter
	var allFound2 atomic.Bool
	allFound2.Store(true)
	bf2.TestVector(testVec, func(exists bool, _ int) {
		allFound2.Store(allFound2.Load() && exists)
	})

	require.Equal(t, allFound1.Load(), allFound2.Load(), "original and unmarshaled filters should behave the same")

	// Test with new data that wasn't added
	newVec := testutil.NewVector(cTestCount*2, types.New(types.T_int64, 0, 0), mp, false, nil)
	defer newVec.Free(mp)

	var allFoundNew1 atomic.Bool
	allFoundNew1.Store(true)
	bf1.TestVector(newVec, func(exists bool, _ int) {
		allFoundNew1.Store(allFoundNew1.Load() && exists)
	})

	var allFoundNew2 atomic.Bool
	allFoundNew2.Store(true)
	bf2.TestVector(newVec, func(exists bool, _ int) {
		allFoundNew2.Store(allFoundNew2.Load() && exists)
	})

	require.Equal(t, allFoundNew1.Load(), allFoundNew2.Load(), "original and unmarshaled filters should behave the same for new data")
}

func TestCBloomFilter_UnmarshalError(t *testing.T) {
	bf := &CBloomFilter{}
	// C function will return null and go code will panic
	assert.Panics(t, func() {
		// malformed data, first 8 bytes is nbits, next 4 bytes is k, but data is too short
		_ = bf.Unmarshal([]byte{1, 2, 3})
	})
}
