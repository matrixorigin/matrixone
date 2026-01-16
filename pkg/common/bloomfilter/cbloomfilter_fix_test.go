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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestCBloomFilter_EmptySlice(t *testing.T) {
	// Create a bloom filter
	bf := NewCBloomFilterWithProbaility(100, 0.01)
	defer bf.Free()

	emptySlice := []byte{}
	var nilSlice []byte

	// 1. Test Add and Test with empty slice
	bf.Add(emptySlice)

	// Should be true after fix
	exists := bf.Test(emptySlice)
	require.True(t, exists, "Empty slice should be found after Add")

	// 2. Test TestAndAdd
	bf2 := NewCBloomFilterWithProbaility(100, 0.01)
	defer bf2.Free()

	present := bf2.TestAndAdd(emptySlice)
	require.False(t, present, "First TestAndAdd should return false (not present)")

	presentAgain := bf2.TestAndAdd(emptySlice)
	require.True(t, presentAgain, "Second TestAndAdd should return true (already present)")

	// 3. Verify nil slice behavior (should remain ignored/false)
	bf2.Add(nilSlice)
	require.False(t, bf2.Test(nilSlice), "Nil slice should not be found")
}

func TestCBloomFilter_VarlenaVectorWithEmptySlices(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.New(types.T_varchar, 0, 0))
	defer vec.Free(mp)

	// idx 0: "hello"
	// idx 1: "" (empty, non-null)
	// idx 2: "world"
	// idx 3: null
	err := vector.AppendBytes(vec, []byte("hello"), false, mp)
	require.NoError(t, err)
	err = vector.AppendBytes(vec, []byte(""), false, mp)
	require.NoError(t, err)
	err = vector.AppendBytes(vec, []byte("world"), false, mp)
	require.NoError(t, err)
	err = vector.AppendBytes(vec, nil, true, mp)
	require.NoError(t, err)

	bf := NewCBloomFilterWithProbaility(10, 0.00001)
	defer bf.Free()

	// 1. Test AddVector
	bf.AddVector(vec)

	// 2. Test TestVector
	bf.TestVector(vec, func(exists bool, isNull bool, idx int) {
		switch idx {
		case 0, 1, 2:
			require.False(t, isNull, "idx %d should not be null", idx)
			require.True(t, exists, "idx %d should exist in bloom filter", idx)
		case 3:
			require.True(t, isNull, "idx %d should be null", idx)
			require.False(t, exists, "idx %d (null) should not exist in bloom filter", idx)
		}
	})

	// 3. Test TestAndAddVector with a new filter
	bf2 := NewCBloomFilterWithProbaility(10, 0.00001)
	defer bf2.Free()

	// First pass: none should exist (except possibly due to collisions)
	bf2.TestAndAddVector(vec, func(exists bool, isNull bool, idx int) {
		if isNull {
			require.False(t, exists, "idx %d (null) should not exist", idx)
		} else {
			// exists could be true due to collision, but unlikely with 0.00001 probability and 3 items
		}
	})

	// Second pass: non-nulls should exist
	bf2.TestAndAddVector(vec, func(exists bool, isNull bool, idx int) {
		if isNull {
			require.False(t, exists, "idx %d (null) should still not exist", idx)
		} else {
			require.True(t, exists, "idx %d (non-null) should exist now", idx)
		}
	})
}
