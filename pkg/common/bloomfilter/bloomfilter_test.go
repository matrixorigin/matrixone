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

const testCount = 20000
const testRate = 0.00001
const vecCount = 10

func newVector(rowCount int, typ types.Type, mp *mpool.MPool) *vector.Vector {
	vec := vector.NewVec(typ)
	for i := 0; i < rowCount; i++ {
		if typ.Oid == types.T_int64 {
			vector.AppendFixed(vec, int64(i), false, mp)
		} else if typ.Oid == types.T_int32 {
			vector.AppendFixed(vec, int32(i), false, mp)
		}
	}
	return vec
}

func TestBloomFilter(t *testing.T) {
	mp := mpool.MustNewZero()
	vecs := make([]*vector.Vector, vecCount)
	for i := 0; i < vecCount; i++ {
		vecs[i] = newVector(testCount/vecCount, types.New(types.T_int64, 0, 0), mp)
	}

	boom := New(testCount, testRate)
	defer boom.Free()

	for j := 0; j < vecCount; j++ {
		boom.TestAndAdd(vecs[j], func(_ bool, _ int) {})
	}
	for j := 0; j < vecCount; j++ {
		vecs[j].Free(mp)
	}

	testVec := newVector(testCount/vecCount, types.New(types.T_int64, 0, 0), mp)
	defer testVec.Free(mp)
	allAdd := true
	boom.Test(testVec, func(exits bool, _ int) {
		allAdd = allAdd && exits
	})
	require.Equal(t, allAdd, true)

	testVec2 := newVector(int(testCount*1.2), types.New(types.T_int64, 0, 0), mp)
	defer testVec2.Free(mp)
	allAdd = true
	boom.Test(testVec2, func(exits bool, _ int) {
		allAdd = allAdd && exits
	})
	require.Equal(t, allAdd, false)
}

func TestBloomFilterReset(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := newVector(int(testCount*1.2), types.New(types.T_int64, 0, 0), mp)
	defer vec.Free(mp)

	boom := New(testCount, testRate)
	defer boom.Free()
	boom.TestAndAdd(vec, func(_ bool, _ int) {})

	allAdd := true
	boom.Test(vec, func(exits bool, _ int) {
		allAdd = allAdd && exits
	})
	require.Equal(t, allAdd, true)

	boom.Reset()

	findOne := false
	boom.Test(vec, func(exits bool, _ int) {
		findOne = findOne || exits
	})
	require.Equal(t, findOne, false)
}

func BenchmarkBloomFiltrerAdd(b *testing.B) {
	mp := mpool.MustNewZero()
	vecs := make([]*vector.Vector, vecCount)
	for i := 0; i < vecCount; i++ {
		vecs[i] = newVector(testCount/vecCount, types.New(types.T_int64, 0, 0), mp)
	}
	for i := 0; i < b.N; i++ {
		boom := New(testCount, testRate)
		for j := 0; j < vecCount; j++ {
			boom.Add(vecs[j])
		}
		boom.Free()
	}
	for i := 0; i < vecCount; i++ {
		vecs[i].Free(mp)
	}
}

func BenchmarkBloomFiltrerTestAndAdd(b *testing.B) {
	mp := mpool.MustNewZero()
	vecs := make([]*vector.Vector, vecCount)
	for i := 0; i < vecCount; i++ {
		vecs[i] = newVector(testCount/vecCount, types.New(types.T_int64, 0, 0), mp)
	}
	for i := 0; i < b.N; i++ {
		boom := New(testCount, testRate)
		for j := 0; j < vecCount; j++ {
			boom.TestAndAdd(vecs[j], func(_ bool, _ int) {})
		}
		boom.Free()
	}
	for i := 0; i < vecCount; i++ {
		vecs[i].Free(mp)
	}
}

func TestMarshal(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := newVector(testCount, types.New(types.T_int64, 0, 0), mp)
	defer vec.Free(mp)

	bf := New(testCount, testRate)
	defer bf.Free()
	bf.Add(vec)

	data, err := bf.Marshal()
	require.NoError(t, err)
	require.NotNil(t, data)
	require.Greater(t, len(data), 0)

	// Verify basic structure: should have at least seedCount (4 bytes) + some seeds + bitmapLen (4 bytes) + bitmap data
	require.GreaterOrEqual(t, len(data), 8, "marshaled data should have at least seedCount and bitmapLen")
}

func TestUnmarshal(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := newVector(testCount, types.New(types.T_int64, 0, 0), mp)
	defer vec.Free(mp)

	// Test normal case: marshal and unmarshal
	bf1 := New(testCount, testRate)
	bf1.Add(vec)

	data, err := bf1.Marshal()
	require.NoError(t, err)

	bf2 := BloomFilter{}
	err = bf2.Unmarshal(data)
	require.NoError(t, err)
	defer bf2.Free()
	defer bf1.Free()

	// Verify that unmarshaled filter has same functionality
	allFound := true
	bf2.Test(vec, func(exists bool, _ int) {
		allFound = allFound && exists
	})
	require.True(t, allFound, "unmarshaled filter should find all added elements")

	// Test error cases
	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{
			name:    "empty data",
			data:    []byte{},
			wantErr: true,
		},
		{
			name:    "data too short (< 4 bytes)",
			data:    []byte{1, 2, 3},
			wantErr: true,
		},
		{
			name:    "invalid seed count (0)",
			data:    []byte{0, 0, 0, 0}, // seedCount = 0
			wantErr: true,
		},
		{
			name:    "seed data truncated",
			data:    []byte{1, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7}, // seedCount=1, but only 7 bytes for seed (need 8)
			wantErr: true,
		},
		{
			name:    "no bitmap length",
			data:    []byte{1, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8}, // seedCount=1, one seed, but no bitmapLen
			wantErr: true,
		},
		{
			name:    "bitmap data truncated",
			data:    []byte{1, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 0, 0, 0, 1, 2, 3}, // seedCount=1, one seed, bitmapLen=10, but only 3 bytes
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bf := BloomFilter{}
			err := bf.Unmarshal(tt.data)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				bf.Free()
			}
		})
	}
}

func TestMarshalUnmarshal(t *testing.T) {
	mp := mpool.MustNewZero()
	vecs := make([]*vector.Vector, vecCount)
	for i := 0; i < vecCount; i++ {
		vecs[i] = newVector(testCount/vecCount, types.New(types.T_int64, 0, 0), mp)
	}
	defer func() {
		for i := 0; i < vecCount; i++ {
			vecs[i].Free(mp)
		}
	}()

	// Create and populate original filter
	bf1 := New(testCount, testRate)
	defer bf1.Free()
	for j := 0; j < vecCount; j++ {
		bf1.Add(vecs[j])
	}

	// Marshal
	data, err := bf1.Marshal()
	require.NoError(t, err)
	require.NotNil(t, data)

	// Unmarshal into new filter
	bf2 := BloomFilter{}
	err = bf2.Unmarshal(data)
	require.NoError(t, err)
	defer bf2.Free()

	// Verify both filters behave the same
	testVec := newVector(testCount/vecCount, types.New(types.T_int64, 0, 0), mp)
	defer testVec.Free(mp)

	// Test original filter
	allFound1 := true
	bf1.Test(testVec, func(exists bool, _ int) {
		if !exists {
			allFound1 = false
		}
	})

	// Test unmarshaled filter
	allFound2 := true
	bf2.Test(testVec, func(exists bool, row int) {
		if !exists {
			allFound2 = false
		}
	})

	require.Equal(t, allFound1, allFound2, "original and unmarshaled filters should behave the same")
}
