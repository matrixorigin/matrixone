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

package usearchex

import (
	"encoding/binary"
	"testing"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	usearch "github.com/unum-cloud/usearch/golang"
)

// Test constants
const (
	defaultTestDimensions = 128
	distanceTolerance     = 1e-2
	bufferSize            = 1024 * 1024
)

// Helper functions to reduce code duplication

func createTestIndex(t *testing.T, dimensions uint, quantization usearch.Quantization) *usearch.Index {
	conf := usearch.DefaultConfig(dimensions)
	conf.Quantization = quantization
	index, err := usearch.NewIndex(conf)
	if err != nil {
		t.Fatalf("Failed to create test index: %v", err)
	}
	return index
}

func generateTestVector(dimensions uint) []float32 {
	vector := make([]float32, dimensions)
	for i := uint(0); i < dimensions; i++ {
		vector[i] = float32(i) + 0.1
	}
	return vector
}

// buildMembership builds a docfilter.MembershipFilter from a set of int64 keys
// (the doc_id PK type usearch keys map to). docfilter selects the structure:
// a dense cbitmap for a bounded id range, else the compact CRoaring bitset.
func buildMembership(t *testing.T, mp *mpool.MPool, keys []int64) docfilter.MembershipFilter {
	v := vector.NewVec(types.T_int64.ToType())
	defer v.Free(mp)
	for _, k := range keys {
		require.NoError(t, vector.AppendFixed(v, k, false, mp))
	}
	payload, err := docfilter.Build(v)
	require.NoError(t, err)
	f, err := docfilter.New(payload)
	require.NoError(t, err)
	return f
}

// buildBloomMembership builds a CBloomFilter-backed MembershipFilter (the
// approximate, non-integer-PK path). usearch keys are uint64, so we feed
// docfilter.Build a varchar column whose values are the raw 8-byte
// little-endian keys — exactly the bytes the C predicate tests (&key, 8).
func buildBloomMembership(t *testing.T, mp *mpool.MPool, keys []uint64) docfilter.MembershipFilter {
	v := vector.NewVec(types.T_varchar.ToType())
	defer v.Free(mp)
	for _, k := range keys {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, k)
		require.NoError(t, vector.AppendBytes(v, b, false, mp))
	}
	payload, err := docfilter.Build(v)
	require.NoError(t, err)
	require.Equal(t, docfilter.TagBloom, payload[0])
	f, err := docfilter.New(payload)
	require.NoError(t, err)
	return f
}

func TestFilteredSearchMembership(t *testing.T) {
	mp := mpool.MustNewZero()
	index := createTestIndex(t, defaultTestDimensions, usearch.F32)
	defer func() {
		if err := index.Destroy(); err != nil {
			t.Errorf("Failed to destroy index: %v", err)
		}
	}()

	if err := index.Reserve(1); err != nil {
		t.Fatalf("Failed to reserve capacity: %v", err)
	}

	vec := generateTestVector(defaultTestDimensions)
	vec[0] = 42.0
	vec[1] = 24.0
	foundkey := uint64(100)
	require.NoError(t, index.Add(foundkey, vec))

	limit := uint(10)

	// cbitmap (dense, bounded id range): filter includes the key.
	f := buildMembership(t, mp, []int64{100})
	require.True(t, f.Exact())
	keys, distances, err := FilteredSearchUnsafeWithMembership(index, unsafe.Pointer(&vec[0]), limit, f)
	require.NoError(t, err)
	_ = distances
	require.Equal(t, 1, len(keys))
	require.Equal(t, foundkey, keys[0])
	f.Free()

	// cbitmap: filter excludes the key.
	f2 := buildMembership(t, mp, []int64{999})
	keys2, _, err := FilteredSearchUnsafeWithMembership(index, unsafe.Pointer(&vec[0]), limit, f2)
	require.NoError(t, err)
	require.NotContains(t, keys2, foundkey)
	f2.Free()

	// CRoaring (sparse id range > MaxCbitmapBits): includes the key.
	f3 := buildMembership(t, mp, []int64{100, int64(docfilter.MaxCbitmapBits) + 10})
	keys3, _, err := FilteredSearchUnsafeWithMembership(index, unsafe.Pointer(&vec[0]), limit, f3)
	require.NoError(t, err)
	require.Contains(t, keys3, foundkey)
	f3.Free()
}

func TestFilteredSearchMembershipBloom(t *testing.T) {
	mp := mpool.MustNewZero()
	index := createTestIndex(t, defaultTestDimensions, usearch.F32)
	defer func() {
		if err := index.Destroy(); err != nil {
			t.Errorf("Failed to destroy index: %v", err)
		}
	}()

	if err := index.Reserve(1); err != nil {
		t.Fatalf("Failed to reserve capacity: %v", err)
	}

	vec := generateTestVector(defaultTestDimensions)
	vec[0] = 42.0
	vec[1] = 24.0
	foundkey := uint64(100)
	require.NoError(t, index.Add(foundkey, vec))

	limit := uint(10)

	// bloom (approximate): filter includes the key -> found.
	f := buildBloomMembership(t, mp, []uint64{foundkey})
	require.False(t, f.Exact())
	keys, _, err := FilteredSearchUnsafeWithMembership(index, unsafe.Pointer(&vec[0]), limit, f)
	require.NoError(t, err)
	require.Contains(t, keys, foundkey)
	f.Free()

	// bloom: filter built from a different key -> the key is excluded (no bloom
	// false positive for this single, well-sized filter).
	f2 := buildBloomMembership(t, mp, []uint64{999})
	keys2, _, err := FilteredSearchUnsafeWithMembership(index, unsafe.Pointer(&vec[0]), limit, f2)
	require.NoError(t, err)
	require.NotContains(t, keys2, foundkey)
	f2.Free()
}

func TestFilteredSearchMembershipEdges(t *testing.T) {
	mp := mpool.MustNewZero()
	index := createTestIndex(t, defaultTestDimensions, usearch.F32)
	defer func() {
		if err := index.Destroy(); err != nil {
			t.Errorf("Failed to destroy index: %v", err)
		}
	}()

	if err := index.Reserve(1); err != nil {
		t.Fatalf("Failed to reserve capacity: %v", err)
	}

	vec := generateTestVector(defaultTestDimensions)
	foundkey := uint64(100)
	require.NoError(t, index.Add(foundkey, vec))

	// Case 1: nil query pointer.
	_, _, err := FilteredSearchUnsafeWithMembership(index, nil, 10, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "query pointer cannot be nil")

	// Case 2: zero limit.
	keys, distances, err := FilteredSearchUnsafeWithMembership(index, unsafe.Pointer(&vec[0]), 0, nil)
	require.NoError(t, err)
	require.Empty(t, keys)
	require.Empty(t, distances)

	// Case 3: nil filter acts as a normal (unfiltered) search.
	keys, _, err = FilteredSearchUnsafeWithMembership(index, unsafe.Pointer(&vec[0]), 10, nil)
	require.NoError(t, err)
	require.Contains(t, keys, foundkey)

	// Case 4: empty filter matches nothing.
	fe := buildMembership(t, mp, []int64{})
	keys, _, err = FilteredSearchUnsafeWithMembership(index, unsafe.Pointer(&vec[0]), 10, fe)
	require.NoError(t, err)
	require.Empty(t, keys)
	fe.Free()
}
