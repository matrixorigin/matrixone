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
	"sort"
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

// buildMembership builds the C membership of a set of int64 keys (the doc_id PK type
// usearch keys map to), as hnsw does: a dense cbitmap, else the smaller of CRoaring and
// a C sorted64.
func buildMembership(t *testing.T, mp *mpool.MPool, keys []int64) docfilter.MembershipFilter {
	v := vector.NewVec(types.T_int64.ToType())
	defer v.Free(mp)
	for _, k := range keys {
		require.NoError(t, vector.AppendFixed(v, k, false, mp))
	}
	f, err := docfilter.NewForC(v, nil)
	require.NoError(t, err)
	return f
}

// buildBloomMembership builds a CBloomFilter-backed MembershipFilter (the
// approximate, non-integer-PK path).
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
	f := buildMembership(t, mp, []int64{100, 101, 102})
	require.True(t, f.Exact())
	keys, distances, err := FilteredSearchUnsafeWithMembership(index, unsafe.Pointer(&vec[0]), limit, f, false)
	require.NoError(t, err)
	_ = distances
	require.Equal(t, 1, len(keys))
	require.Equal(t, foundkey, keys[0])
	f.Free()

	// cbitmap: filter excludes the key.
	f2 := buildMembership(t, mp, []int64{997, 998, 999})
	keys2, _, err := FilteredSearchUnsafeWithMembership(index, unsafe.Pointer(&vec[0]), limit, f2, false)
	require.NoError(t, err)
	require.NotContains(t, keys2, foundkey)
	f2.Free()

	// C sorted64 (one key per CRoaring container): includes the key.
	f3 := buildMembership(t, mp, []int64{100, 100 + 1<<32, 100 + 2<<32})
	require.Equal(t, docfilter.TagSorted64, f3.(docfilter.CFilter).CKind())
	keys3, _, err := FilteredSearchUnsafeWithMembership(index, unsafe.Pointer(&vec[0]), limit, f3, false)
	require.NoError(t, err)
	require.Contains(t, keys3, foundkey)
	f3.Free()

	// CRoaring (moderately sparse keys): includes the key.
	sparse := []int64{100}
	for i := int64(1); i < 2000; i++ {
		sparse = append(sparse, 100+i*100)
	}
	f4 := buildMembership(t, mp, sparse)
	require.Equal(t, docfilter.TagCRoaring, f4.(docfilter.CFilter).CKind())
	keys4, _, err := FilteredSearchUnsafeWithMembership(index, unsafe.Pointer(&vec[0]), limit, f4, false)
	require.NoError(t, err)
	require.Contains(t, keys4, foundkey)
	f4.Free()
}

// A C bloom filter (approximate, for non-integer keys) is a C membership: the search
// keeps the keys it admits.
func TestFilteredSearchMembershipBloom(t *testing.T) {
	mp := mpool.MustNewZero()
	index := createTestIndex(t, defaultTestDimensions, usearch.F32)
	defer func() {
		if err := index.Destroy(); err != nil {
			t.Errorf("Failed to destroy index: %v", err)
		}
	}()
	require.NoError(t, index.Reserve(1))
	vec := generateTestVector(defaultTestDimensions)
	vec[0] = 42.0
	vec[1] = 24.0
	foundkey := uint64(100)
	require.NoError(t, index.Add(foundkey, vec))

	f := buildBloomMembership(t, mp, []uint64{foundkey})
	require.False(t, f.Exact())
	keys, _, err := FilteredSearchUnsafeWithMembership(index, unsafe.Pointer(&vec[0]), 10, f, false)
	require.NoError(t, err)
	require.Contains(t, keys, foundkey)
	f.Free()

	f2 := buildBloomMembership(t, mp, []uint64{999})
	keys2, _, err := FilteredSearchUnsafeWithMembership(index, unsafe.Pointer(&vec[0]), 10, f2, false)
	require.NoError(t, err)
	require.NotContains(t, keys2, foundkey)
	f2.Free()
}

// A Go-owned sorted64 is not a C membership: an error, never searched without it.
func TestFilteredSearchMembershipRejectsGoSorted64(t *testing.T) {
	mp := mpool.MustNewZero()
	index := createTestIndex(t, defaultTestDimensions, usearch.F32)
	defer func() {
		if err := index.Destroy(); err != nil {
			t.Errorf("Failed to destroy index: %v", err)
		}
	}()
	require.NoError(t, index.Reserve(1))
	vec := generateTestVector(defaultTestDimensions)
	require.NoError(t, index.Add(uint64(100), vec))

	v := vector.NewVec(types.T_int64.ToType())
	defer v.Free(mp)
	require.NoError(t, vector.AppendFixed(v, int64(100), false, mp))
	require.NoError(t, vector.AppendFixed(v, int64(1)<<40, false, mp))
	payload, err := docfilter.BuildSorted64Bytes(v)
	require.NoError(t, err)
	goSorted64, err := docfilter.NewSorted64Filter(payload)
	require.NoError(t, err)
	_, _, err = FilteredSearchUnsafeWithMembership(index, unsafe.Pointer(&vec[0]), 10, goSorted64, false)
	require.ErrorContains(t, err, "is not built in C memory")
	goSorted64.Free()
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
	_, _, err := FilteredSearchUnsafeWithMembership(index, nil, 10, nil, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "query pointer cannot be nil")

	// Case 2: zero limit.
	keys, distances, err := FilteredSearchUnsafeWithMembership(index, unsafe.Pointer(&vec[0]), 0, nil, false)
	require.NoError(t, err)
	require.Empty(t, keys)
	require.Empty(t, distances)

	// Case 3: nil filter acts as a normal (unfiltered) search.
	keys, _, err = FilteredSearchUnsafeWithMembership(index, unsafe.Pointer(&vec[0]), 10, nil, false)
	require.NoError(t, err)
	require.Contains(t, keys, foundkey)

	// Case 4: empty filter matches nothing.
	fe := buildMembership(t, mp, []int64{})
	keys, _, err = FilteredSearchUnsafeWithMembership(index, unsafe.Pointer(&vec[0]), 10, fe, false)
	require.NoError(t, err)
	require.Empty(t, keys)
	fe.Free()
}

// An exact membership search returns the nearest members by brute force, even on a
// graph too sparse for the approximate search to find them.
func TestFilteredSearchMembershipExact(t *testing.T) {
	const dimensions = 32
	const count = 2000
	const limit = 10
	mp := mpool.MustNewZero()

	conf := usearch.DefaultConfig(dimensions)
	conf.Metric = usearch.L2sq
	conf.Connectivity = 2
	conf.ExpansionAdd = 4
	index, err := usearch.NewIndex(conf)
	require.NoError(t, err)
	defer func() { require.NoError(t, index.Destroy()) }()
	require.NoError(t, index.Reserve(count))

	vectors := make([][]float32, count)
	state := uint32(1)
	for i := range vectors {
		vectors[i] = make([]float32, dimensions)
		for d := range vectors[i] {
			state = state*1664525 + 1013904223
			vectors[i][d] = float32(state>>8) / float32(1<<24)
		}
		require.NoError(t, index.Add(usearch.Key(i), vectors[i]))
	}

	for _, members := range [][]int64{
		stride(0, count, 7),           // cbitmap
		{3, 3 + 1<<32, 1500, 2 << 32}, // sorted64
		sparseKeys(count),             // croaring
	} {
		f := buildMembership(t, mp, members)
		for q := 0; q < count; q += 199 {
			query := vectors[q]
			keys, distances, err := FilteredSearchUnsafeWithMembership(index, unsafe.Pointer(&query[0]), limit, f, true)
			require.NoError(t, err)

			var truth []float32
			for _, m := range members {
				if m >= 0 && m < count {
					d, err := usearch.Distance(query, vectors[m], dimensions, usearch.L2sq)
					require.NoError(t, err)
					truth = append(truth, d)
				}
			}
			sort.Slice(truth, func(a, b int) bool { return truth[a] < truth[b] })
			require.Equal(t, min(limit, len(truth)), len(keys))
			for j, key := range keys {
				require.Contains(t, members, int64(key))
				require.InDelta(t, truth[j], distances[j], distanceTolerance)
			}
		}
		f.Free()
	}
}

func stride(from, to, step int64) []int64 {
	var keys []int64
	for k := from; k < to; k += step {
		keys = append(keys, k)
	}
	return keys
}

// sparseKeys returns keys spread thinly enough over [0, count*100) to build a CRoaring
// membership.
func sparseKeys(count int64) []int64 {
	var keys []int64
	for k := int64(0); k < count*100; k += 100 {
		keys = append(keys, k)
	}
	return keys
}
