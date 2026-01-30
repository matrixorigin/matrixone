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

	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
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

func TestFilteredSearch(t *testing.T) {

	index := createTestIndex(t, defaultTestDimensions, usearch.F32)
	defer func() {
		if err := index.Destroy(); err != nil {
			t.Errorf("Failed to destroy index: %v", err)
		}
	}()

	// Ensure capacity before first add
	if err := index.Reserve(1); err != nil {
		t.Fatalf("Failed to reserve capacity: %v", err)
	}

	// Add a vector
	vector := generateTestVector(defaultTestDimensions)
	vector[0] = 42.0
	vector[1] = 24.0

	foundkey := uint64(100)

	err := index.Add(foundkey, vector)
	if err != nil {
		t.Fatalf("Failed to add vector: %v", err)
	}

	// Test Contains
	found, err := index.Contains(100)
	if err != nil {
		t.Fatalf("Contains check failed: %v", err)
	}
	if !found {
		t.Fatalf("Expected to find key 100")
	}

	limit := uint(10)
	count := int64(100)

	bf := bloomfilter.NewCBloomFilterWithProbability(int64(count), 0.001)
	require.NotNil(t, bf)
	defer bf.Free()

	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(foundkey))
	bf.Add(b)

	keys, distances, err := FilteredSearchUnsafeWithBloomFilter(index, unsafe.Pointer(&vector[0]), limit, bf)
	require.NoError(t, err)
	_ = distances

	require.Equal(t, len(keys), 1)
	require.Equal(t, keys[0], foundkey)
}

func TestFilteredSearchEdges(t *testing.T) {
	index := createTestIndex(t, defaultTestDimensions, usearch.F32)
	defer func() {
		if err := index.Destroy(); err != nil {
			t.Errorf("Failed to destroy index: %v", err)
		}
	}()

	if err := index.Reserve(1); err != nil {
		t.Fatalf("Failed to reserve capacity: %v", err)
	}

	vector := generateTestVector(defaultTestDimensions)
	foundkey := uint64(100)
	err := index.Add(foundkey, vector)
	require.NoError(t, err)

	// Case 1: Nil query pointer
	_, _, err = FilteredSearchUnsafeWithBloomFilter(index, nil, 10, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "query pointer cannot be nil")

	// Case 2: Limit 0
	keys, distances, err := FilteredSearchUnsafeWithBloomFilter(index, unsafe.Pointer(&vector[0]), 0, nil)
	require.NoError(t, err)
	require.Empty(t, keys)
	require.Empty(t, distances)

	// Case 3: Nil BloomFilter (should act as normal search)
	keys, _, err = FilteredSearchUnsafeWithBloomFilter(index, unsafe.Pointer(&vector[0]), 10, nil)
	require.NoError(t, err)
	require.Contains(t, keys, foundkey)

	// Case 4: BloomFilter excluding the key
	bf := bloomfilter.NewCBloomFilterWithProbability(100, 0.001)
	require.NotNil(t, bf)
	defer bf.Free()

	// Add a different key to BF
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(999))
	bf.Add(b)

	keys, _, err = FilteredSearchUnsafeWithBloomFilter(index, unsafe.Pointer(&vector[0]), 10, bf)
	require.NoError(t, err)
	require.NotContains(t, keys, foundkey)
}
