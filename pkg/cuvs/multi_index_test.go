//go:build gpu

/* 
 * Copyright 2021 Matrix Origin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cuvs

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestMultiGpuIndex(t *testing.T) {
	dimension := uint32(128)
	count1 := uint64(2000)
	count2 := uint64(2000)
	metric := L2Expanded

	dataset1 := make([]float32, count1*uint64(dimension))
	dataset2 := make([]float32, count2*uint64(dimension))

	for i := range dataset1 { dataset1[i] = float32(i) / float32(len(dataset1)) }
	for i := range dataset2 { dataset2[i] = float32(i) / float32(len(dataset2)) + 0.5 }

	devices := []int{0}
	nthread := uint32(4)

	// CAGRA
	bpCagra := DefaultCagraBuildParams()
	idx1, err := NewGpuCagra[float32](dataset1, count1, dimension, metric, bpCagra, devices, nthread, SingleGpu, nil)
	assert.NoError(t, err)
	err = idx1.Start()
	assert.NoError(t, err)
	err = idx1.Build()
	assert.NoError(t, err)

	// IVF-Flat
	bpIvf := DefaultIvfFlatBuildParams()
	idx2, err := NewGpuIvfFlat[float32](dataset2, count2, dimension, metric, bpIvf, devices, nthread, SingleGpu, nil)
	assert.NoError(t, err)
	err = idx2.Start()
	assert.NoError(t, err)
	err = idx2.Build()
	assert.NoError(t, err)

	// Brute Force
	bf, err := NewGpuBruteForce[float32](dataset1, count1, dimension, metric, nthread, 0)
	assert.NoError(t, err)
	err = bf.Start()
	assert.NoError(t, err)
	err = bf.Build()
	assert.NoError(t, err)

	// --- Test Generic MultiGpuIndex ---
	t.Run("Generic", func(t *testing.T) {
		mi := NewMultiGpuIndex[float32]([]GpuIndex[float32]{idx1, idx2}, bf, dimension, metric)
		
		numQueries := uint64(5)
		limit := uint32(10)
		queries := make([]float32, numQueries*uint64(dimension))
		for i := range queries { queries[i] = 0.2 }

		neighbors, distances, err := mi.Search(queries, numQueries, dimension, limit)
		assert.NoError(t, err)
		assert.Equal(t, int(numQueries*uint64(limit)), len(neighbors))
		assert.Equal(t, int(numQueries*uint64(limit)), len(distances))

		for q := uint64(0); q < numQueries; q++ {
			for k := uint32(1); k < limit; k++ {
				assert.True(t, distances[q*uint64(limit)+uint64(k)] >= distances[q*uint64(limit)+uint64(k-1)])
			}
		}
	})

	// --- Test Specialized MultiGpuIvfFlat ---
	t.Run("SpecializedIvfFlat", func(t *testing.T) {
		mivf := NewMultiGpuIvfFlat[float32]([]*GpuIvfFlat[float32]{idx2}, bf, dimension, metric)
		
		numQueries := uint64(5)
		limit := uint32(10)
		queries := make([]float32, numQueries*uint64(dimension))
		for i := range queries { queries[i] = 0.2 }

		sp := DefaultIvfFlatSearchParams()
		sp.NProbes = 32

		neighbors, distances, err := mivf.Search(queries, numQueries, dimension, limit, sp)
		assert.NoError(t, err)
		assert.Equal(t, int(numQueries*uint64(limit)), len(neighbors))
		
		for q := uint64(0); q < numQueries; q++ {
			for k := uint32(1); k < limit; k++ {
				assert.True(t, distances[q*uint64(limit)+uint64(k)] >= distances[q*uint64(limit)+uint64(k-1)])
			}
		}
	})

	// Cleanup
	err = idx1.Destroy()
	assert.NoError(t, err)
	err = idx2.Destroy()
	assert.NoError(t, err)
	err = bf.Destroy()
	assert.NoError(t, err)
}
