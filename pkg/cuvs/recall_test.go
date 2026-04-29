//go:build gpu

// Copyright 2021 - 2022 Matrix Origin
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

package cuvs

import (
	//"fmt"
	"math/rand"
	"testing"
)

type NeighborType interface {
	uint32 | int64
}

// GenerateRandomDataset generates a random float32 dataset.
func GenerateRandomDataset(n_vectors uint64, dimension uint32) []float32 {
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := range dataset {
		dataset[i] = rand.Float32()
	}
	return dataset
}

// ReportRecall reports the self-recall for an index.
// It verifies that querying with a point already in the index returns that point's ID.
func ReportRecall[T NeighborType](b *testing.B, dataset []float32, n_vectors uint64, dimension uint32, limit uint32, searchFunc func(queries []float32, numQueries uint64, limit uint32) ([]T, error)) {
	numQueries := uint64(100)
	if n_vectors < numQueries {
		numQueries = n_vectors
	}

	// Use the first numQueries vectors from the dataset as queries.
	// Since these are the first vectors, we expect their IDs to be 0, 1, 2, ..., numQueries-1.
	recallQueries := dataset[:numQueries*uint64(dimension)]

	// Search approximate index
	approxNeighbors, err := searchFunc(recallQueries, numQueries, limit)
	if err != nil {
		b.Logf("Warning: Approximate search failed: %v", err)
		return
	}

	hitCount := 0
	for i := uint64(0); i < numQueries; i++ {
		// For query i (which is dataset[i]), we expect ID 'i' to be in the results
		expectedID := int64(i)
		found := false
		for j := uint32(0); j < limit; j++ {
			if int64(approxNeighbors[i*uint64(limit)+uint64(j)]) == expectedID {
				found = true
				break
			}
		}
		if found {
			hitCount++
		}
	}

	recall := float64(hitCount) / float64(numQueries)
	//fmt.Printf("Benchmark %s: self_recall_at_%d = %.4f\n", b.Name(), int(limit), recall)
	b.ReportMetric(recall*float64(b.N), "recall")
}
