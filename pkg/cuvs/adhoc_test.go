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
)

func TestAdhocBruteForceSearch(t *testing.T) {
	dim := uint32(3)
	nRows := uint64(2)
	nQueries := uint64(1)
	limit := uint32(1)

	dataset := []float32{
		1.0, 2.0, 3.0,
		4.0, 5.0, 6.0,
	}
	queries := []float32{
		1.1, 2.1, 3.1,
	}

	neighbors, distances, err := AdhocBruteForceSearch[float32](
		dataset, nRows, dim,
		queries, nQueries, limit,
		L2Expanded,
	)

	if err != nil {
		t.Fatalf("AdhocBruteForceSearch failed: %v", err)
	}

	if len(neighbors) != int(nQueries*uint64(limit)) {
		t.Errorf("Expected %d neighbors, got %d", nQueries*uint64(limit), len(neighbors))
	}

	if neighbors[0] != 0 {
		t.Errorf("Expected neighbor 0, got %d", neighbors[0])
	}

	if distances[0] > 0.1 {
		t.Errorf("Expected small distance, got %f", distances[0])
	}
}
