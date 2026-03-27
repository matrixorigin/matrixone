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

func TestPairwiseDistance(t *testing.T) {
	dim := uint32(3)
	nX := uint64(2)
	nY := uint64(2)

	x := []float32{
		1.0, 0.0, 0.0,
		0.0, 1.0, 0.0,
	}
	y := []float32{
		1.0, 0.0, 0.0,
		0.0, 1.0, 0.0,
	}

	dist, err := PairwiseDistance[float32](
		x, nX,
		y, nY,
		dim,
		L2Expanded, 0,
	)

	if err != nil {
		t.Fatalf("PairwiseDistance failed: %v", err)
	}

	if len(dist) != int(nX*nY) {
		t.Errorf("Expected %d distances, got %d", nX*nY, len(dist))
	}

	// Expected results for L2Squared:
	// dist[0,0] = (1-1)^2 + (0-0)^2 + (0-0)^2 = 0
	// dist[0,1] = (1-0)^2 + (0-1)^2 + (0-0)^2 = 2
	// dist[1,0] = (0-1)^2 + (1-0)^2 + (0-0)^2 = 2
	// dist[1,1] = (0-0)^2 + (1-1)^2 + (0-0)^2 = 0
}

func TestPairwiseDistanceAsync(t *testing.T) {
	dim := uint32(3)
	nX := uint64(2)
	nY := uint64(2)

	x := []float32{
		1.0, 0.0, 0.0,
		0.0, 1.0, 0.0,
	}
	y := []float32{
		1.0, 0.0, 0.0,
		0.0, 1.0, 0.0,
	}

	dist := make([]float32, nX*nY)
	jobID, err := PairwiseDistanceLaunch[float32](
		x, nX,
		y, nY,
		dim,
		L2Expanded, 0,
		dist,
	)

	if err != nil {
		t.Fatalf("PairwiseDistanceLaunch failed: %v", err)
	}

	if jobID == 0 {
		t.Fatal("Expected non-zero jobID")
	}

	if err := PairwiseDistanceWait(jobID); err != nil {
		t.Fatalf("PairwiseDistanceWait failed: %v", err)
	}

	expected := []float32{0.0, 2.0, 2.0, 0.0}
	for i := 0; i < len(expected); i++ {
		if dist[i] != expected[i] {
			t.Errorf("Expected dist[%d] = %f, got %f", i, expected[i], dist[i])
		}
	}
}
