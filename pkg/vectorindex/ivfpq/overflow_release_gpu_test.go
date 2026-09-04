// Copyright 2026 Matrix Origin
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

//go:build gpu

package ivfpq

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/cuvs"
)

// The CDC overflow's Go-side vectors are copied into the device index and then dead -- every
// reader is inside buildOverflowBF's loop. They must be released there: the model pointers
// live until Destroy, and GetIndexSize does not count them, so holding them would be
// rows * dim * sizeof(B) of host memory the governor never sees.
func TestIvfpqBuildOverflowBFReleasesHostCopies(t *testing.T) {
	const dim = 3
	models := []*IvfpqModel[float32, float32]{
		{
			Id:                   "m0",
			OverflowPkids:        []int64{10, 20},
			OverflowVecs:         []float32{1, 2, 3, 4, 5, 6},
			OverflowIncludeBytes: []byte{0xaa, 0xbb},
		},
		{
			Id:                   "m1",
			OverflowPkids:        []int64{30},
			OverflowVecs:         []float32{7, 8, 9},
			OverflowIncludeBytes: []byte{0xcc},
		},
	}

	bf, err := buildOverflowBF[float32, float32](models, 3, dim, cuvs.L2Expanded, 0, 1)
	require.NoError(t, err)
	require.NotNil(t, bf)
	t.Cleanup(func() { _ = bf.Destroy() })

	require.Equal(t, uint64(3), bf.Len(), "every overflow row reached the device index")

	for _, m := range models {
		require.Nil(t, m.OverflowVecs, "%s: vectors released after the device copy", m.Id)
		require.Nil(t, m.OverflowIncludeBytes, "%s: include bytes released", m.Id)
		require.NotEmpty(t, m.OverflowPkids, "%s: pkids are kept (8 bytes/row, named in logs)", m.Id)
	}
}

// A model with no overflow rows is skipped by the loop and left untouched.
func TestIvfpqBuildOverflowBFSkipsEmptyModel(t *testing.T) {
	models := []*IvfpqModel[float32, float32]{
		{Id: "empty"},
		{Id: "m1", OverflowPkids: []int64{1}, OverflowVecs: []float32{1, 2, 3}},
	}

	bf, err := buildOverflowBF[float32, float32](models, 1, 3, cuvs.L2Expanded, 0, 1)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bf.Destroy() })

	require.Nil(t, models[0].OverflowVecs)
	require.Nil(t, models[1].OverflowVecs, "the model that had rows is released")
}

// Releasing the Go-side copies must not disturb the device index they were copied into.
// AddChunkQuantize takes the vectors AND their pkids together, so the ids mapping lives on
// the device side -- this searches the built overflow AFTER the release and checks that each
// query still resolves to the pkid of its own vector, not a shifted or dropped one.
func TestIvfpqBuildOverflowBFSearchesCorrectlyAfterRelease(t *testing.T) {
	const dim = 3
	// Three orthogonal unit vectors, deliberately given non-contiguous pkids that do not
	// match their insertion order, so an identity/offset mapping would be visibly wrong.
	models := []*IvfpqModel[float32, float32]{
		{
			Id:            "m0",
			OverflowPkids: []int64{700, 100},
			OverflowVecs:  []float32{1, 0, 0, 0, 1, 0},
		},
		{
			Id:            "m1",
			OverflowPkids: []int64{4200},
			OverflowVecs:  []float32{0, 0, 1},
		},
	}

	bf, err := buildOverflowBF[float32, float32](models, 3, dim, cuvs.L2Expanded, 0, 1)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bf.Destroy() })

	for _, m := range models {
		require.Nil(t, m.OverflowVecs, "%s released before the search", m.Id)
	}
	require.Equal(t, uint64(3), bf.Len())

	// One query per inserted vector, each an exact match for a different pkid.
	queries := []float32{
		1, 0, 0, // -> 700
		0, 1, 0, // -> 100
		0, 0, 1, // -> 4200
	}
	want := []int64{700, 100, 4200}

	const limit = 1
	job, err := bf.SearchQuantizeAsync(queries, uint64(len(want)), dim, limit)
	require.NoError(t, err)
	ids, dists, err := bf.SearchWait(job, uint64(len(want)), limit)
	require.NoError(t, err)
	require.Len(t, ids, len(want))
	require.Len(t, dists, len(want))

	for i, expect := range want {
		require.Equal(t, expect, ids[i],
			"query %d must resolve to its own pkid; the ids mapping survives the release", i)
		require.InDelta(t, 0.0, float64(dists[i]), 1e-4,
			"query %d is an exact match, so squared L2 is 0 -- the vectors reached the device intact", i)
	}
}

// The same, with every row in a single model, and k>1 so the full ordering is checked.
func TestIvfpqBuildOverflowBFRanksCorrectlyAfterRelease(t *testing.T) {
	const dim = 2
	m := &IvfpqModel[float32, float32]{
		Id:            "m0",
		OverflowPkids: []int64{11, 22, 33},
		OverflowVecs:  []float32{0, 0, 10, 0, 20, 0},
	}

	bf, err := buildOverflowBF[float32, float32]([]*IvfpqModel[float32, float32]{m}, 3, dim, cuvs.L2Expanded, 0, 1)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bf.Destroy() })
	require.Nil(t, m.OverflowVecs)

	// Query at the origin: nearest is 11 (d=0), then 22 (d=100), then 33 (d=400).
	job, err := bf.SearchQuantizeAsync([]float32{0, 0}, 1, dim, 3)
	require.NoError(t, err)
	ids, dists, err := bf.SearchWait(job, 1, 3)
	require.NoError(t, err)

	require.Equal(t, []int64{11, 22, 33}, ids, "distance order and ids mapping both intact")
	require.InDelta(t, 0.0, float64(dists[0]), 1e-4)
	require.InDelta(t, 100.0, float64(dists[1]), 1e-3)
	require.InDelta(t, 400.0, float64(dists[2]), 1e-3)
}
