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

//go:build gpu

package cuvs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIvfPqRowsFitting exercises the capacity query on real hardware. No index
// is created: the cost model is a value type in C++, so Go passes the index
// shape and gets row counts back.
func TestIvfPqRowsFitting(t *testing.T) {
	rows, trainRows, perRow, minDev, minFree, err := IvfPqRowsFitting(128, 4, 8, 4, []int{0}, SingleGpu)
	require.NoError(t, err)

	// m=4 at 8 bits = 4 code bytes + the int64 payload. Computed in C++.
	require.Equal(t, uint64(12), perRow)
	require.Greater(t, rows, int64(0))
	require.Greater(t, trainRows, int64(0))
	require.Equal(t, 0, minDev)
	require.Greater(t, minFree, uint64(0))

	// The trainset costs dim*4 = 512 B/row against the same budget, so far fewer
	// training rows fit than index rows.
	require.Less(t, trainRows, rows)
}

// TestCagraRowsFitting is the CAGRA twin: it keeps the vectors resident, so its
// per-row cost is dim*sizeof(storage) plus the intermediate graph.
func TestCagraRowsFitting(t *testing.T) {
	rows, perRow, minDev, minFree, err := CagraRowsFitting(128, 4, 64, []int{0}, SingleGpu)
	require.NoError(t, err)
	require.Equal(t, uint64(1024), perRow) // 128*4 dataset + 64*8 graph
	require.Greater(t, rows, int64(0))
	require.Equal(t, 0, minDev)
	require.Greater(t, minFree, uint64(0))
}

// TestRowsFittingNoDevicesIsZero: an empty device list is not an error, it is
// "nothing to size against" -- the caller falls back to its other bounds.
func TestRowsFittingNoDevicesIsZero(t *testing.T) {
	rows, _, _, _, _, err := IvfPqRowsFitting(128, 4, 8, 4, nil, SingleGpu)
	require.NoError(t, err)
	require.Zero(t, rows)

	crows, _, _, _, err := CagraRowsFitting(128, 4, 64, nil, SingleGpu)
	require.NoError(t, err)
	require.Zero(t, crows)
}

// TestRowsFittingNarrowStorage: a narrower storage type shrinks the CAGRA
// dataset term, so more rows fit -- the opposite direction from the trainset.
func TestRowsFittingNarrowStorage(t *testing.T) {
	f32, perRowF32, _, _, err := CagraRowsFitting(128, 4, 64, []int{0}, SingleGpu)
	require.NoError(t, err)
	f16, perRowF16, _, _, err := CagraRowsFitting(128, 2, 64, []int{0}, SingleGpu)
	require.NoError(t, err)

	require.Equal(t, uint64(1024), perRowF32) // 512 dataset + 512 graph
	require.Equal(t, uint64(768), perRowF16)  // 256 dataset + 512 graph
	require.Greater(t, f16, f32, "cheaper rows means more of them fit")
}
