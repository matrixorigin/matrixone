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

package fulltext2

import (
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// TestMmapLoad validates the base-segment mmap path (the storage LoadFromStorage
// shape, without a cluster): serialize a segment to a file, mmap it, decode over
// the mapping — FST + positions are views into the shared mmap, docIDs/tfs expand
// off-heap — then search (ranking + phrase), and confirm Free() munmaps and deletes
// the file.
func TestMmapLoad(t *testing.T) {
	b := NewBuilder("mm", int32(types.T_int64))
	feed(t, b, int64(0), "alpha", "beta", "alpha") // "alpha beta" phrase present
	feed(t, b, int64(1), "beta", "gamma")
	feed(t, b, int64(2), "alpha", "gamma", "beta")
	built, err := b.Finish()
	require.NoError(t, err)
	blob, err := built.Serialize()
	require.NoError(t, err)

	f, err := os.CreateTemp("", "ft2mmtest")
	require.NoError(t, err)
	path := f.Name()
	_, err = f.Write(blob)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	data, err := mmapReadOnly(f)
	f.Close()
	require.NoError(t, err)

	s := &Segment{Id: "mm", mmapData: data, mmapPath: path}
	require.NoError(t, s.decodeSegment(data))
	require.NotNil(t, s.mmapData, "segment holds the mmap")

	// Every term: docIDs off-heap match; positions (posRaw into the mmap) materialize
	// to exactly the build-side positions.
	for _, term := range built.sortedTerms {
		want, ok := built.Lookup(term)
		require.True(t, ok, term)
		got, ok := s.LookupLoaded(term)
		require.True(t, ok, term)
		require.Equal(t, want.docIDs, got.docIDs, term)
		gp := got.materializePositions()
		require.Equal(t, len(want.positions), len(gp), term)
		for i := range want.positions {
			require.Equalf(t, want.positions[i], gp[i], "%s positions[%d]", term, i)
		}
	}

	idx := NewIndex([]*Segment{s}, nil)
	// ranking (WAND, docIDs off-heap): "beta" hits all three docs.
	require.Len(t, idx.SearchPhrase([]string{"beta"}, BM25, 10), 3)
	// phrase (positions from the mmap): only doc 0 has contiguous "alpha beta".
	ph := idx.SearchPhrase([]string{"alpha", "beta"}, BM25, 10)
	require.Len(t, ph, 1)
	require.Equal(t, int64(0), ph[0].Pk)

	// Free munmaps and deletes the owned file.
	idx.Free()
	require.Nil(t, s.mmapData)
	_, statErr := os.Stat(path)
	require.True(t, os.IsNotExist(statErr), "Free must delete the mmap-owned file")
}
