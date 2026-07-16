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
	"sort"
	"sync"
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
		require.Equal(t, want.docIDs, got.materializeDocIDs(), term)
		gp := got.materializePositions()
		require.Equal(t, len(want.positions), len(gp), term)
		for i := range want.positions {
			require.Equalf(t, want.positions[i], gp[i], "%s positions[%d]", term, i)
		}
	}

	idx := NewIndex([]*Segment{s}, nil)
	// ranking (WAND, docIDs off-heap): "beta" hits all three docs.
	require.Len(t, idx.SearchPhrase([]string{"beta"}, BM25, 10, nil), 3)
	// phrase (positions from the mmap): only doc 0 has contiguous "alpha beta".
	ph := idx.SearchPhrase([]string{"alpha", "beta"}, BM25, 10, nil)
	require.Len(t, ph, 1)
	require.Equal(t, int64(0), ph[0].Pk)

	// Free munmaps and deletes the owned file.
	idx.Free()
	require.Nil(t, s.mmapData)
	_, statErr := os.Stat(path)
	require.True(t, os.IsNotExist(statErr), "Free must delete the mmap-owned file")
}

// TestConcurrentBlockDecode proves the loaded-segment design invariant: docID/tf
// blocks live in ONE shared, read-only mmap, and each WAND cursor decodes into its
// OWN per-cursor buffer — so many queries can hit the same segment concurrently with
// no lock and no race. Terms span many blocks (N/BlockSize each) so skipTo crosses
// block boundaries, and results must match a single-threaded reference exactly. Run
// under -race, this is the guard against a shared/mutable block cache.
func TestConcurrentBlockDecode(t *testing.T) {
	const N = 4000 // ~31 blocks for "alpha"
	b := NewBuilder("cc", int32(types.T_int64))
	for i := 0; i < N; i++ {
		toks := []string{"alpha"}
		if i%2 == 0 {
			toks = append(toks, "beta")
		}
		if i%3 == 0 {
			toks = append(toks, "gamma")
		}
		feed(t, b, int64(i), toks...)
	}
	built, err := b.Finish()
	require.NoError(t, err)
	blob, err := built.Serialize()
	require.NoError(t, err)

	f, err := os.CreateTemp("", "cctest")
	require.NoError(t, err)
	path := f.Name()
	_, err = f.Write(blob)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	data, err := mmapReadOnly(f)
	f.Close()
	require.NoError(t, err)
	s := &Segment{Id: "cc", mmapData: data, mmapPath: path}
	require.NoError(t, s.decodeSegment(data))
	idx := NewIndex([]*Segment{s}, nil)
	defer idx.Free()

	// Reference (single-threaded): a disjunctive WAND top-k over all three terms.
	pkset := func(rs []Result) []int64 {
		out := make([]int64, len(rs))
		for i, r := range rs {
			out[i] = r.Pk.(int64)
		}
		sort.Slice(out, func(a, b int) bool { return out[a] < out[b] })
		return out
	}
	ref, err := idx.SearchQuery([]byte("alpha beta gamma"), true, "default", BM25, 100, nil)
	require.NoError(t, err)
	require.NotEmpty(t, ref)
	want := pkset(ref)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for it := 0; it < 40; it++ {
				got, e := idx.SearchQuery([]byte("alpha beta gamma"), true, "default", BM25, 100, nil)
				require.NoError(t, e)
				require.Equal(t, want, pkset(got)) // same top-k SET (ties aside)
			}
		}()
	}
	wg.Wait()
}
