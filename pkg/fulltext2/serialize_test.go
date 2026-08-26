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
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// buildSegment assembles a build-side segment directly (as the build sink will).
func buildSegment(pkType int32, pks []any, docLen []int32, terms map[string]*termPostings) *Segment {
	s := NewSegment("seg0", pkType)
	s.pks = pks
	s.docLen = docLen
	s.N = int64(len(pks))
	s.setTerms(terms)
	return s
}

func roundtrip(t *testing.T, s *Segment) *Segment {
	t.Helper()
	data, err := s.Serialize()
	require.NoError(t, err)
	// deterministic output (FST + leBuf + tar all deterministic)
	data2, err := s.Serialize()
	require.NoError(t, err)
	require.Equal(t, Checksum(data), Checksum(data2), "Serialize must be deterministic")

	loaded, err := Deserialize(s.Id, bytes.NewReader(data))
	require.NoError(t, err)
	t.Cleanup(func() { _ = loaded.dict.Close() })
	return loaded
}

// assertPostingsEqual compares a loaded term's posting list to the original. The
// loaded side keeps docIDs/tfs block-compressed on the mmap and positions
// compressed, so compare via materialize* rather than the (nil) build-side fields.
func assertPostingsEqual(t *testing.T, want, got *termPostings) {
	t.Helper()
	require.Equal(t, want.docIDs, got.materializeDocIDs())
	require.Equal(t, want.tfs, got.materializeTfs())
	gotPos := got.materializePositions()
	require.Equal(t, len(want.positions), len(gotPos))
	for i := range want.positions {
		require.Equalf(t, want.positions[i], gotPos[i], "positions[%d]", i)
	}
}

// allPks materializes a loaded segment's pks via the on-demand accessor (loaded
// segments hold no resident []any — pk(ord) decodes from the mmap-backed docmap).
func allPks(s *Segment) []any {
	out := make([]any, s.numDocs())
	for i := range out {
		out[i] = s.pk(int64(i))
	}
	return out
}

func TestSegmentRoundtripInt64PK(t *testing.T) {
	terms := map[string]*termPostings{
		"apple": {
			docIDs:    []int64{0, 2},
			tfs:       []uint8{3, 1},
			positions: [][]int32{{1, 5, 9}, {4}},
		},
		"banana": {
			docIDs:    []int64{1},
			tfs:       []uint8{2},
			positions: [][]int32{{0, 7}},
		},
		"中文": {
			docIDs:    []int64{0, 1, 2},
			tfs:       []uint8{1, 1, 1},
			positions: [][]int32{{2}, {3}, {6}},
		},
	}
	orig := buildSegment(int32(types.T_int64), []any{int64(100), int64(200), int64(300)},
		[]int32{5, 7, 3}, terms)

	loaded := roundtrip(t, orig)

	require.Equal(t, int32(types.T_int64), loaded.PkType)
	require.Equal(t, int64(3), loaded.N)
	require.Equal(t, []any{int64(100), int64(200), int64(300)}, allPks(loaded))
	require.Equal(t, []int32{5, 7, 3}, loaded.docLen)

	for term, want := range terms {
		got, ok := loaded.LookupLoaded(term)
		require.True(t, ok, term)
		assertPostingsEqual(t, want, got)
	}
	// a missing term
	_, ok := loaded.LookupLoaded("cherry")
	require.False(t, ok)
	// prefix still works over the loaded FST
	it, ok, err := loaded.dict.prefixIter("app")
	require.NoError(t, err)
	require.True(t, ok)
	term, _ := it.Current()
	require.Equal(t, "apple", string(term))
	_ = it.Close()
}

func TestSegmentRoundtripVarcharPK(t *testing.T) {
	terms := map[string]*termPostings{
		"x": {docIDs: []int64{0, 1}, tfs: []uint8{1, 4}, positions: [][]int32{{0}, {1, 2, 3, 8}}},
	}
	orig := buildSegment(int32(types.T_varchar),
		[]any{[]byte("alpha"), []byte("beta")}, []int32{2, 9}, terms)

	loaded := roundtrip(t, orig)
	require.Equal(t, int32(types.T_varchar), loaded.PkType)
	require.Equal(t, []any{[]byte("alpha"), []byte("beta")}, allPks(loaded))
	require.Equal(t, []int32{2, 9}, loaded.docLen)
	got, ok := loaded.LookupLoaded("x")
	require.True(t, ok)
	assertPostingsEqual(t, terms["x"], got)
}

func TestSegmentRoundtripDatetimePK(t *testing.T) {
	terms := map[string]*termPostings{
		"t": {docIDs: []int64{0}, tfs: []uint8{1}, positions: [][]int32{{0}}},
	}
	orig := buildSegment(int32(types.T_datetime),
		[]any{types.Datetime(1234567890)}, []int32{1}, terms)

	loaded := roundtrip(t, orig)
	require.Equal(t, []any{types.Datetime(1234567890)}, allPks(loaded))
}

// Narrow integer PKs (TINYINT/SMALLINT, signed + unsigned) must round-trip: the
// build path (vector.GetAny) delivers them natively and no DDL guard rejects them,
// so the pk codec has to handle T_int8/T_int16/T_uint8/T_uint16 or CREATE FULLTEXT2
// crashes with "unsupported pk type". Regression cover for the codec gap.
func TestSegmentRoundtripNarrowIntPK(t *testing.T) {
	terms := map[string]*termPostings{
		"n": {docIDs: []int64{0, 1}, tfs: []uint8{1, 1}, positions: [][]int32{{0}, {0}}},
	}
	cases := []struct {
		typ types.T
		pks []any
	}{
		{types.T_int8, []any{int8(-128), int8(127)}},
		{types.T_uint8, []any{uint8(0), uint8(255)}},
		{types.T_int16, []any{int16(-32768), int16(32767)}},
		{types.T_uint16, []any{uint16(0), uint16(65535)}},
	}
	for _, c := range cases {
		t.Run(c.typ.String(), func(t *testing.T) {
			orig := buildSegment(int32(c.typ), c.pks, []int32{1, 1}, terms)
			loaded := roundtrip(t, orig)
			require.Equal(t, int32(c.typ), loaded.PkType)
			require.Equal(t, c.pks, allPks(loaded))
			got, ok := loaded.LookupLoaded("n")
			require.True(t, ok)
			assertPostingsEqual(t, terms["n"], got)
		})
	}
}

func TestSegmentRoundtripEmpty(t *testing.T) {
	orig := buildSegment(int32(types.T_int64), []any{}, []int32{}, map[string]*termPostings{})
	loaded := roundtrip(t, orig)
	require.Equal(t, int64(0), loaded.N)
	require.Equal(t, 0, loaded.dict.len())
	_, ok := loaded.LookupLoaded("anything")
	require.False(t, ok)
}
