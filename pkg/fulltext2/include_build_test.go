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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

var incTypesTest = []int32{int32(types.T_int64), int32(types.T_varchar)}

// TestCdcIncludeRoundTrip: a CDC batch with an INCLUDE schema encodes+decodes its per-event
// include values (typed, incl. NULL) — the wire path from the sink into the tail builder.
func TestCdcIncludeRoundTrip(t *testing.T) {
	c := NewCdc(int32(types.T_int64))
	c.IncludeTypes = incTypesTest
	c.Insert(int64(1), "alpha beta", []any{int64(100), []byte("active")})
	c.Upsert(int64(2), "gamma", []any{int64(200), nil}) // NULL varchar
	c.Delete(int64(3))                                  // delete carries no include

	buf, err := c.Encode()
	require.NoError(t, err)
	got, err := DecodeCdc(buf)
	require.NoError(t, err)

	require.Equal(t, incTypesTest, got.IncludeTypes)
	require.Len(t, got.Events, 3)
	require.Equal(t, []any{int64(100), []byte("active")}, got.Events[0].Include)
	require.Equal(t, int64(200), got.Events[1].Include[0])
	require.Nil(t, got.Events[1].Include[1]) // NULL
	require.Equal(t, cdcDelete, got.Events[2].Op)
}

// TestBuilderIncludeSetDoc: a Builder with WithIncludeTypes carries per-doc include values
// (via SetDoc and SetInclude) into the finished segment, decodable by includeVal.
func TestBuilderIncludeSetDoc(t *testing.T) {
	b := NewBuilder("b", int32(types.T_int64), WithIncludeTypes(incTypesTest))
	// SetDoc (the CDC upsert primitive) sets terms + include together.
	b.SetDoc(int64(1), []WordPos{{Word: "alpha", Pos: 0}}, []any{int64(10), []byte("x")})
	// Add + SetInclude (the base-build primitive).
	require.NoError(t, b.Add("beta", 0, int64(2)))
	b.SetInclude(int64(2), []any{int64(20), []byte("y")})

	seg, err := b.Finish()
	require.NoError(t, err)

	// Round-trip through serialize so we exercise the loaded-side decode path.
	data, err := seg.encodeDocmap()
	require.NoError(t, err)
	loaded := &Segment{}
	require.NoError(t, loaded.decodeDocmap(data))
	require.Equal(t, 2, loaded.nIncludeCols())

	// ords are pk-assignment order: pk1 -> ord0, pk2 -> ord1.
	v0, null0, _ := loaded.includeVal(0, 0)
	require.False(t, null0)
	require.Equal(t, int64(10), v0)
	v1s, _, _ := loaded.includeVal(1, 1)
	require.Equal(t, []byte("y"), v1s)
}

// TestReconstructPreservesInclude: ReconstructLiveDocs (the MERGE input) yields each live
// doc's INCLUDE values, so a compaction folds them into the rebuilt base instead of dropping
// them — the key MERGE correctness invariant for covering/prefilter.
func TestReconstructPreservesInclude(t *testing.T) {
	docs := []TokenizedDoc{
		{Pk: int64(1), Terms: []string{"alpha"}, Positions: []int32{0}, Include: []any{int64(10), []byte("a")}},
		{Pk: int64(2), Terms: []string{"beta"}, Positions: []int32{0}, Include: []any{int64(20), nil}},
	}
	seg, err := BuildSegmentFromTokenized("s", int32(types.T_int64), docs, WithIncludeTypes(incTypesTest))
	require.NoError(t, err)
	idx := NewIndex([]*Segment{seg}, nil)

	got := map[int64][]any{}
	for d, derr := range idx.ReconstructLiveDocs(false) {
		require.NoError(t, derr)
		require.Len(t, d.Include, 2)
		got[d.Pk.(int64)] = d.Include
	}
	require.Equal(t, int64(10), got[1][0])
	require.Equal(t, []byte("a"), got[1][1])
	require.Equal(t, int64(20), got[2][0])
	require.Nil(t, got[2][1]) // NULL preserved
}
