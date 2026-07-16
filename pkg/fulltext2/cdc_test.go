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

// CDC write-path round trip (engine layer, no live DB): Cdc/DeleteLog codecs and
// the streaming TailBuilder -> frames -> Index-with-liveness path that the ISCP
// consumer drives end-to-end.
package fulltext2

import (
	"os"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func wsTokenize(s string) []WordPos {
	var out []WordPos
	i := 0
	for _, f := range strings.Fields(s) {
		pos := i + strings.Index(s[i:], f)
		out = append(out, WordPos{Word: f, Pos: int32(pos)})
		i = pos + len(f)
	}
	return out
}

func TestCdcCodecRoundTrip(t *testing.T) {
	c := NewCdc(int32(types.T_int64))
	c.Insert(int64(2), "cherry pie")
	c.Delete(int64(0))
	c.Upsert(int64(1), "blueberry")
	require.Equal(t, 3, c.Len())

	blob, err := c.Encode()
	require.NoError(t, err)
	got, err := DecodeCdc(blob)
	require.NoError(t, err)
	require.Equal(t, c.PkType, got.PkType)
	require.Equal(t, c.Events, got.Events)

	// corruption is caught.
	blob[len(blob)-1] ^= 0xFF
	_, err = DecodeCdc(blob)
	require.Error(t, err)
}

func TestDeleteLogRoundTrip(t *testing.T) {
	// varlena pk (length-prefixed) and int pk (bare) both round-trip.
	for _, tc := range []struct {
		pkType int32
		recs   []DeleteRecord
	}{
		{int32(types.T_int64), []DeleteRecord{{int64(0)}, {int64(7)}}},
		{int32(types.T_varchar), []DeleteRecord{{[]byte("a")}, {[]byte("bb")}}},
	} {
		blob, err := EncodeDeleteLog(tc.pkType, tc.recs)
		require.NoError(t, err)
		got, err := DecodeDeleteLog(blob)
		require.NoError(t, err)
		require.Len(t, got, len(tc.recs))
		for i := range tc.recs {
			require.Equal(t, normalizeKey(tc.recs[i].Pk), normalizeKey(got[i].Pk))
		}
	}
}

// TestTailBuilderLiveness drives the whole write path: a base segment + a CDC
// batch (insert new / delete old / update existing) -> TailBuilder frames ->
// Index with liveness. The deleted doc disappears, the updated doc's new text
// wins over the base copy, and the new doc appears.
func TestTailBuilderLiveness(t *testing.T) {
	// tag=0 base (recency 0): doc 0 apple, doc 1 banana.
	bb := NewBuilder("base", int32(types.T_int64))
	feed(t, bb, int64(0), "apple")
	feed(t, bb, int64(1), "banana")
	base, err := bb.Finish()
	require.NoError(t, err)
	base.Recency = 0

	// One CDC flush: insert doc 2, delete doc 0, update doc 1's text.
	c := NewCdc(int32(types.T_int64))
	c.Insert(int64(2), "cherry")
	c.Delete(int64(0))
	c.Upsert(int64(1), "blueberry")

	tb, err := NewTailBuilder(int32(types.T_int64), 1000, wsTokenize)
	require.NoError(t, err)
	defer tb.Cleanup()
	require.NoError(t, tb.AddBatch(c))
	segs, err := tb.Finish()
	require.NoError(t, err)
	require.NotEmpty(t, segs)

	// Persist simulation: assign recencies in Finish() order (delete frame first,
	// then insert segments), all above the base recency (0).
	tails := []*Segment{base}
	deletes := map[any]int64{}
	recency := int64(100)
	for i, ts := range segs {
		framed, rerr := os.ReadFile(ts.Path)
		require.NoError(t, rerr)
		seg, dels, uerr := UnframeTail("tail", framed)
		require.NoError(t, uerr)
		switch {
		case seg != nil:
			seg.Recency = recency + int64(i)
			tails = append(tails, seg)
		case dels != nil:
			deletes = foldDeleteFrame(deletes, dels, recency+int64(i))
		}
	}

	idx := NewIndex(tails, deletes)
	q := func(w string) []any {
		res, qerr := idx.SearchQuery([]byte(w), false, ParserDefault, BM25, 100, nil)
		require.NoError(t, qerr)
		return resultIDs(res)
	}
	require.Empty(t, q("apple"), "deleted doc 0 must be gone")
	require.Empty(t, q("banana"), "updated doc 1's old text must be superseded")
	require.Equal(t, []any{int64(1)}, q("blueberry"), "updated doc 1's new text wins")
	require.Equal(t, []any{int64(2)}, q("cherry"), "inserted doc 2 present")
}
