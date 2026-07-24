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
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// TestBitPkRoundTrip pins the self-review blocker: a BIT primary key is accepted by
// build_ddl but the segment pk codec had no T_bit case, so encodePk returned
// "unsupported pk type" and stalled fulltext2 CDC on the first flush. BIT is backed by
// uint64, so it must round-trip through encode -> Serialize -> Deserialize -> pk(ord)
// -> query exactly like T_uint64.
func TestBitPkRoundTrip(t *testing.T) {
	// encode/decode round-trip for the raw codec.
	for _, v := range []uint64{0, 1, 255, 1 << 40, 1<<64 - 1} {
		enc, err := encodePk(int32(types.T_bit), v)
		require.NoError(t, err, "encodePk T_bit must not error (was the CDC-stall bug)")
		dec, err := decodePk(int32(types.T_bit), enc)
		require.NoError(t, err)
		require.Equal(t, v, dec, "T_bit pk must round-trip")
	}

	// full segment round-trip: build with BIT pks, serialize, reload, query back.
	b := NewBuilder("bit", int32(types.T_bit))
	feed(t, b, uint64(10), "alpha", "beta")
	feed(t, b, uint64(20), "alpha")
	seg, err := b.Finish()
	require.NoError(t, err)

	blob, err := seg.Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("bit", bytes.NewReader(blob))
	require.NoError(t, err)

	idx := NewIndex([]*Segment{loaded}, nil)
	require.Equal(t, int64(2), idx.globalN)
	res := idx.SearchPhrase(phr("alpha"), BM25, 10, nil)
	require.Len(t, res, 2, "both BIT-pk docs must be queryable after reload")
	pks := map[uint64]bool{}
	for _, r := range res {
		pks[r.Pk.(uint64)] = true
	}
	require.True(t, pks[10] && pks[20], "both BIT pk values must survive the round-trip")
}

// TestDecodeDocmapRejectsShortFixedPk pins the corrupt-blob guard: a fixed-width pk whose
// stored length prefix is smaller than the type width must be rejected at load (clean
// error) rather than passing decodeDocmap and later panicking in pk(ord) ->
// LittleEndian.Uint64 on a short slice.
func TestDecodeDocmapRejectsShortFixedPk(t *testing.T) {
	// Hand-craft a minimal docmap: [pkType u32][n u64][ per pk: len u32 + bytes ][ docLen n*u32 ].
	// One doc, T_int64 pk, but the stored pk length is 4 (should be 8) -> corrupt.
	var buf bytes.Buffer
	le := func(v uint32) { _ = binary.Write(&buf, binary.LittleEndian, v) }
	le64 := func(v uint64) { _ = binary.Write(&buf, binary.LittleEndian, v) }
	le(uint32(types.T_int64)) // pkType
	le64(1)                   // n = 1 doc
	le(4)                     // pk length prefix = 4 (WRONG: T_int64 needs 8)
	buf.Write([]byte{1, 2, 3, 4})
	le(7) // docLen[0]

	s := &Segment{}
	err := s.decodeDocmap(buf.Bytes())
	require.Error(t, err, "a fixed-width pk with a short stored length must be rejected at load")
	require.Contains(t, err.Error(), "width", "error should name the width mismatch")
}
