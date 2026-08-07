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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

// TestColumnBufferRoundTrip pins the box-free typed streaming codec: a pk encoded into a
// ColumnBuffer (producer, appendAnyPk) must decode back to the identical value in a vector
// of that type (consumer, AppendColumnBuffer), across the fixed-width and varlena paths.
func TestColumnBufferRoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()

	build := func(pkType types.T, vals []any) *vectorindex.ColumnBuffer {
		w, fixed := fixedPkByteWidth(int32(pkType))
		if !fixed {
			w = 0
		}
		k := &vectorindex.ColumnBuffer{Type: pkType}
		for _, v := range vals {
			appendAnyPk(k, w, v)
		}
		require.Equal(t, len(vals), k.N)
		return k
	}
	toVec := func(pkType types.T, vals []any) *vector.Vector {
		vec := vector.NewVec(pkType.ToType())
		require.NoError(t, vectorindex.AppendColumnBuffer(build(pkType, vals), vec, mp))
		require.Equal(t, len(vals), vec.Length())
		return vec
	}

	// fixed-width paths
	require.Equal(t, []int64{1, -5, 1 << 40},
		vector.MustFixedColWithTypeCheck[int64](toVec(types.T_int64, []any{int64(1), int64(-5), int64(1 << 40)})))
	require.Equal(t, []uint64{0, 1, 1<<64 - 1},
		vector.MustFixedColWithTypeCheck[uint64](toVec(types.T_uint64, []any{uint64(0), uint64(1), uint64(1<<64 - 1)})))
	require.Equal(t, []uint64{7, 255},
		vector.MustFixedColWithTypeCheck[uint64](toVec(types.T_bit, []any{uint64(7), uint64(255)})))
	require.Equal(t, []int32{-3, 100000},
		vector.MustFixedColWithTypeCheck[int32](toVec(types.T_int32, []any{int32(-3), int32(100000)})))
	require.Equal(t, []int8{-1, 2},
		vector.MustFixedColWithTypeCheck[int8](toVec(types.T_int8, []any{int8(-1), int8(2)})))
	require.Equal(t, []types.Datetime{types.Datetime(123456789), types.Datetime(-42)},
		vector.MustFixedColWithTypeCheck[types.Datetime](toVec(types.T_datetime, []any{types.Datetime(123456789), types.Datetime(-42)})))
	require.Equal(t, []types.Decimal128{{B0_63: 5, B64_127: 9}},
		vector.MustFixedColWithTypeCheck[types.Decimal128](toVec(types.T_decimal128, []any{types.Decimal128{B0_63: 5, B64_127: 9}})))

	// varlena paths
	vvar := toVec(types.T_varchar, []any{[]byte("abc"), []byte(""), []byte("a longer varchar pk")})
	require.Equal(t, "abc", vvar.GetStringAt(0))
	require.Equal(t, "", vvar.GetStringAt(1))
	require.Equal(t, "a longer varchar pk", vvar.GetStringAt(2))

	u1, _ := types.ParseUuid("00000000-0000-0000-0000-000000000001")
	u2, _ := types.ParseUuid("12345678-1234-1234-1234-1234567890ab")
	require.Equal(t, []types.Uuid{u1, u2},
		vector.MustFixedColWithTypeCheck[types.Uuid](toVec(types.T_uuid, []any{u1, u2})))

	// remaining AppendColumnBuffer arms: decode without error at the right count, so every
	// switch case is exercised (a missing case would panic or mis-count).
	more := []struct {
		typ  types.T
		vals []any
	}{
		{types.T_uint32, []any{uint32(1), uint32(4000000000)}},
		{types.T_int16, []any{int16(-30000), int16(30000)}},
		{types.T_uint16, []any{uint16(60000)}},
		{types.T_uint8, []any{uint8(200)}},
		{types.T_date, []any{types.Date(19000)}},
		{types.T_time, []any{types.Time(123456)}},
		{types.T_timestamp, []any{types.Timestamp(1700000000)}},
		{types.T_decimal64, []any{types.Decimal64(42)}},
		{types.T_char, []any{[]byte("c")}},
		{types.T_text, []any{[]byte("longer text pk value")}},
		{types.T_blob, []any{[]byte{0, 1, 2, 255}}},
		{types.T_json, []any{[]byte(`{"k":1}`)}},
	}
	for _, c := range more {
		require.Equalf(t, len(c.vals), toVec(c.typ, c.vals).Length(), "type %v", c.typ)
	}
}

// TestStreamLoadedSegmentAllPkTypes covers the LOADED-segment streaming path (appendPkTo
// reading the docmap, not the build-side re-encode) plus StreamBagOfWords, across a
// fixed-width, a varlena, and a uuid pk (the encodePkLen default branch). It serializes →
// deserializes each segment, streams every doc, and checks the emitted pks via the
// pooled ColumnBuffer → vector path (AppendColumnBuffer + PutColumnBuffer).
func TestStreamLoadedSegmentAllPkTypes(t *testing.T) {
	mp := mpool.MustNewZero()
	uu, _ := types.ParseUuid("12345678-1234-1234-1234-1234567890ab")

	stream := func(pkType types.T, pks []any) *vector.Vector {
		b := NewBuilder("ld", int32(pkType))
		for _, pk := range pks {
			feed(t, b, pk, "alpha")
		}
		seg, err := b.Finish()
		require.NoError(t, err)
		blob, err := seg.Serialize()
		require.NoError(t, err)
		loaded, err := Deserialize("ld", bytes.NewReader(blob))
		require.NoError(t, err)
		t.Cleanup(func() { _ = loaded.dict.Close() })

		idx := NewIndex([]*Segment{loaded}, nil)
		out := vector.NewVec(pkType.ToType())
		err = idx.StreamBagOfWords([]byte("alpha"), ParserDefault, BM25, nil, false,
			func(k *vectorindex.ColumnBuffer, _ []float64, _ []*vectorindex.ColumnBuffer) error {
				e := vectorindex.AppendColumnBuffer(k, out, mp)
				PutColumnBuffer(k) // recycle, as the real consumer does
				return e
			})
		require.NoError(t, err)
		require.Equal(t, len(pks), out.Length())
		return out
	}

	// int64 pk → fixed-width loaded path
	require.ElementsMatch(t, []int64{10, 20, 30},
		vector.MustFixedColWithTypeCheck[int64](stream(types.T_int64, []any{int64(10), int64(20), int64(30)})))

	// varchar pk → varlena loaded path
	vv := stream(types.T_varchar, []any{[]byte("k1"), []byte("k2")})
	got := []string{vv.GetStringAt(0), vv.GetStringAt(1)}
	require.ElementsMatch(t, []string{"k1", "k2"}, got)

	// uuid pk → encodePkLen default branch + varlena loaded path
	require.ElementsMatch(t, []types.Uuid{uu},
		vector.MustFixedColWithTypeCheck[types.Uuid](stream(types.T_uuid, []any{uu})))
}

// TestStreamBagOfWordsEdges covers the two early-out branches of StreamBagOfWords: an
// empty index (globalN==0) and a pattern that resolves to no tokens.
func TestStreamBagOfWordsEdges(t *testing.T) {
	noEmit := func(*vectorindex.ColumnBuffer, []float64, []*vectorindex.ColumnBuffer) error {
		t.Fatal("emit should not be called")
		return nil
	}
	// empty index → globalN == 0 → nil, no emit.
	require.NoError(t, NewIndex(nil, nil).StreamBagOfWords([]byte("alpha"), ParserDefault, BM25, nil, false, noEmit))

	// non-empty index but a pattern that tokenizes to nothing → no resolvable tokens → nil.
	b := NewBuilder("e", int32(types.T_int64))
	feed(t, b, int64(1), "alpha")
	seg, err := b.Finish()
	require.NoError(t, err)
	require.NoError(t, NewIndex([]*Segment{seg}, nil).StreamBagOfWords([]byte("   "), ParserDefault, BM25, nil, false, noEmit))
}
