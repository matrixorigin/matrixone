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

package batch

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
)

// within reports whether part is a non-empty sub-slice of whole's memory.
func within(part, whole []byte) bool {
	if len(part) == 0 || len(whole) == 0 {
		return false
	}
	for i := range whole {
		if &whole[i] == &part[0] {
			return i+len(part) <= len(whole)
		}
	}
	return false
}

func TestMarshalVectorsViewBinaryPreservesRepresentation(t *testing.T) {
	mp := mpool.MustNewZero()
	vecs := make([]*vector.Vector, 5)
	vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, vector.AppendFixed(vecs[0], int64(1), false, mp))
	vecs[1] = vector.NewVec(types.T_varchar.ToType(), mp)
	longValue := bytes.Repeat([]byte("v"), types.VarlenaInlineSize+17)
	require.NoError(t, vector.AppendBytes(vecs[1], longValue, false, mp))
	var err error
	vecs[2], err = vector.NewConstFixed(types.T_int64.ToType(), int64(7), 1, mp)
	require.NoError(t, err)
	vecs[3] = vector.NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, vector.AppendFixed(vecs[3], int64(0), true, mp))
	vecs[4] = vector.NewConstNull(types.T_varchar.ToType(), 1, mp)
	t.Cleanup(func() {
		for _, vec := range vecs {
			vec.Free(mp)
		}
		require.Zero(t, mp.CurrNB())
	})
	require.NotEmpty(t, vecs[1].GetArea())

	data, err := MarshalVectors(vecs)
	require.NoError(t, err)
	rows, err := EncodedRowCount(data)
	require.NoError(t, err)
	require.Equal(t, 1, rows)

	view, err := ViewBinary(data)
	require.NoError(t, err)
	require.Equal(t, 1, view.RowCount())
	require.Len(t, view.Vecs, len(vecs))
	for i, vec := range view.Vecs {
		require.Equal(t, *vecs[i].GetType(), *vec.GetType(), "vector %d", i)
		require.Equal(t, 1, vec.Length(), "vector %d", i)
		// Views own no memory: they are read-only references to data.
		require.Nil(t, vec.MPool(), "vector %d", i)
	}
	require.False(t, view.Vecs[0].IsConst())
	require.Equal(t, int64(1), vector.MustFixedColNoTypeCheck[int64](view.Vecs[0])[0])
	require.False(t, view.Vecs[1].IsConst())
	require.Equal(t, longValue, view.Vecs[1].GetBytesAt(0))
	require.NotEmpty(t, view.Vecs[1].GetArea())
	require.True(t, view.Vecs[2].IsConst())
	require.False(t, view.Vecs[2].IsConstNull())
	require.Equal(t, int64(7), vector.GetFixedAtNoTypeCheck[int64](view.Vecs[2], 0))
	require.False(t, view.Vecs[3].IsConst())
	require.True(t, view.Vecs[3].IsNull(0))
	require.True(t, view.Vecs[4].IsConstNull())
	require.True(t, view.Vecs[4].IsNull(0))

	// The views alias data: fixed data and the varlen area are sub-slices of
	// it, so a write to data shows through the decoded vectors.
	require.True(t, within(view.Vecs[0].GetData(), data))
	require.True(t, within(view.Vecs[1].GetData(), data))
	require.True(t, within(view.Vecs[1].GetArea(), data))
	view.Vecs[0].GetData()[0] = 2
	require.Equal(t, int64(2), vector.MustFixedColNoTypeCheck[int64](view.Vecs[0])[0])
	view.Vecs[1].GetArea()[0] = 'x'
	require.Equal(t, byte('x'), view.Vecs[1].GetBytesAt(0)[0])

	// The encoding is a copy: the source vectors never alias data.
	require.False(t, within(vecs[0].GetData(), data))
	require.False(t, within(vecs[1].GetArea(), data))
	require.Equal(t, int64(1), vector.MustFixedColNoTypeCheck[int64](vecs[0])[0])
	require.Equal(t, longValue, vecs[1].GetBytesAt(0))
}

func TestMarshalVectorsEmpty(t *testing.T) {
	for _, vecs := range [][]*vector.Vector{nil, {}} {
		data, err := MarshalVectors(vecs)
		require.NoError(t, err)
		rows, err := EncodedRowCount(data)
		require.NoError(t, err)
		require.Zero(t, rows)
		view, err := ViewBinary(data)
		require.NoError(t, err)
		require.Zero(t, view.RowCount())
		require.Empty(t, view.Vecs)
	}
}

func TestViewBinaryRejectsRowCountMismatch(t *testing.T) {
	mp := mpool.MustNewZero()
	short := vector.NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, vector.AppendFixed(short, int64(1), false, mp))
	long := vector.NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, vector.AppendFixedList(long, []int64{1, 2}, nil, mp))
	defer func() {
		short.Free(mp)
		long.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()

	// MarshalVectors takes the row count from the first vector.
	for _, vecs := range [][]*vector.Vector{{short, long}, {long, short}} {
		data, err := MarshalVectors(vecs)
		require.NoError(t, err)
		rows, err := EncodedRowCount(data)
		require.NoError(t, err)
		require.Equal(t, vecs[0].Length(), rows)
		view, err := ViewBinary(data)
		require.Error(t, err)
		require.Nil(t, view)
	}

	// A row count that matches no vector is rejected too.
	bat := &Batch{Vecs: []*vector.Vector{short}, rowCount: 3}
	data, err := bat.MarshalBinary()
	require.NoError(t, err)
	rows, err := EncodedRowCount(data)
	require.NoError(t, err)
	require.Equal(t, 3, rows)
	view, err := ViewBinary(data)
	require.Error(t, err)
	require.Nil(t, view)
}

func TestViewBinaryRejectsMalformedEncoding(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_varchar.ToType(), mp)
	require.NoError(t, vector.AppendBytes(vec, bytes.Repeat([]byte("v"), 40), false, mp))
	defer func() {
		vec.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()
	data, err := MarshalVectors([]*vector.Vector{vec})
	require.NoError(t, err)

	for end := len(data) - 1; end >= 0; end-- {
		var view *Batch
		require.NotPanics(t, func() { view, err = ViewBinary(data[:end]) }, "truncation at %d", end)
		require.Error(t, err, "truncation at %d", end)
		require.Nil(t, view)
	}
	_, err = ViewBinary(append(append([]byte(nil), data...), 0))
	require.Error(t, err, "trailing byte")
}

func TestEncodedRowCountRejectsMalformedHeader(t *testing.T) {
	for end := 0; end < 8; end++ {
		_, err := EncodedRowCount(make([]byte, end))
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	}
	negative := int64(-1)
	_, err := EncodedRowCount(types.EncodeInt64(&negative))
	require.Error(t, err)
}

// partOf reports whether some part of parts starts at buf's memory.
func partOf(buf []byte, parts [][]byte) bool {
	for _, part := range parts {
		if len(part) > 0 && len(buf) > 0 && &part[0] == &buf[0] {
			return true
		}
	}
	return false
}

func newEncodeTestBatch(t *testing.T, mp *mpool.MPool, rows int) *Batch {
	t.Helper()
	bat := NewWithSize(5)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType(), mp)
	bat.Vecs[2] = vector.NewVec(types.T_int32.ToType(), mp)
	for i := 0; i < rows; i++ {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(i), i%7 == 0, mp))
		value := bytes.Repeat([]byte{byte('a' + i%26)}, types.VarlenaInlineSize+1+i%5)
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], value, i%11 == 0, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], int32(i), false, mp))
	}
	var err error
	bat.Vecs[3], err = vector.NewConstFixed(types.T_int64.ToType(), int64(9), rows, mp)
	require.NoError(t, err)
	bat.Vecs[4] = vector.NewConstNull(types.T_varchar.ToType(), rows, mp)
	bat.SetRowCount(rows)
	return bat
}

// Encode produces MarshalBinary's bytes, but borrows every large vector
// buffer and the extra buffer instead of copying them.
func TestEncodeMatchesMarshalBinaryAndBorrows(t *testing.T) {
	for _, rows := range []int{0, 1, 3, 8192} {
		t.Run(fmt.Sprint(rows), func(t *testing.T) {
			mp := mpool.MustNewZero()
			bat := newEncodeTestBatch(t, mp, rows)
			extra := bytes.Repeat([]byte("e"), minBorrowedPart)
			require.NoError(t, bat.SetExtraBuf(mp, extra))
			defer func() {
				bat.Clean(mp)
				require.Zero(t, mp.CurrNB())
			}()

			want, err := bat.MarshalBinary()
			require.NoError(t, err)
			encoded, err := Encode(bat)
			require.NoError(t, err)
			require.Equal(t, len(want), encoded.Len())
			require.Equal(t, want, encoded.Bytes())
			wire, err := encoded.Marshal()
			require.NoError(t, err)
			require.Equal(t, want, wire)

			var w partsWriter
			require.NoError(t, bat.MarshalBinaryTo(&w))
			parts := w.finish()
			require.True(t, partOf(bat.ExtraBuf(), parts), "extra buffer is borrowed")
			for i, vec := range bat.Vecs[:3] {
				for _, buf := range [][]byte{vec.GetData(), vec.GetArea()} {
					if len(buf) >= minBorrowedPart {
						require.True(t, partOf(buf, parts), "vector %d buffer is borrowed", i)
					} else if len(buf) > 0 {
						require.False(t, partOf(buf, parts), "vector %d small buffer is copied", i)
					}
				}
			}

			// The borrowed bytes are the vectors': a change before marshaling
			// shows in the encoding.
			if rows == 8192 {
				vector.MustFixedColNoTypeCheck[int32](bat.Vecs[2])[1] = -1
				changed, err := bat.MarshalBinary()
				require.NoError(t, err)
				require.Equal(t, changed, encoded.Bytes())
			}
		})
	}
}

// An Entry carries the encoding through protobuf unchanged, and the decoded
// entry owns its copy: the wire buffer may be reused.
func TestEncodeEntryRoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := newEncodeTestBatch(t, mp, 1000)
	defer func() {
		bat.Clean(mp)
		require.Zero(t, mp.CurrNB())
	}()
	want, err := bat.MarshalBinary()
	require.NoError(t, err)

	encoded, err := EncodeVectors(bat.Vecs)
	require.NoError(t, err)
	entry := api.Entry{EntryType: api.Entry_Insert, TableName: "t", Attrs: []string{"a", "b", "c", "d", "e"}, Bat: encoded}
	wire, err := entry.Marshal()
	require.NoError(t, err)
	require.Equal(t, entry.ProtoSize(), len(wire))

	var got api.Entry
	require.NoError(t, got.Unmarshal(wire))
	require.Equal(t, want, got.Bat.Bytes())
	for i := range wire {
		wire[i] = 0
	}
	require.Equal(t, want, got.Bat.Bytes())

	view, err := ViewBinary(got.Bat.Bytes())
	require.NoError(t, err)
	require.Equal(t, 1000, view.RowCount())
	require.Equal(t, bat.Vecs[1].GetBytesAt(999), view.Vecs[1].GetBytesAt(999))

	// An entry without a batch round-trips to an empty encoding.
	wire, err = (&api.Entry{TableName: "t"}).Marshal()
	require.NoError(t, err)
	got = api.Entry{}
	require.NoError(t, got.Unmarshal(wire))
	require.Zero(t, got.Bat.Len())
	require.Empty(t, got.Bat.Bytes())
}

// Encode trusts its batch: a varlen area that the checked codec would
// rewrite (here two rows share one span) is sent as is and still decodes to
// the same values, through both views.
func TestEncodeSendsTrustedAreaAsIs(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_varchar.ToType(), mp)
	long := func(c byte) []byte { return bytes.Repeat([]byte{c}, types.VarlenaInlineSize+8) }
	for _, c := range []byte("abc") {
		require.NoError(t, vector.AppendBytes(vec, long(c), false, mp))
	}
	defer func() {
		vec.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()
	descriptors := vector.MustFixedColNoTypeCheck[types.Varlena](vec)
	descriptors[2] = descriptors[0]
	require.False(t, vec.VarlenaAreaIsDisjoint())

	checked, err := MarshalVectors([]*vector.Vector{vec})
	require.NoError(t, err)
	encoded, err := EncodeVectors([]*vector.Vector{vec})
	require.NoError(t, err)
	require.Greater(t, encoded.Len(), len(checked), "the area is sent whole, unreferenced bytes included")

	for name, view := range map[string]func([]byte) (*Batch, error){
		"checked": ViewBinary, "trusted": ViewTrustedBinary,
	} {
		got, err := view(encoded.Bytes())
		require.NoError(t, err, name)
		require.Equal(t, long('a'), got.Vecs[0].GetBytesAt(0), name)
		require.Equal(t, long('b'), got.Vecs[0].GetBytesAt(1), name)
		require.Equal(t, long('a'), got.Vecs[0].GetBytesAt(2), name)
	}
}

// The trusted view skips value checks but not framing or row counts.
func TestViewTrustedBinaryChecksFraming(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := newEncodeTestBatch(t, mp, 10)
	defer func() {
		bat.Clean(mp)
		require.Zero(t, mp.CurrNB())
	}()
	encoded, err := Encode(bat)
	require.NoError(t, err)
	data := encoded.Bytes()

	view, err := ViewTrustedBinary(data)
	require.NoError(t, err)
	require.Equal(t, 10, view.RowCount())
	require.Len(t, view.Vecs, len(bat.Vecs))
	for i := range bat.Vecs {
		require.Equal(t, *bat.Vecs[i].GetType(), *view.Vecs[i].GetType())
		require.Nil(t, view.Vecs[i].MPool(), "views own no memory")
	}
	require.Equal(t, bat.Vecs[1].GetBytesAt(9), view.Vecs[1].GetBytesAt(9))
	require.True(t, view.Vecs[0].IsNull(7))

	_, err = ViewTrustedBinary(data[:len(data)-1])
	require.Error(t, err, "truncated")
	_, err = ViewTrustedBinary(append(bytes.Clone(data), 0))
	require.Error(t, err, "trailing bytes")
	wrongRows := bytes.Clone(data)
	copy(wrongRows, types.EncodeInt64(new(int64(11))))
	_, err = ViewTrustedBinary(wrongRows)
	require.ErrorContains(t, err, "row count")
	_, err = ViewTrustedBinary(nil)
	require.Error(t, err)
}
