// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vector

import (
	"bytes"
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

type selectedRowsOneShotShortWriter struct {
	short bool
}

func (w *selectedRowsOneShotShortWriter) Write(value []byte) (int, error) {
	if !w.short && len(value) != 0 {
		w.short = true
		return len(value) - 1, nil
	}
	return len(value), nil
}

func TestSelectedFlagsCodecResetsEachStreamingPass(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_int64.ToType())
	defer source.Free(mp)
	values := make([]int64, 300)
	for i := range values {
		values[i] = int64(i * 7)
	}
	require.NoError(t, AppendFixedList(source, values, nil, mp))

	flags := make([]uint8, len(values))
	for _, row := range []int{1, 256, 299} {
		flags[row] = 1
	}
	var encoded bytes.Buffer
	written, err := source.MarshalSelectedFlagsTo(&encoded, flags)
	require.NoError(t, err)
	require.Equal(t, 3, written)

	destination := NewOffHeapVecWithType(types.T_int64.ToType())
	defer destination.Free(mp)
	require.NoError(t, destination.UnmarshalSelectedRowsFrom(&encoded, 3, mp))
	require.Equal(t, []int64{7, 1792, 2093},
		MustFixedColWithTypeCheck[int64](destination))
	require.Zero(t, encoded.Len())
}

func TestSelectedRowsCodecMatchesFlagSelectionWire(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_varchar.ToType())
	defer func() {
		source.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()
	for _, value := range []string{"zero", "one", "two", "three", "four", "five", "six", "seven"} {
		require.NoError(t, AppendBytes(source, []byte(value), false, mp))
	}
	source.SetNull(4)
	source.GetGrouping().Add(1, 7)
	require.NoError(t, source.SetPrepareParamKindsWithMP([]PrepareParamKind{
		PrepareParamNone, PrepareParamInteger, PrepareParamNone, PrepareParamNone,
		PrepareParamDecimal, PrepareParamNone, PrepareParamNone, PrepareParamBoolean,
	}, mp))
	require.NoError(t, source.SetBinaryStringRowsWithMP(
		[]bool{false, true, false, false, false, false, false, true}, mp))

	flags := make([]uint8, source.Length())
	rows := []int32{1, 4, 7}
	for _, row := range rows {
		flags[row] = 1
	}
	var fromFlags, fromRows bytes.Buffer
	written, err := source.MarshalSelectedFlagsTo(&fromFlags, flags)
	require.NoError(t, err)
	require.Equal(t, len(rows), written)
	require.NoError(t, source.MarshalSelectedRowsTo(&fromRows, rows))
	require.Equal(t, fromFlags.Bytes(), fromRows.Bytes())
}

func BenchmarkSelectedRowsCodecAvoidsSparseFlagScans(b *testing.B) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_int64.ToType())
	values := make([]int64, 8192)
	for i := range values {
		values[i] = int64(i)
	}
	require.NoError(b, AppendFixedList(source, values, nil, mp))
	flags := make([]uint8, len(values))
	rows := make([]int32, 0, 256)
	for row := 0; row < len(values); row += len(values) / 256 {
		flags[row] = 1
		rows = append(rows, int32(row))
	}
	b.Cleanup(func() {
		source.Free(mp)
		require.Zero(b, mp.CurrNB())
	})

	b.Run("flags", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, err := source.MarshalSelectedFlagsTo(io.Discard, flags)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("rows", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := source.MarshalSelectedRowsTo(io.Discard, rows); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestSelectedRowsCodecPreservesMetadataAndVarlena(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_varchar.ToType())
	defer source.Free(mp)
	require.NoError(t, AppendBytes(source, []byte("zero"), false, mp))
	require.NoError(t, AppendBytes(source, nil, true, mp))
	require.NoError(t, AppendBytes(source, bytes.Repeat([]byte("x"), 80), false, mp))
	require.NoError(t, AppendBytes(source, []byte("three"), false, mp))
	source.GetGrouping().Add(1, 3)
	require.NoError(t, source.SetPrepareParamKindsWithMP([]PrepareParamKind{
		PrepareParamInteger,
		PrepareParamNone,
		PrepareParamDecimal,
		PrepareParamBoolean,
	}, mp))

	var encoded bytes.Buffer
	require.NoError(t, source.MarshalSelectedRowsTo(&encoded, []int32{3, 1, 0}))
	destination := NewOffHeapVecWithType(types.T_varchar.ToType())
	defer destination.Free(mp)
	require.NoError(t, destination.UnmarshalSelectedRowsFrom(&encoded, 3, mp))

	require.Equal(t, "three", string(destination.GetBytesAt(0)))
	require.True(t, destination.IsNull(1))
	require.Equal(t, "zero", string(destination.GetBytesAt(2)))
	require.True(t, destination.GetGrouping().Contains(0))
	require.True(t, destination.GetGrouping().Contains(1))
	require.False(t, destination.GetGrouping().Contains(2))
	require.Equal(t, PrepareParamBoolean, destination.GetPrepareParamKindAt(0))
	require.Equal(t, PrepareParamNone, destination.GetPrepareParamKindAt(1))
	require.Equal(t, PrepareParamInteger, destination.GetPrepareParamKindAt(2))
	require.Zero(t, encoded.Len())
}

func TestSelectedRowsCodecPreservesBinaryStringProvenance(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_text.ToType())
	defer source.Free(mp)
	for _, value := range []string{"binary-zero", "null", "text-two", "binary-three"} {
		require.NoError(t, AppendBytes(source, []byte(value), false, mp))
	}
	source.SetNull(1)
	require.NoError(t, source.SetBinaryStringRowsWithMP(
		[]bool{true, false, false, true}, mp))

	var encoded bytes.Buffer
	require.NoError(t, source.MarshalSelectedRowsTo(
		&encoded, []int32{2, 1, 3, 0}))
	destination := NewOffHeapVecWithType(types.T_text.ToType())
	defer destination.Free(mp)
	require.NoError(t, destination.UnmarshalSelectedRowsFrom(&encoded, 4, mp))

	require.Equal(t, []bool{false, false, true, true}, []bool{
		destination.GetBinaryStringMetadataAt(0),
		destination.GetBinaryStringMetadataAt(1),
		destination.GetBinaryStringMetadataAt(2),
		destination.GetBinaryStringMetadataAt(3),
	})
	require.True(t, destination.IsNull(1))
	require.True(t, destination.HasBinaryStringRows())
	require.Zero(t, encoded.Len())

	uniform := NewOffHeapVecWithType(types.T_text.ToType())
	defer uniform.Free(mp)
	source.SetIsBinaryString(true)
	encoded.Reset()
	require.NoError(t, source.MarshalSelectedRowsTo(&encoded, []int32{0, 1, 3}))
	require.NoError(t, uniform.UnmarshalSelectedRowsFrom(&encoded, 3, mp))
	require.True(t, uniform.GetBinaryStringMetadataAt(0))
	require.False(t, uniform.GetBinaryStringMetadataAt(1))
	require.True(t, uniform.GetBinaryStringMetadataAt(2))
	require.False(t, uniform.HasBinaryStringRows())
}

func TestSelectedRowsCodecPreservesUniformExplicitText(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_varbinary.ToType())
	destination := NewVec(types.T_varbinary.ToType())
	t.Cleanup(func() {
		destination.Free(mp)
		source.Free(mp)
		require.Zero(t, mp.CurrNB())
	})
	require.NoError(t, AppendBytesList(source, [][]byte{[]byte("a"), []byte("b")}, nil, mp))
	require.NoError(t, source.SetSelectedValueBinaryStringRowsWithMP([]bool{false, false}, mp))

	var encoded bytes.Buffer
	require.NoError(t, source.MarshalSelectedRowsTo(&encoded, []int32{0, 1}))
	require.NoError(t, destination.UnmarshalSelectedRowsFrom(&encoded, 2, mp))
	for row := 0; row < destination.Length(); row++ {
		require.Equal(t, types.RuntimeStringText, destination.GetRuntimeStringDomainAt(row))
		require.False(t, destination.GetIsBinaryStringAt(row))
	}
}

func TestSelectedRowsCodecRejectsInvalidOrTruncatedRecords(t *testing.T) {
	mp := mpool.MustNewZero()
	destination := NewOffHeapVecWithType(types.T_int64.ToType())
	defer destination.Free(mp)
	var nilVector *Vector
	require.Error(t, nilVector.MarshalSelectedRowsTo(io.Discard, nil))
	require.Error(t, destination.marshalSelectedRowsTo(nil, 0, func(int) int { return 0 }))
	require.Error(t, destination.marshalSelectedRowsTo(
		io.Discard, -1, func(int) int { return 0 }))
	require.Error(t, destination.MarshalSelectedRowsTo(io.Discard, []int32{0}))
	require.Error(t, nilVector.UnmarshalSelectedRowsFrom(bytes.NewReader(nil), 0, mp))
	require.Error(t, destination.UnmarshalSelectedRowsFrom(nil, 0, mp))
	require.Error(t, destination.UnmarshalSelectedRowsFrom(bytes.NewReader(nil), 0, nil))
	require.Error(t, destination.UnmarshalSelectedRowsFrom(bytes.NewReader(nil), -1, mp))
	constant := NewOffHeapVecWithType(types.T_int64.ToType())
	constant.ToConst()
	defer constant.Free(mp)
	require.ErrorContains(t,
		constant.UnmarshalSelectedRowsFrom(bytes.NewReader(nil), 0, mp),
		"non-constant destination")

	var wrongCount bytes.Buffer
	require.NoError(t, types.WriteInt32(&wrongCount, 2))
	require.ErrorContains(t,
		destination.UnmarshalSelectedRowsFrom(&wrongCount, 1, mp),
		"does not match")

	source := NewVec(types.T_int64.ToType())
	defer source.Free(mp)
	require.NoError(t, AppendFixedList(source, []int64{42}, nil, mp))
	var encoded bytes.Buffer
	require.NoError(t, source.MarshalSelectedRowsTo(&encoded, []int32{0}))
	payload := encoded.Bytes()
	require.Greater(t, len(payload), 1)
	require.Error(t, destination.UnmarshalSelectedRowsFrom(
		bytes.NewReader(payload[:len(payload)-1]), 1, mp))
	require.Zero(t, destination.Length())

	short := &selectedRowsOneShotShortWriter{}
	require.ErrorIs(t,
		source.MarshalSelectedRowsTo(short, []int32{0}),
		io.ErrShortWrite,
	)
}

func TestSelectedRowsCodecRejectsInvalidMetadataBeforePublishingRows(t *testing.T) {
	mp := mpool.MustNewZero()
	destination := NewOffHeapVecWithType(types.T_int64.ToType())
	defer destination.Free(mp)

	tests := []struct {
		name    string
		payload func(*bytes.Buffer)
		want    string
	}{
		{
			name: "reserved-vector-metadata",
			payload: func(buf *bytes.Buffer) {
				require.NoError(t, types.WriteInt32(buf, 1))
				require.NoError(t, buf.WriteByte(0x80))
			},
			want: "metadata",
		},
		{
			name: "invalid-uniform-kind",
			payload: func(buf *bytes.Buffer) {
				require.NoError(t, types.WriteInt32(buf, 1))
				require.NoError(t, buf.WriteByte(selectedRowsKindUniform<<selectedRowsKindShift))
				require.NoError(t, buf.WriteByte(0xff))
			},
			want: "parameter kind",
		},
		{
			name: "undeclared-row-flag",
			payload: func(buf *bytes.Buffer) {
				require.NoError(t, types.WriteInt32(buf, 1))
				require.NoError(t, buf.WriteByte(selectedRowsHasNull))
				require.NoError(t, buf.WriteByte(selectedRowsHasGrouping))
			},
			want: "row metadata",
		},
		{
			name: "undeclared-binary-row-flag",
			payload: func(buf *bytes.Buffer) {
				require.NoError(t, types.WriteInt32(buf, 1))
				require.NoError(t, buf.WriteByte(selectedRowsHasNull))
				require.NoError(t, buf.WriteByte(selectedRowsRowBinary))
			},
			want: "row metadata",
		},
		{
			name: "null-row-carries-binary-provenance",
			payload: func(buf *bytes.Buffer) {
				require.NoError(t, types.WriteInt32(buf, 1))
				require.NoError(t, buf.WriteByte(
					selectedRowsHasNull|
						selectedRowsBinaryRows<<selectedRowsBinaryShift))
				require.NoError(t, buf.WriteByte(
					selectedRowsHasNull|selectedRowsRowBinary))
			},
			want: "row metadata",
		},
		{
			name: "fixed-width-mismatch",
			payload: func(buf *bytes.Buffer) {
				require.NoError(t, types.WriteInt32(buf, 1))
				require.NoError(t, buf.WriteByte(0))
				require.NoError(t, types.WriteInt32(buf, 4))
				require.NoError(t, writeVectorMarshalBytes(buf, make([]byte, 4)))
			},
			want: "value size",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var payload bytes.Buffer
			tc.payload(&payload)
			err := destination.UnmarshalSelectedRowsFrom(&payload, 1, mp)
			require.ErrorContains(t, err, tc.want)
			require.Zero(t, destination.Length())
		})
	}
}

func TestSelectedRowsCodecEveryWireBoundaryIsAtomic(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_varchar.ToType())
	require.NoError(t, AppendBytes(source, bytes.Repeat([]byte("x"), 80), false, mp))
	require.NoError(t, AppendBytes(source, []byte("ordinary"), false, mp))
	source.GetGrouping().Add(0)
	require.NoError(t, source.SetPrepareParamKindsWithMP(
		[]PrepareParamKind{PrepareParamDecimal, PrepareParamNone}, mp))
	require.NoError(t, source.SetBinaryStringRowsWithMP([]bool{true, false}, mp))

	var encoded bytes.Buffer
	require.NoError(t, source.MarshalSelectedRowsTo(&encoded, []int32{0, 1}))
	payload := encoded.Bytes()
	for cut := 0; cut < len(payload); cut++ {
		destination := NewOffHeapVecWithType(types.T_varchar.ToType())
		require.Error(t, destination.UnmarshalSelectedRowsFrom(
			bytes.NewReader(payload[:cut]), 2, mp), "cut=%d", cut)
		require.Zero(t, destination.Length(), "cut=%d", cut)
		require.False(t, destination.HasBinaryStringMetadata(), "cut=%d", cut)
		destination.Free(mp)
	}
	for cut := 0; cut < len(payload); cut++ {
		require.Error(t, source.MarshalSelectedRowsTo(
			&failSelectedRowsWriter{remaining: cut}, []int32{0, 1}), "cut=%d", cut)
	}
	source.Free(mp)
	require.Zero(t, mp.CurrNB())
}

type failSelectedRowsWriter struct {
	remaining int
}

func (w *failSelectedRowsWriter) Write(value []byte) (int, error) {
	if len(value) <= w.remaining {
		w.remaining -= len(value)
		return len(value), nil
	}
	if w.remaining == 0 {
		return 0, io.ErrClosedPipe
	}
	n := w.remaining
	w.remaining = 0
	return n, io.ErrClosedPipe
}
