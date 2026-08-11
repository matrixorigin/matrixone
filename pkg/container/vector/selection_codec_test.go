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

func TestSelectedRowsCodecRejectsInvalidOrTruncatedRecords(t *testing.T) {
	mp := mpool.MustNewZero()
	destination := NewOffHeapVecWithType(types.T_int64.ToType())
	defer destination.Free(mp)
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
