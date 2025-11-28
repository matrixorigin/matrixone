// Copyright 2025 Matrix Origin
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

package frontend

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestBufferPool(t *testing.T) {
	buf := acquireBuffer()
	buf.WriteString("data")
	releaseBuffer(buf)

	buf2 := acquireBuffer()
	require.Equal(t, 0, buf2.Len())
	releaseBuffer(buf2)
}

func TestFormatValIntoString_StringEscaping(t *testing.T) {
	var buf bytes.Buffer
	ses := &Session{}

	val := "a'b\"c\\\n\t\r\x1a\x00"
	formatValIntoString(ses, val, types.New(types.T_varchar, 0, 0), &buf)
	require.Equal(t, `'a\'b"c\\\n\t\r\Z\0'`, buf.String())
}

func TestFormatValIntoString_ByteEscaping(t *testing.T) {
	var buf bytes.Buffer
	ses := &Session{}

	val := []byte{'x', 0x00, '\\', 0x07, '\''}
	formatValIntoString(ses, val, types.New(types.T_varbinary, 0, 0), &buf)
	require.Equal(t, `'x\0\\\x07\''`, buf.String())
}

func TestFormatValIntoString_JSONEscaping(t *testing.T) {
	var buf bytes.Buffer
	ses := &Session{}

	val := `{"k":"` + string([]byte{0x01, '\n'}) + `"}`
	formatValIntoString(ses, val, types.New(types.T_json, 0, 0), &buf)
	require.Equal(t, `'{"k":"\\u0001\\u000a"}'`, buf.String())
}

func TestFormatValIntoString_JSONByteJson(t *testing.T) {
	var buf bytes.Buffer
	ses := &Session{}

	bj, err := types.ParseStringToByteJson(`{"a":1}`)
	require.NoError(t, err)

	formatValIntoString(ses, bj, types.New(types.T_json, 0, 0), &buf)
	require.Equal(t, `'{"a": 1}'`, buf.String())
}

func TestFormatValIntoString_Nil(t *testing.T) {
	var buf bytes.Buffer
	ses := &Session{}

	formatValIntoString(ses, nil, types.New(types.T_varchar, 0, 0), &buf)
	require.Equal(t, "NULL", buf.String())
}

func TestFormatValIntoString_UnsupportedType(t *testing.T) {
	var buf bytes.Buffer
	ses := &Session{}

	require.Panics(t, func() {
		formatValIntoString(ses, true, types.New(types.T_bool, 0, 0), &buf)
	})
}

func TestCompareSingleValInVector_AllTypes(t *testing.T) {
	ctx := context.Background()
	ses := &Session{}
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	cases := []struct {
		name  string
		build func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int)
	}{
		{
			name: "json",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_json.ToType()
				leftVec := vector.NewVec(typ)
				rightVec := vector.NewVec(typ)
				leftVal, err := types.ParseStringToByteJson(`{"k":1}`)
				require.NoError(t, err)
				rightVal, err := types.ParseStringToByteJson(`{"k":2}`)
				require.NoError(t, err)
				leftBytes, err := leftVal.Marshal()
				require.NoError(t, err)
				rightBytes, err := rightVal.Marshal()
				require.NoError(t, err)
				require.NoError(t, vector.AppendBytes(leftVec, leftBytes, false, mp))
				require.NoError(t, vector.AppendBytes(rightVec, rightBytes, false, mp))
				expected := types.CompareValue(types.DecodeJson(leftBytes), types.DecodeJson(rightBytes))
				return leftVec, rightVec, expected
			},
		},
		{
			name: "bool",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_bool.ToType()
				leftVal, rightVal := false, true
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "bit",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_bit.ToType()
				leftVal, rightVal := uint64(1), uint64(3)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "int8",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_int8.ToType()
				leftVal, rightVal := int8(1), int8(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "uint8",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_uint8.ToType()
				leftVal, rightVal := uint8(1), uint8(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "int16",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_int16.ToType()
				leftVal, rightVal := int16(1), int16(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "uint16",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_uint16.ToType()
				leftVal, rightVal := uint16(1), uint16(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "int32",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_int32.ToType()
				leftVal, rightVal := int32(1), int32(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "uint32",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_uint32.ToType()
				leftVal, rightVal := uint32(1), uint32(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "int64",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_int64.ToType()
				leftVal, rightVal := int64(1), int64(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "uint64",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_uint64.ToType()
				leftVal, rightVal := uint64(1), uint64(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "float32",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_float32.ToType()
				leftVal, rightVal := float32(1.1), float32(2.2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "float64",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_float64.ToType()
				leftVal, rightVal := float64(1.1), float64(2.2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "char",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.New(types.T_char, 8, 0)
				leftVal, rightVal := []byte("a"), []byte("b")
				leftVec := buildBytesVector(t, mp, typ, leftVal)
				rightVec := buildBytesVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "varchar",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.New(types.T_varchar, 16, 0)
				leftVal, rightVal := []byte("alpha"), []byte("beta")
				leftVec := buildBytesVector(t, mp, typ, leftVal)
				rightVec := buildBytesVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "blob",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_blob.ToType()
				leftVal, rightVal := []byte("blob-a"), []byte("blob-b")
				leftVec := buildBytesVector(t, mp, typ, leftVal)
				rightVec := buildBytesVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "text",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_text.ToType()
				leftVal, rightVal := []byte("text-a"), []byte("text-b")
				leftVec := buildBytesVector(t, mp, typ, leftVal)
				rightVec := buildBytesVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "binary",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.New(types.T_binary, 4, 0)
				leftVal, rightVal := []byte{0x00, 0x01}, []byte{0x00, 0x02}
				leftVec := buildBytesVector(t, mp, typ, leftVal)
				rightVec := buildBytesVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "varbinary",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.New(types.T_varbinary, 8, 0)
				leftVal, rightVal := []byte{0x01, 0x02}, []byte{0x02, 0x03}
				leftVec := buildBytesVector(t, mp, typ, leftVal)
				rightVec := buildBytesVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "datalink",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_datalink.ToType()
				leftVal, rightVal := []byte("link-a"), []byte("link-b")
				leftVec := buildBytesVector(t, mp, typ, leftVal)
				rightVec := buildBytesVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "array_float32",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.New(types.T_array_float32, 2, 0)
				leftVal := []float32{1, 2}
				rightVal := []float32{1, 3}
				leftVec := buildArrayVector(t, mp, typ, [][]float32{leftVal})
				rightVec := buildArrayVector(t, mp, typ, [][]float32{rightVal})
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "array_float64",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.New(types.T_array_float64, 2, 0)
				leftVal := []float64{1, 2}
				rightVal := []float64{2, 3}
				leftVec := buildArrayVector(t, mp, typ, [][]float64{leftVal})
				rightVec := buildArrayVector(t, mp, typ, [][]float64{rightVal})
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "date",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_date.ToType()
				leftVal, rightVal := types.Date(1), types.Date(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "datetime",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_datetime.ToType()
				leftVal, rightVal := types.Datetime(1), types.Datetime(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "time",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_time.ToType()
				leftVal, rightVal := types.Time(1), types.Time(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "timestamp",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_timestamp.ToType()
				leftVal, rightVal := types.Timestamp(1), types.Timestamp(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "decimal64",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.New(types.T_decimal64, 18, 2)
				leftVal, err := types.Decimal64FromFloat64(12.34, 18, 2)
				require.NoError(t, err)
				rightVal, err := types.Decimal64FromFloat64(23.45, 18, 2)
				require.NoError(t, err)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "decimal128",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.New(types.T_decimal128, 38, 4)
				leftVal, err := types.Decimal128FromFloat64(12.34, 38, 4)
				require.NoError(t, err)
				rightVal, err := types.Decimal128FromFloat64(23.45, 38, 4)
				require.NoError(t, err)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "uuid",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_uuid.ToType()
				var leftVal, rightVal types.Uuid
				copy(leftVal[:], []byte("aaaaaaaaaaaaaaaa"))
				copy(rightVal[:], []byte("bbbbbbbbbbbbbbbb"))
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "rowid",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_Rowid.ToType()
				var blk types.Blockid
				copy(blk[:], []byte("block-identifier"))
				leftVal := types.NewRowid(&blk, 1)
				rightVal := types.NewRowid(&blk, 2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "blockid",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_Blockid.ToType()
				var leftVal, rightVal types.Blockid
				copy(leftVal[:], []byte("block-00000000001"))
				copy(rightVal[:], []byte("block-00000000002"))
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "ts",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_TS.ToType()
				leftVal := types.BuildTS(1, 0)
				rightVal := types.BuildTS(2, 0)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "enum",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_enum.ToType()
				leftVal, rightVal := types.Enum(1), types.Enum(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			leftVec, rightVec, expected := tc.build(t, mp)
			defer leftVec.Free(mp)
			defer rightVec.Free(mp)

			cmp, err := compareSingleValInVector(ctx, ses, 0, 0, leftVec, rightVec)
			require.NoError(t, err)
			require.Equal(t, expected, cmp)
		})
	}
}

func TestCompareSingleValInVector_ConstVectors(t *testing.T) {
	ctx := context.Background()
	ses := &Session{}
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	typ := types.T_int32.ToType()
	leftVec := buildFixedVector(t, mp, typ, int32(5))
	rightVec, err := vector.NewConstFixed[int32](typ, int32(7), 3, mp)
	require.NoError(t, err)
	defer leftVec.Free(mp)
	defer rightVec.Free(mp)

	cmp, err := compareSingleValInVector(ctx, ses, 0, 2, leftVec, rightVec)
	require.NoError(t, err)
	require.Equal(t, types.CompareValue(int32(5), int32(7)), cmp)
}

func buildFixedVector[T any](t *testing.T, mp *mpool.MPool, typ types.Type, vals ...T) *vector.Vector {
	vec := vector.NewVec(typ)
	for _, v := range vals {
		require.NoError(t, vector.AppendFixed(vec, v, false, mp))
	}
	return vec
}

func buildBytesVector(t *testing.T, mp *mpool.MPool, typ types.Type, vals ...[]byte) *vector.Vector {
	vec := vector.NewVec(typ)
	for _, v := range vals {
		require.NoError(t, vector.AppendBytes(vec, v, false, mp))
	}
	return vec
}

func buildArrayVector[T types.RealNumbers](t *testing.T, mp *mpool.MPool, typ types.Type, vals [][]T) *vector.Vector {
	vec := vector.NewVec(typ)
	require.NoError(t, vector.AppendArrayList(vec, vals, nil, mp))
	return vec
}
