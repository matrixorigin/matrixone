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

package function

import (
	"math"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestSerialEncodedTypeSizeBound(t *testing.T) {
	for _, tc := range []struct {
		name      string
		typ       types.Type
		want      uint64
		supported bool
	}{
		{name: "bool", typ: types.T_bool.ToType(), want: 1, supported: true},
		{name: "int8", typ: types.T_int8.ToType(), want: 3, supported: true},
		{name: "uint8", typ: types.T_uint8.ToType(), want: 3, supported: true},
		{name: "int16", typ: types.T_int16.ToType(), want: 4, supported: true},
		{name: "uint16", typ: types.T_uint16.ToType(), want: 4, supported: true},
		{name: "int32", typ: types.T_int32.ToType(), want: 6, supported: true},
		{name: "uint32", typ: types.T_uint32.ToType(), want: 6, supported: true},
		{name: "int64", typ: types.T_int64.ToType(), want: 10, supported: true},
		{name: "uint64", typ: types.T_uint64.ToType(), want: 10, supported: true},
		{name: "bit", typ: types.T_bit.ToType(), want: 10, supported: true},
		{name: "float32", typ: types.T_float32.ToType(), want: 5, supported: true},
		{name: "float64", typ: types.T_float64.ToType(), want: 9, supported: true},
		{name: "date", typ: types.T_date.ToType(), want: 6, supported: true},
		{name: "time", typ: types.T_time.ToType(), want: 10, supported: true},
		{name: "datetime", typ: types.T_datetime.ToType(), want: 10, supported: true},
		{name: "timestamp", typ: types.T_timestamp.ToType(), want: 10, supported: true},
		{name: "year", typ: types.T_year.ToType(), want: 4, supported: true},
		{name: "enum", typ: types.T_enum.ToType(), want: 5, supported: true},
		{name: "decimal64", typ: types.T_decimal64.ToType(), want: 9, supported: true},
		{name: "decimal128", typ: types.T_decimal128.ToType(), want: 17, supported: true},
		{name: "decimal256", typ: types.T_decimal256.ToType(), want: 33, supported: true},
		{name: "uuid", typ: types.T_uuid.ToType(), want: 17, supported: true},
		{
			name:      "declared varchar",
			typ:       types.New(types.T_varchar, 128, 0),
			want:      2*(128*utf8.UTFMax) + 3,
			supported: true,
		},
		{
			name:      "unspecified varchar",
			typ:       types.New(types.T_varchar, 0, 0),
			want:      2*(types.MaxVarcharLen*utf8.UTFMax) + 3,
			supported: true,
		},
		{
			name:      "array bytes not descriptors",
			typ:       types.New(types.T_array_float64, 16, 0),
			want:      2*(16*8) + 3,
			supported: true,
		},
		{
			name:      "blob hard bound",
			typ:       types.T_blob.ToType(),
			want:      2*types.MaxBlobLen + 3,
			supported: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := SerialEncodedTypeSizeBound(tc.typ)
			require.Equal(t, tc.supported, ok)
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.supported, SerialTypeSupported(tc.typ.Oid))
		})
	}
}

func TestSerialEncodedTypeSizeBoundCoversRuntimeValue(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	for _, tc := range []struct {
		name string
		vec  *vector.Vector
	}{
		{
			name: "maximum int32 encoding",
			vec: func() *vector.Vector {
				vec := vector.NewVec(types.T_int32.ToType())
				require.NoError(t, vector.AppendFixed(vec, int32(math.MinInt32), false, mp))
				return vec
			}(),
		},
		{
			name: "all bytes escaped",
			vec: func() *vector.Vector {
				vec := vector.NewVec(types.New(types.T_varchar, 128, 0))
				require.NoError(t, vector.AppendBytes(vec, make([]byte, 128), false, mp))
				return vec
			}(),
		},
		{
			name: "declared width counts multibyte characters",
			vec: func() *vector.Vector {
				vec := vector.NewVec(types.New(types.T_varchar, 128, 0))
				require.NoError(t, vector.AppendBytes(
					vec, []byte(strings.Repeat("𐍈", 128)), false, mp))
				return vec
			}(),
		},
		{
			name: "decimal256 encoding",
			vec: func() *vector.Vector {
				vec := vector.NewVec(types.New(types.T_decimal256, 65, 2))
				require.NoError(t, vector.AppendFixed(
					vec, mustParseDecimal256(t, "123.45", 2), false, mp))
				return vec
			}(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer tc.vec.Free(mp)
			static, ok := SerialEncodedTypeSizeBound(*tc.vec.GetType())
			require.True(t, ok)
			runtimeBound, err := SerialEncodedValueSizeBound(tc.vec, 0)
			require.NoError(t, err)

			encoder, err := NewSerialValueEncoder(tc.vec)
			require.NoError(t, err)
			packer := types.NewPacker()
			defer packer.Close()
			encoder(tc.vec, 0, packer)
			actual := uint64(len(packer.GetBuf()))

			require.LessOrEqual(t, actual, runtimeBound,
				"the runtime bound must cover the production encoder")
			require.LessOrEqual(t, runtimeBound, static,
				"the type contract must cover every admitted runtime value")
		})
	}
}

func TestSerialDecimal256Encoding(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	typ := types.New(types.T_decimal256, 65, 2)
	values := []types.Decimal256{
		mustParseDecimal256(t, "-123.45", 2),
		mustParseDecimal256(t, "678.90", 2),
	}
	vec := vector.NewVec(typ)
	defer vec.Free(mp)
	require.NoError(t, vector.AppendFixedList(vec, values, nil, mp))

	want := make([][]byte, len(values))
	for i, value := range values {
		packer := types.NewPacker()
		packer.EncodeDecimal256(value)
		want[i] = packer.Bytes()
		packer.Close()
	}

	encoder, err := NewSerialValueEncoder(vec)
	require.NoError(t, err)
	for i := range values {
		packer := types.NewPacker()
		encoder(vec, i, packer)
		require.Equal(t, want[i], packer.GetBuf())
		packer.Close()
	}

	packers := types.NewPackerArray(len(values))
	defer func() {
		for _, packer := range packers {
			packer.Close()
		}
	}()
	SerialHelper(vec, nil, packers, true)
	for i := range values {
		require.Equal(t, want[i], packers[i].GetBuf())
	}

	t.Run("nulls use both serial contracts", func(t *testing.T) {
		nullVec := vector.NewVec(typ)
		defer nullVec.Free(mp)
		require.NoError(t, vector.AppendFixedList(
			nullVec, values, []bool{false, true}, mp))

		fullPackers := types.NewPackerArray(len(values))
		defer func() {
			for _, packer := range fullPackers {
				packer.Close()
			}
		}()
		SerialHelper(nullVec, nil, fullPackers, true)
		require.Equal(t, want[0], fullPackers[0].GetBuf())
		nullPacker := types.NewPacker()
		nullPacker.EncodeNull()
		require.Equal(t, nullPacker.GetBuf(), fullPackers[1].GetBuf())
		nullPacker.Close()

		bitmap := nulls.NewWithSize(len(values))
		compactPackers := types.NewPackerArray(len(values))
		defer func() {
			for _, packer := range compactPackers {
				packer.Close()
			}
		}()
		SerialHelper(nullVec, bitmap, compactPackers, false)
		require.Equal(t, want[0], compactPackers[0].GetBuf())
		require.True(t, bitmap.Contains(1))
		require.False(t, bitmap.Contains(0))
		require.Empty(t, compactPackers[1].GetBuf())
	})
}
