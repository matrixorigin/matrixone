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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestStringResultBoundArithmetic(t *testing.T) {
	tests := []struct {
		name    string
		bound   stringResultBound
		count   uint64
		want    uint64
		unknown bool
	}{
		{name: "zero", bound: stringResultBound{}, count: math.MaxUint64, want: 0},
		{name: "one", bound: stringResultBound{bytes: 1}, count: 1, want: 1},
		{name: "70000", bound: stringResultBound{bytes: 70000}, count: 1, want: 70000},
		{name: "overflow", bound: stringResultBound{bytes: math.MaxUint64}, count: 2, unknown: true},
		{name: "unknown", bound: unknownStringResultBound(), count: 0, unknown: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := multiplyStringResultBound(test.bound, test.count)
			require.Equal(t, test.unknown, got.unknown)
			require.Equal(t, test.want, got.bytes)
		})
	}
	require.True(t, addStringResultBounds(
		stringResultBound{bytes: math.MaxUint64}, stringResultBound{bytes: 1}).unknown)
}

func TestStringTypeBoundClassification(t *testing.T) {
	boundedText := types.New(types.T_varchar, 7, 0)
	boundedBinary := types.New(types.T_varbinary, 9, 0)
	boundedTextFamily := types.New(types.T_text, types.MaxTinyTextLen, 0)
	boundedBlob := types.New(types.T_blob, 11, 0)

	for _, test := range []struct {
		name    string
		typ     types.Type
		bytes   uint64
		chars   uint64
		unknown bool
	}{
		{name: "varchar", typ: boundedText, bytes: 28, chars: 7},
		{name: "varbinary", typ: boundedBinary, bytes: 9, chars: 0},
		{name: "bounded text", typ: boundedTextFamily, bytes: types.MaxTinyTextLen, chars: types.MaxTinyTextLen},
		{name: "bounded blob", typ: boundedBlob, bytes: 11, chars: 0},
		{name: "unbounded text", typ: types.T_text.ToType(), unknown: true},
		{name: "unbounded blob", typ: types.T_blob.ToType(), unknown: true},
		{name: "negative width", typ: types.New(types.T_varchar, -1, 0), unknown: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := declaredStringByteBound(test.typ)
			require.Equal(t, test.unknown, got.unknown)
			if !test.unknown {
				require.Equal(t, test.bytes, got.bytes)
			}
			if test.typ.Oid == types.T_varchar || test.typ.Oid == types.T_text {
				chars := declaredTextCharacterBound(test.typ)
				require.Equal(t, test.unknown, chars.unknown)
				if !test.unknown {
					require.Equal(t, test.chars, chars.bytes)
				}
			}
		})
	}
	require.True(t, declaredTextCharacterBound(types.T_any.ToType()).unknown)
}

func TestFormattedStringByteBounds(t *testing.T) {
	decimal := types.New(types.T_decimal128, 38, 6)
	for _, test := range []struct {
		typ     types.Type
		want    uint64
		unknown bool
	}{
		{typ: types.T_bool.ToType(), want: 1},
		{typ: types.T_int8.ToType(), want: 4},
		{typ: types.T_int16.ToType(), want: 6},
		{typ: types.T_int32.ToType(), want: 11},
		{typ: types.T_int64.ToType(), want: 20},
		{typ: types.T_uint8.ToType(), want: 3},
		{typ: types.T_uint16.ToType(), want: 5},
		{typ: types.T_uint32.ToType(), want: 10},
		{typ: types.T_uint64.ToType(), want: 20},
		{typ: types.T_float32.ToType(), want: 15},
		{typ: types.T_float64.ToType(), want: 24},
		{typ: decimal, want: 40},
		{typ: types.T_date.ToType(), want: 10},
		{typ: types.T_time.ToType(), want: 17},
		{typ: types.T_datetime.ToType(), want: 26},
		{typ: types.T_timestamp.ToType(), want: 26},
		{typ: types.T_year.ToType(), want: 4},
		{typ: types.T_uuid.ToType(), want: 36},
		{typ: types.T_enum.ToType(), want: 5},
		{typ: types.T_any.ToType(), unknown: true},
	} {
		got := formattedStringByteBound(test.typ)
		require.Equal(t, test.unknown, got.unknown, test.typ.Oid.String())
		if !test.unknown {
			require.Equal(t, test.want, got.bytes, test.typ.Oid.String())
		}
	}
	require.True(t, formattedStringByteBound(types.New(types.T_decimal128, 0, 0)).unknown)
}

func TestBinaryStringResultTypePromotionBoundaries(t *testing.T) {
	for _, width := range []uint64{0, 1, types.MaxVarBinaryLen - 1, types.MaxVarBinaryLen} {
		result := binaryStringResultType(stringResultBound{bytes: width})
		require.Equal(t, types.T_varbinary, result.Oid)
		require.Equal(t, int32(width), result.Width)
		require.Equal(t, types.CharsetBinary, result.Charset)
	}
	for _, bound := range []stringResultBound{
		{bytes: types.MaxVarBinaryLen + 1},
		{bytes: 70000},
		unknownStringResultBound(),
	} {
		result := binaryStringResultType(bound)
		require.Equal(t, types.T_blob, result.Oid)
		require.Equal(t, types.CharsetBinary, result.Charset)
	}
}

func TestConvertReturnTypeUsesSourceMaximumAndTargetDomain(t *testing.T) {
	binaryTarget := types.NewWithCharset(types.T_varchar, 6, 0, types.CharsetBinary)
	textTarget := types.NewWithCharset(types.T_varchar, 7, 0, types.CharsetUTF8)

	tests := []struct {
		name      string
		source    types.Type
		target    types.Type
		wantOID   types.T
		wantWidth int32
		charset   uint8
	}{
		{name: "bool binary", source: types.T_bool.ToType(), target: binaryTarget, wantOID: types.T_varbinary, wantWidth: 1, charset: types.CharsetBinary},
		{name: "int64 binary", source: types.T_int64.ToType(), target: binaryTarget, wantOID: types.T_varbinary, wantWidth: 20, charset: types.CharsetBinary},
		{name: "year binary", source: types.T_year.ToType(), target: binaryTarget, wantOID: types.T_varbinary, wantWidth: 4, charset: types.CharsetBinary},
		{name: "uuid binary", source: types.T_uuid.ToType(), target: binaryTarget, wantOID: types.T_varbinary, wantWidth: 36, charset: types.CharsetBinary},
		{name: "unknown binary", source: types.T_any.ToType(), target: binaryTarget, wantOID: types.T_blob, charset: types.CharsetBinary},
		{name: "text target", source: types.T_int64.ToType(), target: textTarget, wantOID: types.T_varchar, wantWidth: 20, charset: types.CharsetUTF8},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := convertReturnType([]types.Type{test.source, test.target})
			require.Equal(t, test.wantOID, got.Oid)
			require.Equal(t, test.wantWidth, got.Width)
			require.Equal(t, test.charset, got.Charset)
		})
	}
}

func TestConcatReturnTypePromotesWithoutCapping(t *testing.T) {
	binary := func(width int32) types.Type {
		return types.NewWithCharset(types.T_varbinary, width, 0, types.CharsetBinary)
	}
	for _, test := range []struct {
		name    string
		inputs  []types.Type
		wantOID types.T
		width   int32
	}{
		{name: "maximum minus one", inputs: []types.Type{binary(types.MaxVarBinaryLen - 1)}, wantOID: types.T_varbinary, width: types.MaxVarBinaryLen - 1},
		{name: "maximum", inputs: []types.Type{binary(types.MaxVarBinaryLen)}, wantOID: types.T_varbinary, width: types.MaxVarBinaryLen},
		{name: "maximum plus one", inputs: []types.Type{binary(types.MaxVarBinaryLen), binary(1)}, wantOID: types.T_blob},
		{name: "70000", inputs: []types.Type{binary(35000), binary(35000)}, wantOID: types.T_blob},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := concatReturnType(test.inputs)
			require.Equal(t, test.wantOID, got.Oid)
			if got.Oid == types.T_varbinary {
				require.Equal(t, test.width, got.Width)
			}
		})
	}
}

func TestCharReturnTypeIsBinaryAndPromotesLargeArity(t *testing.T) {
	small := make([]types.Type, 2)
	large := make([]types.Type, types.MaxVarBinaryLen/4+1)

	got := binaryStringResultType(stringResultBound{bytes: uint64(len(small)) * 4})
	require.Equal(t, types.T_varbinary, got.Oid)
	require.Equal(t, int32(8), got.Width)
	got = binaryStringResultType(stringResultBound{bytes: uint64(len(large)) * 4})
	require.Equal(t, types.T_blob, got.Oid)
}
