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
	"context"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
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

func TestBoundedBuiltinReturnTypes(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	varchar := func(width int32) types.Type { return types.New(types.T_varchar, width, 0) }
	varbinary := func(width int32) types.Type { return types.New(types.T_varbinary, width, 0) }
	assertType := func(t *testing.T, name string, inputs []types.Type, oid types.T, width int32, charset uint8) {
		t.Helper()
		resolved, err := GetFunctionByName(proc.Ctx, name, inputs)
		require.NoError(t, err, "%s(%v)", name, inputs)
		result := resolved.GetReturnType()
		require.Equal(t, oid, result.Oid, "%s(%v)", name, inputs)
		require.Equal(t, width, result.Width, "%s(%v)", name, inputs)
		require.Equal(t, charset, result.Charset, "%s(%v)", name, inputs)
	}

	for _, oid := range []types.T{
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
	} {
		t.Run("bin/"+oid.String(), func(t *testing.T) {
			assertType(t, "bin", []types.Type{oid.ToType()}, types.T_varchar, 65, types.CharsetUTF8)
		})
	}

	for _, input := range []types.Type{varchar(12), types.T_int64.ToType()} {
		t.Run("conv/"+input.Oid.String(), func(t *testing.T) {
			assertType(t, "conv", []types.Type{input, types.T_int64.ToType(), types.T_int64.ToType()}, types.T_varchar, 65, types.CharsetUTF8)
		})
	}

	assertType(t, "inet_ntoa", []types.Type{types.T_uint64.ToType()}, types.T_varchar, 31, types.CharsetUTF8)
	assertType(t, "inet6_ntoa", []types.Type{varbinary(16)}, types.T_varchar, 39, types.CharsetUTF8)
	assertType(t, "inet6_aton", []types.Type{varchar(39)}, types.T_varbinary, 16, types.CharsetBinary)

	for _, test := range []struct {
		name      string
		fn        string
		input     types.Type
		wantOID   types.T
		wantWidth int32
	}{
		{name: "hex int", fn: "hex", input: types.T_int64.ToType(), wantOID: types.T_varchar, wantWidth: 16},
		{name: "hex varchar", fn: "hex", input: varchar(12), wantOID: types.T_varchar, wantWidth: 96},
		{name: "hex varbinary", fn: "hex", input: varbinary(12), wantOID: types.T_varchar, wantWidth: 24},
		{name: "hex array", fn: "hex", input: types.T_array_float64.ToType(), wantOID: types.T_text, wantWidth: 0},
		{name: "unhex varchar", fn: "unhex", input: varchar(2), wantOID: types.T_varbinary, wantWidth: 4},
		{name: "unhex varbinary", fn: "unhex", input: varbinary(12), wantOID: types.T_varbinary, wantWidth: 6},
	} {
		t.Run(test.name, func(t *testing.T) {
			assertType(t, test.fn, []types.Type{test.input}, test.wantOID, test.wantWidth,
				map[types.T]uint8{types.T_varchar: types.CharsetUTF8, types.T_text: types.CharsetUTF8, types.T_varbinary: types.CharsetBinary}[test.wantOID])
		})
	}

	for _, test := range []struct {
		name string
		fn   string
		args []types.Type
		want int32
	}{
		{name: "md5", fn: "md5", args: []types.Type{types.T_blob.ToType()}, want: 32},
		{name: "sha1", fn: "sha1", args: []types.Type{varchar(12)}, want: 40},
		{name: "sha2", fn: "sha2", args: []types.Type{varchar(12), types.T_int64.ToType()}, want: 128},
		{name: "compress varchar", fn: "compress", args: []types.Type{varchar(12)}, want: 68},
		{name: "compress varbinary", fn: "compress", args: []types.Type{varbinary(12)}, want: 32},
		{name: "aes encrypt varchar", fn: "aes_encrypt", args: []types.Type{varchar(12), varchar(3)}, want: 64},
		{name: "aes encrypt varbinary", fn: "aes_encrypt", args: []types.Type{varbinary(12), varchar(3)}, want: 16},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := GetFunctionByName(proc.Ctx, test.fn, test.args)
			require.NoError(t, err)
			result := resolved.GetReturnType()
			if test.fn == "md5" || test.fn == "sha1" || test.fn == "sha2" {
				require.Equal(t, types.T_varchar, result.Oid)
				require.Equal(t, types.CharsetUTF8, result.Charset)
			} else {
				require.Equal(t, types.T_varbinary, result.Oid)
				require.Equal(t, types.CharsetBinary, result.Charset)
			}
			require.Equal(t, test.want, result.Width)
		})
	}

	for _, fn := range []string{"uncompressed_length"} {
		assertType(t, fn, []types.Type{types.T_blob.ToType()}, types.T_int32, 0, types.CharsetLegacy)
	}
}

func TestBoundedBuiltinRegistryCoversEveryChangedOverload(t *testing.T) {
	ctx := context.Background()
	resolve := func(t *testing.T, name string, args []types.Type) types.Type {
		t.Helper()
		resolved, err := GetFunctionByName(ctx, name, args)
		require.NoError(t, err, "%s(%v)", name, args)
		result := resolved.GetReturnType()
		require.NotEqual(t, types.T_any, result.Oid, "%s(%v) returned ANY", name, args)
		return result
	}

	convInputs := []types.Type{
		types.T_varchar.ToType(), types.T_char.ToType(), types.T_text.ToType(),
		types.T_int8.ToType(), types.T_int16.ToType(), types.T_int32.ToType(), types.T_int64.ToType(),
		types.T_uint8.ToType(), types.T_uint16.ToType(), types.T_uint32.ToType(), types.T_uint64.ToType(),
		types.T_float32.ToType(), types.T_float64.ToType(),
	}
	for overloadID, input := range convInputs {
		t.Run("conv/"+input.Oid.String(), func(t *testing.T) {
			args := []types.Type{input, types.T_int64.ToType(), types.T_int64.ToType()}
			resolved, err := GetFunctionByNameWithOverload(ctx, "conv", args, int32(overloadID))
			require.NoError(t, err, "conv overload %d (%v)", overloadID, args)
			result := resolved.GetReturnType()
			require.Equal(t, types.T_varchar, result.Oid)
			require.Equal(t, int32(65), result.Width)
		})
	}

	for _, input := range []types.Type{
		types.T_varchar.ToType(), types.T_char.ToType(), types.T_text.ToType(), types.T_blob.ToType(),
	} {
		t.Run("compress/"+input.Oid.String(), func(t *testing.T) {
			result := resolve(t, "compress", []types.Type{input})
			require.Contains(t, []types.T{types.T_varbinary, types.T_blob}, result.Oid)
		})
		t.Run("uncompressed_length/"+input.Oid.String(), func(t *testing.T) {
			result := resolve(t, "uncompressed_length", []types.Type{input})
			require.Equal(t, types.T_int32, result.Oid)
		})
	}

	for _, input := range []types.Type{
		types.T_varchar.ToType(), types.T_char.ToType(), types.T_text.ToType(), types.T_blob.ToType(),
	} {
		for _, arity := range []int{2, 3} {
			name := input.Oid.String() + "/" + strconv.Itoa(arity)
			t.Run("aes_encrypt/"+name, func(t *testing.T) {
				args := []types.Type{input, types.T_varchar.ToType()}
				if arity == 3 {
					args = append(args, types.T_varchar.ToType())
				}
				result := resolve(t, "aes_encrypt", args)
				require.Contains(t, []types.T{types.T_varbinary, types.T_blob}, result.Oid)
			})
		}
	}

	for _, input := range []types.Type{
		types.T_uint8.ToType(), types.T_uint16.ToType(), types.T_uint32.ToType(), types.T_uint64.ToType(),
		types.T_int8.ToType(), types.T_int16.ToType(), types.T_int32.ToType(), types.T_int64.ToType(),
		types.T_float32.ToType(), types.T_float64.ToType(),
	} {
		t.Run("bin/"+input.Oid.String(), func(t *testing.T) {
			result := resolve(t, "bin", []types.Type{input})
			require.Equal(t, types.T_varchar, result.Oid)
		})
	}

	for _, input := range []types.Type{
		types.T_varchar.ToType(), types.T_char.ToType(),
		types.T_int64.ToType(), types.T_uint64.ToType(), types.T_float32.ToType(), types.T_float64.ToType(),
		types.T_array_float32.ToType(), types.T_array_float64.ToType(),
	} {
		t.Run("hex/"+input.Oid.String(), func(t *testing.T) {
			result := resolve(t, "hex", []types.Type{input})
			require.Contains(t, []types.T{types.T_varchar, types.T_text}, result.Oid)
		})
	}

	for _, input := range []types.Type{types.T_varchar.ToType(), types.T_text.ToType(), types.T_blob.ToType()} {
		t.Run("md5/"+input.Oid.String(), func(t *testing.T) {
			result := resolve(t, "md5", []types.Type{input})
			require.Equal(t, types.T_varchar, result.Oid)
			require.Equal(t, int32(32), result.Width)
		})
	}

	for _, input := range []types.Type{types.T_varbinary.ToType(), types.T_binary.ToType(), types.T_blob.ToType()} {
		t.Run("inet6_ntoa/"+input.Oid.String(), func(t *testing.T) {
			result := resolve(t, "inet6_ntoa", []types.Type{input})
			require.Equal(t, types.T_varchar, result.Oid)
			require.Equal(t, int32(39), result.Width)
		})
	}

	for _, input := range []types.Type{
		types.T_uint64.ToType(), types.T_uint32.ToType(), types.T_int64.ToType(), types.T_int32.ToType(),
	} {
		t.Run("inet_ntoa/"+input.Oid.String(), func(t *testing.T) {
			result := resolve(t, "inet_ntoa", []types.Type{input})
			require.Equal(t, types.T_varchar, result.Oid)
			require.Equal(t, int32(31), result.Width)
		})
	}

	result := resolve(t, "sha2", []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()})
	require.Equal(t, types.T_varchar, result.Oid)
	require.Equal(t, int32(128), result.Width)
	result = resolve(t, "sha1", []types.Type{types.T_varchar.ToType()})
	require.Equal(t, types.T_varchar, result.Oid)
	require.Equal(t, int32(40), result.Width)
}

func TestBoundedBuiltinResultBoundsFailClosed(t *testing.T) {
	for _, test := range []struct {
		name string
		got  types.Type
		want types.T
	}{
		{name: "compress blob", got: compressReturnType([]types.Type{types.T_blob.ToType()}), want: types.T_blob},
		{name: "aes encrypt blob", got: aesEncryptReturnType([]types.Type{types.T_blob.ToType()}), want: types.T_blob},
		{name: "unhex wide varchar", got: unhexReturnType([]types.Type{types.T_varchar.ToType()}), want: types.T_blob},
		{name: "hex wide varchar", got: stringHexReturnType([]types.Type{types.T_varchar.ToType()}), want: types.T_text},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, test.got.Oid)
		})
	}

	require.Equal(t, uint64(68), compressResultBound(stringResultBound{bytes: 48}).bytes)
	require.Equal(t, uint64(16), aesPaddedResultBound(stringResultBound{bytes: 0}).bytes)
	require.Equal(t, uint64(32), aesPaddedResultBound(stringResultBound{bytes: 16}).bytes)
	require.Equal(t, uint64(32), aesPaddedResultBound(stringResultBound{bytes: 17}).bytes)
	require.Equal(t, uint64(4), roundedUpHalfStringResultBound(stringResultBound{bytes: 7}).bytes)
	require.True(t, compressResultBound(unknownStringResultBound()).unknown)
	require.True(t, roundedUpHalfStringResultBound(unknownStringResultBound()).unknown)
	require.True(t, aesPaddedResultBound(stringResultBound{bytes: math.MaxUint64}).unknown)
	require.Equal(t, types.T_text, stringHexReturnType(nil).Oid)
	require.Equal(t, types.T_blob, unhexReturnType(nil).Oid)
	require.Equal(t, types.T_blob, compressReturnType(nil).Oid)
	require.Equal(t, types.T_blob, aesEncryptReturnType(nil).Oid)
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
	decimalEqualScale64 := types.New(types.T_decimal64, 2, 2)
	decimalEqualScale128 := types.New(types.T_decimal128, 2, 2)
	decimalEqualScale256 := types.New(types.T_decimal256, 2, 2)
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
		{typ: types.T_float32.ToType(), want: 24},
		{typ: types.T_float64.ToType(), want: 32},
		{typ: decimal, want: 40},
		{typ: decimalEqualScale64, want: 5},
		{typ: decimalEqualScale128, want: 5},
		{typ: decimalEqualScale256, want: 5},
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
		{name: "varchar text target keeps character width", source: types.New(types.T_varchar, 20000, 0), target: textTarget, wantOID: types.T_varchar, wantWidth: 20000, charset: types.CharsetUTF8},
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

func TestDerivedTextReturnTypeKeepsCharacterWidth(t *testing.T) {
	source := types.New(types.T_varchar, 20000, 0)
	got := derivedStringReturnType([]types.Type{source}, 0, types.T_varchar)
	require.Equal(t, types.T_varchar, got.Oid)
	require.Equal(t, int32(20000), got.Width)
	require.Equal(t, types.CharsetUTF8, got.Charset)

	binary := types.New(types.T_varbinary, 20000, 0)
	got = derivedStringReturnType([]types.Type{binary}, 0, types.T_varbinary)
	require.Equal(t, types.T_varbinary, got.Oid)
	require.Equal(t, int32(20000), got.Width)
	require.Equal(t, types.CharsetBinary, got.Charset)
}

func TestDerivedTextFunctionsKeepVarcharMetadata(t *testing.T) {
	proc := testutil.NewProcess(t)
	text := types.New(types.T_varchar, 20000, 0)
	integer := types.T_int64.ToType()
	for _, test := range []struct {
		name   string
		inputs []types.Type
	}{
		{name: "left", inputs: []types.Type{text, integer}},
		{name: "right", inputs: []types.Type{text, integer}},
		{name: "substring", inputs: []types.Type{text, integer, integer}},
		{name: "reverse", inputs: []types.Type{text}},
		{name: "trim", inputs: []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), text}},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := GetFunctionByName(proc.Ctx, test.name, test.inputs)
			require.NoError(t, err)
			result := resolved.GetReturnType()
			require.Equal(t, types.T_varchar, result.Oid)
			require.LessOrEqual(t, result.Width, int32(types.MaxVarcharLen))
		})
	}
}

func TestExpandingReplacementAndInsertBounds(t *testing.T) {
	varchar := func(width int32) types.Type { return types.New(types.T_varchar, width, 0) }
	varbinary := func(width int32) types.Type { return types.New(types.T_varbinary, width, 0) }

	replaced := replacementStringReturnType([]types.Type{varchar(2), varchar(1), varchar(2)})
	require.Equal(t, types.T_varchar, replaced.Oid)
	require.Equal(t, int32(4), replaced.Width)

	inserted := insertStringReturnType([]types.Type{varbinary(1), types.T_int64.ToType(), types.T_int64.ToType(), varbinary(1)})
	require.Equal(t, types.T_varbinary, inserted.Oid)
	require.Equal(t, int32(2), inserted.Width)

	binaryReplacement := replacementStringReturnType([]types.Type{varchar(2), varchar(1), varbinary(1)})
	require.Equal(t, types.T_varchar, binaryReplacement.Oid)
	require.Equal(t, types.CharsetUTF8, binaryReplacement.Charset)
	binaryInsertion := insertStringReturnType([]types.Type{varchar(1), types.T_int64.ToType(), types.T_int64.ToType(), varbinary(1)})
	require.Equal(t, types.T_varchar, binaryInsertion.Oid)
	require.Equal(t, types.CharsetUTF8, binaryInsertion.Charset)
}

func TestRegexpReplaceReturnTypeCoversZeroWidthExpansion(t *testing.T) {
	varchar := func(width int32) types.Type { return types.New(types.T_varchar, width, 0) }
	varbinary := func(width int32) types.Type { return types.New(types.T_varbinary, width, 0) }

	text := regexpReplaceReturnType([]types.Type{varchar(2), varchar(1), varchar(3)})
	require.Equal(t, types.T_varchar, text.Oid)
	require.Equal(t, int32(11), text.Width, "S+(S+1)*R")
	for _, args := range [][]types.Type{
		{varchar(2), varchar(1), varchar(3)},
		{varchar(2), varchar(1), varchar(3), types.T_int64.ToType()},
		{varchar(2), varchar(1), varchar(3), types.T_int64.ToType(), types.T_int64.ToType()},
	} {
		resolved, err := GetFunctionByName(context.Background(), "regexp_replace", args)
		require.NoError(t, err)
		require.Equal(t, int32(11), resolved.GetReturnType().Width,
			"every REGEXP_REPLACE arity must use the expansion-aware callback")
	}

	textWithBlobReplacement := regexpReplaceReturnType([]types.Type{
		varchar(2), varchar(1), varbinary(3),
	})
	require.Equal(t, types.StringDomainText, types.StaticStringDomain(textWithBlobReplacement))
	require.Equal(t, int32(11), textWithBlobReplacement.Width)

	binary := regexpReplaceReturnType([]types.Type{
		varbinary(2), varbinary(1), varchar(3),
	})
	require.Equal(t, types.T_varbinary, binary.Oid)
	// A VARCHAR(3) replacement can occupy twelve UTF-8 bytes in byte mode.
	require.Equal(t, int32(38), binary.Width)

	unbounded := regexpReplaceReturnType([]types.Type{
		types.T_text.ToType(), varchar(1), varchar(3),
	})
	require.Equal(t, types.T_text, unbounded.Oid)
}

func TestStringConsumersPreserveTextAndBoundedWidths(t *testing.T) {
	proc := testutil.NewProcess(t)
	binaryReverse, err := GetFunctionByName(proc.Ctx, "reverse", []types.Type{types.New(types.T_varbinary, 1, 0)})
	require.NoError(t, err)
	casts, needCast := binaryReverse.ShouldDoImplicitTypeCast()
	require.False(t, needCast)
	require.Empty(t, casts)
	require.Equal(t, types.T_varbinary, binaryReverse.GetReturnType().Oid)
	require.Equal(t, int32(1), binaryReverse.GetReturnType().Width)

	blobReverse, err := GetFunctionByName(proc.Ctx, "reverse", []types.Type{types.T_blob.ToType()})
	require.NoError(t, err)
	casts, needCast = blobReverse.ShouldDoImplicitTypeCast()
	require.False(t, needCast)
	require.Empty(t, casts)
	require.Equal(t, types.T_blob, blobReverse.GetReturnType().Oid)

	for _, name := range []string{"lower", "upper"} {
		boundedText := types.T_text.ToType()
		boundedText.Width = 255
		resolved, err := GetFunctionByName(proc.Ctx, name, []types.Type{boundedText})
		require.NoError(t, err)
		require.Equal(t, types.T_varchar, resolved.GetReturnType().Oid)
		require.Equal(t, int32(255), resolved.GetReturnType().Width)

		for _, width := range []int32{32, types.MaxMediumTextLen, types.MaxLongTextLen} {
			wideText := types.T_text.ToType()
			wideText.Width = width
			resolved, err = GetFunctionByName(proc.Ctx, name, []types.Type{wideText})
			require.NoError(t, err)
			require.Equal(t, types.T_text, resolved.GetReturnType().Oid)
			require.Zero(t, resolved.GetReturnType().Width)
		}

		resolved, err = GetFunctionByName(proc.Ctx, name, []types.Type{types.New(types.T_char, 4, 0)})
		require.NoError(t, err)
		require.Equal(t, types.T_varchar, resolved.GetReturnType().Oid)
		require.Equal(t, int32(4), resolved.GetReturnType().Width)
	}

	for _, test := range []struct {
		name      string
		inputs    []types.Type
		wantOID   types.T
		wantWidth int32
	}{
		{name: "reverse", inputs: []types.Type{types.T_text.ToType()}, wantOID: types.T_text},
		{name: "left", inputs: []types.Type{types.T_text.ToType(), types.T_int64.ToType()}, wantOID: types.T_text},
		{name: "ltrim", inputs: []types.Type{types.New(types.T_varchar, 40, 0)}, wantOID: types.T_varchar, wantWidth: 40},
		{name: "rtrim", inputs: []types.Type{types.New(types.T_varchar, 40, 0)}, wantOID: types.T_varchar, wantWidth: 40},
		{name: "lower", inputs: []types.Type{types.T_text.ToType()}, wantOID: types.T_text},
		{name: "upper", inputs: []types.Type{types.T_text.ToType()}, wantOID: types.T_text},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := GetFunctionByName(proc.Ctx, test.name, test.inputs)
			require.NoError(t, err)
			result := resolved.GetReturnType()
			require.Equal(t, test.wantOID, result.Oid)
			if test.wantWidth != 0 {
				require.Equal(t, test.wantWidth, result.Width)
			}
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

func TestConcatPreservesFormattedScalarBoundsAfterCast(t *testing.T) {
	proc := testutil.NewProcess(t)
	resolved, err := GetFunctionByName(proc.Ctx, "concat", []types.Type{types.T_int64.ToType(), types.T_int64.ToType()})
	require.NoError(t, err)
	casts, needCast := resolved.ShouldDoImplicitTypeCast()
	require.True(t, needCast)
	require.Equal(t, int32(20), casts[0].Width)
	require.Equal(t, int32(20), casts[1].Width)
	require.Equal(t, types.T_varchar, resolved.GetReturnType().Oid)
	require.Equal(t, int32(40), resolved.GetReturnType().Width)

	resolved, err = GetFunctionByName(proc.Ctx, "quote", []types.Type{types.T_int64.ToType()})
	require.NoError(t, err)
	casts, needCast = resolved.ShouldDoImplicitTypeCast()
	require.True(t, needCast)
	require.Equal(t, int32(20), casts[0].Width)
	require.Equal(t, types.T_varchar, resolved.GetReturnType().Oid)
	require.Equal(t, int32(42), resolved.GetReturnType().Width)
}

func TestStringDomainFunctionsPreserveBinaryInputsBeforeExecution(t *testing.T) {
	proc := testutil.NewProcess(t)
	binaryCharset := types.NewWithCharset(types.T_varchar, 6, 0, types.CharsetBinary)
	for _, test := range []struct {
		name    string
		fn      string
		inputs  []types.Type
		wantOID types.T
	}{
		{name: "convert", inputs: []types.Type{types.T_blob.ToType(), binaryCharset}, wantOID: types.T_blob},
		{name: "repeat", inputs: []types.Type{types.T_blob.ToType(), types.T_int64.ToType()}, wantOID: types.T_blob},
		{name: "replace", inputs: []types.Type{
			types.New(types.T_varbinary, 8, 0), types.New(types.T_varchar, 1, 0), types.New(types.T_varchar, 2, 0),
		}, wantOID: types.T_varbinary},
		{name: "quote varbinary", fn: "quote", inputs: []types.Type{types.New(types.T_varbinary, 1, 0)}, wantOID: types.T_varbinary},
		{name: "quote blob", fn: "quote", inputs: []types.Type{types.T_blob.ToType()}, wantOID: types.T_blob},
	} {
		t.Run(test.name, func(t *testing.T) {
			fn := test.fn
			if fn == "" {
				fn = test.name
			}
			resolved, err := GetFunctionByName(proc.Ctx, fn, test.inputs)
			require.NoError(t, err)
			require.Equal(t, test.wantOID, resolved.GetReturnType().Oid)
			casts, needCast := resolved.ShouldDoImplicitTypeCast()
			require.False(t, needCast)
			require.Empty(t, casts)
		})
	}
}

func TestQuotePreservesInvalidUTF8Bytes(t *testing.T) {
	input := string([]byte{0xff, '\'', '\\', 0})
	require.Equal(t, []byte{'\'', 0xff, '\\', '\'', '\\', '\\', '\\', '0', '\''}, []byte(QuoteString(input)))
}

func TestExpandingReturnTypeBounds(t *testing.T) {
	one := types.New(types.T_varchar, 1, 0)
	makeSet := makeSetReturnType([]types.Type{types.T_uint64.ToType(), one, one, one})
	require.Equal(t, types.T_varchar, makeSet.Oid)
	require.Equal(t, int32(5), makeSet.Width)

	exportSet := exportSetReturnType([]types.Type{types.T_uint64.ToType(), one, one, one})
	require.Equal(t, types.T_varchar, exportSet.Oid)
	require.Equal(t, int32(127), exportSet.Width)

	quoted := quoteReturnType([]types.Type{types.New(types.T_varchar, 0, 0)})
	require.Equal(t, types.T_varchar, quoted.Oid)
	require.Equal(t, int32(4), quoted.Width)

	quotedBinary := quoteReturnType([]types.Type{types.New(types.T_varbinary, 0, 0)})
	require.Equal(t, types.T_varbinary, quotedBinary.Oid)
	require.Equal(t, int32(4), quotedBinary.Width)
}

func TestPadResultByteLengthEnforcesEncodedBudget(t *testing.T) {
	length, rejected := padResultByteLength("😀", 2, "😀", 8)
	require.False(t, rejected)
	require.Equal(t, 8, length)
	_, rejected = padResultByteLength("😀", int64(types.MaxBlobLen), "😀", int64(types.MaxBlobLen))
	require.True(t, rejected)

	length, rejected = padResultByteLength("a", 2, "", int64(types.MaxVarcharLen))
	require.False(t, rejected)
	require.Zero(t, length)

	length, rejected = padResultByteLength("abc", 2, "", int64(types.MaxVarcharLen))
	require.False(t, rejected)
	require.Equal(t, 2, length)

	_, rejected = padResultByteLength("a", int64(types.MaxVarcharLen)+1, "", int64(types.MaxVarcharLen))
	require.True(t, rejected)

	const utf8mb4Boundary = int64(16_777_216)
	length, rejected = padResultByteLength("a", utf8mb4Boundary, "a", int64(types.MaxBlobLen))
	require.False(t, rejected)
	require.Equal(t, int(utf8mb4Boundary), length)
	_, rejected = padResultByteLength("a", utf8mb4Boundary+1, "a", int64(types.MaxBlobLen))
	require.True(t, rejected)
	_, rejected = padResultByteLength("a", utf8mb4Boundary+1, "", int64(types.MaxBlobLen))
	require.True(t, rejected)
	length, rejected = padResultByteLength(strings.Repeat("a", 100), utf8mb4Boundary+1, "", int64(types.MaxBlobLen))
	require.False(t, rejected)
	require.Zero(t, length)

	longSource := strings.Repeat("a", 17_000_000)
	length, rejected = padResultByteLength(longSource, utf8mb4Boundary+1, "", int64(types.MaxBlobLen))
	require.False(t, rejected)
	require.Equal(t, int(utf8mb4Boundary+1), length)

	const utf8mb3Boundary = int64(22_369_622)
	length, rejected = padResultByteLengthWithCharacterWidth(
		"a", utf8mb3Boundary, "a", int64(types.MaxBlobLen), 3)
	require.False(t, rejected)
	require.Equal(t, int(utf8mb3Boundary), length)
	_, rejected = padResultByteLengthWithCharacterWidth(
		"a", utf8mb3Boundary+1, "a", int64(types.MaxBlobLen), 3)
	require.True(t, rejected)

	legacy := types.NewWithCharset(types.T_text, 0, 0, types.CharsetLegacy)
	require.Equal(t, 3, maxPadTextCharacterWidth(&legacy))
}

func TestExpandingTextResultsUseTextCapacity(t *testing.T) {
	text := expandingStringReturnType([]types.Type{types.New(types.T_varchar, 1, 0)}, 0)
	require.Equal(t, types.T_text, text.Oid)
	require.Equal(t, types.CharsetUTF8, text.Charset)

	binary := expandingStringReturnType([]types.Type{types.New(types.T_varbinary, 1, 0)}, 0)
	require.Equal(t, types.T_blob, binary.Oid)
	require.Equal(t, types.CharsetBinary, binary.Charset)
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
