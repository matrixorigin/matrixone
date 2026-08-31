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

	replaced := replacementStringReturnType([]types.Type{varchar(2), varchar(1), varchar(2)}, false)
	require.Equal(t, types.T_varchar, replaced.Oid)
	require.Equal(t, int32(4), replaced.Width)

	zeroWidthRegexp := replacementStringReturnType([]types.Type{varchar(1), varchar(0), varchar(1)}, true)
	require.Equal(t, types.T_varchar, zeroWidthRegexp.Oid)
	require.Equal(t, int32(3), zeroWidthRegexp.Width)

	inserted := insertStringReturnType([]types.Type{varbinary(1), types.T_int64.ToType(), types.T_int64.ToType(), varbinary(1)})
	require.Equal(t, types.T_varbinary, inserted.Oid)
	require.Equal(t, int32(4), inserted.Width)

	binaryReplacement := replacementStringReturnType([]types.Type{varchar(2), varchar(1), varbinary(1)}, false)
	require.Equal(t, types.T_varbinary, binaryReplacement.Oid)
	binaryInsertion := insertStringReturnType([]types.Type{varchar(1), types.T_int64.ToType(), types.T_int64.ToType(), varbinary(1)})
	require.Equal(t, types.T_varbinary, binaryInsertion.Oid)
}

func TestStringConsumersPreserveTextAndBoundedWidths(t *testing.T) {
	proc := testutil.NewProcess(t)
	binaryReverse, err := GetFunctionByName(proc.Ctx, "reverse", []types.Type{types.New(types.T_varbinary, 1, 0)})
	require.NoError(t, err)
	casts, needCast := binaryReverse.ShouldDoImplicitTypeCast()
	require.True(t, needCast)
	require.Equal(t, types.T_blob, casts[0].Oid)
	require.Equal(t, types.T_blob, binaryReverse.GetReturnType().Oid)

	blobReverse, err := GetFunctionByName(proc.Ctx, "reverse", []types.Type{types.T_blob.ToType()})
	require.NoError(t, err)
	casts, needCast = blobReverse.ShouldDoImplicitTypeCast()
	require.False(t, needCast)
	require.Empty(t, casts)
	require.Equal(t, types.T_blob, blobReverse.GetReturnType().Oid)

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
		{name: "regexp_replace", inputs: []types.Type{types.New(types.T_varchar, 2, 0), types.New(types.T_varchar, 1, 0), types.New(types.T_varchar, 2, 0)}, wantOID: types.T_varchar, wantWidth: 8},
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
	require.Equal(t, []byte{'\'', 0xff, '\'', '\'', '\\', '\\', '\\', '0', '\''}, []byte(QuoteString(input)))
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
	require.Equal(t, int32(2), quoted.Width)
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
	dst := make([]byte, length)
	require.NotPanics(t, func() { writePadResult(dst, "a", 2, "", true) })
	require.Empty(t, dst)
	require.NotPanics(t, func() { writePadResult(dst, "a", 2, "", false) })
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
