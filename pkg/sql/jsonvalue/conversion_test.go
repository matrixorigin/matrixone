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

package jsonvalue

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"math/big"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func parseConversionValue(t *testing.T, text string) bytejson.ByteJson {
	t.Helper()
	value, err := bytejson.ParseFromString(text)
	require.NoError(t, err)
	return value
}

func referenceDecimalCoefficient(input string, scale int32) (*big.Int, bool) {
	text := strings.ReplaceAll(strings.TrimSpace(input), " ", "")
	negative := false
	if strings.HasPrefix(text, "+") || strings.HasPrefix(text, "-") {
		negative = text[0] == '-'
		text = text[1:]
	}
	exponent := int64(0)
	if index := strings.IndexAny(text, "eE"); index >= 0 {
		parsed, err := strconv.ParseInt(text[index+1:], 10, 32)
		if err != nil {
			return nil, false
		}
		exponent = parsed
		text = text[:index]
	}
	point := len(text)
	if index := strings.IndexByte(text, '.'); index >= 0 {
		point = index
		text = text[:index] + text[index+1:]
	}
	numerator := new(big.Int)
	if text == "" {
		return nil, false
	}
	if _, ok := numerator.SetString(text, 10); !ok {
		return nil, false
	}
	shift := int64(point) + exponent + int64(scale) - int64(len(text))
	truncated := false
	if shift >= 0 {
		factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(shift), nil)
		numerator.Mul(numerator, factor)
	} else {
		divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(-shift), nil)
		quotient, remainder := new(big.Int), new(big.Int)
		quotient.QuoRem(numerator, divisor, remainder)
		if remainder.Sign() != 0 {
			truncated = true
			doubled := new(big.Int).Lsh(remainder, 1)
			if doubled.Cmp(divisor) >= 0 {
				quotient.Add(quotient, big.NewInt(1))
			}
		}
		numerator = quotient
	}
	if negative && numerator.Sign() != 0 {
		numerator.Neg(numerator)
	}
	return numerator, truncated
}

func decimalReferenceForTarget(input string, target types.Type) (*big.Int, bool, bool) {
	coefficient, truncated := referenceDecimalCoefficient(input, target.Scale)
	if coefficient == nil {
		return nil, false, true
	}
	width := target.Width
	if limit := decimalWidthLimit(target.Oid); width > limit {
		width = limit
	}
	limit := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(width)), nil)
	if new(big.Int).Abs(new(big.Int).Set(coefficient)).Cmp(limit) >= 0 {
		return nil, false, true
	}
	return coefficient, truncated, false
}

func decimalValueCoefficient(value any) *big.Int {
	var formatted string
	switch value := value.(type) {
	case types.Decimal64:
		formatted = value.Format(0)
	case types.Decimal128:
		formatted = value.Format(0)
	case types.Decimal256:
		formatted = value.Format(0)
	default:
		return nil
	}
	coefficient, ok := new(big.Int).SetString(formatted, 10)
	if !ok {
		return nil
	}
	return coefficient
}

func conversionIterator(t *testing.T, document, pathText string) *bytejson.PathIterator {
	t.Helper()
	root := parseConversionValue(t, document)
	path, err := bytejson.ParseJsonPath(pathText)
	require.NoError(t, err)
	return bytejson.NewPathIterator(root, &path)
}

func TestConvertScalarReportsIndependentStatusClasses(t *testing.T) {
	cases := []struct {
		name       string
		document   string
		target     types.Type
		wantStatus ConversionStatus
		wantValue  any
	}{
		{
			name:       "integer success",
			document:   `42`,
			target:     types.T_int64.ToType(),
			wantStatus: StatusSuccess,
			wantValue:  int64(42),
		},
		{
			name:       "boolean coerces to integer",
			document:   `true`,
			target:     types.T_int8.ToType(),
			wantStatus: StatusSuccess,
			wantValue:  int8(1),
		},
		{
			name:       "unsigned integer success",
			document:   `18446744073709551615`,
			target:     types.T_uint64.ToType(),
			wantStatus: StatusSuccess,
			wantValue:  uint64(18446744073709551615),
		},
		{
			name:       "unsigned integer range failure",
			document:   `-1`,
			target:     types.T_uint64.ToType(),
			wantStatus: StatusRangeError,
		},
		{
			name:       "float success",
			document:   `1.25`,
			target:     types.T_float64.ToType(),
			wantStatus: StatusSuccess,
			wantValue:  float64(1.25),
		},
		{
			name:       "json null",
			document:   `null`,
			target:     types.T_int64.ToType(),
			wantStatus: StatusJSONNull,
		},
		{
			name:       "composite target scalar",
			document:   `[1]`,
			target:     types.T_int64.ToType(),
			wantStatus: StatusComposite,
		},
		{
			name:       "text conversion failure",
			document:   `"not-a-number"`,
			target:     types.T_int64.ToType(),
			wantStatus: StatusConversionError,
		},
		{
			name:       "integer range failure",
			document:   `9223372036854775808`,
			target:     types.T_int64.ToType(),
			wantStatus: StatusRangeError,
		},
		{
			name:       "narrow integer range failure",
			document:   `129`,
			target:     types.T_int8.ToType(),
			wantStatus: StatusRangeError,
		},
		{
			name:       "date success",
			document:   `"2024-02-03"`,
			target:     types.T_date.ToType(),
			wantStatus: StatusSuccess,
		},
		{
			name:       "decimal success",
			document:   `"12.34"`,
			target:     types.New(types.T_decimal64, 10, 2),
			wantStatus: StatusSuccess,
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			result := ConvertScalar(parseConversionValue(t, test.document), test.target)
			require.Equal(t, test.wantStatus, result.Status)
			if test.wantValue != nil {
				require.Equal(t, test.wantValue, result.Value)
			}
			if test.wantStatus != StatusSuccess && test.wantStatus != StatusJSONNull {
				require.Error(t, result.Err)
			}
		})
	}
}

func TestConvertScalarTruncationAndTimestampLocation(t *testing.T) {
	short := ConvertScalar(
		parseConversionValue(t, `"四文字"`),
		types.New(types.T_varchar, 2, 0),
	)
	require.Equal(t, StatusTruncated, short.Status)
	require.Equal(t, []byte("四文"), short.Value)
	require.NotNil(t, short.Warning)
	require.Equal(t, uint16(1265), short.Warning.Code)

	dateTime := parseConversionValue(t, `"2024-02-03 04:05:06"`)
	utc := ConvertScalarWithLocation(dateTime, types.T_timestamp.ToType(), time.UTC)
	plusEight := ConvertScalarWithLocation(dateTime, types.T_timestamp.ToType(), time.FixedZone("UTC+8", 8*60*60))
	require.Equal(t, StatusSuccess, utc.Status)
	require.Equal(t, StatusSuccess, plusEight.Status)
	require.NotEqual(t, utc.Value, plusEight.Value, "timestamp conversion receives execution timezone")

	invalidDate := ConvertScalar(parseConversionValue(t, `"2024-99-99"`), types.T_date.ToType())
	require.Equal(t, StatusConversionError, invalidDate.Status)
	require.Error(t, invalidDate.Err)
}

func TestConvertScalarDecimalTruncationPreservesValueAndWarning(t *testing.T) {
	cases := []struct {
		name   string
		target types.Type
		want   any
	}{
		{name: "decimal64", target: types.New(types.T_decimal64, 10, 2), want: types.Decimal64(1235)},
		{name: "decimal128", target: types.New(types.T_decimal128, 20, 2), want: types.Decimal128{B0_63: 1235}},
		{name: "decimal256", target: types.New(types.T_decimal256, 40, 2), want: types.Decimal256{B0_63: 1235}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			result := ConvertScalar(parseConversionValue(t, `"12.345"`), test.target)
			require.Equal(t, StatusTruncated, result.Status)
			require.Equal(t, test.want, result.Value)
			require.NotNil(t, result.Warning)
			require.Equal(t, moerr.WARN_DATA_TRUNCATED, result.Warning.Code)

			exact := ConvertScalar(parseConversionValue(t, `"12.340"`), test.target)
			require.Equal(t, StatusSuccess, exact.Status)
			require.Nil(t, exact.Warning)
		})
	}
}

func TestConvertScalarDecimalMatchesIndependentBoundedReference(t *testing.T) {
	inputs := []string{
		"0", "1", "-1", "000001.2300", "12.340", "12.345", "-12.345",
		"0.0049", "0.005", "-0.005", "9.994", "9.995", "-9.995",
		"99.95", "999.5", "1e2", "1e-2", "1.234e+2", "-1.234e-2",
		"1.2349999999999999999", "-1.2349999999999999999",
		"12.3 45", "+12.345", "000000000000000000000000123.450000",
		"12345678901234567890123456789012345678901234567890123456789012345.5",
	}
	targets := []types.Type{
		types.New(types.T_decimal64, 3, 0),
		types.New(types.T_decimal64, 10, 2),
		types.New(types.T_decimal128, 20, 2),
		types.New(types.T_decimal256, 40, 2),
		types.New(types.T_decimal256, 76, 10),
	}
	for _, target := range targets {
		for _, input := range inputs {
			want, truncated, outOfRange := decimalReferenceForTarget(input, target)
			result := ConvertScalar(parseConversionValue(t, strconv.Quote(input)), target)
			if outOfRange {
				require.Equal(t, StatusRangeError, result.Status, "%s %q", target.DescString(), input)
				require.Error(t, result.Err)
				continue
			}
			if truncated {
				require.Equal(t, StatusTruncated, result.Status, "%s %q", target.DescString(), input)
				require.NotNil(t, result.Warning)
			} else {
				require.Equal(t, StatusSuccess, result.Status, "%s %q", target.DescString(), input)
				require.Nil(t, result.Warning)
			}
			require.Equal(t, want.String(), decimalValueCoefficient(result.Value).String(), "%s %q", target.DescString(), input)
		}
	}
}

func TestConvertScalarDecimalBoundaryMatrix(t *testing.T) {
	type decimalCase struct {
		name   string
		target types.Type
		want   any
	}
	cases := []decimalCase{
		{name: "decimal64", target: types.New(types.T_decimal64, 10, 2), want: types.Decimal64(1235)},
		{name: "decimal128", target: types.New(types.T_decimal128, 20, 2), want: types.Decimal128FromInt64(1235)},
		{name: "decimal256", target: types.New(types.T_decimal256, 40, 2), want: types.Decimal256FromInt64(1235)},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			for _, input := range []string{`"12.3 45"`, `"+12.345"`} {
				result := ConvertScalar(parseConversionValue(t, input), test.target)
				require.Equal(t, StatusTruncated, result.Status)
				require.Equal(t, test.want, result.Value)
				require.NotNil(t, result.Warning)
			}

			negative := ConvertScalar(parseConversionValue(t, `" -12.345"`), test.target)
			require.Equal(t, StatusTruncated, negative.Status)
			require.NotNil(t, negative.Warning)
			switch test.target.Oid {
			case types.T_decimal64:
				require.Equal(t, types.Decimal64(1235).Minus(), negative.Value)
			case types.T_decimal128:
				require.Equal(t, types.Decimal128FromInt64(-1235), negative.Value)
			case types.T_decimal256:
				require.Equal(t, types.Decimal256FromInt64(-1235), negative.Value)
			}

			doubleRound := ConvertScalar(parseConversionValue(t, `"1.2349999999999999999"`), test.target)
			require.Equal(t, StatusTruncated, doubleRound.Status)
			switch test.target.Oid {
			case types.T_decimal64:
				require.Equal(t, types.Decimal64(123), doubleRound.Value)
			case types.T_decimal128:
				require.Equal(t, types.Decimal128FromInt64(123), doubleRound.Value)
			case types.T_decimal256:
				require.Equal(t, types.Decimal256FromInt64(123), doubleRound.Value)
			}

			exactExponent := ConvertScalar(parseConversionValue(t, `"1.234e+2"`), test.target)
			require.Equal(t, StatusSuccess, exactExponent.Status)
			require.Nil(t, exactExponent.Warning)

			exactHex := ConvertScalar(parseConversionValue(t, `"0x1"`), test.target)
			require.Equal(t, StatusSuccess, exactHex.Status)
			require.Nil(t, exactHex.Warning)
			switch test.target.Oid {
			case types.T_decimal64:
				require.Equal(t, types.Decimal64(100), exactHex.Value)
			case types.T_decimal128:
				require.Equal(t, types.Decimal128FromInt64(100), exactHex.Value)
			case types.T_decimal256:
				require.Equal(t, types.Decimal256FromInt64(100), exactHex.Value)
			}

			lossyFallback := ConvertScalar(parseConversionValue(t, `"1e2-3"`), test.target)
			require.Equal(t, StatusTruncated, lossyFallback.Status)
			require.NotNil(t, lossyFallback.Warning)
		})
	}

	for _, oid := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256} {
		t.Run(oid.String()+" range endpoints", func(t *testing.T) {
			target := types.New(oid, 3, 0)
			for _, input := range []string{`"-1000"`, `"1000"`} {
				result := ConvertScalar(parseConversionValue(t, input), target)
				require.Equal(t, StatusRangeError, result.Status)
				require.Error(t, result.Err)
			}
			for _, input := range []string{`"-0x3e8"`, `"0x3e8"`, `"-0xFFFFFFFFFFFFFFFF"`, `"0xFFFFFFFFFFFFFFFF"`} {
				result := ConvertScalar(parseConversionValue(t, input), target)
				require.Equal(t, StatusRangeError, result.Status)
				require.Error(t, result.Err)
			}
			for _, input := range []string{`"-0x3e7"`, `"0x3e7"`} {
				result := ConvertScalar(parseConversionValue(t, input), target)
				require.Equal(t, StatusSuccess, result.Status)
				require.Nil(t, result.Warning)
				negative := strings.HasPrefix(input, `"-`)
				switch oid {
				case types.T_decimal64:
					want := types.Decimal64(999)
					if negative {
						want = want.Minus()
					}
					require.Equal(t, want, result.Value)
				case types.T_decimal128:
					want := types.Decimal128FromInt64(999)
					if negative {
						want = types.Decimal128FromInt64(-999)
					}
					require.Equal(t, want, result.Value)
				case types.T_decimal256:
					want := types.Decimal256FromInt64(999)
					if negative {
						want = types.Decimal256FromInt64(-999)
					}
					require.Equal(t, want, result.Value)
				}
			}
		})
	}
}

func TestConvertScalarDecimalResultAppendsWithWarningAndRejectsRange(t *testing.T) {
	cases := []struct {
		name   string
		target types.Type
		want   any
		read   func(*vector.Vector) any
	}{
		{
			name:   "decimal64",
			target: types.New(types.T_decimal64, 10, 2),
			want:   types.Decimal64(123),
			read: func(vec *vector.Vector) any {
				return vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, 0)
			},
		},
		{
			name:   "decimal128",
			target: types.New(types.T_decimal128, 20, 2),
			want:   types.Decimal128FromInt64(123),
			read: func(vec *vector.Vector) any {
				return vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, 0)
			},
		},
		{
			name:   "decimal256",
			target: types.New(types.T_decimal256, 40, 2),
			want:   types.Decimal256FromInt64(123),
			read: func(vec *vector.Vector) any {
				return vector.GetFixedAtNoTypeCheck[types.Decimal256](vec, 0)
			},
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			vec := vector.NewVec(test.target)
			defer vec.Free(mp)

			result := ConvertScalar(parseConversionValue(t, `"1.2349999999999999999"`), test.target)
			require.Equal(t, StatusTruncated, result.Status)
			require.NotNil(t, result.Warning)
			require.Equal(t, moerr.WARN_DATA_TRUNCATED, result.Warning.Code)
			require.NoError(t, AppendResult(vec, result, mp))
			require.Equal(t, 1, vec.Length())
			require.Equal(t, test.want, test.read(vec))

			rangeResult := ConvertScalar(parseConversionValue(t, `"-1000"`), types.New(test.target.Oid, 3, 0))
			require.Equal(t, StatusRangeError, rangeResult.Status)
			require.Error(t, rangeResult.Err)
			require.ErrorIs(t, AppendResult(vec, rangeResult, mp), rangeResult.Err)
			require.Equal(t, 1, vec.Length())
		})
	}
}

func TestConvertScalarMalformedByteJsonFailsClosed(t *testing.T) {
	malformed := bytejson.ByteJson{Type: bytejson.TpCodeInt64, Data: []byte{1}}
	result := ConvertScalar(malformed, types.T_int64.ToType())
	require.Equal(t, StatusStatementError, result.Status)
	require.Error(t, result.Err)
	result = ConvertScalar(malformed, types.T_json.ToType())
	require.Equal(t, StatusStatementError, result.Status)
	require.Error(t, result.Err)

	unknown := bytejson.ByteJson{Type: bytejson.TpCode(0xff), Data: []byte{1}}
	result = ConvertScalar(unknown, types.T_json.ToType())
	require.Equal(t, StatusStatementError, result.Status)
	require.Error(t, result.Err)
	result = ConvertScalar(unknown, types.T_varchar.ToType())
	require.Equal(t, StatusStatementError, result.Status)
	require.Error(t, result.Err)
	malformedString := bytejson.ByteJson{Type: bytejson.TpCodeString, Data: []byte{0x80}}
	result = ConvertScalar(malformedString, types.T_varchar.ToType())
	require.Equal(t, StatusStatementError, result.Status)
	require.Error(t, result.Err)
	for _, floating := range []float64{math.NaN(), math.Inf(1)} {
		data := make([]byte, 8)
		binary.LittleEndian.PutUint64(data, math.Float64bits(floating))
		result = ConvertScalar(bytejson.ByteJson{Type: bytejson.TpCodeFloat64, Data: data}, types.T_varchar.ToType())
		require.Equal(t, StatusStatementError, result.Status)
		require.Error(t, result.Err)
	}

	malformedComposite := bytejson.ByteJson{Type: bytejson.TpCodeArray, Data: []byte{1}}
	result = ConvertScalar(malformedComposite, types.T_json.ToType())
	require.Equal(t, StatusStatementError, result.Status)
	require.Error(t, result.Err)
}

func TestConvertScalarDecimalAnalysisIsBounded(t *testing.T) {
	target := types.New(types.T_decimal64, 10, 2)
	_, _, known, _ := canonicalDecimalInput("1e2-3", target)
	require.False(t, known, "an exponent sign is valid only before exponent digits")
	_, _, known, _ = canonicalDecimalInput("1E2", target)
	require.False(t, known, "legacy decimal parser accepts only lowercase e")
	for _, input := range []string{"1e", "1e+", "1e-"} {
		_, _, known, _ = canonicalDecimalInput(input, target)
		require.True(t, known, "legacy zero exponent compatibility for %q", input)
	}

	tooManyDigits := parseConversionValue(t, `"`+strings.Repeat("9", 4096)+`"`)
	result := ConvertScalar(tooManyDigits, target)
	require.Equal(t, StatusRangeError, result.Status)
	require.Error(t, result.Err)

	tinyExponent := parseConversionValue(t, `"1e-`+strings.Repeat("9", 4096)+`"`)
	result = ConvertScalar(tinyExponent, target)
	require.Equal(t, StatusTruncated, result.Status)
	require.Equal(t, types.Decimal64(0), result.Value)
	require.NotNil(t, result.Warning)
}

func TestConvertPathMatchesMalformedRootJSONFailsClosed(t *testing.T) {
	path, err := bytejson.ParseJsonPath(`$`)
	require.NoError(t, err)
	for _, value := range []bytejson.ByteJson{
		{Type: bytejson.TpCodeInt64, Data: []byte{1}},
		{Type: bytejson.TpCode(0xff), Data: []byte{1}},
	} {
		iterator := bytejson.NewPathIterator(value, &path)
		result := ConvertPathMatches(iterator, types.T_json.ToType())
		iterator.Close()
		require.Equal(t, StatusStatementError, result.Status)
		require.Error(t, result.Err)
	}
}

func aliasedInt64ArrayForConversion() bytejson.ByteJson {
	const headerSize = 8
	const valueEntrySize = 5
	const numberSize = 8
	data := make([]byte, headerSize+2*valueEntrySize+numberSize)
	binary.LittleEndian.PutUint32(data[:4], 2)
	binary.LittleEndian.PutUint32(data[4:], uint32(len(data)))
	for i := 0; i < 2; i++ {
		entryOffset := headerSize + i*valueEntrySize
		data[entryOffset] = byte(bytejson.TpCodeInt64)
		binary.LittleEndian.PutUint32(data[entryOffset+1:], uint32(headerSize+2*valueEntrySize))
	}
	binary.LittleEndian.PutUint64(data[headerSize+2*valueEntrySize:], 1)
	return bytejson.ByteJson{Type: bytejson.TpCodeArray, Data: data}
}

func TestConvertPathMatchesRejectsAliasedSerializedWorkSingleAndMulti(t *testing.T) {
	aliased := aliasedInt64ArrayForConversion()
	scalar := ConvertScalar(aliased, types.T_json.ToType())
	require.Equal(t, StatusStatementError, scalar.Status)
	require.Error(t, scalar.Err)

	singlePath, err := bytejson.ParseJsonPath(`$`)
	require.NoError(t, err)
	single := bytejson.NewPathIterator(aliased, &singlePath)
	singleResult := ConvertPathMatchesWithLimit(single, types.T_json.ToType(), 1024)
	single.Close()
	require.Equal(t, StatusStatementError, singleResult.Status)
	require.Error(t, singleResult.Err)

	outer, err := bytejson.CreateByteJSON([]any{aliased, aliased})
	require.NoError(t, err)
	multiPath, err := bytejson.ParseJsonPath(`$[*]`)
	require.NoError(t, err)
	multi := bytejson.NewPathIterator(outer, &multiPath)
	multiResult := ConvertPathMatchesWithLimit(multi, types.T_json.ToType(), 1024)
	multi.Close()
	require.Equal(t, StatusStatementError, multiResult.Status)
	require.Error(t, multiResult.Err)

	nested := conversionIterator(t, `{"nested":[1,2]}`, `$`)
	defer nested.Close()
	nestedResult := ConvertPathMatchesWithLimit(nested, types.T_json.ToType(), 1024)
	require.Equal(t, StatusSuccess, nestedResult.Status)
	encoded, ok := nestedResult.Value.([]byte)
	require.True(t, ok)
	require.Equal(t, `{"nested": [1, 2]}`, types.DecodeJson(encoded).String())
}

func TestConvertPathMatchesDistinguishesMissingNullAndMultipleValues(t *testing.T) {
	missing := conversionIterator(t, `{"present":null}`, `$.missing`)
	defer missing.Close()
	result := ConvertPathMatches(missing, types.T_int64.ToType())
	require.Equal(t, StatusMissing, result.Status)
	mp := mpool.MustNewZero()
	missingVector := vector.NewVec(types.T_int64.ToType())
	defer missingVector.Free(mp)
	require.ErrorIs(t, AppendResult(missingVector, result, mp), ErrMissingJSONTableValue)

	null := conversionIterator(t, `{"present":null}`, `$.present`)
	defer null.Close()
	result = ConvertPathMatches(null, types.T_int64.ToType())
	require.Equal(t, StatusJSONNull, result.Status)
	require.NoError(t, result.Err)

	multiple := conversionIterator(t, `[1,2,3]`, `$[*]`)
	defer multiple.Close()
	result = ConvertPathMatches(multiple, types.T_int64.ToType())
	require.Equal(t, StatusConversionError, result.Status)
	require.Error(t, result.Err)
	// The non-JSON ON ERROR decision consumes only the first two matches. It
	// must not eagerly collect or silently discard the third value.
	remaining, matched, err := multiple.Next()
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "3", remaining.String())
}

func TestConvertPathMatchesBuildsBoundedJSONCell(t *testing.T) {
	multiple := conversionIterator(t, `[1,2]`, `$[*]`)
	defer multiple.Close()
	result := ConvertPathMatches(multiple, types.T_json.ToType())
	require.Equal(t, StatusSuccess, result.Status)
	encoded, ok := result.Value.([]byte)
	require.True(t, ok)
	decoded := types.DecodeJson(encoded)
	require.Equal(t, `[1, 2]`, decoded.String())

	limited := conversionIterator(t, `[1,2]`, `$[*]`)
	defer limited.Close()
	result = ConvertPathMatchesWithLimit(limited, types.T_json.ToType(), 34)
	require.Equal(t, StatusStatementError, result.Status)
	require.ErrorIs(t, result.Err, bytejson.ErrJSONTableCellLimit)

	cancelled := conversionIterator(t, `[1,2]`, `$[*]`)
	defer cancelled.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result = ConvertPathMatchesContext(ctx, cancelled, types.T_json.ToType())
	require.Equal(t, StatusStatementError, result.Status)
	require.ErrorIs(t, result.Err, context.Canceled)
}

func TestConvertPathMatchesEnforcesJSONCellLimitForSingleMatch(t *testing.T) {
	for _, document := range []string{
		`"0123456789"`,
		`[1,2,3]`,
		`{"value":"0123456789"}`,
	} {
		t.Run(document, func(t *testing.T) {
			value := parseConversionValue(t, document)
			encoded, err := value.Marshal()
			require.NoError(t, err)
			require.Greater(t, len(encoded), 1)

			exact := conversionIterator(t, document, `$`)
			defer exact.Close()
			result := ConvertPathMatchesWithLimit(exact, types.T_json.ToType(), len(encoded))
			require.Equal(t, StatusSuccess, result.Status)
			require.Equal(t, encoded, result.Value)

			under := conversionIterator(t, document, `$`)
			defer under.Close()
			result = ConvertPathMatchesWithLimit(under, types.T_json.ToType(), len(encoded)-1)
			require.Equal(t, StatusStatementError, result.Status)
			require.ErrorIs(t, result.Err, bytejson.ErrJSONTableCellLimit)
		})
	}

	null := conversionIterator(t, `null`, `$`)
	defer null.Close()
	result := ConvertPathMatchesWithLimit(null, types.T_json.ToType(), 1)
	require.Equal(t, StatusJSONNull, result.Status)
	require.NoError(t, result.Err)
}

func TestConvertPathMatchesEnforcesDefaultJSONCellLimitForSingleMatch(t *testing.T) {
	payloadLength := types.MaxBlobLen
	data := make([]byte, binary.MaxVarintLen64+payloadLength)
	prefixLength := binary.PutUvarint(data, uint64(payloadLength))
	data = data[:prefixLength+payloadLength]
	value := bytejson.ByteJson{Type: bytejson.TpCodeBlob, Data: data}
	require.Greater(t, len(data)+1, types.MaxBlobLen)

	path, err := bytejson.ParseJsonPath(`$`)
	require.NoError(t, err)
	iterator := bytejson.NewPathIterator(value, &path)
	result := ConvertPathMatches(iterator, types.T_json.ToType())
	iterator.Close()
	require.Equal(t, StatusStatementError, result.Status)
	require.ErrorIs(t, result.Err, bytejson.ErrJSONTableCellLimit)
}

func TestConvertPathMatchesRejectsLargeOpaqueBeforeEncoding(t *testing.T) {
	for _, test := range []struct {
		name          string
		payloadLength int
	}{
		{name: "1MiB", payloadLength: 1 << 20},
		{name: "8MiB", payloadLength: 8 << 20},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Build the source before measuring conversion. This isolates the
			// rejected storage encoding from the caller-owned input allocation.
			data := make([]byte, binary.MaxVarintLen64+test.payloadLength)
			prefixLength := binary.PutUvarint(data, uint64(test.payloadLength))
			data = data[:prefixLength+test.payloadLength]
			value := bytejson.ByteJson{Type: bytejson.TpCodeOpaque, Data: data}
			path, err := bytejson.ParseJsonPath(`$`)
			require.NoError(t, err)
			iterator := bytejson.NewPathIterator(value, &path)

			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			result := ConvertPathMatchesWithLimit(iterator, types.T_json.ToType(), 14)
			runtime.ReadMemStats(&after)
			iterator.Close()

			require.Equal(t, StatusStatementError, result.Status)
			require.ErrorIs(t, result.Err, bytejson.ErrJSONTableCellLimit)
			require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(test.payloadLength/2))
			heapDelta := uint64(0)
			if after.HeapAlloc > before.HeapAlloc {
				heapDelta = after.HeapAlloc - before.HeapAlloc
			}
			t.Logf("payload=%d total_alloc_delta=%d heap_alloc_delta=%d", test.payloadLength, after.TotalAlloc-before.TotalAlloc, heapDelta)
			require.Less(t, heapDelta, uint64(test.payloadLength/2))
		})
	}
}

func TestConvertPathMatchesUsesSharedOpaqueAdmissionForSingleAndMulti(t *testing.T) {
	data := make([]byte, binary.MaxVarintLen64+1<<20)
	prefixLength := binary.PutUvarint(data, uint64(1<<20))
	data = data[:prefixLength+1<<20]
	opaque := bytejson.ByteJson{Type: bytejson.TpCodeOpaque, Data: data}

	singlePath, err := bytejson.ParseJsonPath(`$`)
	require.NoError(t, err)
	single := bytejson.NewPathIterator(opaque, &singlePath)
	singleResult := ConvertPathMatchesWithLimit(single, types.T_json.ToType(), 14)
	single.Close()
	require.Equal(t, StatusStatementError, singleResult.Status)
	require.ErrorIs(t, singleResult.Err, bytejson.ErrJSONTableCellLimit)

	array, err := bytejson.CreateByteJSON([]any{opaque})
	require.NoError(t, err)
	multiPath, err := bytejson.ParseJsonPath(`$[*]`)
	require.NoError(t, err)
	multi := bytejson.NewPathIterator(array, &multiPath)
	multiResult := ConvertPathMatchesWithLimit(multi, types.T_json.ToType(), 14)
	multi.Close()
	require.Equal(t, StatusStatementError, multiResult.Status)
	require.ErrorIs(t, multiResult.Err, bytejson.ErrJSONTableCellLimit)
}

func TestConvertUint32AppendResultRoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()
	vectorValue := vector.NewVec(types.T_uint32.ToType())
	defer vectorValue.Free(mp)

	for _, test := range []struct {
		document string
		want     uint32
	}{
		{document: `0`, want: 0},
		{document: `4294967295`, want: math.MaxUint32},
	} {
		result := ConvertScalar(parseConversionValue(t, test.document), types.T_uint32.ToType())
		require.Equal(t, StatusSuccess, result.Status)
		require.IsType(t, uint32(0), result.Value)
		require.NoError(t, AppendResult(vectorValue, result, mp))
		require.Equal(t, test.want, vector.GetFixedAtNoTypeCheck[uint32](vectorValue, vectorValue.Length()-1))
	}

	null := ConvertScalar(parseConversionValue(t, `null`), types.T_uint32.ToType())
	require.Equal(t, StatusJSONNull, null.Status)
	require.NoError(t, AppendResult(vectorValue, null, mp))
	require.True(t, vectorValue.IsNull(uint64(vectorValue.Length()-1)))

	over := ConvertScalar(parseConversionValue(t, `4294967296`), types.T_uint32.ToType())
	require.Equal(t, StatusRangeError, over.Status)
	require.Error(t, over.Err)
	require.Error(t, AppendResult(vectorValue, over, mp))
	require.Equal(t, 3, vectorValue.Length())
}

func TestAppendResultRejectsErrorsWithoutMutatingVector(t *testing.T) {
	mp := mpool.MustNewZero()
	vectorValue := vector.NewVec(types.T_int32.ToType())
	defer vectorValue.Free(mp)
	require.NoError(t, vector.AppendAny(vectorValue, int32(7), false, mp))

	bad := Result{Status: StatusConversionError, Err: errors.New("conversion failed")}
	require.ErrorIs(t, AppendResult(vectorValue, bad, mp), bad.Err)
	require.Equal(t, 1, vectorValue.Length())
	require.Equal(t, int32(7), vector.GetFixedAtNoTypeCheck[int32](vectorValue, 0))

	require.NoError(t, AppendResult(vectorValue, Result{Status: StatusSuccess, Value: int32(9)}, mp))
	require.NoError(t, AppendResult(vectorValue, Result{Status: StatusJSONNull}, mp))
	require.Equal(t, 3, vectorValue.Length())
	require.Equal(t, int32(9), vector.GetFixedAtNoTypeCheck[int32](vectorValue, 1))
	require.True(t, vectorValue.IsNull(2))

	missing := Result{Status: StatusMissing}
	require.ErrorIs(t, AppendResult(vectorValue, missing, mp), ErrMissingJSONTableValue)
	require.Equal(t, 3, vectorValue.Length())
}

func TestConversionStatusNamesAndScalarBoundaries(t *testing.T) {
	for status, want := range map[ConversionStatus]string{
		StatusMissing:         "missing",
		StatusSuccess:         "success",
		StatusJSONNull:        "json-null",
		StatusComposite:       "composite",
		StatusConversionError: "conversion-error",
		StatusRangeError:      "range-error",
		StatusTruncated:       "truncated",
		StatusStatementError:  "statement-error",
	} {
		require.Equal(t, want, status.String())
	}
	require.Equal(t, "unknown", ConversionStatus(0xff).String())

	successes := []struct {
		name     string
		document string
		target   types.Type
	}{
		{name: "bool false", document: `false`, target: types.T_bool.ToType()},
		{name: "int16", document: `32767`, target: types.T_int16.ToType()},
		{name: "int32", document: `2147483647`, target: types.T_int32.ToType()},
		{name: "uint8", document: `255`, target: types.T_uint8.ToType()},
		{name: "uint16", document: `65535`, target: types.T_uint16.ToType()},
		{name: "float32", document: `1.25`, target: types.T_float32.ToType()},
		{name: "float64", document: `1.25`, target: types.T_float64.ToType()},
		{name: "decimal128", document: `12.34`, target: types.New(types.T_decimal128, 20, 2)},
		{name: "decimal256", document: `12.34`, target: types.New(types.T_decimal256, 40, 2)},
		{name: "time", document: `"04:05:06"`, target: types.T_time.ToType()},
		{name: "datetime", document: `"2024-02-03 04:05:06"`, target: types.T_datetime.ToType()},
		{name: "year", document: `"2024"`, target: types.T_year.ToType()},
		{name: "bit", document: `3`, target: types.New(types.T_bit, 2, 0)},
		{name: "binary", document: `"abc"`, target: types.New(types.T_binary, 8, 0)},
	}
	for _, test := range successes {
		t.Run(test.name, func(t *testing.T) {
			result := ConvertScalar(parseConversionValue(t, test.document), test.target)
			require.Equal(t, StatusSuccess, result.Status)
			require.NoError(t, result.Err)
			require.NotNil(t, result.Value)
		})
	}

	var nilCtx context.Context
	result := ConvertScalarWithContext(nilCtx, parseConversionValue(t, `1`), types.T_int64.ToType(), ConversionOptions{})
	require.Equal(t, StatusSuccess, result.Status)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	result = ConvertScalarWithContext(cancelled, parseConversionValue(t, `1`), types.T_int64.ToType(), ConversionOptions{})
	require.Equal(t, StatusStatementError, result.Status)
	require.ErrorIs(t, result.Err, context.Canceled)

	invalid := []struct {
		name     string
		document string
		target   types.Type
		status   ConversionStatus
	}{
		{name: "signed int16 range", document: `32768`, target: types.T_int16.ToType(), status: StatusRangeError},
		{name: "signed int32 range", document: `2147483648`, target: types.T_int32.ToType(), status: StatusRangeError},
		{name: "uint8 range", document: `256`, target: types.T_uint8.ToType(), status: StatusRangeError},
		{name: "uint16 range", document: `65536`, target: types.T_uint16.ToType(), status: StatusRangeError},
		{name: "float parse", document: `"not-a-float"`, target: types.T_float64.ToType(), status: StatusConversionError},
		{name: "float range", document: `"1e1000"`, target: types.T_float64.ToType(), status: StatusRangeError},
		{name: "float32 range", document: `"1e39"`, target: types.T_float32.ToType(), status: StatusRangeError},
		{name: "decimal64 range", document: `"12345"`, target: types.New(types.T_decimal64, 3, 0), status: StatusRangeError},
		{name: "decimal128 conversion", document: `"not-decimal"`, target: types.New(types.T_decimal128, 20, 2), status: StatusConversionError},
		{name: "date conversion", document: `"2024-99-99"`, target: types.T_date.ToType(), status: StatusConversionError},
		{name: "time conversion", document: `"99:99:99"`, target: types.T_time.ToType(), status: StatusConversionError},
		{name: "datetime conversion", document: `"2024-99-99 99:99:99"`, target: types.T_datetime.ToType(), status: StatusConversionError},
		{name: "timestamp conversion", document: `"2024-99-99 99:99:99"`, target: types.T_timestamp.ToType(), status: StatusConversionError},
		{name: "year conversion", document: `"not-a-year"`, target: types.T_year.ToType(), status: StatusConversionError},
		{name: "bit range", document: `4`, target: types.New(types.T_bit, 2, 0), status: StatusRangeError},
		{name: "bool conversion", document: `"maybe"`, target: types.T_bool.ToType(), status: StatusConversionError},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			result := ConvertScalar(parseConversionValue(t, test.document), test.target)
			require.Equal(t, test.status, result.Status)
			require.Error(t, result.Err)
		})
	}

	for _, test := range []struct {
		name   string
		value  bytejson.ByteJson
		target types.Type
		status ConversionStatus
	}{
		{name: "numeric text range", value: parseConversionValue(t, `"999999999999999999999999999"`), target: types.T_int64.ToType(), status: StatusRangeError},
		{name: "non numeric text", value: parseConversionValue(t, `"x"`), target: types.T_int64.ToType(), status: StatusConversionError},
		{name: "float NaN bool", value: bytejson.ByteJson{Type: bytejson.TpCodeFloat64, Data: func() []byte {
			data := make([]byte, 8)
			binary.LittleEndian.PutUint64(data, math.Float64bits(math.NaN()))
			return data
		}()}, target: types.T_bool.ToType(), status: StatusConversionError},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := ConvertScalar(test.value, test.target)
			require.Equal(t, test.status, result.Status)
			require.Error(t, result.Err)
		})
	}
}

func TestJSONValueConversionAndAppendBoundaryStates(t *testing.T) {
	invalidJSONValues := []bytejson.ByteJson{
		{Type: bytejson.TpCodeString, Data: []byte{0x80}},
		{Type: bytejson.TpCodeFloat64, Data: func() []byte {
			data := make([]byte, 8)
			binary.LittleEndian.PutUint64(data, math.Float64bits(math.Inf(1)))
			return data
		}()},
		{Type: bytejson.TpCodeLiteral, Data: []byte{0xff}},
	}
	for _, value := range invalidJSONValues {
		result := ConvertScalar(value, types.T_json.ToType())
		require.Equal(t, StatusStatementError, result.Status)
		require.Error(t, result.Err)
	}

	result := convertJSONValueWithLimit(context.Background(), parseConversionValue(t, `null`), 1)
	require.Equal(t, StatusJSONNull, result.Status)
	result = convertJSONValueWithLimit(context.Background(), parseConversionValue(t, `1`), 0)
	require.Equal(t, StatusStatementError, result.Status)
	require.Error(t, result.Err)
	result = convertJSONValueWithLimit(context.Background(), parseConversionValue(t, `1`), 64)
	require.Equal(t, StatusSuccess, result.Status)

	result = ConvertPathMatchesWithLimitContext(nil, nil, types.T_json.ToType(), 64)
	require.Equal(t, StatusStatementError, result.Status)
	require.Error(t, result.Err)
	result = ConvertPathMatchesWithLimit(nil, types.T_json.ToType(), 1)
	require.Equal(t, StatusStatementError, result.Status)
	require.Error(t, result.Err)

	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_int32.ToType())
	defer vec.Free(mp)
	require.Error(t, AppendResult(nil, Result{Status: StatusJSONNull}, mp))
	require.Error(t, AppendResult(vec, Result{Status: StatusJSONNull}, nil))
	require.Error(t, AppendResult(vec, Result{Status: StatusSuccess, Value: int64(1)}, mp))
	require.Error(t, AppendResult(vec, Result{Status: StatusStatementError}, mp))
	require.NoError(t, AppendConvertedResult(vec, Result{Status: StatusSuccess, Value: int32(1)}, mp))
	require.Equal(t, 1, vec.Length())

	constVec, err := vector.NewConstFixed(types.T_int32.ToType(), int32(1), 1, mp)
	require.NoError(t, err)
	defer constVec.Free(mp)
	require.Error(t, AppendResult(constVec, Result{Status: StatusSuccess, Value: int32(2)}, mp))

	bytesVec := vector.NewVec(types.T_varchar.ToType())
	defer bytesVec.Free(mp)
	require.NoError(t, AppendResult(bytesVec, Result{Status: StatusTruncated, Value: []byte("x")}, mp))
	require.Equal(t, 1, bytesVec.Length())
	require.False(t, appendValueCompatible(types.T_any, nil))
}
