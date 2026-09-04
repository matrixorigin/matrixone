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

package bytejson

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCanonicalNumberCommonPathDoesNotAllocate(t *testing.T) {
	integer := makeJson(t, "1")
	floating := makeJson(t, "1.25")
	integralFloat := makeJsonWithoutParse(1)
	for _, value := range []ByteJson{integer, floating} {
		var storage [9]byte
		allocs := testing.AllocsPerRun(100, func() {
			encoded, ok := AppendCanonicalNumber(storage[:0], value)
			if !ok || len(encoded) != len(storage) {
				t.Fatalf("unexpected canonical number result: ok=%v len=%d", ok, len(encoded))
			}
		})
		require.Zero(t, allocs)
	}

	allocs := testing.AllocsPerRun(100, func() {
		if CompareByteJson(integer, integralFloat) != 0 {
			t.Fatal("integer and integral float must compare equal")
		}
	})
	require.Zero(t, allocs)
}

func TestNumericTextIntegerConversionIsExact(t *testing.T) {
	t.Run("signed", func(t *testing.T) {
		tests := []struct {
			text string
			want int64
			ok   bool
		}{
			{text: "9007199254740993.9", want: 9007199254740993, ok: true},
			{text: "-9007199254740993.9", want: -9007199254740993, ok: true},
			{text: "9.223372036854775807e18", want: math.MaxInt64, ok: true},
			{text: "-9223372036854775808.99", want: math.MinInt64, ok: true},
			{text: "9223372036854775808", ok: false},
			{text: "-9223372036854775809", ok: false},
			{text: "1e-2147483647", want: 0, ok: true},
			{text: "1e2147483647", ok: false},
			{text: "not-a-number", ok: false},
		}
		for _, test := range tests {
			got, ok := NumericTextToInt64(test.text)
			require.Equal(t, test.ok, ok, test.text)
			if test.ok {
				require.Equal(t, test.want, got, test.text)
			}
		}
	})

	t.Run("unsigned", func(t *testing.T) {
		tests := []struct {
			text string
			want uint64
			ok   bool
		}{
			{text: "18446744073709551615.99", want: math.MaxUint64, ok: true},
			{text: "1.8446744073709551615e19", want: math.MaxUint64, ok: true},
			{text: "18446744073709551616", ok: false},
			{text: "+0e999", want: 0, ok: true},
			{text: "-0.0", want: 0, ok: true},
			{text: "-0.1", ok: false},
		}
		for _, test := range tests {
			got, ok := NumericTextToUint64(test.text)
			require.Equal(t, test.ok, ok, test.text)
			if test.ok {
				require.Equal(t, test.want, got, test.text)
			}
		}
	})

}

func TestNumericByteJSONIntegerConversionPreservesSourceDomain(t *testing.T) {
	signed, ok := NumericToInt64(makeDecimalJson("9007199254740993.9"))
	require.True(t, ok)
	require.Equal(t, int64(9007199254740993), signed)

	unsigned, ok := NumericToUint64(makeDecimalJson("18446744073709551615.9"))
	require.True(t, ok)
	require.Equal(t, uint64(math.MaxUint64), unsigned)

	_, ok = NumericToInt64(makeJsonWithoutParse(9223372036854775808.0))
	require.False(t, ok)
	_, ok = NumericToUint64(makeJsonWithoutParse(-0.5))
	require.False(t, ok)
	_, ok = NumericToInt64(ByteJson{Type: TpCodeInt64, Data: []byte{1}})
	require.False(t, ok)
}

func TestCompareNumericFailsClosedForMalformedValues(t *testing.T) {
	comparison, ok := CompareNumeric(
		makeDecimalJson("9007199254740992.1"),
		makeDecimalJson("9007199254740993.1"),
	)
	require.True(t, ok)
	require.Less(t, comparison, 0)

	_, ok = CompareNumeric(makeDecimalJson("1.25tail"), makeDecimalJson("1.25tail"))
	require.False(t, ok, "identical malformed payloads must not compare equal")
	_, ok = CompareNumeric(makeJsonWithoutParse(1), makeJson(t, `"1"`))
	require.False(t, ok)
	_, ok = CompareNumeric(
		ByteJson{Type: TpCodeInt64, Data: []byte{1}},
		ByteJson{Type: TpCodeInt64, Data: []byte{1}},
	)
	require.False(t, ok)
	notANumber := makeJsonWithoutParse(math.NaN())
	_, ok = CompareNumeric(notANumber, notANumber)
	require.False(t, ok)
	_, ok = ParseNumeric(notANumber)
	require.False(t, ok)

	parsedLeft, ok := ParseNumeric(makeDecimalJson("9007199254740992.1"))
	require.True(t, ok)
	parsedRight, ok := ParseNumeric(makeDecimalJson("9007199254740993.1"))
	require.True(t, ok)
	comparison, ok = CompareParsedNumeric(parsedLeft, parsedRight)
	require.True(t, ok)
	require.Less(t, comparison, 0)
	_, ok = CompareParsedNumeric(ParsedNumeric{}, parsedRight)
	require.False(t, ok, "a zero-value parsed number must fail closed")
}

func makeJsonWithoutParse(value float64) ByteJson {
	var data [8]byte
	endian.PutUint64(data[:], math.Float64bits(value))
	return ByteJson{Type: TpCodeFloat64, Data: data[:]}
}

func makeIntJSONWithoutParse(value int64) ByteJson {
	var data [8]byte
	endian.PutUint64(data[:], uint64(value))
	return ByteJson{Type: TpCodeInt64, Data: data[:]}
}

func BenchmarkCompareByteJsonNumeric(b *testing.B) {
	tests := []struct {
		name  string
		left  ByteJson
		right ByteJson
	}{
		{name: "integer-float", left: makeIntJSONWithoutParse(1), right: makeJsonWithoutParse(1)},
		{name: "decimal-float", left: makeDecimalJson("0.100"), right: makeJsonWithoutParse(0.1)},
		{name: "extreme-decimal", left: makeDecimalJson("1e2147483647"), right: makeDecimalJson("10e2147483646")},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if CompareByteJson(test.left, test.right) != 0 {
					b.Fatal("values must compare equal")
				}
			}
		})
	}
}

func BenchmarkAppendCanonicalNumber(b *testing.B) {
	tests := []struct {
		name  string
		value ByteJson
	}{
		{name: "float", value: makeJsonWithoutParse(0.1)},
		{name: "decimal", value: makeDecimalJson("0.100")},
		{name: "extreme-decimal", value: makeDecimalJson("1e2147483647")},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			dst := make([]byte, 0, 64)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				encoded, ok := AppendCanonicalNumber(dst[:0], test.value)
				if !ok || len(encoded) == 0 {
					b.Fatal("value must have a canonical number key")
				}
			}
		})
	}
}
