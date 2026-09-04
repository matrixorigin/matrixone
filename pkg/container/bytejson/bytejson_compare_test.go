// Copyright 2024 Matrix Origin
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
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func makeJson(t *testing.T, s string) ByteJson {
	bj, err := ParseFromString(s)
	require.NoError(t, err)
	return bj
}

// makeDecimalJson creates a ByteJson with TpCodeDecimal type and the given
// string value (in uvarint-prefixed layout, matching TpCodeString/DECIMAL layout).
func makeDecimalJson(s string) ByteJson {
	l := len(s)
	data := make([]byte, binary.MaxVarintLen64+l)
	n := binary.PutUvarint(data, uint64(l))
	copy(data[n:], s)
	return ByteJson{Type: TpCodeDecimal, Data: data[:n+l]}
}

func makeBinaryJson(tp TpCode, payload []byte) ByteJson {
	data := make([]byte, binary.MaxVarintLen64+len(payload))
	n := binary.PutUvarint(data, uint64(len(payload)))
	copy(data[n:], payload)
	return ByteJson{Type: tp, Data: data[:n+len(payload)]}
}

func TestCompareByteJsonMySQLTypePrecedence(t *testing.T) {
	values := []struct {
		name  string
		value ByteJson
	}{
		{name: "json-null", value: makeJson(t, "null")},
		{name: "number", value: makeJson(t, "0")},
		{name: "string", value: makeJson(t, `""`)},
		{name: "object", value: makeJson(t, `{}`)},
		{name: "array", value: makeJson(t, `[]`)},
		{name: "false", value: makeJson(t, `false`)},
		{name: "true", value: makeJson(t, `true`)},
		{name: "date", value: makeBinaryJson(TpCodeDate, []byte("2024-01-01"))},
		{name: "time", value: makeBinaryJson(TpCodeTime, []byte("12:34:56"))},
		{name: "datetime", value: makeBinaryJson(TpCodeDatetime, []byte("2024-01-01 12:34:56"))},
		{name: "bit", value: makeBinaryJson(TpCodeBit, []byte{0x01})},
		{name: "blob", value: makeBinaryJson(TpCodeOpaque, []byte{0x01})},
	}

	for i := range values {
		for j := range values {
			got := compareSign(CompareByteJson(values[i].value, values[j].value))
			want := compareSign(i - j)
			require.Equalf(t, want, got, "%s compared with %s", values[i].name, values[j].name)
			require.Equalf(t, -got,
				compareSign(CompareByteJson(values[j].value, values[i].value)),
				"reverse comparison for %s and %s", values[i].name, values[j].name)
		}
	}

	for i := range values {
		for j := i + 1; j < len(values); j++ {
			for k := j + 1; k < len(values); k++ {
				require.Lessf(t, CompareByteJson(values[i].value, values[k].value), 0,
					"transitive order for %s, %s, %s", values[i].name, values[j].name, values[k].name)
			}
		}
	}

	require.Less(t, CompareByteJson(makeJson(t, `[false]`), makeJson(t, `[true]`)), 0)
	require.Zero(t, CompareByteJson(makeJson(t, `1`), makeJson(t, `1.0`)))

	legacyBit := makeBinaryJson(TpCodeBlob, []byte(persistedBitPrefix+"AQ=="))
	legacyBlob := makeBinaryJson(TpCodeBlob, []byte("AQ=="))
	require.Greater(t, CompareByteJson(legacyBit, values[9].value), 0)
	require.Less(t, CompareByteJson(legacyBit, values[11].value), 0)
	require.Greater(t, CompareByteJson(legacyBlob, values[10].value), 0)
}

func TestCompareByteJsonUnknownTypeHasDeterministicFallback(t *testing.T) {
	left := ByteJson{Type: 0xfd, Data: []byte{0x01}}
	right := ByteJson{Type: 0xfd, Data: []byte{0x02}}
	otherType := ByteJson{Type: 0xfe, Data: []byte{0x01}}

	require.Less(t, CompareByteJson(left, right), 0)
	require.Less(t, CompareByteJson(left, otherType), 0)
	require.Greater(t, CompareByteJson(otherType, left), 0)
}

func TestCompareByteJsonMalformedEncodingHasDeterministicFallback(t *testing.T) {
	values := []ByteJson{
		{Type: TpCodeLiteral},
		{Type: TpCodeInt64, Data: []byte{0x01}},
		{Type: TpCodeString, Data: []byte{0x02, 'x'}},
		{Type: TpCodeArray, Data: []byte{0x01}},
		{Type: TpCodeObject, Data: []byte{0x01}},
		{Type: TpCodeBlob, Data: []byte{0x02, 0x01}},
	}
	for _, value := range values {
		right := ByteJson{Type: value.Type, Data: append(bytes.Clone(value.Data), 0xfe, 0xff)}
		require.NotPanics(t, func() {
			require.Less(t, CompareByteJson(value, right), 0)
			require.Greater(t, CompareByteJson(right, value), 0)
		})
	}
}

func TestCompareByteJsonMalformedValuesUseGlobalFallbackDomain(t *testing.T) {
	malformedNested := makeJson(t, `[[]]`)
	endian.PutUint32(malformedNested.Data[headerSize+valTypeSize:], 0)
	valid := []ByteJson{
		makeJson(t, `""`),
		makeJson(t, `{}`),
		makeBinaryJson(TpCodeBlob, []byte("AQ==")),
		makeBinaryJson(TpCodeOpaque, []byte{0x01}),
	}
	malformed := []ByteJson{
		{Type: TpCodeArray, Data: []byte{0x01}},
		malformedNested,
		makeBinaryJson(TpCodeBlob, []byte("not-base64")),
		makeBinaryJson(TpCodeBlob, []byte(persistedBitPrefix+"not-base64")),
		{Type: TpCodeLiteral, Data: []byte{LiteralNull, 0xff}},
	}

	for _, left := range valid {
		for _, right := range malformed {
			require.Less(t, CompareByteJson(left, right), 0)
			require.Greater(t, CompareByteJson(right, left), 0)
		}
	}

	values := append(append([]ByteJson{}, valid...), malformed...)
	for i := range values {
		for j := range values {
			leftRight := compareSign(CompareByteJson(values[i], values[j]))
			rightLeft := compareSign(CompareByteJson(values[j], values[i]))
			require.Equal(t, -leftRight, rightLeft, "antisymmetry for (%d, %d)", i, j)
			for k := range values {
				if CompareByteJson(values[i], values[j]) < 0 && CompareByteJson(values[j], values[k]) < 0 {
					require.Less(t, CompareByteJson(values[i], values[k]), 0,
						"transitivity for (%d, %d, %d)", i, j, k)
				}
			}
		}
	}
}

func TestCompareByteJsonRejectsOversizedLiteral(t *testing.T) {
	oversized := ByteJson{Type: TpCodeLiteral, Data: []byte{LiteralNull, 0xff}}
	require.NotZero(t, CompareByteJson(makeJson(t, "null"), oversized))
	require.Zero(t, CompareByteJson(oversized, oversized))
}

func TestCompareByteJsonOpaqueBinaryUsesRawBytes(t *testing.T) {
	zero := makeBinaryJson(TpCodeOpaque, []byte{0x00})
	d0 := makeBinaryJson(TpCodeOpaque, []byte{0xd0})
	bitZero := makeBinaryJson(TpCodeBit, []byte{0x00})
	bitD0 := makeBinaryJson(TpCodeBit, []byte{0xd0})
	bit := makeBinaryJson(TpCodeBit, []byte{0x01})
	legacyZero := makeBinaryJson(TpCodeBlob, []byte("AA=="))

	require.Less(t, CompareByteJson(zero, d0), 0)
	require.Less(t, CompareByteJson(bitZero, bitD0), 0)
	require.Zero(t, CompareByteJson(legacyZero, zero))
	require.Less(t, CompareByteJson(bit, makeBinaryJson(TpCodeOpaque, []byte{0x01})), 0)
	require.Equal(t, "BLOB", zero.TYPE())
	require.Equal(t, "BIT", bit.TYPE())
	require.Equal(t, `"AA=="`, zero.String())
	require.Equal(t, "AA==", mustUnquote(t, zero))
	require.Equal(t, "AQ==", mustUnquote(t, bit))
}

func TestCompareByteJsonLegacyBlobLargePayloadAllocations(t *testing.T) {
	payload := bytes.Repeat([]byte{0xab}, 1<<20)
	legacy := makeBinaryJson(TpCodeBlob, []byte(base64.StdEncoding.EncodeToString(payload)))
	raw := makeBinaryJson(TpCodeOpaque, payload)

	allocs := testing.AllocsPerRun(10, func() {
		if cmp := CompareByteJson(legacy, raw); cmp != 0 {
			t.Fatalf("unexpected compare result: %d", cmp)
		}
	})
	require.Zero(t, allocs, "legacy blob compare should not allocate decoded payload buffers")
}

func TestCompareByteJsonLegacyBlobPreservesBase64Newlines(t *testing.T) {
	payload := bytes.Repeat([]byte{0xab}, 16*1024)
	encoded := base64.StdEncoding.EncodeToString(payload)
	legacyWithNewlines := makeBinaryJson(TpCodeBlob, []byte(encoded[:4095]+"\r\n"+encoded[4095:]))
	raw := makeBinaryJson(TpCodeOpaque, payload)

	require.Zero(t, CompareByteJson(legacyWithNewlines, raw))
}

func TestCompareByteJsonLegacyBitPreservesBase64Newlines(t *testing.T) {
	payload := bytes.Repeat([]byte{0x01}, 16*1024)
	encoded := base64.StdEncoding.EncodeToString(payload)
	legacyWithNewlines := makeBinaryJson(TpCodeBlob, []byte(persistedBitPrefix+encoded[:4095]+"\r\n"+encoded[4095:]))
	raw := makeBinaryJson(TpCodeBit, payload)

	require.Equal(t, "BIT", legacyWithNewlines.TYPE())
	require.Zero(t, CompareByteJson(legacyWithNewlines, raw))
}

func BenchmarkCompareByteJsonLegacyBlobLargePayload(b *testing.B) {
	payload := bytes.Repeat([]byte{0xcd}, 1<<20)
	legacyLeft := makeBinaryJson(TpCodeBlob, []byte(base64.StdEncoding.EncodeToString(payload)))
	legacyRight := makeBinaryJson(TpCodeBlob, []byte(base64.StdEncoding.EncodeToString(payload)))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if cmp := CompareByteJson(legacyLeft, legacyRight); cmp != 0 {
			b.Fatalf("unexpected compare result: %d", cmp)
		}
	}
}

func mustUnquote(t *testing.T, bj ByteJson) string {
	t.Helper()
	value, err := bj.Unquote()
	require.NoError(t, err)
	return value
}

// TestCompareByteJson_DecimalCrossType tests that DECIMAL vs numeric types
// (Int64/Uint64/Float64/DECIMAL) are compared correctly instead of falling
// through as equal (cmp == 0).
func TestCompareByteJson_DecimalCrossType(t *testing.T) {
	decimal := makeDecimalJson("3.14")

	// DECIMAL < INT64
	bigInt := makeJson(t, "10")
	require.Less(t, CompareByteJson(decimal, bigInt), 0, "3.14 < 10")

	// DECIMAL > INT64
	require.Greater(t, CompareByteJson(decimal, makeJson(t, "1")), 0, "3.14 > 1")

	// DECIMAL == same value decimal
	require.Equal(t, 0, CompareByteJson(decimal, makeDecimalJson("3.14")))

	// DECIMAL < UINT64
	bigUint := makeJson(t, "18446744073709551615")
	require.Less(t, CompareByteJson(decimal, bigUint), 0, "3.14 < max uint64")

	// DECIMAL vs FLOAT64
	require.Less(t, CompareByteJson(makeDecimalJson("3.14"), makeJson(t, "5.5")), 0)

	// INT64 vs DECIMAL
	require.Greater(t, CompareByteJson(makeJson(t, "100"), decimal), 0, "100 > 3.14")

	// UINT64 vs DECIMAL
	require.Greater(t, CompareByteJson(makeJson(t, "18446744073709551615"), decimal), 0)

	// FLOAT64 vs DECIMAL
	require.Greater(t, CompareByteJson(makeJson(t, "5.5"), decimal), 0, "5.5 > 3.14")
	require.Equal(t, 0, CompareByteJson(makeDecimalJson("0.1"), makeJson(t, "0.1")),
		"decimal should compare against the JSON-visible float value")

	// Exact same DECIMAL values
	d1 := makeDecimalJson("123.456")
	d2 := makeDecimalJson("123.456")
	require.Equal(t, 0, CompareByteJson(d1, d2), "same DECIMAL values should be equal")

	// DECIMAL-vs-DECIMAL numeric order: "10" > "2"
	require.Greater(t, CompareByteJson(makeDecimalJson("10"), makeDecimalJson("2")), 0, "10 > 2 numerically")
	require.Less(t, CompareByteJson(makeDecimalJson("2"), makeDecimalJson("10")), 0, "2 < 10 numerically")

	// Large/high-precision DECIMAL values must not collapse through float64.
	require.Greater(t,
		CompareByteJson(makeDecimalJson("9007199254740993"), makeDecimalJson("9007199254740992")),
		0, "values beyond exact float64 integer precision must compare exactly")
	require.Greater(t,
		CompareByteJson(makeDecimalJson("0.123456789123456789"), makeDecimalJson("0.123456789123456788")),
		0, "high-precision fractional digits must compare exactly")
	require.Greater(t,
		CompareByteJson(makeDecimalJson("18446744073709551615.1"), makeJson(t, "18446744073709551615")),
		0, "decimal just above max uint64 must compare greater")
}

// TestCompareByteJson_Int64Uint64CrossType verifies that INT64-vs-UINT64
// comparisons are handled correctly even though both report TYPE()="INTEGER"
// (same numeric rank).  Without the cross-type check, the same-type branch
// would use the wrong accessor.
func TestCompareByteJson_Int64Uint64CrossType(t *testing.T) {
	// INT64 == small UINT64
	require.Equal(t, 0, CompareByteJson(makeJson(t, "42"), makeJson(t, "42")))

	// INT64 < UINT64
	require.Less(t, CompareByteJson(makeJson(t, "-1"), makeJson(t, "1")), 0,
		"-1 < 1")

	// Large UINT64 (above max int64) > INT64
	bigUint := makeJson(t, "18446744073709551615") // max uint64
	require.Greater(t, CompareByteJson(bigUint, makeJson(t, "1")), 0,
		"max uint64 > 1")

	// Large UINT64 > negative INT64
	require.Greater(t, CompareByteJson(bigUint, makeJson(t, "-1")), 0,
		"max uint64 > -1")

	// Both UINT64
	require.Equal(t, 0, CompareByteJson(makeJson(t, "100"), makeJson(t, "100")))

	// UINT64 > smaller UINT64
	require.Greater(t, CompareByteJson(makeJson(t, "100"), makeJson(t, "99")), 0)
}

func TestCompareByteJsonNumericEqualityIsTransitive(t *testing.T) {
	values := []ByteJson{
		makeDecimalJson("-1e2147483647"),
		makeJson(t, "-9223372036854775808"),
		makeJson(t, "-9.223372036854776e18"),
		makeJson(t, "0"),
		makeJson(t, "1"),
		makeJson(t, "1.0"),
		makeJson(t, "1.000000001"),
		makeDecimalJson("1.00"),
		makeJson(t, "18446744073709551615"),
		makeJson(t, "1.8446744073709552e19"),
		makeDecimalJson("1e2147483647"),
	}

	for i := range values {
		for j := range values {
			cmp := CompareByteJson(values[i], values[j])
			reverse := CompareByteJson(values[j], values[i])
			require.Equal(t, compareSign(cmp), -compareSign(reverse),
				"numeric ordering must be antisymmetric for indexes %d, %d", i, j)
			for k := range values {
				if cmp == 0 &&
					CompareByteJson(values[j], values[k]) == 0 {
					require.Zero(t, CompareByteJson(values[i], values[k]),
						"numeric equality must be transitive for indexes %d, %d, %d", i, j, k)
				}
				if cmp < 0 && CompareByteJson(values[j], values[k]) < 0 {
					require.Less(t, CompareByteJson(values[i], values[k]), 0,
						"numeric ordering must be transitive for indexes %d, %d, %d", i, j, k)
				}
			}
		}
	}

	require.NotZero(t, CompareByteJson(makeJson(t, "1"), makeJson(t, "1.000000001")),
		"nearby JSON numbers are ordered exactly, not by an equality tolerance")
}

func compareSign(value int) int {
	if value < 0 {
		return -1
	}
	if value > 0 {
		return 1
	}
	return 0
}

func TestCompareByteJsonArrayLengthAfterEqualPrefix(t *testing.T) {
	tests := []struct {
		left  string
		right string
	}{
		{left: "[]", right: "[0]"},
		{left: "[0]", right: "[]"},
		{left: "[0]", right: "[0,1]"},
	}
	for _, test := range tests {
		require.NotZero(t, CompareByteJson(makeJson(t, test.left), makeJson(t, test.right)),
			"array lengths differ for %s and %s", test.left, test.right)
	}
}

func TestCompareByteJsonMalformedDecimalFallback(t *testing.T) {
	invalid := makeDecimalJson("invalid")
	require.Zero(t, CompareByteJson(invalid, makeDecimalJson("invalid")))
	require.NotZero(t, CompareByteJson(invalid, makeDecimalJson("invalid-2")))
	require.NotZero(t, CompareByteJson(invalid, makeJson(t, "1")))
}
