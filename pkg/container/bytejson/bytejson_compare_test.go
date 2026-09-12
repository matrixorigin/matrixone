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

	"github.com/matrixorigin/matrixone/pkg/defines"
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

func makeMySQLOpaque(t *testing.T, fieldType uint8, payload []byte) ByteJson {
	t.Helper()
	value, err := NewMySQLOpaque(defines.MORPCLatestVersion, fieldType, payload)
	require.NoError(t, err)
	return value
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
	require.Zero(t, CompareByteJson(makeMySQLOpaque(t, 16, []byte{0x01}), bit))
}

func TestCompareByteJsonMySQLOpaqueUsesFieldTypeAndRawBytes(t *testing.T) {
	varbinary := makeMySQLOpaque(t, 15, []byte{0x00})
	varbinaryNext := makeMySQLOpaque(t, 15, []byte{0x01})
	blob := makeMySQLOpaque(t, 252, []byte{0x00})
	legacyBlob := makeBinaryJson(TpCodeBlob, []byte("AA=="))
	legacyOpaque := makeBinaryJson(TpCodeOpaque, []byte{0x00})

	require.Less(t, CompareByteJson(varbinary, varbinaryNext), 0)
	require.Less(t, CompareByteJson(varbinary, blob), 0)
	require.Zero(t, CompareByteJson(blob, legacyBlob))
	require.Zero(t, CompareByteJson(blob, legacyOpaque))
	varbinaryKey, ok := AppendCanonicalBinary(nil, varbinary)
	require.True(t, ok)
	blobKey, ok := AppendCanonicalBinary(nil, blob)
	require.True(t, ok)
	require.NotEqual(t, varbinaryKey, blobKey)
	blobSize, ok := CanonicalBinarySize(blob)
	require.True(t, ok)
	require.Equal(t, blobSize, len(blobKey))
}

func TestCompareByteJsonMalformedMySQLOpaqueTagFallsBackToBlob(t *testing.T) {
	malformed := makeBinaryJson(TpCodeBlob, []byte("base64:type16:not-base64"))

	require.Equal(t, "BLOB", malformed.TYPE())
	require.NotZero(t, CompareByteJson(malformed, makeBinaryJson(TpCodeBit, []byte{0x00})))
	key, ok := AppendCanonicalBinary(nil, malformed)
	require.True(t, ok)
	require.Equal(t, []byte{canonicalBinaryMarker, byte(binaryJSONBlob)}, key[:2])
}

func TestCompareByteJsonMalformedBinaryPreservesCanonicalEquivalence(t *testing.T) {
	for _, stored := range []string{
		"base64:type16:not-base64",
		"base64:type252:not-base64",
	} {
		t.Run(stored, func(t *testing.T) {
			malformed := makeBinaryJson(TpCodeBlob, []byte(stored))
			valid, err := NewMySQLOpaque(defines.MORPCVersion52, 252, []byte(stored))
			require.NoError(t, err)

			require.Zero(t, CompareByteJson(malformed, valid))
			require.Zero(t, CompareByteJson(valid, malformed))
			malformedKey, ok := AppendCanonicalBinary(nil, malformed)
			require.True(t, ok)
			validKey, ok := AppendCanonicalBinary(nil, valid)
			require.True(t, ok)
			require.Equal(t, malformedKey, validKey)
		})
	}

	legacyMalformed := makeBinaryJson(TpCodeBlob, []byte("not-base64"))
	raw := makeBinaryJson(TpCodeOpaque, []byte("not-base64"))
	require.Zero(t, CompareByteJson(legacyMalformed, raw))
	require.Zero(t, CompareByteJson(raw, legacyMalformed))
	legacyKey, ok := AppendCanonicalBinary(nil, legacyMalformed)
	require.True(t, ok)
	rawKey, ok := AppendCanonicalBinary(nil, raw)
	require.True(t, ok)
	require.Equal(t, legacyKey, rawKey)
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

func TestCompareByteJsonMySQLOpaqueLargePayloadAllocations(t *testing.T) {
	payload := bytes.Repeat([]byte{0xab}, 1<<20)
	left := makeBinaryJson(TpCodeBlob, []byte(mysqlOpaqueText(252, payload)))
	right := makeBinaryJson(TpCodeBlob, []byte(mysqlOpaqueText(252, payload)))

	allocs := testing.AllocsPerRun(10, func() {
		if cmp := CompareByteJson(left, right); cmp != 0 {
			t.Fatalf("unexpected compare result: %d", cmp)
		}
	})
	require.Zero(t, allocs, "tagged blob compare should not allocate decoded payload buffers")
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

func BenchmarkCompareByteJsonMySQLOpaqueLargePayload(b *testing.B) {
	payload := bytes.Repeat([]byte{0xcd}, 1<<20)
	left := makeBinaryJson(TpCodeBlob, []byte(mysqlOpaqueText(252, payload)))
	right := makeBinaryJson(TpCodeBlob, []byte(mysqlOpaqueText(252, payload)))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if cmp := CompareByteJson(left, right); cmp != 0 {
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
// comparisons are handled correctly even though both share one numeric type
// order. Without the cross-type check, the same-type branch would use the
// wrong accessor.
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
