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

	// Exact same DECIMAL values
	d1 := makeDecimalJson("123.456")
	d2 := makeDecimalJson("123.456")
	require.Equal(t, 0, CompareByteJson(d1, d2), "same DECIMAL values should be equal")
}
