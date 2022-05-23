// Copyright 2021 Matrix Origin
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

package types

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCompareDecimal64Decimal64(t *testing.T) {
	aScale := int32(2)
	bScale := int32(3)
	a0, _ := ParseStringToDecimal64("123.45", 18, aScale)
	b0, _ := ParseStringToDecimal64("123.45", 18, bScale)
	result0 := CompareDecimal64Decimal64(a0, b0, aScale, bScale)
	a1, _ := ParseStringToDecimal64("123.35", 18, aScale)
	b1, _ := ParseStringToDecimal64("123.45", 18, bScale)
	result1 := CompareDecimal64Decimal64(a1, b1, aScale, bScale)
	a2, _ := ParseStringToDecimal64("123.35", 18, aScale)
	b2, _ := ParseStringToDecimal64("-123.45", 18, bScale)
	result2 := CompareDecimal64Decimal64(a2, b2, aScale, bScale)

	require.Equal(t, int64(0), result0)
	require.Equal(t, int64(-1), result1)
	require.Equal(t, int64(1), result2)
}

func TestParseStringToDecimal64(t *testing.T) {
	a0, _ := ParseStringToDecimal64("12.34", 18, 2)
	a1, _ := ParseStringToDecimal64("12.34", 18, 3)
	a2, _ := ParseStringToDecimal64("-12.34", 18, 3)
	_, err := ParseStringToDecimal64("-124.34", 5, 3)

	require.Equal(t, Decimal64(1234), a0)
	require.Equal(t, Decimal64(12340), a1)
	require.Equal(t, Decimal64(-12340), a2)
	require.Error(t, err)
}

func TestParseStringToDecimal128(t *testing.T) {
	a0, _ := ParseStringToDecimal128("12.34", 20, 2)
	a1, _ := ParseStringToDecimal128("12.34", 20, 3)
	a2, _ := ParseStringToDecimal128("-12.34", 20, 3)
	a3, _ := ParseStringToDecimal128("-3421", 20, 3)
	_, err := ParseStringToDecimal128("-12345123451234512345.34", 20, 3)

	require.Equal(t, Decimal128{1234, 0}, a0)
	require.Equal(t, Decimal128{12340, 0}, a1)
	require.Equal(t, Decimal128{-12340, -1}, a2)
	require.Equal(t, Decimal128{-3421000, -1}, a3)
	require.Error(t, err)
}

func TestDecimal64_Decimal64ToString(t *testing.T) {
	a0 := Decimal64(1230)
	result0 := a0.Decimal64ToString(1)
	a1 := Decimal64(1230)
	result1 := a1.Decimal64ToString(2)
	a2 := Decimal64(-1230)
	result2 := a2.Decimal64ToString(2)
	a3 := Decimal64(-1230)
	result3 := a3.Decimal64ToString(0)
	require.Equal(t, []byte("123.0"), result0)
	require.Equal(t, []byte("12.30"), result1)
	require.Equal(t, []byte("-12.30"), result2)
	require.Equal(t, []byte("-1230"), result3)
}

func TestDecimal128_Decimal128ToString(t *testing.T) {
	a0 := Decimal128{1230, 0}
	result0 := a0.Decimal128ToString(1)
	a1 := Decimal128{1230, 0}
	result1 := a1.Decimal128ToString(2)
	a2 := Decimal128{1230, 0}
	result2 := a2.Decimal128ToString(2)
	a3 := Decimal128{-1230, -1}
	result3 := a3.Decimal128ToString(2)

	require.Equal(t, []byte("123.0"), result0)
	require.Equal(t, []byte("12.30"), result1)
	require.Equal(t, []byte("12.30"), result2)
	require.Equal(t, []byte("-12.30"), result3)
}

func TestDecimal64Add(t *testing.T) {
	a0 := Decimal64(123)
	b0 := Decimal64(123)
	result0 := Decimal64Add(a0, b0, 1, 1)
	require.Equal(t, Decimal64(246), result0)
	a1 := Decimal64(1230)
	b1 := Decimal64(123)
	result1 := Decimal64Add(a1, b1, 1, 1)
	require.Equal(t, Decimal64(1353), result1)
	a2 := Decimal64(-1230)
	b2 := Decimal64(123)
	result2 := Decimal64Add(a2, b2, 1, 1)
	require.Equal(t, Decimal64(-1107), result2)
}

func TestDecimal64Sub(t *testing.T) {
	a0 := Decimal64(123)
	b0 := Decimal64(123)
	result0 := Decimal64Sub(a0, b0, 1, 1)
	require.Equal(t, Decimal64(0), result0)
	a1 := Decimal64(1230)
	b1 := Decimal64(123)
	result1 := Decimal64Sub(a1, b1, 1, 1)
	require.Equal(t, Decimal64(1107), result1)
	a2 := Decimal64(-1230)
	b2 := Decimal64(123)
	result2 := Decimal64Sub(a2, b2, 1, 1)
	require.Equal(t, Decimal64(-1353), result2)
}

func TestDecimal128Add(t *testing.T) {
	a0 := Decimal128{123, 0}
	b0 := Decimal128{123, 0}
	result0 := Decimal128Add(a0, b0, 1, 1)
	require.Equal(t, Decimal128{246, 0}, result0)
	a1 := Decimal128{1230, 0}
	b1 := Decimal128{123, 0}
	result1 := Decimal128Add(a1, b1, 1, 1)
	require.Equal(t, Decimal128{1353, 0}, result1)
	a2 := Decimal128{-1230, -1}
	b2 := Decimal128{123, 0}
	result2 := Decimal128Add(a2, b2, 1, 1)
	require.Equal(t, Decimal128{-1107, -1}, result2)
}

func TestDecimal128Sub(t *testing.T) {
	a0 := Decimal128{123, 0}
	b0 := Decimal128{123, 0}
	result0 := Decimal128Sub(a0, b0, 1, 1)
	require.Equal(t, Decimal128{0, 0}, result0)

	a1 := Decimal128{1230, 0}
	b1 := Decimal128{123, 0}
	result1 := Decimal128Sub(a1, b1, 1, 1)
	require.Equal(t, Decimal128{1107, 0}, result1)

	a2 := Decimal128{-1230, -1}
	b2 := Decimal128{123, 0}
	result2 := Decimal128Sub(a2, b2, 1, 1)
	require.Equal(t, Decimal128{-1353, -1}, result2)
}

func TestDecimal64Decimal64Mul(t *testing.T) {
	a0 := Decimal64(123)
	b0 := Decimal64(123)
	result0 := Decimal64Decimal64Mul(a0, b0)
	require.Equal(t, Decimal128{15129, 0}, result0)
	a1 := Decimal64(123)
	b1 := Decimal64(-123)
	result1 := Decimal64Decimal64Mul(a1, b1)
	require.Equal(t, Decimal128{-15129, -1}, result1)
	a2 := Decimal64(-1230)
	b2 := Decimal64(123)
	result2 := Decimal64Decimal64Mul(a2, b2)
	require.Equal(t, Decimal128{-151290, -1}, result2)
}

func TestDecimal64Decimal64Div(t *testing.T) {
	a0 := Decimal64(123)
	b0 := Decimal64(123)
	aScale := int32(1)
	bScale := int32(2)
	result0 := Decimal64Decimal64Div(a0, b0, aScale, bScale)
	require.Equal(t, Decimal128{100, 0}, result0)
	a1 := Decimal64(123)
	b1 := Decimal64(-123)
	result1 := Decimal64Decimal64Div(a1, b1, aScale, bScale)
	require.Equal(t, Decimal128{-100, -1}, result1)
	a2 := Decimal64(-1230)
	b2 := Decimal64(123)
	result2 := Decimal64Decimal64Div(a2, b2, aScale, bScale)
	require.Equal(t, Decimal128{-1000, -1}, result2)
}

func TestDecimal128Mul(t *testing.T) {
	a0 := Decimal128{123, 0}
	b0 := Decimal128{123, 0}
	result0 := Decimal128Decimal128Mul(a0, b0)
	require.Equal(t, Decimal128{15129, 0}, result0)

	a1 := Decimal128{1230, 0}
	b1 := Decimal128{123, 0}
	result1 := Decimal128Decimal128Mul(a1, b1)
	require.Equal(t, Decimal128{151290, 0}, result1)

	a2 := Decimal128{-1230, -1}
	b2 := Decimal128{123, 0}
	result2 := Decimal128Decimal128Mul(a2, b2)
	require.Equal(t, Decimal128{-151290, -1}, result2)
}

func TestDecimal128Div(t *testing.T) {
	a0 := Decimal128{123, 0}
	b0 := Decimal128{123, 0}
	aScale := int32(1)
	bScale := int32(2)
	result0 := Decimal128Decimal128Div(a0, b0, aScale, bScale)
	require.Equal(t, Decimal128{100, 0}, result0)

	a1 := Decimal128{1230, 0}
	b1 := Decimal128{123, 0}
	result1 := Decimal128Decimal128Div(a1, b1, aScale, bScale)
	require.Equal(t, Decimal128{1000, 0}, result1)

	a2 := Decimal128{-1230, -1}
	b2 := Decimal128{123, 0}
	result2 := Decimal128Decimal128Div(a2, b2, aScale, bScale)
	require.Equal(t, Decimal128{-1000, -1}, result2)
}

func TestDecimal64ToDecimal128(t *testing.T) {
	a0 := Decimal64(123)
	result0 := Decimal64ToDecimal128(a0)
	require.Equal(t, Decimal128{123, 0}, result0)
	a1 := Decimal64(-123)
	result1 := Decimal64ToDecimal128(a1)
	require.Equal(t, Decimal128{-123, -1}, result1)
}

func TestAlignDecimal64UsingScaleDiffBatch(t *testing.T) {
	src := []Decimal64{12, 13, 4321, 987}
	dst := make([]Decimal64, len(src))
	AlignDecimal64UsingScaleDiffBatch(src, dst, 3)
	expectedResult := []Decimal64{12000, 13000, 4321000, 987000}
	for i := range dst {
		require.Equal(t, expectedResult[i], dst[i])
	}
}

func TestAlignDecimal128UsingScaleDiffBatch(t *testing.T) {
	src := make([]Decimal128, 6)
	src[0], _ = ParseStringToDecimal128("12345.67890", 38, 5)
	src[1], _ = ParseStringToDecimal128("54321.54321", 38, 5)
	src[2], _ = ParseStringToDecimal128("54321.6789", 38, 5)
	src[3], _ = ParseStringToDecimal128("54321", 38, 5)
	src[4], _ = ParseStringToDecimal128("1.2", 38, 5)
	src[5], _ = ParseStringToDecimal128("-12.34", 38, 5)
	dst := make([]Decimal128, 6)
	AlignDecimal128UsingScaleDiffBatch(src, dst, 3)
	// assert using the internal representation of Decimal128
	require.Equal(t, Decimal128{1234567890000, 0}, dst[0])
	require.Equal(t, Decimal128{5432154321000, 0}, dst[1])
	require.Equal(t, Decimal128{5432167890000, 0}, dst[2])
	require.Equal(t, Decimal128{5432100000000, 0}, dst[3])
	require.Equal(t, Decimal128{120000000, 0}, dst[4])
	require.Equal(t, Decimal128{-1234000000, -1}, dst[5])
}

func TestDecimal128Int64Div(t *testing.T) {
	src := make([]Decimal128, 6)
	src[0], _ = ParseStringToDecimal128("12345.67890", 38, 5)
	src[1], _ = ParseStringToDecimal128("54321.54321", 38, 5)
	src[2], _ = ParseStringToDecimal128("54321.6789", 38, 5)
	src[3], _ = ParseStringToDecimal128("54321", 38, 5)
	src[4], _ = ParseStringToDecimal128("1.2", 38, 5)
	src[5], _ = ParseStringToDecimal128("-12.34", 38, 5)
	result := make([]Decimal128, 6)
	for i := range src {
		result[i] = Decimal128Int64Div(src[i], 10)
	}
	require.Equal(t, Decimal128{123456789, 0}, result[0])
	require.Equal(t, Decimal128{543215432, 0}, result[1])
	require.Equal(t, Decimal128{543216789, 0}, result[2])
	require.Equal(t, Decimal128{543210000, 0}, result[3])
	require.Equal(t, Decimal128{12000, 0}, result[4])
	require.Equal(t, Decimal128{-123400, -1}, result[5])

}
