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
	d0, _ := ParseStringToDecimal64("12.34", 18, 2)
	d1, _ := ParseStringToDecimal64("12.34", 18, 3)
	d2, _ := ParseStringToDecimal64("-12.34", 18, 3)
	d3, _ := ParseStringToDecimal64("1e2", 18, 3)
	d4, _ := ParseStringToDecimal64("-1e2", 18, 3)
	d5, _ := ParseStringToDecimal64("1e6", 18, 3)
	d6, _ := ParseStringToDecimal64("-1e6", 18, 3)
	_, err := ParseStringToDecimal64("-124.34", 5, 3)
	_, err2 := ParseStringToDecimal64("12345e6", 10, 3)
	d7, _ := ParseStringToDecimal64("12345e-6", 10, 3)
	d8, _ := ParseStringToDecimal64("12345e-5", 10, 3)
	d9, _ := ParseStringToDecimal64("12345e-8", 10, 3)
	d10, _ := ParseStringToDecimal64("52345e-8", 10, 3)
	d11, _ := ParseStringToDecimal64("62345e-8", 10, 3)
	d12, _ := ParseStringToDecimal64("52345e-9", 10, 3)
	_, err3 := ParseStringToDecimal64("52345e9", 10, 3)
	_, err4 := ParseStringToDecimal64("52345e2", 10, 3)
	_, err5 := ParseStringToDecimal64("52345e3", 10, 3)
	d13, _ := ParseStringToDecimal64("12.345", 18, 2)
	d14, _ := ParseStringToDecimal64("12.0007", 18, 3)
	d15, _ := ParseStringToDecimal64("-12.0007", 18, 3)
	d16, _ := ParseStringToDecimal64("-12.0004", 18, 3)
	d17, _ := ParseStringToDecimal64("-12345", 10, 5)
	d18, _ := ParseStringToDecimal64("12345", 10, 5)
	d19, _ := ParseStringToDecimal64(".12345", 10, 5)
	d20, _ := ParseStringToDecimal64("-.12345", 10, 5)
	d21, _ := ParseStringToDecimal64(".123451", 10, 5)
	d22, _ := ParseStringToDecimal64(".123456", 10, 5)
	d23, _ := ParseStringToDecimal64(".123456e3", 10, 5)
	d24, _ := ParseStringToDecimal64(".123456e-3", 10, 5)
	_, err6 := ParseStringToDecimal64(".123456e6", 10, 5)
	d25, _ := ParseStringToDecimal64(".123456e6", 11, 5)
	_, err7 := ParseStringToDecimal64("1234567", 11, 5)
	d26, _ := ParseStringToDecimal64("1234.567", 11, 5)
	d27, _ := ParseStringToDecimal64("-1234.567", 11, 5)
	d28, _ := ParseStringToDecimal64("-.123456", 10, 5)
	d29, _ := ParseStringToDecimal64("-.123456e3", 10, 5)
	d30, _ := ParseStringToDecimal64("-00000.123456e3", 10, 5)
	d31, _ := ParseStringToDecimal64("00000.123456e3", 10, 5)
	d32, _ := ParseStringToDecimal64("+0.123456", 18, 0)
	d33, _ := ParseStringToDecimal64("-0.123456", 18, 0)
	d34, _ := ParseStringToDecimal64("+0.6", 18, 0)
	d35, _ := ParseStringToDecimal64("-0.6", 18, 0)
	_, err8 := ParseStringToDecimal64("--0.6", 18, 0)
	_, err9 := ParseStringToDecimal64("guten tag", 18, 0)
	d36, _ := ParseStringToDecimal64("-0.6", 10, 10)
	d37, _ := ParseStringToDecimal64("0.6", 10, 10)
	d38, _ := ParseStringToDecimal64("0.12345678999", 10, 10)
	d39, _ := ParseStringToDecimal64("-0.12345678999", 10, 10)
	d40, _ := ParseStringToDecimal64("-12.345678999e-2", 10, 10)
	d41, _ := ParseStringToDecimal64("12.345678999e-2", 10, 10)
	d42, _ := ParseStringToDecimal64("0.12345E-3", 10, 5)

	require.Equal(t, Decimal64(1234), d0)
	require.Equal(t, Decimal64(12340), d1)
	require.Equal(t, Decimal64(-12340), d2)
	require.Equal(t, Decimal64(100000), d3)
	require.Equal(t, Decimal64(-100000), d4)
	require.Equal(t, Decimal64(1000000000), d5)
	require.Equal(t, Decimal64(-1000000000), d6)
	require.Equal(t, Decimal64(12), d7)
	require.Equal(t, Decimal64(123), d8)
	require.Equal(t, Decimal64(0), d9)
	require.Equal(t, Decimal64(1), d10)
	require.Equal(t, Decimal64(1), d11)
	require.Equal(t, Decimal64(0), d12)
	require.Error(t, err)
	require.Error(t, err2)
	require.Error(t, err3)
	require.NoError(t, err4)
	require.Error(t, err5)
	//
	require.Equal(t, Decimal64(1235), d13)
	require.Equal(t, Decimal64(12001), d14)
	require.Equal(t, Decimal64(-12001), d15)
	require.Equal(t, Decimal64(-12000), d16)
	require.Equal(t, Decimal64(-1234500000), d17)
	require.Equal(t, Decimal64(1234500000), d18)
	require.Equal(t, Decimal64(12345), d19)
	require.Equal(t, Decimal64(-12345), d20)
	require.Equal(t, Decimal64(12345), d21)
	require.Equal(t, Decimal64(12346), d22)
	require.Equal(t, Decimal64(12345600), d23)
	require.Equal(t, Decimal64(12), d24)
	require.Error(t, err6)
	require.Equal(t, Decimal64(12345600000), d25)
	require.Error(t, err7)
	require.Equal(t, Decimal64(123456700), d26)
	require.Equal(t, Decimal64(-123456700), d27)
	require.Equal(t, Decimal64(-12346), d28)
	require.Equal(t, Decimal64(-12345600), d29)
	require.Equal(t, Decimal64(-12345600), d30)
	require.Equal(t, Decimal64(12345600), d31)
	require.Equal(t, Decimal64(0), d32)
	require.Equal(t, Decimal64(0), d33)
	require.Equal(t, Decimal64(1), d34)
	require.Equal(t, Decimal64(-1), d35)
	require.Equal(t, Decimal64(-6000000000), d36)
	require.Equal(t, Decimal64(6000000000), d37)
	require.Equal(t, Decimal64(1234567900), d38)
	require.Equal(t, Decimal64(-1234567900), d39)
	require.Equal(t, Decimal64(-1234567900), d40)
	require.Equal(t, Decimal64(1234567900), d41)
	require.Equal(t, Decimal64(12), d42)
	require.Error(t, err8)
	require.Error(t, err9)
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

	a4, _ := ParseStringToDecimal128("1e2", 18, 3)
	a5, _ := ParseStringToDecimal128("-1e2", 18, 3)
	a6, _ := ParseStringToDecimal128("1e6", 18, 3)
	a7, _ := ParseStringToDecimal128("-1e6", 18, 3)
	require.Equal(t, Decimal128{100000, 0}, a4)
	require.Equal(t, Decimal128{-100000, -1}, a5)
	require.Equal(t, Decimal128{1000000000, 0}, a6)
	require.Equal(t, Decimal128{-1000000000, -1}, a7)

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
