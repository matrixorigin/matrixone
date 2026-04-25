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
	"fmt"
	"math/rand"
	"testing"
)

func TestParse64(t *testing.T) {
	x, y := ParseDecimal64("99999.99999999999999999999999999999999", 12, 6)
	if y != nil || x != 100000000000 {
		panic("Decimal64Parse wrong")
	}
}

// TestParse64ScientificNotation tests parsing decimal with scientific notation including + sign
func TestParse64ScientificNotation(t *testing.T) {
	// Test case 1: e+06 format (the bug case from issue #22396)
	x1, err1 := ParseDecimal64("1.23456789e+06", 10, 2)
	if err1 != nil {
		t.Errorf("Failed to parse '1.23456789e+06': %v", err1)
	}
	expected1 := Decimal64(123456789) // 1234567.89 in scale 2
	if x1 != expected1 {
		t.Errorf("ParseDecimal64('1.23456789e+06', 10, 2) = %v, expected %v", x1, expected1)
	}

	// Test case 2: e6 format (should also work)
	x2, err2 := ParseDecimal64("1.23456789e6", 10, 2)
	if err2 != nil {
		t.Errorf("Failed to parse '1.23456789e6': %v", err2)
	}
	if x2 != expected1 {
		t.Errorf("ParseDecimal64('1.23456789e6', 10, 2) = %v, expected %v", x2, expected1)
	}

	// Test case 3: e-06 format (negative exponent)
	x3, err3 := ParseDecimal64("1.23456789e-06", 10, 8)
	if err3 != nil {
		t.Errorf("Failed to parse '1.23456789e-06': %v", err3)
	}
	expected3 := Decimal64(123) // 0.00000123 in scale 8
	if x3 != expected3 {
		t.Errorf("ParseDecimal64('1.23456789e-06', 10, 8) = %v, expected %v", x3, expected3)
	}

	// Test case 4: e+2 format (small positive exponent)
	x4, err4 := ParseDecimal64("12.34e+2", 10, 2)
	if err4 != nil {
		t.Errorf("Failed to parse '12.34e+2': %v", err4)
	}
	expected4 := Decimal64(123400) // 1234.00 in scale 2
	if x4 != expected4 {
		t.Errorf("ParseDecimal64('12.34e+2', 10, 2) = %v, expected %v", x4, expected4)
	}
}

func TestParse128(t *testing.T) {
	x, y := ParseDecimal128("99999.999999999999999999999999999999999", 12, 6)
	if y != nil || x.B0_63 != 100000000000 {
		panic("Decimal128Parse wrong")
	}
}

func TestParse256(t *testing.T) {
	x, err := ParseDecimal256("12345678901234567890123456789012345.123456789012345678901234567890", 65, 30)
	if err != nil {
		t.Fatalf("ParseDecimal256 failed: %v", err)
	}
	if got := x.Format(30); got != "12345678901234567890123456789012345.123456789012345678901234567890" {
		t.Fatalf("unexpected decimal256 format: %s", got)
	}
}

func TestDecimal256Format(t *testing.T) {
	cases := []struct {
		name  string
		input string
		width int32
		scale int32
		want  string
	}{
		{
			name:  "zero integer",
			input: "0",
			width: 65,
			scale: 0,
			want:  "0",
		},
		{
			name:  "zero fraction",
			input: "0",
			width: 65,
			scale: 4,
			want:  "0.0000",
		},
		{
			name:  "leading fractional zeros",
			input: "0.0012",
			width: 65,
			scale: 4,
			want:  "0.0012",
		},
		{
			name:  "negative fraction",
			input: "-123.4500",
			width: 65,
			scale: 4,
			want:  "-123.4500",
		},
		{
			name:  "large integer",
			input: "12345678901234567890123456789012345678901234567890123456789012345",
			width: 65,
			scale: 0,
			want:  "12345678901234567890123456789012345678901234567890123456789012345",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			x, err := ParseDecimal256(tc.input, tc.width, tc.scale)
			if err != nil {
				t.Fatalf("ParseDecimal256(%q) failed: %v", tc.input, err)
			}
			if got := x.Format(tc.scale); got != tc.want {
				t.Fatalf("Format(%q, scale=%d) = %q, want %q", tc.input, tc.scale, got, tc.want)
			}
		})
	}
}

func TestDecimal256ToFloat64NegativeScale(t *testing.T) {
	x := Decimal256FromInt64(123)
	if got := Decimal256ToFloat64(x, -2); got != 12300 {
		t.Fatalf("unexpected Decimal256ToFloat64 result: %v", got)
	}
}

func TestDecimal256FromInt64SignExtension(t *testing.T) {
	// positive: high limbs must be zero
	pos := Decimal256FromInt64(42)
	if pos.B64_127 != 0 || pos.B128_191 != 0 || pos.B192_255 != 0 {
		t.Fatalf("positive int64: expected zero high limbs, got %v", pos)
	}
	if pos.B0_63 != 42 {
		t.Fatalf("positive int64: B0_63 expected 42, got %d", pos.B0_63)
	}

	// negative: high limbs must all be 0xFFFFFFFFFFFFFFFF (sign extension)
	neg := Decimal256FromInt64(-1)
	const allOnes = ^uint64(0)
	if neg.B0_63 != allOnes || neg.B64_127 != allOnes || neg.B128_191 != allOnes || neg.B192_255 != allOnes {
		t.Fatalf("Decimal256FromInt64(-1): expected all-ones, got %v", neg)
	}

	// round-trip via Format: -42 with scale=0 should format as "-42"
	n42 := Decimal256FromInt64(-42)
	if got := n42.Format(0); got != "-42" {
		t.Fatalf("Decimal256FromInt64(-42).Format(0) = %q, want \"-42\"", got)
	}
}
func TestCompare64(t *testing.T) {
	x := Decimal64(0)
	y := ^x
	if CompareDecimal64(x, y) != 1 {
		panic("CompareDecimal64 wrong")
	}
}

func TestCompare128(t *testing.T) {
	x := Decimal128{0, 0}
	y := Decimal128{^x.B0_63, ^x.B64_127}
	if CompareDecimal128(x, y) != 1 {
		panic("CompareDecimal128 wrong")
	}
}

func TestCompare256(t *testing.T) {
	x := Decimal256{0, 0, 0, 0}
	y := Decimal256{^x.B0_63, ^x.B64_127, ^x.B128_191, ^x.B192_255}
	if CompareDecimal256(x, y) != 1 {
		panic("CompareDecimal256 wrong")
	}
}

func TestDecimal64Float(t *testing.T) {
	x := Decimal64(rand.Int())
	y := Decimal64ToFloat64(x, 15)
	z, _ := Decimal64FromFloat64(y, 18, 5)
	x, _ = x.Scale(-10)
	if x != z {
		panic("DecimalFloat wrong")
	}
}
func TestDecimal128Float(t *testing.T) {
	// This test is flaky, so skip it for now.
	t.Skip()

	x := Decimal128{uint64(rand.Int()), uint64(rand.Int())}
	y := Decimal128ToFloat64(x, 30)
	z, _ := Decimal128FromFloat64(y, 38, 7)
	x, _ = x.Scale(-23)
	if x != z {
		panic("DecimalFloat wrong")
	}
}

func TestDecimal64AddSub(t *testing.T) {
	x := Decimal64(rand.Int() >> 1)
	z := x
	err := error(nil)
	y := Decimal64(rand.Int() >> 1)
	x, _, err = x.Add(y, 0, 0)
	if err == nil {
		x, _, err = x.Sub(y, 0, 0)
	}
	if err != nil || x != z {
		panic("Decimal64AddSub wrong")
	}
}
func TestDecimal128AddSub(t *testing.T) {
	x := Decimal128{uint64(rand.Int()), uint64(rand.Int()) >> 1}
	z := x
	err := error(nil)
	y := Decimal128{uint64(rand.Int()), uint64(rand.Int()) >> 1}
	x, _, err = x.Add(y, 0, 0)
	if err == nil {
		x, _, err = x.Sub(y, 0, 0)
	}
	if err != nil || x != z {
		panic("Decimal128AddSub wrong")
	}
}

func TestDecimal64MulDiv(t *testing.T) {
	x := Decimal64(rand.Int() >> 32)
	z := x
	err := error(nil)
	y := Decimal64(rand.Int() >> 32)
	x, _, err = x.Mul(y, 0, 0)
	if err == nil {
		x, _, err = x.Div(y, 12, 0)
	}
	if err != nil || x != z {
		panic("Decimal64MulDiv wrong")
	}
}
func TestDecimal128MulDiv(t *testing.T) {
	x := Decimal128{uint64(rand.Int()) >> 8, 0}
	z := x
	err := error(nil)
	y := Decimal128{uint64(rand.Int()), uint64(rand.Int() & 255)}
	x, _, err = x.Mul(y, 0, 0)
	if err == nil {
		x, _, err = x.Div(y, 12, 0)
	}
	if err != nil || x != z {
		panic("Decimal128MulDiv wrong")
	}
}

func TestDecimal128OverDiv(t *testing.T) {
	x, _, _ := Parse128("99999999999999999999999999999999999999")
	y, _, _ := Parse128("10000000000")
	z, _, err := x.Div(y, 0, 0)
	if err != nil || z.Format(0) != "10000000000000000000000000000000000" {
		panic("wrong")
	}
}

func TestParseFormat(t *testing.T) {
	x := Decimal128{0, 1}
	c := x.Format(5)
	y, err := ParseDecimal128(c, 30, 5)
	if err != nil {
		panic("error")
	}
	if x != y {
		fmt.Println(x.B64_127, x.B0_63)
		fmt.Println(y.B64_127, y.B0_63)
		panic("wrong")
	}
}

func decimalFormat[T DecimalWithFormat](x T, scale int32) string {
	return x.Format(scale)
}

var decimalFormatSink string

func TestDecimalFormat(t *testing.T) {
	d64 := Decimal64(0)
	d128 := Decimal128{0, 0}

	d64str := decimalFormat(d64, 5)
	if d64str != "0.00000" {
		t.Error("Decimal64 format failed")
	}

	d128str := decimalFormat(d128, 5)
	if d128str != "0.00000" {
		t.Error("Decimal128 format failed")
	}
}

func BenchmarkFor(b *testing.B) {
	for i := 0; i < b.N; i++ {
	}
}
func BenchmarkFloatAdd(b *testing.B) {
	x := float64(rand.Int())
	y := float64(rand.Int())
	for i := 0; i < b.N; i++ {
		x += y
	}
	z := Decimal128{uint64(x), 0}
	z.Add128(z)
}

func BenchmarkAdd(b *testing.B) {
	x := Decimal128{uint64(rand.Int()), uint64(rand.Int()) >> 1}
	y := Decimal128{uint64(rand.Int()), uint64(rand.Int()) >> 1}
	for i := 0; i < b.N; i++ {
		x.Add128(y)
	}
}

func BenchmarkFloatSub(b *testing.B) {
	x := float64(rand.Int())
	y := float64(rand.Int())
	for i := 0; i < b.N; i++ {
		x -= y
	}
	z := Decimal128{uint64(x), 0}
	z.Add128(z)
}

func BenchmarkSub(b *testing.B) {
	x := Decimal128{uint64(rand.Int()), uint64(rand.Int())}
	y := Decimal128{uint64(rand.Int()), uint64(rand.Int())}
	for i := 0; i < b.N; i++ {
		x.Sub128(y)
	}
}

func BenchmarkFloatMul(b *testing.B) {
	x := float64(rand.Int())
	y := float64(1.0001)
	for i := 0; i < b.N; i++ {
		x *= y
	}
	z := Decimal128{uint64(x), 0}
	z.Add128(z)
}

func BenchmarkMul64(b *testing.B) {
	x := Decimal64(rand.Int() >> 32)
	y := Decimal64(rand.Int() >> 32)
	for i := 0; i < b.N; i++ {
		_, _ = x.Mul64(y)
	}
}
func BenchmarkMul(b *testing.B) {
	x := Decimal128{uint64(rand.Int()) >> 8, 0}
	y := Decimal128{uint64(rand.Int()), uint64(rand.Int()) & 255}
	for i := 0; i < b.N; i++ {
		x.Mul128(y)
	}
}

func BenchmarkFloatDiv(b *testing.B) {
	x := float64(rand.Int())
	y := float64(1.0000001)
	for i := 0; i < b.N; i++ {
		x /= y
	}
	z := Decimal128{uint64(x), 0}
	z.Add128(z)
}

func BenchmarkDiv(b *testing.B) {
	x := Decimal128{uint64(rand.Int()), uint64(rand.Int())}
	y := Decimal128{uint64(rand.Int()), uint64(rand.Int()) >> 4}
	for i := 0; i < b.N; i++ {
		x.Div128(y)
	}
}

func BenchmarkIntMod(b *testing.B) {
	x := rand.Int()
	y := rand.Int()
	z := int(0)
	for i := 0; i < b.N; i++ {
		z += x % y
	}
	w := Decimal128{uint64(z), 0}
	w.Add128(w)

}

func BenchmarkMod(b *testing.B) {
	x := Decimal128{uint64(rand.Int()), uint64(rand.Int())}
	y := Decimal128{uint64(rand.Int()), uint64(rand.Int())}
	for i := 0; i < b.N; i++ {
		x.Mod128(y)
	}
}

func BenchmarkDecimal256Format(b *testing.B) {
	x, err := ParseDecimal256("12345678901234567890123456789012345.123456789012345678901234567890", 65, 30)
	if err != nil {
		b.Fatalf("ParseDecimal256 failed: %v", err)
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		decimalFormatSink = x.Format(30)
	}
}
