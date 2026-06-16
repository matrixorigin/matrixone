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

// TestAdd128Unchecked tests the branchless Add128Unchecked for SUM aggregation.
func TestAdd128Unchecked(t *testing.T) {
	cases := []struct {
		name string
		x, y Decimal128
	}{
		{"both_pos", Decimal128{100, 0}, Decimal128{200, 0}},
		{"pos_neg", Decimal128{100, 0}, Decimal128{B0_63: ^uint64(99), B64_127: ^uint64(0)}}, // -100
		{"neg_neg", Decimal128{B0_63: ^uint64(99), B64_127: ^uint64(0)}, Decimal128{B0_63: ^uint64(49), B64_127: ^uint64(0)}},
		{"zero_add", Decimal128{}, Decimal128{42, 0}},
		{"large", Decimal128{^uint64(0), 0x3FFFFFFFFFFFFFFF}, Decimal128{1, 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.x.Add128Unchecked(tc.y)
			want, _ := tc.x.Add128(tc.y)
			if got != want {
				t.Fatalf("Add128Unchecked(%v, %v) = %v, want %v", tc.x, tc.y, got, want)
			}
		})
	}
}

// TestDecimal128DivFallbackSign exercises the 256-bit fallback in Decimal128.Div
// where the sign correction was added (signx != signy → negate result).
func TestDecimal128DivFallbackSign(t *testing.T) {
	// We need x.Scale(scaleAdj) to fail so we enter the D256 fallback path.
	// Large x with high scaleAdj will trigger this.
	// x * 10^6 must overflow D128 but x * 10^6 / y must fit D128.
	// 3e32 * 10^6 = 3e38 > D128max ≈ 1.7e38, but 3e38/3 = 1e38 < D128max.
	largePos, _ := ParseDecimal128("300000000000000000000000000000000", 38, 0)
	largeNeg, _ := ParseDecimal128("-300000000000000000000000000000000", 38, 0)
	smallPos, _ := ParseDecimal128("3", 38, 0)
	smallNeg, _ := ParseDecimal128("-3", 38, 0)

	t.Run("pos_div_neg", func(t *testing.T) {
		z, _, err := largePos.Div(smallNeg, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if !z.Sign() {
			t.Fatalf("expected negative result, got %s", z.Format(12))
		}
	})
	t.Run("neg_div_pos", func(t *testing.T) {
		z, _, err := largeNeg.Div(smallPos, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if !z.Sign() {
			t.Fatalf("expected negative result, got %s", z.Format(12))
		}
	})
	t.Run("neg_div_neg", func(t *testing.T) {
		z, _, err := largeNeg.Div(smallNeg, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if z.Sign() {
			t.Fatalf("expected positive result, got %s", z.Format(12))
		}
	})
}

// TestDecimal256AddErrorFormat exercises the Decimal256.Add error message path
// where origX/origY are preserved for formatting.
func TestDecimal256AddErrorFormat(t *testing.T) {
	// Two very large Decimal256 whose sum overflows.
	maxVal := Decimal256{B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: 0x7FFFFFFFFFFFFFFF}
	one := Decimal256{B0_63: 1}
	_, _, err := maxVal.Add(one, 0, 0)
	if err == nil {
		t.Fatal("expected overflow error")
	}
}

// TestDecimal64AddSubErrorFormat exercises the Decimal64.Add/Sub error paths.
func TestDecimal64AddSubErrorFormat(t *testing.T) {
	maxD64 := Decimal64(^uint64(0) >> 1) // max positive Decimal64
	one := Decimal64(1)

	_, _, err := maxD64.Add(one, 0, 0)
	if err == nil {
		t.Fatal("expected overflow from Add")
	}
	_, _, err = Decimal64(0).Sub(maxD64, 0, 10)
	// This may or may not overflow depending on scale; we just exercise the path.
	_ = err
}

// TestDecimal128AddSubErrorFormat exercises Decimal128.Add/Sub with origX/origY.
func TestDecimal128AddSubErrorFormat(t *testing.T) {
	maxD128 := Decimal128{B0_63: ^uint64(0), B64_127: 0x7FFFFFFFFFFFFFFF}
	one := Decimal128{B0_63: 1, B64_127: 0}

	_, _, err := maxD128.Add(one, 0, 0)
	if err == nil {
		t.Fatal("expected overflow from Add")
	}
	_, _, err = maxD128.Sub(Decimal128{B0_63: ^uint64(0), B64_127: ^uint64(0)}, 0, 0) // sub a negative = add
	_ = err
}

// TestDecimalScaleOverflowErrors exercises the error formatting in Scale
// functions where we changed the error messages.
func TestDecimalScaleOverflowErrors(t *testing.T) {
	// Decimal64.Scale: need large d64 that overflows when scaled up.
	maxD64 := Decimal64(^uint64(0) >> 1) // max positive

	t.Run("d64_scale_up_overflow", func(t *testing.T) {
		_, err := maxD64.Scale(18)
		if err == nil {
			t.Fatal("expected overflow")
		}
	})
	t.Run("d64_scale_down_overflow", func(t *testing.T) {
		_, err := maxD64.Scale(-18)
		_ = err // may or may not error; exercises the path
	})

	// Decimal128.ScaleInplace: large D128 scaling up.
	maxD128 := Decimal128{B0_63: ^uint64(0), B64_127: 0x7FFFFFFFFFFFFFFF}
	t.Run("d128_scaleinplace_up", func(t *testing.T) {
		x := maxD128
		err := x.ScaleInplace(18)
		if err == nil {
			t.Fatal("expected overflow")
		}
	})
	t.Run("d128_scaleinplace_down", func(t *testing.T) {
		x := maxD128
		err := x.ScaleInplace(-38) // scale down by a lot
		_ = err
	})

	// Decimal128.Scale (non-inplace).
	t.Run("d128_scale_up", func(t *testing.T) {
		_, err := maxD128.Scale(18)
		if err == nil {
			t.Fatal("expected overflow")
		}
	})

	// Decimal128.ScaleTruncate.
	t.Run("d128_scaletrunc_up", func(t *testing.T) {
		_, err := maxD128.ScaleTruncate(18)
		if err == nil {
			t.Fatal("expected overflow")
		}
	})

	// Decimal256.Scale: large D256 scaling up.
	maxD256 := Decimal256{B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: 0x7FFFFFFFFFFFFFFF}
	t.Run("d256_scale_up", func(t *testing.T) {
		_, err := maxD256.Scale(18)
		if err == nil {
			t.Fatal("expected overflow")
		}
	})

	// Decimal256.ScaleTruncate.
	t.Run("d256_scaletrunc_up", func(t *testing.T) {
		_, err := maxD256.ScaleTruncate(18)
		if err == nil {
			t.Fatal("expected overflow")
		}
	})
}

// TestDecimalArithOverflowErrors exercises the arithmetic overflow error paths
// where error messages were reformatted.
func TestDecimalArithOverflowErrors(t *testing.T) {
	maxD64 := Decimal64(^uint64(0) >> 1)
	maxD128 := Decimal128{B0_63: ^uint64(0), B64_127: 0x7FFFFFFFFFFFFFFF}
	maxD256 := Decimal256{B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: 0x7FFFFFFFFFFFFFFF}

	// D64 Mul overflow (enters D128 fallback, result too big for D64).
	t.Run("d64_mul_overflow", func(t *testing.T) {
		_, _, err := maxD64.Mul(maxD64, 0, 0)
		if err == nil {
			t.Fatal("expected overflow")
		}
	})

	// D128 Mul overflow (enters D256 fallback, result too big for D128).
	t.Run("d128_mul_overflow", func(t *testing.T) {
		_, _, err := maxD128.Mul(maxD128, 0, 0)
		if err == nil {
			t.Fatal("expected overflow")
		}
	})

	// D64 Div overflow (scale overflow in fallback).
	t.Run("d64_div_scale_overflow", func(t *testing.T) {
		_, _, err := maxD64.Div(Decimal64(1), 0, 0)
		_ = err // exercises the path
	})

	// D128 Div overflow (scale overflow).
	t.Run("d128_div_scale_overflow", func(t *testing.T) {
		_, _, err := maxD128.Div(Decimal128{B0_63: 1}, 0, 0)
		_ = err
	})

	// D64 Mod overflow.
	t.Run("d64_mod_overflow", func(t *testing.T) {
		_, _, err := maxD64.Mod(Decimal64(1), 0, 18)
		_ = err
	})

	// D128 Mod overflow.
	t.Run("d128_mod_overflow", func(t *testing.T) {
		_, _, err := maxD128.Mod(Decimal128{B0_63: 1}, 0, 18)
		_ = err
	})

	// D256 Div overflow.
	t.Run("d256_div_scale_overflow", func(t *testing.T) {
		_, _, err := maxD256.Div(Decimal256{B0_63: 1}, 0, 0)
		_ = err
	})

	// D256 Mod overflow.
	t.Run("d256_mod_overflow", func(t *testing.T) {
		_, _, err := maxD256.Mod(Decimal256{B0_63: 1}, 0, 18)
		_ = err
	})

	// D64 Sub error path.
	t.Run("d64_sub_scale_overflow", func(t *testing.T) {
		_, _, err := maxD64.Sub(Decimal64(1), 0, 18)
		_ = err
	})

	// D128 Sub error path.
	t.Run("d128_sub_scale_overflow", func(t *testing.T) {
		_, _, err := maxD128.Sub(Decimal128{B0_63: 1}, 0, 18)
		_ = err
	})
}

// TestDecimalScaleLoopOverflow exercises the for-loop body in Scale functions
// where n > 19. These are the modified error-message lines inside the loop.
func TestDecimalScaleLoopOverflow(t *testing.T) {
	maxD64 := Decimal64(^uint64(0) >> 1)
	maxD128 := Decimal128{B0_63: ^uint64(0), B64_127: 0x7FFFFFFFFFFFFFFF}
	maxD256 := Decimal256{B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: 0x7FFFFFFFFFFFFFFF}

	// Scale by 38 triggers the loop (38-0 > 19).
	t.Run("d64_loop", func(t *testing.T) {
		_, err := maxD64.Scale(38)
		if err == nil {
			t.Fatal("expected overflow")
		}
	})
	t.Run("d128_scaleinplace_loop", func(t *testing.T) {
		x := maxD128
		err := x.ScaleInplace(38)
		if err == nil {
			t.Fatal("expected overflow")
		}
	})
	t.Run("d128_scale_loop", func(t *testing.T) {
		_, err := maxD128.Scale(38)
		if err == nil {
			t.Fatal("expected overflow")
		}
	})
	t.Run("d128_scaletrunc_loop", func(t *testing.T) {
		_, err := maxD128.ScaleTruncate(38)
		if err == nil {
			t.Fatal("expected overflow")
		}
	})
	t.Run("d256_scale_loop", func(t *testing.T) {
		_, err := maxD256.Scale(38)
		if err == nil {
			t.Fatal("expected overflow")
		}
	})
	t.Run("d256_scaletrunc_loop", func(t *testing.T) {
		_, err := maxD256.ScaleTruncate(38)
		if err == nil {
			t.Fatal("expected overflow")
		}
	})
}
