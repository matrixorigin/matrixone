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
func TestParse128(t *testing.T) {
	x, y := ParseDecimal128("99999.999999999999999999999999999999999", 12, 6)
	if y != nil || x.B0_63 != 100000000000 {
		panic("Decimal128Parse wrong")
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

func TestDecimal256AddSub(t *testing.T) {
	x := Decimal256{uint64(rand.Int()), uint64(rand.Int()), uint64(rand.Int()), uint64(rand.Int()) >> 1}
	z := x
	err := error(nil)
	y := Decimal256{uint64(rand.Int()), uint64(rand.Int()), uint64(rand.Int()), uint64(rand.Int()) >> 1}
	x, _, err = x.Add(y, 0, 0)
	if err == nil {
		x, _, err = x.Sub(y, 0, 0)
	}
	if err != nil || x != z {
		panic("Decimal256AddSub wrong")
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
	x, _, _ := ParseDecimal128FromString("99999999999999999999999999999999999999")
	y, _, _ := ParseDecimal128FromString("10000000000")
	z, _, err := x.Div(y, 0, 0)
	if err != nil || z.Format(0) != "10000000000000000000000000000000000" {
		panic("wrong")
	}
}

func TestDecimal256MulDiv(t *testing.T) {
	x := Decimal256{uint64(rand.Int()), uint64(rand.Int()), uint64(rand.Int()), 0}
	z := x
	err := error(nil)
	y := Decimal256{uint64(rand.Int()), 0, 0, 0}
	x, _, err = x.Mul(y, 0, 0)
	if err == nil {
		x, _, err = x.Div(y, 12, 0)
	}
	if err != nil || x != z {
		fmt.Println(err)
		panic("Decimal256MulDiv wrong")
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
