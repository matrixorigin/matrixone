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
	"math/big"
	"math/rand"
	"testing"
)

func Test2(t *testing.T) {
	x, _, y := Parse128("0xFFFFFFFFFFFFFE")
	if y == nil {
		fmt.Println(x)
	}
}
func Test1(t *testing.T) {
	a1 := rand.Int()
	a2 := rand.Int()
	a3 := rand.Int()
	a4 := rand.Int()
	x := Decimal256{uint64(a1), uint64(a2), 0, 0}
	y := Decimal256{uint64(a3), uint64(a4), 0, 0}
	z := big.NewInt(int64(x.B64_127))
	z1 := big.NewInt(1 << 32)
	z2 := big.NewInt(int64(x.B0_63))
	z1.Mul(z1, z1)
	z.Mul(z, z1).Add(z, z2)
	w := big.NewInt(int64(y.B64_127))
	w2 := big.NewInt(int64(y.B0_63))
	w.Mul(w, z1).Add(w, w2)
	z.Mul(z, w)
	err := error(nil)
	x, err = x.Mul256(y)
	if err != nil {
		panic("wrong")
	}
	if w.Mod(z, z1).Uint64() != x.B0_63 {
		panic("wrong")
	}
	z.Div(z, z1)
	if w.Mod(z, z1).Uint64() != x.B64_127 {
		panic("wrong")
	}
	z.Div(z, z1)
	if w.Mod(z, z1).Uint64() != x.B128_191 {
		panic("wrong")
	}
	z.Div(z, z1)
	if z.Uint64() != x.B192_255 {
		panic("wrong")
	}
}
func TestDecimalFloat(t *testing.T) {
	x := Decimal128{uint64(rand.Int()), uint64(rand.Int())}
	y := Decimal128ToFloat64(x, 30)
	z, _ := Decimal128FromFloat64(y, 38, 7)
	x, _ = x.Scale(-23)
	if x != z {
		fmt.Println(x.B64_127, x.B0_63)
		fmt.Println(z.B64_127, z.B0_63)
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
	y := float64(1.0000001)
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
