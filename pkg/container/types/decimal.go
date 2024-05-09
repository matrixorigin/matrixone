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
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

var Pow10 = [20]uint64{
	1,
	10,
	100,
	1000,
	10000,
	100000,
	1000000,
	10000000,
	100000000,
	1000000000,
	10000000000,
	100000000000,
	1000000000000,
	10000000000000,
	100000000000000,
	1000000000000000,
	10000000000000000,
	100000000000000000,
	1000000000000000000,
	10000000000000000000,
}

var FloatHigh = float64(1<<63) * 2
var Decimal64Min = Decimal64(uint64(1) << 63)
var Decimal64Max = ^Decimal64Min
var Decimal128Min = Decimal128{0, uint64(1) << 63}
var Decimal128Max = Decimal128{^uint64(0), ^Decimal128Min.B64_127}

func (x Decimal64) Sign() bool {
	return x>>63 == 1
}

func (x Decimal128) Sign() bool {
	return x.B64_127>>63 == 1
}

func (x Decimal256) Sign() bool {
	return x.B192_255>>63 == 1
}

func (x Decimal64) Minus() Decimal64 {
	return ^x + 1
}

func (x *Decimal128) MinusInplace() {
	if x.B0_63 == 0 {
		x.B64_127 = ^x.B64_127 + 1
	} else {
		x.B64_127 = ^x.B64_127
		x.B0_63 = ^x.B0_63 + 1
	}
}

func (x Decimal128) Minus() Decimal128 {
	if x.B0_63 == 0 {
		x.B64_127 = ^x.B64_127 + 1
	} else {
		x.B64_127 = ^x.B64_127
		x.B0_63 = ^x.B0_63 + 1
	}
	return x
}

func (x Decimal256) Minus() Decimal256 {
	x.B192_255 = ^x.B192_255
	x.B128_191 = ^x.B128_191
	x.B64_127 = ^x.B64_127
	x.B0_63 = ^x.B0_63 + 1
	if x.B0_63 == 0 {
		x.B64_127++
		if x.B64_127 == 0 {
			x.B128_191++
			if x.B128_191 == 0 {
				x.B192_255++
			}
		}
	}
	return x
}

func (x Decimal64) Less(y Decimal64) bool {
	if x.Sign() != y.Sign() {
		return x.Sign()
	}
	return x < y
}

func (x Decimal128) Less(y Decimal128) bool {
	if x.Sign() != y.Sign() {
		return x.Sign()
	}
	if x.Sign() {
		x = x.Minus()
		y = y.Minus()
		if x.B64_127 != y.B64_127 {
			return x.B64_127 > y.B64_127
		} else {
			return x.B0_63 > y.B0_63
		}
	}
	if x.B64_127 != y.B64_127 {
		return x.B64_127 < y.B64_127
	} else {
		return x.B0_63 < y.B0_63
	}
}

func (x Decimal256) Less(y Decimal256) bool {
	if x.Sign() != y.Sign() {
		return x.Sign()
	}
	if x.Sign() {
		x = x.Minus()
		y = y.Minus()
		if x.B192_255 != y.B192_255 {
			return x.B192_255 > y.B192_255
		} else if x.B128_191 != y.B128_191 {
			return x.B128_191 > y.B128_191
		} else if x.B64_127 != y.B64_127 {
			return x.B64_127 > y.B64_127
		} else {
			return x.B0_63 > y.B0_63
		}
	}
	if x.B192_255 != y.B192_255 {
		return x.B192_255 < y.B192_255
	} else if x.B128_191 != y.B128_191 {
		return x.B128_191 < y.B128_191
	} else if x.B64_127 != y.B64_127 {
		return x.B64_127 < y.B64_127
	} else {
		return x.B0_63 < y.B0_63
	}
}

func (x Decimal64) Compare(y Decimal64) int {
	if x.Less(y) {
		return -1
	} else if x == y {
		return 0
	} else {
		return 1
	}
}

func (x Decimal128) Compare(y Decimal128) int {
	if x.Less(y) {
		return -1
	} else if x == y {
		return 0
	} else {
		return 1
	}
}

func (x Decimal256) Compare(y Decimal256) int {
	if x.Less(y) {
		return -1
	} else if x == y {
		return 0
	} else {
		return 1
	}
}

func CompareDecimal64(x Decimal64, y Decimal64) int {
	return x.Compare(y)
}

func CompareDecimal128(x Decimal128, y Decimal128) int {
	return x.Compare(y)
}

func CompareDecimal64WithScale(x, y Decimal64, scale1, scale2 int32) int {
	if x.Sign() != y.Sign() {
		if x.Sign() {
			return -1
		} else {
			return 1
		}
	}
	var err error
	if scale1 < scale2 {
		x, err = x.Scale(scale2 - scale1)
		if err != nil {
			if x.Sign() {
				return -1
			} else {
				return 1
			}
		}
		return x.Compare(y)
	} else {
		y, err = y.Scale(scale1 - scale2)
		if err != nil {
			if x.Sign() {
				return 1
			} else {
				return -1
			}
		}
		return x.Compare(y)
	}
}

func CompareDecimal128WithScale(x, y Decimal128, scale1, scale2 int32) int {
	if x.Sign() != y.Sign() {
		if x.Sign() {
			return -1
		} else {
			return 1
		}
	}
	var err error
	if scale1 < scale2 {
		x, err = x.Scale(scale2 - scale1)
		if err != nil {
			if x.Sign() {
				return -1
			} else {
				return 1
			}
		}
		return x.Compare(y)
	} else {
		y, err = y.Scale(scale1 - scale2)
		if err != nil {
			if x.Sign() {
				return 1
			} else {
				return -1
			}
		}
		return x.Compare(y)
	}
}

func CompareDecimal256(x Decimal256, y Decimal256) int {
	return x.Compare(y)
}

func (x Decimal64) Lt(y Decimal64) bool {
	return x.Less(y)
}

func (x Decimal128) Lt(y Decimal128) bool {
	return x.Less(y)
}

func (x Decimal64) Left(n int) (y Decimal64) {
	return x << n
}

func (x Decimal64) Right(n int) (y Decimal64) {
	return Decimal64(int64(x) >> n)
}

func (x Decimal128) Left(n int) (y Decimal128) {
	if n > 64 {
		y.B64_127 = x.B0_63 << (n - 64)
		y.B0_63 = 0

	} else {
		y.B64_127 = x.B64_127<<n | x.B0_63>>(64-n)
		y.B0_63 = x.B0_63 << n
	}
	return
}

func (x Decimal128) Right(n int) (y Decimal128) {
	sign := x.Sign()
	if sign {
		x = x.Minus()
	}
	if n > 64 {
		y.B64_127 = 0
		y.B0_63 = x.B64_127 >> (n - 64)
	} else {
		y.B64_127 = x.B64_127 >> n
		y.B0_63 = x.B0_63>>n | x.B64_127<<(64-n)
	}
	if sign {
		y = y.Minus()
	}
	return
}

func (x Decimal256) Left(n int) (y Decimal256) {
	for n > 64 {
		n -= 64
		x.B192_255 = x.B128_191
		x.B128_191 = x.B64_127
		x.B64_127 = x.B0_63
		x.B0_63 = 0
	}
	y.B192_255 = x.B192_255<<n | x.B128_191>>(64-n)
	y.B128_191 = x.B128_191<<n | x.B64_127>>(64-n)
	y.B64_127 = x.B64_127<<n | x.B0_63>>(64-n)
	y.B0_63 = x.B0_63 << n
	return
}

func (x Decimal256) Right(n int) (y Decimal256) {
	sign := x.Sign()
	if sign {
		x = x.Minus()
	}
	for n > 64 {
		n -= 64
		x.B0_63 = x.B64_127
		x.B64_127 = x.B128_191
		x.B128_191 = x.B192_255
		x.B192_255 = 0
	}
	y.B0_63 = x.B0_63>>n | x.B64_127<<(64-n)
	y.B64_127 = x.B64_127>>n | x.B128_191<<(64-n)
	y.B128_191 = x.B128_191>>n | x.B192_255<<(64-n)
	y.B192_255 = x.B192_255 >> n
	if sign {
		y = y.Minus()
	}
	return
}

func (x Decimal64) Scale(n int32) (Decimal64, error) {
	signx := x.Sign()
	x1 := x
	if signx {
		x1 = x1.Minus()
	}
	err := error(nil)
	m := int32(0)
	for n-m > 19 || n-m < -19 {
		if n > 0 {
			m += 19
			x1, err = x1.Mul64(Decimal64(Pow10[19]))
		} else {
			m -= 19
			x1, err = x1.Div64(Decimal64(Pow10[19]))
		}
		if err != nil {
			err = moerr.NewInvalidInputNoCtx("Decimal64 scale overflow: %s(Scale:%d)", x.Format(0), n)
			return x, err
		}
	}
	if n == m {
		if signx {
			x1 = x1.Minus()
		}
		return x1, nil
	}
	if n-m > 0 {
		x1, err = x1.Mul64(Decimal64(Pow10[n-m]))
	} else {
		x1, err = x1.Div64(Decimal64(Pow10[m-n]))
	}
	if err != nil {
		err = moerr.NewInvalidInputNoCtx("Decimal64 scale overflow: %s(Scale:%d)", x.Format(0), n)
		return x, err
	}
	if signx {
		x1 = x1.Minus()
	}
	return x1, err
}

func (x *Decimal128) ScaleInplace(n int32) error {
	if n == 0 {
		return nil
	}
	signx := x.Sign()
	if signx {
		x.MinusInplace()
	}
	m := int32(0)
	var err error
	for n-m > 19 || n-m < -19 {
		if n > 0 {
			m += 19
			err = x.Mul64InPlace(Decimal64(Pow10[19]))
		} else {
			m -= 19
			err = x.Div128InPlace(&Decimal128{Pow10[19], 0})
		}
		if err != nil {
			err = moerr.NewInvalidInputNoCtx("Decimal128 scale overflow: %s(Scale:%d)", x.Format(0), n)
			return err
		}
	}
	if n == m {
		if signx {
			x.MinusInplace()
		}
		return nil
	}
	if n-m > 0 {
		err = x.Mul64InPlace(Decimal64(Pow10[n-m]))
	} else {
		err = x.Div128InPlace(&Decimal128{Pow10[m-n], 0})
	}
	if err != nil {
		err = moerr.NewInvalidInputNoCtx("Decimal128 scale overflow: %s(Scale:%d)", x.Format(0), n)
		return err
	}
	if signx {
		x.MinusInplace()
	}
	return nil
}

func (x Decimal128) Scale(n int32) (Decimal128, error) {
	if n == 0 {
		return x, nil
	}
	signx := x.Sign()
	x1 := x
	if signx {
		x1 = x1.Minus()
	}
	err := error(nil)
	m := int32(0)
	for n-m > 19 || n-m < -19 {
		if n > 0 {
			m += 19
			x1, err = x1.Mul128(Decimal128{Pow10[19], 0})
		} else {
			m -= 19
			x1, err = x1.Div128(Decimal128{Pow10[19], 0})
		}
		if err != nil {
			err = moerr.NewInvalidInputNoCtx("Decimal128 scale overflow: %s(Scale:%d)", x.Format(0), n)
			return x, err
		}
	}
	if n == m {
		if signx {
			x1 = x1.Minus()
		}
		return x1, nil
	}
	if n-m > 0 {
		x1, err = x1.Mul128(Decimal128{Pow10[n-m], 0})
	} else {
		x1, err = x1.Div128(Decimal128{Pow10[m-n], 0})
	}
	if err != nil {
		err = moerr.NewInvalidInputNoCtx("Decimal128 scale overflow: %s(Scale:%d)", x.Format(0), n)
		return x, err
	}
	if signx {
		x1 = x1.Minus()
	}
	return x1, err
}

func (x Decimal256) Scale(n int32) (Decimal256, error) {
	signx := x.Sign()
	x1 := x
	if signx {
		x1 = x1.Minus()
	}
	err := error(nil)
	m := int32(0)
	for n-m > 19 || n-m < -19 {
		if n > 0 {
			m += 19
			x1, err = x1.Mul256(Decimal256{Pow10[19], 0, 0, 0})
		} else {
			m -= 19
			x1, err = x1.Div256(Decimal256{Pow10[19], 0, 0, 0})
		}
		if err != nil {
			err = moerr.NewInvalidInputNoCtx("Decimal256 scale overflow: %s(Scale:%d)", x.Format(0), n)
			return x, err
		}
	}
	if n == m {
		if signx {
			x1 = x1.Minus()
		}
		return x1, nil
	}
	if n-m > 0 {
		x1, err = x1.Mul256(Decimal256{Pow10[n-m], 0, 0, 0})
	} else {
		x1, err = x1.Div256(Decimal256{Pow10[m-n], 0, 0, 0})
	}
	if err != nil {
		err = moerr.NewInvalidInputNoCtx("Decimal256 Scale overflow: %s(Scale:%d)", x.Format(0), n)
		return x, err
	}
	if signx {
		x1 = x1.Minus()
	}
	return x1, err
}

func (x Decimal64) Add64(y Decimal64) (Decimal64, error) {
	signx := x.Sign()
	err := error(nil)
	z := x + y
	if signx == y.Sign() && signx != z.Sign() {
		err = moerr.NewInvalidInputNoCtx("Decimal64 Add overflow: %s+%s", x.Format(0), y.Format(0))
	}
	return z, err
}

func (x Decimal128) Add128(y Decimal128) (Decimal128, error) {
	signx := x.Sign()
	err := error(nil)
	if signx == y.Sign() {
		z := x
		z.B0_63, z.B64_127 = bits.Add64(x.B0_63, y.B0_63, 0)
		z.B64_127, _ = bits.Add64(x.B64_127, y.B64_127, z.B64_127)
		if signx != z.Sign() {
			err = moerr.NewInvalidInputNoCtx("Decimal128 Add overflow: %s+%s", x.Format(0), y.Format(0))
		}
		return z, err
	} else {
		x.B0_63, y.B0_63 = bits.Add64(x.B0_63, y.B0_63, 0)
		x.B64_127, _ = bits.Add64(x.B64_127, y.B64_127, y.B0_63)
	}
	return x, err
}

func (x Decimal256) Add256(y Decimal256) (Decimal256, error) {
	signx := x.Sign()
	err := error(nil)
	if signx == y.Sign() {
		z := x
		z.B0_63, z.B64_127 = bits.Add64(x.B0_63, y.B0_63, 0)
		z.B64_127, z.B128_191 = bits.Add64(x.B64_127, y.B64_127, z.B64_127)
		z.B128_191, z.B192_255 = bits.Add64(x.B128_191, y.B128_191, z.B128_191)
		z.B192_255, _ = bits.Add64(x.B192_255, y.B192_255, z.B192_255)
		if signx != z.Sign() {
			err = moerr.NewInvalidInputNoCtx("Decimal256 Add overflow: %s+%s", x.Format(0), y.Format(0))
		}
		return z, err
	} else {
		x.B0_63, y.B0_63 = bits.Add64(x.B0_63, y.B0_63, 0)
		x.B64_127, y.B64_127 = bits.Add64(x.B64_127, y.B64_127, y.B0_63)
		x.B128_191, y.B128_191 = bits.Add64(x.B128_191, y.B128_191, y.B64_127)
		x.B192_255, _ = bits.Add64(x.B192_255, y.B192_255, y.B128_191)
	}
	return x, err
}

func (x Decimal64) Sub64(y Decimal64) (Decimal64, error) {
	z, err := x.Add64(y.Minus())
	if err != nil {
		err = moerr.NewInvalidInputNoCtx("Decimal64 Sub overflow: %s-%s", x.Format(0), y.Format(0))
	}
	return z, err
}

func (x Decimal128) Sub128(y Decimal128) (Decimal128, error) {
	z, err := x.Add128(y.Minus())
	if err != nil {
		err = moerr.NewInvalidInputNoCtx("Decimal128 Sub overflow: %s-%s", x.Format(0), y.Format(0))
	}
	return z, err
}

func (x Decimal256) Sub256(y Decimal256) (Decimal256, error) {
	z, err := x.Add256(y.Minus())
	if err != nil {
		err = moerr.NewInvalidInputNoCtx("Decimal256 Sub overflow: %s-%s", x.Format(0), y.Format(0))
	}
	return z, err
}

func (x Decimal64) Mul64(y Decimal64) (Decimal64, error) {
	err := error(nil)
	if x == 0 || y == 0 {
		return Decimal64(0), err
	}
	z := x * y
	if z/x != y {
		err = moerr.NewInvalidInputNoCtx("Decimal64 Mul overflow: %s*%s", x.Format(0), y.Format(0))
	}
	return z, err
}

func (x *Decimal128) Mul64InPlace(y Decimal64) error {
	z := Decimal128{0, 0}
	z.B64_127, z.B0_63 = bits.Mul64(x.B0_63, uint64(y))
	w := Decimal128{0, 0}
	if x.B64_127 != 0 {
		w.B0_63, w.B64_127 = bits.Mul64(x.B64_127, uint64(y))
	}
	if w.B0_63 != 0 || w.Sign() || z.Sign() {
		return moerr.NewInvalidInputNoCtx("Decimal128 Mul overflow: %s*%s", x.Format(0), y.Format(0))
	}
	z.B64_127 += w.B64_127
	if z.Sign() {
		return moerr.NewInvalidInputNoCtx("Decimal128 Mul overflow: %s*%s", x.Format(0), y.Format(0))
	}
	*x = z
	return nil
}

func (x *Decimal128) Mul128InPlace(y *Decimal128) error {
	if x.B64_127 != 0 && y.B64_127 != 0 {
		return moerr.NewInvalidInputNoCtx("Decimal128 Mul overflow: %s*%s", x.Format(0), y.Format(0))
	}
	z := Decimal128{0, 0}
	z.B64_127, z.B0_63 = bits.Mul64(x.B0_63, y.B0_63)
	w := Decimal128{0, 0}
	if x.B64_127 == 0 {
		w.B0_63, w.B64_127 = bits.Mul64(x.B0_63, y.B64_127)
	} else {
		w.B0_63, w.B64_127 = bits.Mul64(x.B64_127, y.B0_63)
	}
	if w.B0_63 != 0 || w.Sign() || z.Sign() {
		return moerr.NewInvalidInputNoCtx("Decimal128 Mul overflow: %s*%s", x.Format(0), y.Format(0))
	}
	z.B64_127 += w.B64_127
	if z.Sign() {
		return moerr.NewInvalidInputNoCtx("Decimal128 Mul overflow: %s*%s", x.Format(0), y.Format(0))
	}
	*x = z
	return nil
}

func (x Decimal128) Mul128(y Decimal128) (Decimal128, error) {
	if x.B64_127 != 0 && y.B64_127 != 0 {
		return x, moerr.NewInvalidInputNoCtx("Decimal128 Mul overflow: %s*%s", x.Format(0), y.Format(0))
	}
	z := Decimal128{0, 0}
	z.B64_127, z.B0_63 = bits.Mul64(x.B0_63, y.B0_63)
	w := Decimal128{0, 0}
	if x.B64_127 == 0 {
		w.B0_63, w.B64_127 = bits.Mul64(x.B0_63, y.B64_127)
	} else {
		w.B0_63, w.B64_127 = bits.Mul64(x.B64_127, y.B0_63)
	}
	if w.B0_63 != 0 || w.Sign() || z.Sign() {
		return z, moerr.NewInvalidInputNoCtx("Decimal128 Mul overflow: %s*%s", x.Format(0), y.Format(0))
	}
	z.B64_127 += w.B64_127
	if z.Sign() {
		return z, moerr.NewInvalidInputNoCtx("Decimal128 Mul overflow: %s*%s", x.Format(0), y.Format(0))
	}
	return z, nil
}

func (x Decimal256) Mul256(y Decimal256) (Decimal256, error) {
	if x.B192_255 != 0 && (y.B192_255 != 0 || y.B128_191 != 0 || y.B64_127 != 0) {
		return x, moerr.NewInvalidInputNoCtx("Decimal256 Mul overflow")
	}
	if x.B128_191 != 0 && (y.B192_255 != 0 || y.B128_191 != 0) {
		return x, moerr.NewInvalidInputNoCtx("Decimal256 Mul overflow")
	}
	if x.B64_127 != 0 && y.B192_255 != 0 {
		return x, moerr.NewInvalidInputNoCtx("Decimal256 Mul overflow")
	}
	z := Decimal256{0, 0, 0, 0}
	var hi, lo, ca uint64
	z.B64_127, z.B0_63 = bits.Mul64(x.B0_63, y.B0_63)
	z.B128_191, lo = bits.Mul64(x.B0_63, y.B64_127)
	z.B64_127, ca = bits.Add64(z.B64_127, lo, 0)
	hi, lo = bits.Mul64(x.B64_127, y.B0_63)
	z.B128_191, z.B192_255 = bits.Add64(z.B128_191, hi, ca)
	z.B64_127, ca = bits.Add64(z.B64_127, lo, 0)
	hi, lo = bits.Mul64(x.B128_191, y.B0_63)
	z.B128_191, ca = bits.Add64(z.B128_191, lo, ca)
	z.B192_255, ca = bits.Add64(z.B192_255, hi, ca)
	if ca != 0 {
		return x, moerr.NewInvalidInputNoCtx("Decimal256 Mul overflow")
	}
	hi, lo = bits.Mul64(x.B64_127, y.B64_127)
	z.B128_191, ca = bits.Add64(z.B128_191, lo, 0)
	z.B192_255, ca = bits.Add64(z.B192_255, hi, ca)
	if ca != 0 {
		return x, moerr.NewInvalidInputNoCtx("Decimal256 Mul overflow")
	}
	hi, lo = bits.Mul64(x.B0_63, y.B128_191)
	z.B128_191, ca = bits.Add64(z.B128_191, lo, 0)
	z.B192_255, ca = bits.Add64(z.B192_255, hi, ca)
	if ca != 0 {
		return x, moerr.NewInvalidInputNoCtx("Decimal256 Mul overflow")
	}
	hi, lo = bits.Mul64(x.B192_255, y.B0_63)
	if hi != 0 {
		return x, moerr.NewInvalidInputNoCtx("Decimal256 Mul overflow")
	}
	z.B192_255, ca = bits.Add64(z.B192_255, lo, 0)
	if ca != 0 {
		return x, moerr.NewInvalidInputNoCtx("Decimal256 Mul overflow")
	}
	hi, lo = bits.Mul64(x.B128_191, y.B64_127)
	if hi != 0 {
		return x, moerr.NewInvalidInputNoCtx("Decimal256 Mul overflow")
	}
	z.B192_255, ca = bits.Add64(z.B192_255, lo, 0)
	if ca != 0 {
		return x, moerr.NewInvalidInputNoCtx("Decimal256 Mul overflow")
	}
	hi, lo = bits.Mul64(x.B64_127, y.B128_191)
	if hi != 0 {
		return x, moerr.NewInvalidInputNoCtx("Decimal256 Mul overflow")
	}
	z.B192_255, ca = bits.Add64(z.B192_255, lo, 0)
	if ca != 0 {
		return x, moerr.NewInvalidInputNoCtx("Decimal256 Mul overflow")
	}
	hi, lo = bits.Mul64(x.B0_63, y.B192_255)
	if hi != 0 {
		return x, moerr.NewInvalidInputNoCtx("Decimal256 Mul overflow")
	}
	z.B192_255, ca = bits.Add64(z.B192_255, lo, 0)
	if ca != 0 || z.B192_255>>63 != 0 {
		return x, moerr.NewInvalidInputNoCtx("Decimal256 Mul overflow")
	}
	return z, nil
}

func (x Decimal64) Div64(y Decimal64) (Decimal64, error) {
	if y == 0 {
		return x, moerr.NewInvalidInputNoCtx("Decimal64 Div by Zero: %s/%s", x.Format(0), y.Format(0))
	}
	z := x / y
	if (x-z*y)*2 >= y {
		z++
	}
	return z, nil
}

func (x *Decimal128) Div128InPlace(y *Decimal128) error {
	if y.B0_63 == 0 && y.B64_127 == 0 {
		return moerr.NewInvalidInputNoCtx("Decimal128 Div by Zero: %s/%s", x.Format(0), y.Format(0))
	}
	if y.B64_127 == 0 {
		x.B64_127, y.B64_127 = bits.Div64(0, x.B64_127, y.B0_63)
		x.B0_63, y.B64_127 = bits.Div64(y.B64_127, x.B0_63, y.B0_63)
		if y.B64_127*2 >= y.B0_63 || y.B64_127>>63 != 0 {
			x.B0_63++
			if x.B0_63 == 0 {
				x.B64_127++
			}
		}
	} else {
		if x.Less(*y) {
			x.B64_127 = 0
			x.B0_63 = 0
		} else {
			n := bits.LeadingZeros64(y.B64_127)
			v, _ := bits.Div64(x.B64_127, x.B0_63, y.Right(64-n).B0_63)
			v >>= 63 - n
			if v&1 == 0 {
				x.B0_63 = v >> 1
			} else {
				z, _ := y.Mul128(Decimal128{v, 0})
				if x.Left(1).Less(z) {
					x.B0_63 = v >> 1
				} else {
					x.B0_63 = (v >> 1) + 1
				}
			}
			x.B64_127 = 0
		}
	}
	return nil
}

func (x Decimal128) Div128(y Decimal128) (Decimal128, error) {
	if y.B0_63 == 0 && y.B64_127 == 0 {
		return x, moerr.NewInvalidInputNoCtx("Decimal128 Div by Zero: %s/%s", x.Format(0), y.Format(0))
	}
	if y.B64_127 == 0 {
		x.B64_127, y.B64_127 = bits.Div64(0, x.B64_127, y.B0_63)
		x.B0_63, y.B64_127 = bits.Div64(y.B64_127, x.B0_63, y.B0_63)
		if y.B64_127*2 >= y.B0_63 || y.B64_127>>63 != 0 {
			x.B0_63++
			if x.B0_63 == 0 {
				x.B64_127++
			}
		}
	} else {
		if x.Less(y) {
			x.B64_127 = 0
			x.B0_63 = 0
		} else {
			n := bits.LeadingZeros64(y.B64_127)
			v, _ := bits.Div64(x.B64_127, x.B0_63, y.Right(64-n).B0_63)
			v >>= 63 - n
			if v&1 == 0 {
				x.B0_63 = v >> 1
			} else {
				z, _ := y.Mul128(Decimal128{v, 0})
				if x.Left(1).Less(z) {
					x.B0_63 = v >> 1
				} else {
					x.B0_63 = (v >> 1) + 1
				}
			}
			x.B64_127 = 0
		}
	}
	return x, nil
}

func (x Decimal256) Div256(y Decimal256) (Decimal256, error) {
	if y.B128_191 == 0 && y.B192_255 == 0 && y.B64_127 == 0 {
		if y.B0_63 == 0 {
			return x, moerr.NewInvalidInputNoCtx("Decimal256 Div by Zero")
		}
		x = x.Left(1)
		z := Decimal256{0, 0, 0, 0}
		z.B192_255, z.B128_191 = bits.Div64(0, x.B192_255, y.B0_63)
		z.B128_191, z.B64_127 = bits.Div64(z.B128_191, x.B128_191, y.B0_63)
		z.B64_127, z.B0_63 = bits.Div64(z.B64_127, x.B64_127, y.B0_63)
		z.B0_63, _ = bits.Div64(z.B0_63, x.B0_63, y.B0_63)
		if z.B0_63&1 == 0 {
			z = z.Right(1)
		} else {
			z, _ = z.Right(1).Add256(Decimal256{1, 0, 0, 0})
		}
		return z, nil
	} else {
		x = x.Left(1)
		w := Decimal256{1, 0, 0, 0}
		z := Decimal256{0, 0, 0, 0}
		for y.Compare(x) <= 0 {
			y = y.Left(1)
			w = w.Left(1)
		}
		for y.B0_63 != 0 || y.B64_127 != 0 || y.B128_191 != 0 || y.B192_255 != 0 {
			y = y.Right(1)
			w = w.Right(1)
			if y.Compare(x) <= 0 {
				z, _ = z.Add256(w)
				x, _ = x.Sub256(y)
			}
		}
		if z.B0_63&1 == 0 {
			z = z.Right(1)
		} else {
			z, _ = z.Right(1).Add256(Decimal256{1, 0, 0, 0})
		}
		return z, nil
	}
}

func (x Decimal64) Mod64(y Decimal64) (Decimal64, error) {
	if y == 0 {
		return x, moerr.NewInvalidInputNoCtx("Decimal64 Mod by Zero: %s%%%s", x.Format(0), y.Format(0))
	}
	return x % y, nil
}

func (x Decimal128) Mod128(y Decimal128) (Decimal128, error) {
	if y.B0_63 == 0 && y.B64_127 == 0 {
		return x, moerr.NewInvalidInputNoCtx("Decimal128 Mod by Zero: %s%%%s", x.Format(0), y.Format(0))
	}
	if y.B64_127 == 0 {
		_, x.B64_127 = bits.Div64(0, x.B64_127, y.B0_63)
		_, x.B0_63 = bits.Div64(x.B64_127, x.B0_63, y.B0_63)
		x.B64_127 = 0
	} else {
		if !x.Less(y) {
			n := bits.LeadingZeros64(y.B64_127)
			x1 := x.Right(1)
			v, _ := bits.Div64(x1.B64_127, x1.B0_63, y.Right(64-n).B0_63)
			v >>= 63 - n
			x1, _ = y.Mul128(Decimal128{v - 1, 0})
			x, _ = x.Sub128(x1)
			if !x.Less(y) {
				x, _ = x.Sub128(y)
			}
		}
	}
	return x, nil
}

func (x Decimal256) Mod256(y Decimal256) (Decimal256, error) {
	z, err := x.Div256(y)
	if err != nil {
		return x, err
	}
	z, err = z.Mul256(y)
	if err != nil {
		return x, err
	}
	z, err = x.Sub256(z)
	return z, err
}

func (x Decimal64) Add(y Decimal64, scale1, scale2 int32) (z Decimal64, scale int32, err error) {
	if scale1 > scale2 {
		scale = scale1
		y, err = y.Scale(scale - scale2)
	} else {
		scale = scale2
		x, err = x.Scale(scale - scale1)
	}
	if err == nil {
		z, err = x.Add64(y)
	}
	if err != nil {
		err = moerr.NewInvalidInputNoCtx("Decimal64 Scales in Add overflow: %s(Scale:%d)+%s(Scale:%d)", x.Format(0), scale1, y.Format(0), scale2)
	}
	return
}

func (x Decimal128) Add64(y Decimal64) (Decimal128, error) {
	var err error
	z := x
	z.B0_63, z.B64_127 = bits.Add64(x.B0_63, uint64(y), 0)
	if !y.Sign() {
		z.B64_127, _ = bits.Add64(x.B64_127, 0, z.B64_127)
	} else {
		z.B64_127, _ = bits.Add64(x.B64_127, ^uint64(0), z.B64_127)
	}
	if x.Sign() == y.Sign() && x.Sign() != z.Sign() {
		err = moerr.NewInvalidInputNoCtx("Decimal128 Add overflow: %s+%s", x.Format(0), y.Format(0))
	}
	return z, err
}

// for performance sake, must make sure x and y has same scale, then call this function
func (x *Decimal128) AddInplace(y *Decimal128) (err error) {
	signx := x.Sign()
	var carryout uint64
	if signx == y.Sign() {
		x.B0_63, carryout = bits.Add64(x.B0_63, y.B0_63, 0)
		x.B64_127, _ = bits.Add64(x.B64_127, y.B64_127, carryout)
		if signx != x.Sign() {
			err = moerr.NewInvalidInputNoCtx("Decimal128 Add overflow: %s+%s", x.Format(0), y.Format(0))
			return
		}
	} else {
		x.B0_63, carryout = bits.Add64(x.B0_63, y.B0_63, 0)
		x.B64_127, _ = bits.Add64(x.B64_127, y.B64_127, carryout)
	}
	return
}

func (x Decimal128) Add(y Decimal128, scale1, scale2 int32) (z Decimal128, scale int32, err error) {
	if scale1 > scale2 {
		scale = scale1
		err = y.ScaleInplace(scale - scale2)
	} else if scale1 < scale2 {
		scale = scale2
		err = x.ScaleInplace(scale - scale1)
	}
	if err == nil {
		z, err = x.Add128(y)
	}
	if err != nil {
		err = moerr.NewInvalidInputNoCtx("Decimal128 Scales in Add overflow: %s(Scale:%d)+%s(Scale:%d)", x.Format(0), scale1, y.Format(0), scale2)
	}
	return
}

func (x Decimal64) Sub(y Decimal64, scale1, scale2 int32) (z Decimal64, scale int32, err error) {
	if scale1 > scale2 {
		scale = scale1
		y, err = y.Scale(scale - scale2)
	} else {
		scale = scale2
		x, err = x.Scale(scale - scale1)
	}
	if err == nil {
		z, err = x.Sub64(y)
	}
	if err != nil {
		err = moerr.NewInvalidInputNoCtx("Decimal64 Scales in Sub overflow: %s(Scale:%d)-%s(Scale:%d)", x.Format(0), scale1, y.Format(0), scale2)
	}
	return
}

func (x Decimal128) Sub(y Decimal128, scale1, scale2 int32) (z Decimal128, scale int32, err error) {
	if scale1 > scale2 {
		scale = scale1
		err = y.ScaleInplace(scale - scale2)
	} else if scale1 < scale2 {
		scale = scale2
		err = x.ScaleInplace(scale - scale1)
	}
	if err == nil {
		z, err = x.Sub128(y)
	}
	if err != nil {
		err = moerr.NewInvalidInputNoCtx("Decimal128 Scales in Sub overflow: %s(Scale:%d)-%s(Scale:%d)", x.Format(0), scale1, y.Format(0), scale2)
	}
	return
}

func (x Decimal64) Mul(y Decimal64, scale1, scale2 int32) (z Decimal64, scale int32, err error) {
	scale = 12
	if scale1 > scale {
		scale = scale1
	}
	if scale2 > scale {
		scale = scale2
	}
	if scale1+scale2 < scale {
		scale = scale1 + scale2
	}
	signx := x.Sign()
	x1 := x
	signy := y.Sign()
	y1 := y
	if signx {
		x1 = x1.Minus()
	}
	if signy {
		y1 = y1.Minus()
	}
	z, err = x1.Mul64(y1)
	if err != nil {
		x2 := Decimal128{uint64(x1), 0}
		y2 := Decimal128{uint64(y1), 0}
		x2, _ = x2.Mul128(y2)
		x2, _ = x2.Scale(scale - scale1 - scale2)
		if x2.B64_127 != 0 || x2.B0_63>>63 != 0 {
			err = moerr.NewInvalidInputNoCtx("Decimal64 Mul overflow: %s(Scale:%d)*%s(Scale:%d)", x.Format(0), scale1, y.Format(0), scale2)
			return
		} else {
			err = nil
		}
		z = Decimal64(x2.B0_63)
		if signx != signy {
			z = z.Minus()
		}
		return
	}
	if signx != signy {
		z = z.Minus()
	}
	return
}

func (x *Decimal128) MulInplace(y *Decimal128, scale, scale1, scale2 int32) (err error) {
	signx := x.Sign()
	signy := y.Sign()
	if signx {
		x.MinusInplace()
	}
	if signy {
		tmp := *y
		tmp.MinusInplace()
		y = &tmp
	}
	err = x.Mul128InPlace(y)
	if err != nil {
		x2 := Decimal256{x.B0_63, x.B64_127, 0, 0}
		y2 := Decimal256{y.B0_63, y.B64_127, 0, 0}
		x2, _ = x2.Mul256(y2)
		x2, _ = x2.Scale(scale)
		if x2.B128_191 != 0 || x2.B192_255 != 0 || x2.B64_127>>63 != 0 {
			if signy {
				y.MinusInplace()
			}
			err = moerr.NewInvalidInputNoCtx("Decimal128 Mul overflow: %s(Scale:%d)*%s(Scale:%d)", x.Format(0), scale1, y.Format(0), scale2)
			return
		} else {
			err = nil
		}
		x.B0_63 = x2.B0_63
		x.B64_127 = x2.B64_127
		if signx != signy {
			x.MinusInplace()
		}
		return
	}
	if scale != 0 {
		x.ScaleInplace(scale)
	}
	if signx != signy {
		x.MinusInplace()
	}
	return
}

func (x Decimal128) Mul(y Decimal128, scale1, scale2 int32) (z Decimal128, scale int32, err error) {
	scale = 12
	if scale1 > scale {
		scale = scale1
	}
	if scale2 > scale {
		scale = scale2
	}
	if scale1+scale2 < scale {
		scale = scale1 + scale2
	}
	signx := x.Sign()
	x1 := x
	signy := y.Sign()
	y1 := y
	if signx {
		x1.MinusInplace()
	}
	if signy {
		y1.MinusInplace()
	}
	z, err = x1.Mul128(y1)
	if err != nil {
		x2 := Decimal256{x1.B0_63, x1.B64_127, 0, 0}
		y2 := Decimal256{y1.B0_63, y1.B64_127, 0, 0}
		x2, _ = x2.Mul256(y2)
		x2, _ = x2.Scale(scale - scale1 - scale2)
		if x2.B128_191 != 0 || x2.B192_255 != 0 || x2.B64_127>>63 != 0 {
			err = moerr.NewInvalidInputNoCtx("Decimal128 Mul overflow: %s(Scale:%d)*%s(Scale:%d)", x.Format(0), scale1, y.Format(0), scale2)
			return
		} else {
			err = nil
		}
		z.B0_63 = x2.B0_63
		z.B64_127 = x2.B64_127
		if signx != signy {
			z = z.Minus()
		}
		return
	}
	if scale-scale1-scale2 != 0 {
		z.ScaleInplace(scale - scale1 - scale2)
	}
	if signx != signy {
		z.MinusInplace()
	}
	return
}

func (x Decimal64) Div(y Decimal64, scale1, scale2 int32) (z Decimal64, scale int32, err error) {
	scale = 12
	if scale > scale1+6 {
		scale = scale1 + 6
	}
	if scale < scale1 {
		scale = scale1
	}
	signx := x.Sign()
	x1 := x
	signy := y.Sign()
	y1 := y
	if signx {
		x1 = x1.Minus()
	}
	if signy {
		y1 = y1.Minus()
	}
	z, err = x1.Scale(scale - scale1 + scale2)
	if err != nil {
		x2 := Decimal128{uint64(x1), 0}
		y2 := Decimal128{uint64(y1), 0}
		x2, err = x2.Scale(scale - scale1 + scale2)
		if err != nil {
			err = moerr.NewInvalidInputNoCtx("Decimal64 Scales in Div overflow: %s(Scale:%d)/%s(Scale:%d)", x.Format(0), scale1, y.Format(0), scale2)
			return
		}
		x2, err = x2.Div128(y2)
		if err != nil || x2.B64_127 != 0 || x2.B0_63>>63 != 0 {
			err = moerr.NewInvalidInputNoCtx("Decimal64 Div overflow: %s(Scale:%d)/%s(Scale:%d)", x.Format(0), scale1, y.Format(0), scale2)
		}
		z = Decimal64(x2.B0_63)
		return
	}
	z, err = z.Div64(y1)
	if signx != signy {
		z = z.Minus()
	}
	return
}

func (x Decimal128) Div(y Decimal128, scale1, scale2 int32) (z Decimal128, scale int32, err error) {
	scale = 12
	if scale > scale1+6 {
		scale = scale1 + 6
	}
	if scale < scale1 {
		scale = scale1
	}
	signx := x.Sign()
	x1 := x
	signy := y.Sign()
	y1 := y
	if signx {
		x1 = x1.Minus()
	}
	if signy {
		y1 = y1.Minus()
	}
	z, err = x1.Scale(scale - scale1 + scale2)
	if err != nil {
		x2 := Decimal256{x1.B0_63, x1.B64_127, 0, 0}
		y2 := Decimal256{y1.B0_63, y1.B64_127, 0, 0}
		x2, err = x2.Scale(scale - scale1 + scale2)
		if err != nil {
			err = moerr.NewInvalidInputNoCtx("Decimal128 Div overflow: %s(Scale:%d)/%s(Scale:%d)", x.Format(0), scale1, y.Format(0), scale2)
			return
		}
		x2, err = x2.Div256(y2)
		if err != nil || x2.B192_255 != 0 || x2.B128_191 != 0 || x2.B64_127>>63 != 0 {
			err = moerr.NewInvalidInputNoCtx("Decimal128 Div overflow: %s(Scale:%d)/%s(Scale:%d)", x.Format(0), scale1, y.Format(0), scale2)
		}
		z.B0_63 = x2.B0_63
		z.B64_127 = x2.B64_127
		return
	}
	z, err = z.Div128(y1)
	if signx != signy {
		z = z.Minus()
	}
	return
}

func (x Decimal64) Mod(y Decimal64, scale1, scale2 int32) (z Decimal64, scale int32, err error) {
	signx := x.Sign()
	x1 := x
	signy := y.Sign()
	y1 := y
	if signx {
		x1 = x1.Minus()
	}
	if signy {
		y1 = y1.Minus()
	}
	if scale1 > scale2 {
		scale = scale1
		y1, err = y1.Scale(scale - scale2)
	} else {
		scale = scale2
		x1, err = x1.Scale(scale - scale1)
	}
	if err != nil {
		x2 := Decimal128{uint64(x1), 0}
		y2 := Decimal128{uint64(y1), 0}
		if scale1 > scale2 {
			scale = scale1
			y2, err = y2.Scale(scale - scale2)
		} else {
			scale = scale2
			x2, err = x2.Scale(scale - scale1)
		}
		if err != nil {
			err = moerr.NewInvalidInputNoCtx("Decimal64 Mod overflow: %s(Scale:%d)%%%s(Scale:%d)", x.Format(0), scale1, y.Format(0), scale2)
			return
		}
		x2, err = x2.Mod128(y2)
		z = Decimal64(x2.B0_63)
		if signx {
			z = z.Minus()
		}
		return
	}
	z, err = x1.Mod64(y1)
	if signx {
		z = z.Minus()
	}
	return
}

func (x Decimal128) Mod(y Decimal128, scale1, scale2 int32) (z Decimal128, scale int32, err error) {
	signx := x.Sign()
	x1 := x
	signy := y.Sign()
	y1 := y
	if signx {
		x1 = x1.Minus()
	}
	if signy {
		y1 = y1.Minus()
	}
	if scale1 > scale2 {
		scale = scale1
		y1, err = y1.Scale(scale - scale2)
	} else {
		scale = scale2
		x1, err = x1.Scale(scale - scale1)
	}
	if err != nil {
		x2 := Decimal256{x1.B0_63, x1.B64_127, 0, 0}
		y2 := Decimal256{y1.B0_63, y1.B64_127, 0, 0}
		if scale1 > scale2 {
			scale = scale1
			y2, err = y2.Scale(scale - scale2)
		} else {
			scale = scale2
			x2, err = x2.Scale(scale - scale1)
		}
		if err != nil {
			err = moerr.NewInvalidInputNoCtx("Decimal128 Mod overflow: %s(Scale:%d)%%%s(Scale:%d)", x.Format(0), scale1, y.Format(0), scale2)
			return
		}
		x2, err = x2.Mod256(y2)
		z.B0_63 = x2.B0_63
		z.B64_127 = x2.B64_127
		if signx {
			z = z.Minus()
		}
		return
	}
	z, err = x1.Mod128(y1)
	if signx {
		z = z.Minus()
	}
	return
}

func Decimal64ToFloat64(x Decimal64, scale int32) float64 {
	signx := x.Sign()
	if signx {
		x = x.Minus()
	}
	y := float64(x)
	for scale > 19 {
		scale -= 19
		y /= float64(Pow10[19])
	}
	y /= float64(Pow10[scale])
	if signx {
		y = -y
	}
	return y
}

func Decimal128ToFloat64(x Decimal128, scale int32) float64 {
	signx := x.Sign()
	if signx {
		x = x.Minus()
	}
	y := float64(x.B64_127) * FloatHigh
	y += float64(x.B0_63)
	for scale > 19 {
		scale -= 19
		y /= float64(Pow10[19])
	}
	y /= float64(Pow10[scale])
	if signx {
		y = -y
	}
	return y
}

func Decimal64FromFloat64(x float64, width, scale int32) (y Decimal64, err error) {
	err = nil
	if x != x || x+1 == x || width > 18 || width < 1 || scale > width || scale < 0 {
		err = moerr.NewInvalidInputNoCtx("Can't convert Float64 To Decimal64: %f(%d,%d)", x, width, scale)
		return
	}
	z := x
	signx := false
	if x < 0 {
		z = -z
		signx = true
	}
	for scale > 19 {
		scale -= 19
		z *= float64(Pow10[19])
	}
	z *= float64(Pow10[scale])
	if z > float64(Pow10[width]) {
		err = moerr.NewInvalidInputNoCtx("Can't convert Float64 To Decimal64: %f(%d,%d)", x, width, scale)
		return
	}
	y = Decimal64(uint64(z * 2))
	if y&1 == 1 {
		y++
	}
	y >>= 1
	if uint64(y) >= Pow10[width] {
		err = moerr.NewInvalidInputNoCtx("Can't convert Float64 To Decimal64: %f(%d,%d)", x, width, scale)
		return
	}
	if signx {
		y = y.Minus()
	}
	return
}

func Decimal128FromFloat64(x float64, width, scale int32) (y Decimal128, err error) {
	err = nil
	if math.IsInf(x, 0) || math.IsNaN(x) {
		err = moerr.NewInvalidInputNoCtx("Can't convert Float64 To Decimal128, Float64 is Inf or NaN")
		return
	}
	if width > 38 || width < 1 || scale > width || scale < 0 {
		err = moerr.NewInvalidInputNoCtx("Can't convert Float64 To Decimal128: %f(%d,%d)", x, width, scale)
		return
	}
	z := x
	signx := false
	n := scale
	if x < 0 {
		z = -z
		signx = true
	}
	for n > 19 {
		n -= 19
		z *= float64(Pow10[19])
	}
	z *= float64(Pow10[n])
	if z > FloatHigh {
		n = 0
		for z > FloatHigh {
			z /= 10
			n++
		}
		if z < float64(Pow10[19]) {
			if n > 19 || width-n < 19 {
				err = moerr.NewInvalidInputNoCtx("Can't convert Float64 To Decimal128: %f(%d,%d)", x, width, scale)
				return
			}
		} else {
			if n > 18 || width-n < 20 {
				err = moerr.NewInvalidInputNoCtx("Can't convert Float64 To Decimal128: %f(%d,%d)", x, width, scale)
				return
			}
		}
		y.B64_127 = 0
		y.B0_63 = uint64(z)
		y, _ = y.Scale(n)
	} else {
		if z*2 > FloatHigh || uint64(z*2)&1 == 0 {
			y.B64_127 = 0
			y.B0_63 = uint64(z)
		} else {
			y.B64_127 = 0
			y.B0_63 = uint64(z) + 1
		}
		if width <= 19 && y.B0_63 >= Pow10[width] {
			err = moerr.NewInvalidInputNoCtx("Can't convert Float64 To Decimal128: %f(%d,%d)", x, width, scale)
			return
		}
	}
	if signx {
		y = y.Minus()
	}
	return
}

func Decimal256FromInt64(x int64) Decimal256 {
	return Decimal256{uint64(x), 0, 0, 0}
}

func Decimal256FromDecimal128(x Decimal128) Decimal256 {
	y := Decimal256{x.B0_63, x.B64_127, 0, 0}
	if x.Sign() {
		y.B128_191 = ^y.B128_191
		y.B192_255 = ^y.B192_255
	}
	return y
}

func Decimal256ToFloat64(x Decimal256, scale int32) float64 {
	sign := x.Sign()
	if sign {
		x = x.Minus()
	}
	for x.B128_191 != 0 || x.B192_255 != 0 {
		x, _ = x.Scale(-1)
		scale--
	}
	y := Decimal128ToFloat64(Decimal128{x.B0_63, x.B64_127}, scale)
	if sign {
		y = -y
	}
	return y
}

func Parse64(x string) (y Decimal64, scale int32, err error) {
	y = Decimal64(0)
	z := Decimal64(0)
	scale = -1
	width := 0
	i := 0
	flag := false
	t := false
	floatflag := false
	scalecount := int32(0)
	scalesign := false
	signx := false
	err = nil
	if x[0] == '-' {
		i++
		signx = true
	}
	for i < len(x) {
		if x[i] == ' ' {
			i++
			continue
		}
		if x[i] == 'x' && i != 0 && x[i-1] == '0' && y == 0 {
			t = true
			i++
			continue
		}
		if t {
			if (x[i] >= '0' && x[i] <= '9') || (x[i] >= 'a' && x[i] <= 'f') || (x[i] >= 'A' && x[i] <= 'F') {
				xx := uint64(0)
				if x[i] >= '0' && x[i] <= '9' {
					xx = uint64(x[i] - '0')
				} else if x[i] >= 'a' && x[i] <= 'f' {
					xx = uint64(x[i]-'a') + 10
				} else {
					xx = uint64(x[i]-'A') + 10
				}
				flag = true
				z, err = y.Mul64(Decimal64(16))
				if err == nil {
					y, err = z.Add64(Decimal64(xx))
				}
				if err != nil {
					err = moerr.NewInvalidInputNoCtx("%s beyond the range, can't be converted to Decimal64.", x)
					return
				}
			} else {
				err = moerr.NewInvalidInputNoCtx("%s is illegal string, can't be converted to Decimal64.", x)
				return
			}
			i++
			continue
		}
		if x[i] == '.' {
			if scale == -1 {
				scale = 0
			} else {
				err = moerr.NewInvalidInputNoCtx("%s is illegal string, can't be converted to Decimal64.", x)
				return
			}
		} else if x[i] < '0' || x[i] > '9' {
			if x[i] == 'e' {
				floatflag = true
				i++
				continue
			}
			if x[i] == '-' && floatflag {
				scalesign = true
				i++
				continue
			}
			err = moerr.NewInvalidInputNoCtx("%s is illegal string, can't be converted to Decimal64.", x)
			return
		} else if !floatflag {
			if width == 18 {
				if scale == -1 {
					err = moerr.NewInvalidInputNoCtx("%s beyond the range, can't be converted to Decimal64.", x)
					return
				}
				if x[i] >= '5' {
					y, _ = y.Add64(1)
					if y1, _ := y.Mod64(10); y1 == 0 {
						scale--
						y, _ = y.Scale(-1)
					}
				}
				break
			}
			flag = true
			z = y
			y = y*10 + Decimal64(x[i]-'0')
			if y>>63 != 0 || y/10 != z {
				err = moerr.NewInvalidInputNoCtx("%s beyond the range, can't be converted to Decimal64.", x)
				return
			}
			width++
			if scale != -1 {
				scale++
			}
		} else {
			scalecount = scalecount*10 + int32(x[i]-'0')
		}
		i++
	}
	if !flag {
		err = moerr.NewInvalidInputNoCtx("%s is illegal string, can't be converted to Decimal64.", x)
		return
	}
	if scale == -1 {
		scale = 0
	}
	if floatflag {
		if scalesign {
			scalecount = -scalecount
		}
		scale -= scalecount
		if scale < 0 {
			y, err = y.Scale(-scale)
			scale = 0
			if err != nil {
				err = moerr.NewInvalidInputNoCtx("%s beyond the range, can't be converted to Decimal64.", x)
			}
		}
	}
	if signx {
		y = y.Minus()
	}
	return
}

func ParseDecimal64(x string, width, scale int32) (y Decimal64, err error) {
	if width > 18 {
		width = 18
	}
	n := int32(0)
	y, n, err = Parse64(x)
	if err != nil {
		err = moerr.NewInvalidInputNoCtx("%s beyond the range, can't be converted to Decimal64(%d,%d).", x, width, scale)
		return
	}
	if n > scale {
		y, _ = y.Scale(scale - n)
	} else {
		y, err = y.Scale(scale - n)
		if err != nil {
			err = moerr.NewInvalidInputNoCtx("%s beyond the range, can't be converted to Decimal64(%d,%d).", x, width, scale)
			return
		}
	}
	if y.Sign() {
		if y.Less(Decimal64(Pow10[width]).Minus()) {
			err = moerr.NewInvalidInputNoCtx("%s beyond the range, can't be converted to Decimal64(%d,%d).", x, width, scale)
			return
		}
	} else {
		if !y.Less(Decimal64(Pow10[width])) {
			err = moerr.NewInvalidInputNoCtx("%s beyond the range, can't be converted to Decimal64(%d,%d).", x, width, scale)
			return
		}
	}
	return
}

func ParseDecimal64FromByte(x string, width, scale int32) (y Decimal64, err error) {
	y = 0
	err = nil
	n := len(x)
	for i := 0; i < n; i++ {
		y, err = y.Mul64(256)
		if err != nil {
			return
		}
		y, err = y.Add64(Decimal64(x[i]))
		if err != nil {
			return
		}
	}
	return
}

func Parse128(x string) (y Decimal128, scale int32, err error) {
	var z Decimal128
	width := 0
	t := false
	scale = -1
	i := 0
	flag := false
	floatflag := false
	scalecount := int32(0)
	scalesign := false
	signx := false
	if x[0] == '-' {
		i++
		signx = true
	}
	for i < len(x) {
		if x[i] == ' ' || x[i] == '+' {
			i++
			continue
		}
		if x[i] == 'x' && i != 0 && x[i-1] == '0' && y.B0_63 == 0 && y.B64_127 == 0 {
			t = true
			i++
			continue
		}
		if t {
			if (x[i] >= '0' && x[i] <= '9') || (x[i] >= 'a' && x[i] <= 'f') || (x[i] >= 'A' && x[i] <= 'F') {
				xx := uint64(0)
				if x[i] >= '0' && x[i] <= '9' {
					xx = uint64(x[i] - '0')
				} else if x[i] >= 'a' && x[i] <= 'f' {
					xx = uint64(x[i]-'a') + 10
				} else {
					xx = uint64(x[i]-'A') + 10
				}
				flag = true
				z, err = y.Mul128(Decimal128{16, 0})
				if err == nil {
					y, err = z.Add128(Decimal128{xx, 0})
				}
				if err != nil {
					err = moerr.NewInvalidInputNoCtx("%s beyond the range, can't be converted to Decimal128.", x)
					return
				}
			} else {
				err = moerr.NewInvalidInputNoCtx("%s is illegal string, can't be converted to Decimal128.", x)
				return
			}
			i++
			continue
		}
		if x[i] == '.' {
			if scale == -1 {
				scale = 0
			} else {
				err = moerr.NewInvalidInputNoCtx("%s is illegal string, can't be converted to Decimal128.", x)
				return
			}
		} else if x[i] < '0' || x[i] > '9' {
			if x[i] == 'e' {
				floatflag = true
				i++
				continue
			}
			if x[i] == '-' && floatflag {
				scalesign = true
				i++
				continue
			}
			err = moerr.NewInvalidInputNoCtx("%s is illegal string, can't be converted to Decimal128.", x)
			return
		} else if !floatflag {
			if width == 38 {
				if scale == -1 {
					err = moerr.NewInvalidInputNoCtx("%s beyond the range, can't be converted to Decimal128.", x)
					return
				}
				if x[i] >= '5' {
					y, err = y.Add128(Decimal128{1, 0})
					if y1, _ := y.Mod128(Decimal128{10, 0}); y1.B0_63 == 0 {
						scale--
						y, _ = y.Scale(-1)
					}
				}
				break
			}
			flag = true
			z, err = y.Mul128(Decimal128{Pow10[1], 0})
			if err == nil {
				y, err = z.Add128(Decimal128{uint64(x[i] - '0'), 0})
			}
			if err != nil {
				err = moerr.NewInvalidInputNoCtx("%s beyond the range, can't be converted to Decimal128.", x)
				return
			}
			width++
			if scale != -1 {
				scale++
			}
		} else {
			scalecount = scalecount*10 + int32(x[i]-'0')
		}
		i++
	}
	if !flag {
		err = moerr.NewInvalidInputNoCtx("%s is illegal string, can't be converted to Decimal128.", x)
		return
	}
	if scale == -1 {
		scale = 0
	}
	if floatflag {
		if scalesign {
			scalecount = -scalecount
		}
		scale -= scalecount
		if scale < 0 {
			y, err = y.Scale(-scale)
			scale = 0
			if err != nil {
				err = moerr.NewInvalidInputNoCtx("%s beyond the range, can't be converted to Decimal128.", x)
			}
		}
	}
	if signx {
		y = y.Minus()
	}
	return
}

func ParseDecimal128(x string, width, scale int32) (y Decimal128, err error) {
	if width > 38 {
		width = 38
	}
	n := int32(0)
	y, n, err = Parse128(x)
	if err != nil {
		err = moerr.NewInvalidInputNoCtx("%s beyond the range, can't be converted to Decimal128(%d,%d).", x, width, scale)
		return
	}
	if n > scale {
		y, _ = y.Scale(scale - n)
	} else {
		y, err = y.Scale(scale - n)
		if err != nil {
			err = moerr.NewInvalidInputNoCtx("%s beyond the range, can't be converted to Decimal128(%d,%d).", x, width, scale)
			return
		}
	}
	var z Decimal128
	if width <= 19 {
		z = Decimal128{Pow10[width], 0}
	} else {
		z, _ = Decimal128{Pow10[19], 0}.Mul128(Decimal128{Pow10[width-19], 0})
	}
	if y.Sign() {
		if y.Less(z.Minus()) {
			err = moerr.NewInvalidInputNoCtx("%s beyond the range, can't be converted to Decimal128(%d,%d).", x, width, scale)
			return
		}
	} else {
		if !y.Less(z) {
			err = moerr.NewInvalidInputNoCtx("%s beyond the range, can't be converted to Decimal128(%d,%d).", x, width, scale)
			return
		}
	}
	return
}

func ParseDecimal128FromByte(x string, width, scale int32) (y Decimal128, err error) {
	y = Decimal128{0, 0}
	err = nil
	n := len(x)
	for i := 0; i < n; i++ {
		y, err = y.Mul128(Decimal128{256, 0})
		if err != nil {
			return
		}
		y, err = y.Add128(Decimal128{uint64(x[i]), 0})
		if err != nil {
			return
		}
	}
	return
}

func (x Decimal64) Format(scale int32) string {
	a := ""
	signx := (x >> 63) != 0
	if signx {
		x = x.Minus()
	}
	for x != 0 {
		a = fmt.Sprintf("%c", x%10+'0') + a
		x /= 10
		scale--
		if scale == 0 {
			a = "." + a
		}
	}
	for scale > 0 {
		a = "0" + a
		scale--
		if scale == 0 {
			a = "." + a
		}
	}
	if scale == 0 {
		a = "0" + a
	}
	if signx {
		a = "-" + a
	}
	return a
}

func (x Decimal128) Format(scale int32) string {
	a := ""
	signx := x.Sign()
	if signx {
		x = x.Minus()
	}
	for x.B64_127 != 0 || x.B0_63 != 0 {
		y, _ := x.Mod128(Decimal128{Pow10[1], 0})
		a = fmt.Sprintf("%c", y.B0_63+'0') + a
		x, _ = x.Div128(Decimal128{Pow10[1], 0})
		if y.B0_63 >= 5 {
			x, _ = x.Sub128(Decimal128{1, 0})
		}
		scale--
		if scale == 0 {
			a = "." + a
		}
	}
	for scale > 0 {
		a = "0" + a
		scale--
		if scale == 0 {
			a = "." + a
		}
	}
	if scale == 0 {
		a = "0" + a
	}
	if signx {
		a = "-" + a
	}
	return a
}

func (x Decimal256) Format(scale int32) string {
	a := ""
	return a
}

func (x Decimal64) Ceil(scale1, scale2 int32) Decimal64 {
	if x.Sign() {
		return x.Minus().Floor(scale1, scale2).Minus()
	}
	if scale1 > scale2 {
		k := scale1 - scale2
		if k > 18 {
			k = 18
		}
		y, _, _ := x.Mod(Decimal64(1), k, 0)
		if y != 0 {
			x, _ = x.Sub64(y)
			x, _, _ = x.Add(Decimal64(1), k, 0)
		}
	}
	return x
}
func (x Decimal64) Floor(scale1, scale2 int32) Decimal64 {
	if x.Sign() {
		return x.Minus().Ceil(scale1, scale2).Minus()
	}
	if scale1 > scale2 {
		k := scale1 - scale2
		if k > 18 {
			k = 18
		}
		y, _, _ := x.Mod(Decimal64(1), k, 0)
		x, _ = x.Sub64(y)
	}
	return x
}

func (x Decimal64) Round(scale1, scale2 int32) Decimal64 {
	if scale2 >= scale1 {
		return x
	}
	k := scale1 - scale2
	if k > 18 {
		k = 18
	}
	x, _ = x.Scale(-k)
	x, _ = x.Scale(k)
	return x
}

func (x Decimal128) Ceil(scale1, scale2 int32) Decimal128 {
	if x.Sign() {
		return x.Minus().Floor(scale1, scale2).Minus()
	}
	if scale1 > scale2 {
		k := scale1 - scale2
		if k > 38 {
			k = 38
		}
		y, _, _ := x.Mod(Decimal128{1, 0}, k, 0)
		if y.B0_63 != 0 || y.B64_127 != 0 {
			x, _ = x.Sub128(y)
			x, _, _ = x.Add(Decimal128{1, 0}, k, 0)
		}
	}
	return x
}
func (x Decimal128) Floor(scale1, scale2 int32) Decimal128 {
	if x.Sign() {
		return x.Minus().Ceil(scale1, scale2).Minus()
	}
	if scale1 > scale2 {
		k := scale1 - scale2
		if k > 38 {
			k = 38
		}
		y, _, _ := x.Mod(Decimal128{1, 0}, k, 0)
		x, _ = x.Sub128(y)
	}
	return x
}

func (x Decimal128) Round(scale1, scale2 int32) Decimal128 {
	if scale2 >= scale1 {
		return x
	}
	k := scale1 - scale2
	if k > 38 {
		k = 38
	}
	x, _ = x.Scale(-k)
	x, _ = x.Scale(k)
	return x
}

// ProtoSize is used by gogoproto.
func (x *Decimal64) ProtoSize() int {
	return 8
}

// MarshalToSizedBuffer is used by gogoproto.
func (x *Decimal64) MarshalToSizedBuffer(data []byte) (int, error) {
	if len(data) < x.ProtoSize() {
		panic("invalid byte slice")
	}
	binary.BigEndian.PutUint64(data[0:], uint64(*x))
	return 8, nil
}

// MarshalTo is used by gogoproto.
func (x *Decimal64) MarshalTo(data []byte) (int, error) {
	size := x.ProtoSize()
	return x.MarshalToSizedBuffer(data[:size])
}

// Marshal is used by gogoproto.
func (x *Decimal64) Marshal() ([]byte, error) {
	data := make([]byte, x.ProtoSize())
	n, err := x.MarshalToSizedBuffer(data)
	if err != nil {
		return nil, err
	}
	return data[:n], err
}

// Unmarshal is used by gogoproto.
func (x *Decimal64) Unmarshal(data []byte) error {
	*x = Decimal64(binary.BigEndian.Uint64(data))
	return nil
}

// ProtoSize is used by gogoproto.
func (x *Decimal128) ProtoSize() int {
	return 8 + 8
}

func (x *Decimal128) MarshalToSizedBuffer(data []byte) (int, error) {
	if len(data) < x.ProtoSize() {
		panic("invalid byte slice")
	}
	binary.BigEndian.PutUint64(data[0:], x.B0_63)
	binary.BigEndian.PutUint64(data[8:], x.B64_127)
	return 16, nil
}

// MarshalTo is used by gogoproto.
func (x *Decimal128) MarshalTo(data []byte) (int, error) {
	size := x.ProtoSize()
	return x.MarshalToSizedBuffer(data[:size])
}

// Marshal is used by gogoproto.
func (x *Decimal128) Marshal() ([]byte, error) {
	data := make([]byte, x.ProtoSize())
	n, err := x.MarshalToSizedBuffer(data)
	if err != nil {
		return nil, err
	}
	return data[:n], err
}

// Unmarshal is used by gogoproto.
func (x *Decimal128) Unmarshal(data []byte) error {
	if len(data) < x.ProtoSize() {
		panic("invalid byte slice")
	}
	x.B0_63 = binary.BigEndian.Uint64(data[0:])
	x.B64_127 = binary.BigEndian.Uint64(data[8:])
	return nil
}
