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

/*
#include "mo.h"

#cgo CFLAGS: -I../../../cgo
#cgo LDFLAGS: -L../../../cgo -lmo -lm

*/
import "C"

import (
	"encoding/hex"
	"strconv"
	"strings"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	// Max string buf + 1 (\0) needed by decimal64
	DECIMAL64_ZSTR_LEN = 25
	DECIMAL64_WIDTH    = 16
	DECIMAL64_NBYTES   = 8
	// Max string buf + 1 (\0) needed by decimal128
	DECIMAL128_ZSTR_LEN = 43
	DECIMAL128_WIDTH    = 34
	DECIMAL128_NBYTES   = 16
	MYSQL_DEFAULT_SCALE = 4
)

func dec64PtrToC(p *Decimal64) *C.int64_t {
	return (*C.int64_t)(unsafe.Pointer(p))
}
func dec128PtrToC(p *Decimal128) *C.int64_t {
	return (*C.int64_t)(unsafe.Pointer(p))
}
func bytesPtrToC(bs []byte) *C.char {
	return (*C.char)(unsafe.Pointer(&bs[0]))
}
func zstrToString(p []byte) string {
	str := string(p)
	zidx := strings.IndexByte(str, 0)
	if zidx > 0 {
		str = str[:zidx]
	}
	return str
}

func Decimal64FromInt64Raw(a int64) Decimal64 {
	var d Decimal64
	var pd = unsafe.Pointer(&d)
	var pda = (*int64)(pd)
	*pda = a
	return d
}
func Decimal128FromInt64Raw(a, b int64) Decimal128 {
	var d Decimal128
	var pd = unsafe.Pointer(&d)
	var pda = (*int64)(pd)
	var pdb = (*int64)(unsafe.Add(pd, 8))
	*pda = a
	*pdb = b
	return d
}

func Decimal64ToInt64Raw(d Decimal64) int64 {
	var pd = unsafe.Pointer(&d)
	var pda = (*int64)(pd)
	return *pda
}

func Decimal128ToInt64Raw(d Decimal128) (int64, int64) {
	var pd = unsafe.Pointer(&d)
	var pda = (*int64)(pd)
	var pdb = (*int64)(unsafe.Add(pd, 8))
	return *pda, *pdb
}

var Decimal64_Zero Decimal64 = Decimal64FromInt32(0)
var Decimal64_One Decimal64 = Decimal64FromInt32(1)
var Decimal64_Two Decimal64 = Decimal64FromInt32(2)
var Decimal64_Three Decimal64 = Decimal64FromInt32(3)
var Decimal64_Ten Decimal64 = Decimal64FromInt32(10)
var Decimal64Min = Decimal64_NegInf()
var Decimal64Max = Decimal64_Inf()
var Decimal128_Zero Decimal128 = Decimal128FromInt32(0)
var Decimal128_One Decimal128 = Decimal128FromInt32(1)
var Decimal128_Two Decimal128 = Decimal128FromInt32(2)
var Decimal128_Three Decimal128 = Decimal128FromInt32(3)
var Decimal128_Ten Decimal128 = Decimal128FromInt32(10)
var Decimal128Min = Decimal128_NegInf()
var Decimal128Max = Decimal128_Inf()

// Return a null terminated copy of the string.
func zstr(s string) []byte {
	trims := strings.TrimSpace(s)
	buf := make([]byte, len(trims)+1)
	copy(buf, []byte(trims))
	return buf
}

// Conversions, from go type to decimal64
func Decimal64_FromInt32(i int32) Decimal64 {
	var d Decimal64
	C.Decimal64_FromInt32(dec64PtrToC(&d), C.int32_t(i))
	return d
}

func Decimal64_FromUint32(i uint32) Decimal64 {
	var d Decimal64
	C.Decimal64_FromUint32(dec64PtrToC(&d), C.uint32_t(i))
	return d
}

func Decimal64_FromInt64(i int64, width, scale int32) (Decimal64, error) {
	if width == 0 {
		width = 34
	}
	var d Decimal64
	rc := C.Decimal64_FromInt64(dec64PtrToC(&d), C.int64_t(i), C.int32_t(width-scale))
	if rc != 0 {
		return d, moerr.NewInvalidArgNoCtx("decimal64", i)
	}
	return d, nil
}
func Decimal64_FromUint64(i uint64, width, scale int32) (Decimal64, error) {
	if width == 0 {
		width = 34
	}
	var d Decimal64
	rc := C.Decimal64_FromUint64(dec64PtrToC(&d), C.uint64_t(i), C.int32_t(width-scale))
	if rc != 0 {
		return d, moerr.NewInvalidArgNoCtx("decimal64", i)
	}
	return d, nil
}

func Decimal64_FromFloat64(f float64, width int32, scale int32) (Decimal64, error) {
	var d Decimal64
	rc := C.Decimal64_FromFloat64(dec64PtrToC(&d), C.double(f), C.int32_t(width), C.int32_t(scale))
	if rc != 0 {
		return d, moerr.NewInvalidArgNoCtx("decimal64", f)
	}
	return d, nil
}

func Decimal64_FromString(s string) (Decimal64, error) {
	var d Decimal64
	buf := zstr(s)
	rc := uint16(C.Decimal64_FromString(dec64PtrToC(&d), bytesPtrToC(buf)))
	if rc == moerr.ErrDataTruncated {
		return d, moerr.NewDataTruncatedNoCtx("DECIMAL64", "%v", s)
	} else if rc != 0 {
		return d, moerr.NewInvalidArgNoCtx("DECIMAL64", s)
	}
	return d, nil
}
func Decimal64_FromStringWithScale(s string, width, scale int32) (Decimal64, error) {
	var d Decimal64
	buf := zstr(s)
	rc := uint16(C.Decimal64_FromStringWithScale(dec64PtrToC(&d), bytesPtrToC(buf), C.int32_t(width), C.int32_t(scale)))
	if rc != 0 {
		return d, moerr.NewInvalidArgNoCtx("DECIMAL64", s)
	}
	return d, nil
}

// Conversions, from go type to decimal128
func Decimal128_FromInt32(i int32) Decimal128 {
	var d Decimal128
	C.Decimal128_FromInt32(dec128PtrToC(&d), C.int32_t(i))
	return d
}
func Decimal128_FromUint32(i uint32) Decimal128 {
	var d Decimal128
	C.Decimal128_FromUint32(dec128PtrToC(&d), C.uint32_t(i))
	return d
}

func Decimal128_FromInt64(i int64, width, scale int32) (Decimal128, error) {
	if width == 0 {
		width = 34
	}
	var d Decimal128
	rc := C.Decimal128_FromInt64(dec128PtrToC(&d), C.int64_t(i), C.int32_t(width-scale))
	if rc != 0 {
		return d, moerr.NewInvalidArgNoCtx("decimal128", i)
	}
	return d, nil
}
func Decimal128_FromUint64(i uint64, width, scale int32) (Decimal128, error) {
	if width == 0 {
		width = 34
	}
	var d Decimal128
	rc := C.Decimal128_FromUint64(dec128PtrToC(&d), C.uint64_t(i), C.int32_t(width-scale))
	if rc != 0 {
		return d, moerr.NewInvalidArgNoCtx("decimal128", i)
	}
	return d, nil
}

func Decimal128_FromFloat64(f float64, width, scale int32) (Decimal128, error) {
	var d Decimal128
	rc := C.Decimal128_FromFloat64(dec128PtrToC(&d), C.double(f), C.int32_t(width), C.int32_t(scale))
	if rc != 0 {
		return d, moerr.NewInvalidArgNoCtx("decimal128", f)
	}
	return d, nil
}
func Decimal128_FromString(s string) (Decimal128, error) {
	var d Decimal128
	buf := zstr(s)
	rc := uint16(C.Decimal128_FromString(dec128PtrToC(&d), bytesPtrToC(buf)))
	if rc == moerr.ErrDataTruncated {
		return d, moerr.NewDataTruncatedNoCtx("DECIMAL64", "%v", s)
	} else if rc != 0 {
		return d, moerr.NewInvalidArgNoCtx("decimal128", s)
	}
	return d, nil
}

func Decimal128_FromStringWithScale(s string, width, scale int32) (Decimal128, error) {
	var d Decimal128
	buf := zstr(s)
	rc := C.Decimal128_FromStringWithScale(dec128PtrToC(&d), bytesPtrToC(buf), C.int32_t(width), C.int32_t(scale))
	if rc != 0 {
		return d, moerr.NewInvalidArgNoCtx("decimal128", s)
	}
	return d, nil
}

// Conversions. decimal to go types.
func (d Decimal64) ToFloat64() float64 {
	var ret C.double
	rc := C.Decimal64_ToFloat64(&ret, dec64PtrToC(&d))
	if rc == 0 {
		return float64(ret)
	}
	panic(moerr.NewInvalidArgNoCtx("deciaml64 to float64", d))
}
func (d Decimal64) ToInt64() int64 {
	var ret C.int64_t
	rc := C.Decimal64_ToInt64(&ret, dec64PtrToC(&d))
	if rc == 0 {
		return int64(ret)
	}
	panic(moerr.NewInvalidArgNoCtx("deciaml64 to int64", d))
}

func (d Decimal64) String() string {
	return d.ToString()
}
func (d Decimal64) ToString() string {
	buf := make([]byte, DECIMAL64_ZSTR_LEN)
	C.Decimal64_ToString(bytesPtrToC(buf), dec64PtrToC(&d))
	return zstrToString(buf)
}
func (d Decimal64) ToStringWithScale(scale int32) string {
	buf := make([]byte, DECIMAL64_ZSTR_LEN)
	C.Decimal64_ToStringWithScale(bytesPtrToC(buf), dec64PtrToC(&d), C.int32_t(scale))
	return zstrToString(buf)
}

func (d Decimal128) ToFloat64() float64 {
	var ret C.double
	rc := C.Decimal128_ToFloat64(&ret, dec128PtrToC(&d))
	if int32(rc) == 0 {
		return float64(ret)
	}
	panic(moerr.NewInvalidArgNoCtx("deciaml128 to float64", d))
}
func (d Decimal128) ToInt64() int64 {
	var ret C.int64_t
	rc := C.Decimal128_ToInt64(&ret, dec128PtrToC(&d))
	if rc == 0 {
		return int64(ret)
	}
	panic(moerr.NewInvalidArgNoCtx("deciaml128 to int64", d))
}

func (d Decimal128) String() string {
	return d.ToString()
}
func (d Decimal128) ToString() string {
	buf := make([]byte, DECIMAL128_ZSTR_LEN)
	C.Decimal128_ToString(bytesPtrToC(buf), dec128PtrToC(&d))
	return zstrToString(buf)
}
func (d Decimal128) ToStringWithScale(scale int32) string {
	buf := make([]byte, DECIMAL128_ZSTR_LEN)
	C.Decimal128_ToStringWithScale(bytesPtrToC(buf), dec128PtrToC(&d), C.int32_t(scale))
	return zstrToString(buf)
}

func Decimal128_FromDecimal64(d64 Decimal64) Decimal128 {
	var d Decimal128
	C.Decimal64_ToDecimal128(dec128PtrToC(&d), dec64PtrToC(&d64))
	return d
}

func Decimal128_FromDecimal64WithScale(d64 Decimal64, width, scale int32) (Decimal128, error) {
	var d Decimal128
	rc := C.Decimal64_ToDecimal128WithScale(dec128PtrToC(&d), dec64PtrToC(&d64), C.int32_t(width), C.int32_t(scale))
	if rc != 0 {
		return d, moerr.NewOutOfRangeNoCtx("decimal128", "%v", d64)
	}
	return d, nil
}

func (d Decimal128) ToDecimal64(width, scale int32) (Decimal64, error) {
	var d64 Decimal64
	rc := C.Decimal128_ToDecimal64WithScale(dec64PtrToC(&d64), dec128PtrToC(&d), C.int32_t(width), C.int32_t(scale))
	if rc != 0 {
		return d64, moerr.NewOutOfRangeNoCtx("decimal64", "%v", d)
	}
	return d64, nil
}

// Comapres
func CompareDecimal64(a, b Decimal64) int {
	var rc, ret C.int32_t
	rc = C.Decimal64_Compare(&ret, dec64PtrToC(&a), dec64PtrToC(&b))
	if rc != 0 {
		panic(moerr.NewInvalidArgNoCtx("decimal64 compare", ""))
	}
	return int(ret)
}

func CompareDecimal128(a, b Decimal128) int {
	var rc, ret C.int32_t
	rc = C.Decimal128_Compare(&ret, dec128PtrToC(&a), dec128PtrToC(&b))
	if rc != 0 {
		panic(moerr.NewInvalidArgNoCtx("decimal128 compare", ""))
	}
	return int(ret)
}

func (d Decimal64) Compare(other Decimal64) int {
	return CompareDecimal64(d, other)
}
func (d Decimal64) Eq(other Decimal64) bool {
	return d.Compare(other) == 0
}
func (d Decimal64) Le(other Decimal64) bool {
	return d.Compare(other) <= 0
}
func (d Decimal64) Lt(other Decimal64) bool {
	return d.Compare(other) < 0
}
func (d Decimal64) Ge(other Decimal64) bool {
	return d.Compare(other) >= 0
}
func (d Decimal64) Gt(other Decimal64) bool {
	return d.Compare(other) > 0
}
func (d Decimal64) Ne(other Decimal64) bool {
	return d.Compare(other) != 0
}

func (d Decimal128) Compare(other Decimal128) int {
	return CompareDecimal128(d, other)
}
func (d Decimal128) Eq(other Decimal128) bool {
	return d.Compare(other) == 0
}
func (d Decimal128) Le(other Decimal128) bool {
	return d.Compare(other) <= 0
}
func (d Decimal128) Lt(other Decimal128) bool {
	return d.Compare(other) < 0
}
func (d Decimal128) Ge(other Decimal128) bool {
	return d.Compare(other) >= 0
}
func (d Decimal128) Gt(other Decimal128) bool {
	return d.Compare(other) > 0
}
func (d Decimal128) Ne(other Decimal128) bool {
	return d.Compare(other) != 0
}

// Arithmatics
func (d Decimal64) Add(x Decimal64) Decimal64 {
	var ret Decimal64
	rc := C.Decimal64_Add(dec64PtrToC(&ret), dec64PtrToC(&d), dec64PtrToC(&x))
	if rc != 0 {
		panic(moerr.NewOutOfRangeNoCtx("decimal64", "decimal add overflow"))
	}
	return ret
}
func (d Decimal128) Add(x Decimal128) Decimal128 {
	var ret Decimal128
	rc := C.Decimal128_Add(dec128PtrToC(&ret), dec128PtrToC(&d), dec128PtrToC(&x))
	if rc != 0 {
		panic(moerr.NewOutOfRangeNoCtx("decimal128", "decimal add overflow"))
	}
	return ret
}

func (d Decimal64) AddInt64(i int64) Decimal64 {
	var ret Decimal64
	rc := C.Decimal64_AddInt64(dec64PtrToC(&ret), dec64PtrToC(&d), C.int64_t(i))
	if rc != 0 {
		panic(moerr.NewOutOfRangeNoCtx("decimal64", "decimal add overflow"))
	}
	return ret
}
func (d Decimal128) AddInt64(i int64) Decimal128 {
	var ret Decimal128
	rc := C.Decimal128_AddInt64(dec128PtrToC(&ret), dec128PtrToC(&d), C.int64_t(i))
	if rc != 0 {
		panic(moerr.NewOutOfRangeNoCtx("decimal128", "decimal add overflow"))
	}
	return ret
}
func (d Decimal128) AddDecimal64(d64 Decimal64) Decimal128 {
	var ret Decimal128
	rc := C.Decimal128_AddDecimal64(dec128PtrToC(&ret), dec128PtrToC(&d), dec64PtrToC(&d64))
	if rc != 0 {
		panic(moerr.NewOutOfRangeNoCtx("decimal128", "decimal add overflow"))
	}
	return ret
}

func (d Decimal64) Sub(x Decimal64) Decimal64 {
	var ret Decimal64
	rc := C.Decimal64_Sub(dec64PtrToC(&ret), dec64PtrToC(&d), dec64PtrToC(&x))
	if rc != 0 {
		panic(moerr.NewOutOfRangeNoCtx("decimal64", "decimal sub overflow"))
	}
	return ret
}
func (d Decimal128) Sub(x Decimal128) Decimal128 {
	var ret Decimal128
	rc := C.Decimal128_Sub(dec128PtrToC(&ret), dec128PtrToC(&d), dec128PtrToC(&x))
	if rc != 0 {
		panic(moerr.NewOutOfRangeNoCtx("decimal128", "decimal sub overflow"))
	}
	return ret
}

func (d Decimal64) SubInt64(i int64) Decimal64 {
	var ret Decimal64
	rc := C.Decimal64_SubInt64(dec64PtrToC(&ret), dec64PtrToC(&d), C.int64_t(i))
	if rc != 0 {
		panic(moerr.NewOutOfRangeNoCtx("decimal64", "decimal sub overflow"))
	}
	return ret
}
func (d Decimal128) SubInt64(i int64) Decimal128 {
	var ret Decimal128
	rc := C.Decimal128_SubInt64(dec128PtrToC(&ret), dec128PtrToC(&d), C.int64_t(i))
	if rc != 0 {
		panic(moerr.NewOutOfRangeNoCtx("decimal128", "decimal sub overflow"))
	}
	return ret
}

func (d Decimal64) Mul(x Decimal64) Decimal64 {
	var ret Decimal64
	rc := C.Decimal64_Mul(dec64PtrToC(&ret), dec64PtrToC(&d), dec64PtrToC(&x))
	if rc != 0 {
		panic(moerr.NewOutOfRangeNoCtx("decimal64", "decimal mul overflow"))
	}
	return ret
}
func (d Decimal64) MulWiden(x Decimal64) Decimal128 {
	var ret Decimal128
	rc := C.Decimal64_MulWiden(dec128PtrToC(&ret), dec64PtrToC(&d), dec64PtrToC(&x))
	if rc != 0 {
		panic(moerr.NewOutOfRangeNoCtx("decimal64", "decimal mul overflow"))
	}
	return ret
}
func (d Decimal128) Mul(x Decimal128) Decimal128 {
	var ret Decimal128
	rc := C.Decimal128_Mul(dec128PtrToC(&ret), dec128PtrToC(&d), dec128PtrToC(&x))
	if rc != 0 {
		panic(moerr.NewOutOfRangeNoCtx("decimal128", "decimal mul overflow"))
	}
	return ret
}

func (d Decimal64) MulInt64(x int64) Decimal64 {
	var ret Decimal64
	rc := C.Decimal64_MulInt64(dec64PtrToC(&ret), dec64PtrToC(&d), C.int64_t(x))
	if rc != 0 {
		panic(moerr.NewOutOfRangeNoCtx("decimal64", "decimal mul overflow"))
	}
	return ret
}
func (d Decimal128) MulInt64(x int64) Decimal128 {
	var ret Decimal128
	rc := C.Decimal128_MulInt64(dec128PtrToC(&ret), dec128PtrToC(&d), C.int64_t(x))
	if rc != 0 {
		panic(moerr.NewOutOfRangeNoCtx("decimal128", "decimal mul overflow"))
	}
	return ret
}

func (d Decimal64) Div(x Decimal64) Decimal64 {
	var ret Decimal64
	rc := C.Decimal64_Div(dec64PtrToC(&ret), dec64PtrToC(&d), dec64PtrToC(&x))
	if rc != 0 {
		panic(moerr.NewOutOfRangeNoCtx("decimal64", "decimal div overflow"))
	}
	return ret
}
func (d Decimal64) DivWiden(x Decimal64) Decimal128 {
	var ret Decimal128
	rc := C.Decimal64_DivWiden(dec128PtrToC(&ret), dec64PtrToC(&d), dec64PtrToC(&x))
	if rc != 0 {
		panic(moerr.NewOutOfRangeNoCtx("decimal64", "decimal div overflow"))
	}
	return ret
}
func (d Decimal128) Div(x Decimal128) Decimal128 {
	var ret Decimal128
	rc := C.Decimal128_Div(dec128PtrToC(&ret), dec128PtrToC(&d), dec128PtrToC(&x))
	if rc != 0 {
		panic(moerr.NewOutOfRangeNoCtx("decimal128", "decimal div overflow"))
	}
	return ret
}

func (d Decimal64) DivInt64(x int64) Decimal64 {
	var ret Decimal64
	rc := C.Decimal64_DivInt64(dec64PtrToC(&ret), dec64PtrToC(&d), C.int64_t(x))
	if rc != 0 {
		panic(moerr.NewOutOfRangeNoCtx("decimal64", "decimal div overflow"))
	}
	return ret
}
func (d Decimal128) DivInt64(x int64) Decimal128 {
	var ret Decimal128
	rc := C.Decimal128_DivInt64(dec128PtrToC(&ret), dec128PtrToC(&d), C.int64_t(x))
	if rc != 0 {
		panic(moerr.NewOutOfRangeNoCtx("decimal128", "decimal div overflow"))
	}
	return ret
}

// Wrap old decimal api.   Most likely we should delete them.
func ParseStringToDecimal64(s string, width int32, scale int32, isBin bool) (Decimal64, error) {
	var res Decimal64
	if isBin {
		ss := hex.EncodeToString(*(*[]byte)(unsafe.Pointer(&s)))
		ival, err := strconv.ParseUint(ss, 16, 64)
		if err != nil {
			return res, err
		}
		return Decimal64_FromUint64(ival, width, scale)
	}
	return Decimal64_FromStringWithScale(s, width, scale)
}

func ParseStringToDecimal128(s string, width int32, scale int32, isBin bool) (Decimal128, error) {
	var res Decimal128
	if isBin {
		ss := hex.EncodeToString(*(*[]byte)(unsafe.Pointer(&s)))
		ival, err := strconv.ParseUint(ss, 16, 64)
		if err != nil {
			return res, err
		}
		return Decimal128_FromUint64(ival, width, scale)
	}
	return Decimal128_FromStringWithScale(s, width, scale)
}

func MustDecimal64FromString(s string) Decimal64 {
	d, err := Decimal64_FromString(s)
	if err != nil {
		panic(err)
	}
	return d
}
func MustDecimal128FromString(s string) Decimal128 {
	d, err := Decimal128_FromString(s)
	if err != nil {
		panic(err)
	}
	return d
}

func Decimal64_Inf() Decimal64 {
	return MustDecimal64FromString("inf")
}
func Decimal64_NegInf() Decimal64 {
	return MustDecimal64FromString("-inf")
}
func Decimal64_NaN() Decimal64 {
	return MustDecimal64FromString("NaN")
}

func Decimal128_Inf() Decimal128 {
	return MustDecimal128FromString("inf")
}
func Decimal128_NegInf() Decimal128 {
	return MustDecimal128FromString("-inf")
}
func Decimal128_NaN() Decimal128 {
	return MustDecimal128FromString("NaN")
}

func Decimal64FromInt32(i int32) Decimal64 {
	return Decimal64_FromInt32(i)
}
func Decimal128FromInt32(i int32) Decimal128 {
	return Decimal128_FromInt32(i)
}
func Decimal64FromFloat64(f float64, width, scale int32) (Decimal64, error) {
	return Decimal64_FromFloat64(f, width, scale)
}
func Decimal128FromFloat64(f float64, width, scale int32) (Decimal128, error) {
	return Decimal128_FromFloat64(f, width, scale)
}

func InitDecimal128(i int64, width, scale int32) (Decimal128, error) {
	return Decimal128_FromInt64(i, width, scale)
}
func InitDecimal128UsingUint(i uint64, width, scale int32) (Decimal128, error) {
	return Decimal128_FromUint64(i, width, scale)
}

func InitDecimal64(i int64, width, scale int32) (Decimal64, error) {
	return Decimal64_FromInt64(i, width, scale)
}
func InitDecimal64UsingUint(i uint64, width, scale int32) (Decimal64, error) {
	return Decimal64_FromUint64(i, width, scale)
}

func Decimal64Add(a, b Decimal64, s1, s2 int32) Decimal64 {
	return a.Add(b)
}
func Decimal128Add(a, b Decimal128, s1, s2 int32) Decimal128 {
	return a.Add(b)
}

func Decimal64AddAligned(a, b Decimal64) Decimal64 {
	return a.Add(b)
}
func Decimal128AddAligned(a, b Decimal128) Decimal128 {
	return a.Add(b)
}

func Decimal64Sub(a, b Decimal64, s1, s2 int32) Decimal64 {
	return a.Sub(b)
}
func Decimal128Sub(a, b Decimal128, s1, s2 int32) Decimal128 {
	return a.Sub(b)
}

func Decimal64SubAligned(a, b Decimal64) Decimal64 {
	return a.Sub(b)
}
func Decimal128SubAligned(a, b Decimal128) Decimal128 {
	return a.Sub(b)
}

func Decimal64Decimal64Mul(a, b Decimal64) Decimal128 {
	return a.MulWiden(b)
}
func Decimal128Decimal128Mul(a, b Decimal128) Decimal128 {
	return a.Mul(b)
}

func Decimal64Int64Mul(a Decimal64, b int64) Decimal64 {
	return a.MulInt64(b)
}
func Decimal128Int64Mul(a Decimal128, b int64) Decimal128 {
	return a.MulInt64(b)
}

func Decimal64Decimal64Div(a, b Decimal64) Decimal128 {
	return a.DivWiden(b)
}
func Decimal128Decimal128Div(a, b Decimal128) Decimal128 {
	return a.Div(b)
}

func Decimal128Int64Div(a Decimal128, b int64) Decimal128 {
	return a.DivInt64(b)
}

func AlignDecimal128UsingScaleDiffBatch(src, dst []Decimal128, _ int32) []Decimal128 {
	copy(dst, src)
	return dst
}

func AlignDecimal64UsingScaleDiffBatch(src, dst []Decimal64, _ int32) []Decimal64 {
	copy(dst, src)
	return dst
}

func ParseStringToDecimal64WithoutTable(s string, isBin ...bool) (Decimal64, int32, error) {
	var ss string
	if len(isBin) > 0 && isBin[0] {
		// binary string
		ss = strings.TrimSpace(hex.EncodeToString(*(*[]byte)(unsafe.Pointer(&s))))
	} else {
		ss = strings.TrimSpace(s)
	}
	d, err := Decimal64_FromString(ss)
	var scale int32
	idx := int32(strings.LastIndex(ss, "."))
	if idx >= 0 {
		scale = int32(len(ss)) - idx - 1
	}
	return d, scale, err
}

func ParseStringToDecimal128WithoutTable(s string, isBin ...bool) (Decimal128, int32, error) {
	var ss string
	if len(isBin) > 0 && isBin[0] {
		// binary string
		ss = strings.TrimSpace(hex.EncodeToString(*(*[]byte)(unsafe.Pointer(&s))))
	} else {
		ss = strings.TrimSpace(s)
	}
	d, err := Decimal128_FromString(ss)

	var scale int32
	idx := int32(strings.LastIndex(ss, "."))
	if idx >= 0 {
		scale = int32(len(ss)) - idx - 1
	}
	return d, scale, err
}

func CompareDecimal64Decimal64(a, b Decimal64, s1, s2 int32) int64 {
	return int64(a.Compare(b))
}
func CompareDecimal128Decimal128(a, b Decimal128, s1, s2 int32) int64 {
	return int64(a.Compare(b))
}
func CompareDecimal64Decimal64Aligned(a, b Decimal64) int64 {
	return int64(a.Compare(b))
}
func CompareDecimal128Decimal128Aligned(a, b Decimal128) int64 {
	return int64(a.Compare(b))
}

func Decimal64IsZero(d Decimal64) bool {
	return d.Compare(Decimal64_Zero) == 0
}
func Decimal128IsZero(d Decimal128) bool {
	return d.Compare(Decimal128_Zero) == 0
}

func NegDecimal128(d Decimal128) Decimal128 {
	return Decimal128_Zero.Sub(d)
}
