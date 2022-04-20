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

// MatrixOne's decimal(fixed point numeric) data type
// https://github.com/matrixorigin/matrixone/issues/1867
// in many database management systems, the DECIMAL type may also referred as NUMERIC.
//
// Usage: Decimal(precision, scale), Decimal(precision), Decimal
// creat table t1 (a decimal);       the default precision 10, default scale 0 is applied, this is the same as "create table t1 (a decimal(10, 0));
// create table t1(a decimal(20);	 the default scale 0 is applied, that means, this is the same as create table t1(a decimal(20, 0);
// create table t1(a decimal(20, 5);
//
// MatrixOne supports decimal data type with precision range (0, 38], scale range (0, 38], and scale <= precision.
// internally, precision in range 0~18 is represented as Decimal64, precision in range 19~38 is represented as Decimal128
//
// we support addition, subtraction, multiplication, division for decimal data type

// for decimal64, addition and subtraction operation
// have result of type decimal64, the result's scale is the maximum of its two operands, overflow may happen in these two operations and no precaution is implemented,
// nor does any indication. for multiplication and division on decimal64, the result is of type Decimal128 and therefore these two operations are safe on Decimal64, that is,
// overflow is guaranteed IMPOSSIBLE.
//
// For Decimal128, operations on this data type may overflow and no precautions nor indications are implemented, but these will only happen in extreme use cases, that is, when the result can not fit into a 128 bit representation
//
// Comparison operations <, >, =, !=, <=, >= is also supported in decimal type
//
// in the case where a literal string needs to be interpreted as decimal, for example, "select * from decimal_table where a = 1.23",
// it will be interpreted as decimal128
// for operations between decimals and integers, the result is of type decimal128
// operations between decimals and floats are not defined.

package types

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unsafe"
)

// #include <stdint.h>
// #include <stdbool.h>
// void add_int128_int64(void* a, void* b, void* result) {
//      *(__int128*)result = *(__int128*)a + (*(int64_t*)b);
//      return ;
// }
// void add_int128_int128(void* a, void* b, void* result) {
//      *(__int128*)result = (*(__int128*)a) + (*(__int128*)b);
//      return ;
// }
// void sub_int128_int64(void* a, void* b, void* result) {
//      *(__int128*)result = *(__int128*)a - (*(int64_t*)b);
//      return ;
// }
// void sub_int128_int128(void* a, void* b, void* result) {
//      *(__int128*)result = (*(__int128*)a) - (*(__int128*)b);
//      return ;
// }
// void mul_int128_int64(void* a, void* b, void* result) {
//      *(__int128*)result = *(__int128*)a * (*(int64_t*)b);
//      return ;
// }
// void mul_int128_int128(void* a, void* b, void* result) {
//      *(__int128*)result = (*(__int128*)a) * (*(__int128*)b);
//      return ;
// }
// void mul_int64_int64_ret_int128(void* a, void* b, void* result) {
// 		__int128 aInInt128 = *(int64_t*)a;
// 		__int128 bInInt128 = *(int64_t*)b;
//      *(__int128*)result = aInInt128 * bInInt128;
//      return ;
// }
// void div_int128_int64(void* a, void* b, void* result) {
//      *(__int128*)result = *(__int128*)a / (*(int64_t*)b);
//      return ;
// }
// void div_int128_int128(void* a, void* b, void* result) {
//      *(__int128*)result = (*(__int128*)a) / (*(__int128*)b);
//      return ;
// }
// void mod_int128_int64(void* a, void* b, void* result) {
//      *(int64_t*)result = (*(__int128*)a) % (*(int64_t*)b);
// }
// // The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
// void cmp_int128_int64(void* a, void* b, void* result) {
//      if ((*(__int128*)a) < (*(int64_t*)b)) {
//          *(int*)result = -1;
//      } else if ((*(__int128*)a) == (*(int64_t*)b)) {
//          *(int*)result = 0;
//      } else {
//          *(int*)result = 1;
//      }
//      return;
// }
// // The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
// void cmp_int128_int128(void* a, void* b, void* result) {
//      if ((*(__int128*)a) < (*(__int128*)b)) {
//          *(int64_t*)result = -1;
//      } else if ((*(__int128*)a) == (*(__int128*)b)) {
//          *(int64_t*)result = 0;
//      } else {
//          *(int64_t*)result = 1;
//      }
//      return;
// }
// void scale_int128_by_10(void* a, void* result) {
//      *(__int128*)result = (*(__int128*)a) * 10;
// }
// void div_int128_by_10(void* a, void* result) {
//      *(__int128*)result = (*(__int128*)a) / 10;
// }
// void mod_int128_by_10_abs(void* a, void* result) {
//      *(int64_t*)result = (*(__int128*)a) % 10;
// 		if (*(int64_t*)result < 0) {
// 			*(int64_t*)result = - *(int64_t*)result;
// 		}
// }
// void int128_is_negative(void* a, void* result) {
//      *(bool*)result = (*(__int128*)a) < 0;
// }
// void int128_is_zero(void* a, void* result) {
//      *(bool*)result = (*(__int128*)a) == 0;
// }
// void int128_is_not_zero(void* a, void* result) {
//      *(bool*)result = (*(__int128*)a) != 0;
// }
// void init_int128_as_1(void* a) {
//      *(__int128*)a = 1;
// }
// void init_int128_as_minus_1(void* a) {
//      *(__int128*)a = -1;
// }
// void neg_int128(void* a, void* result) {
//      *(__int128*)result = - *(__int128*)a;
// }
// void int64_to_int128(void*a, void* result) {
// 		*(__int128*)result = *(int64_t*)a;
// }
import "C"

func ScaleDecimal64(a Decimal64, b int64) (result Decimal64) {
	return Decimal64(int64(a) * b)
}

func ScaleDecimal64By10(a Decimal64) (result Decimal64) {
	return Decimal64(int64(a) * 10)
}

func CompareDecimal64Decimal64Aligned(a, b Decimal64) (result int64) {
	if int64(a) < int64(b) {
		return -1
	} else if int64(a) == int64(b) {
		return 0
	} else {
		return 1
	}
}

func CompareDecimal64Decimal64(a, b Decimal64, aScale, bScale int32) (result int64) {
	if aScale > bScale {
		scaleDiff := aScale - bScale
		scale := int64(math.Pow10(int(scaleDiff)))
		bScaled := ScaleDecimal64(b, scale)
		result = CompareDecimal64Decimal64Aligned(a, bScaled)
	} else if aScale < bScale {
		scaleDiff := bScale - aScale
		scale := int64(math.Pow10(int(scaleDiff)))
		aScaled := ScaleDecimal64(a, scale)
		result = CompareDecimal64Decimal64Aligned(aScaled, b)
	} else {
		result = CompareDecimal64Decimal64Aligned(a, b)
	}
	return result
}

func CompareDecimal128Decimal128Aligned(a, b Decimal128) (result int64) {
	C.cmp_int128_int128(unsafe.Pointer(&a), unsafe.Pointer(&b), unsafe.Pointer(&result))
	return result
}

func CompareDecimal128Decimal128(a, b Decimal128, aScale, bScale int32) (result int64) {
	if aScale > bScale {
		scaleDiff := aScale - bScale
		bScaled := b
		for i := 0; i < int(scaleDiff); i++ {
			bScaled = ScaleDecimal128By10(b)
		}
		result = CompareDecimal128Decimal128Aligned(a, bScaled)
	} else if aScale < bScale {
		scaleDiff := bScale - aScale
		aScaled := a
		for i := 0; i < int(scaleDiff); i++ {
			aScaled = ScaleDecimal128By10(aScaled)
		}
		result = CompareDecimal128Decimal128Aligned(aScaled, b)
	} else {
		result = CompareDecimal128Decimal128Aligned(a, b)
	}
	return result
}

func ScaleDecimal128By10(a Decimal128) (result Decimal128) {
	C.scale_int128_by_10(unsafe.Pointer(&a), unsafe.Pointer(&result))
	return result
}

func DivideDecimal128By10(a Decimal128) (result Decimal128) {
	C.div_int128_by_10(unsafe.Pointer(&a), unsafe.Pointer(&result))
	return result
}

func Decimal64Decimal64Mul(a Decimal64, b Decimal64) (result Decimal128) {
	C.mul_int64_int64_ret_int128(unsafe.Pointer(&a), unsafe.Pointer(&b), unsafe.Pointer(&result))
	return result
}

func Decimal128Decimal128Mul(a Decimal128, b Decimal128) (result Decimal128) {
	C.mul_int128_int128(unsafe.Pointer(&a), unsafe.Pointer(&b), unsafe.Pointer(&result))
	return result
}

func InitDecimal128(value int64) (result Decimal128) {
	if value == 1 {
		C.init_int128_as_1(unsafe.Pointer(&result))
	} else if value == -1 {
		C.init_int128_as_minus_1(unsafe.Pointer(&result))
	} else {
		C.init_int128_as_1(unsafe.Pointer(&result))
		C.mul_int128_int64(unsafe.Pointer(&result), unsafe.Pointer(&value), unsafe.Pointer(&result))
	}
	return result
}

func Decimal128IsNegative(a Decimal128) (result bool) {
	C.int128_is_negative(unsafe.Pointer(&a), unsafe.Pointer(&result))
	return result
}

func Decimal128IsZero(a Decimal128) (result bool) {
	C.int128_is_zero(unsafe.Pointer(&a), unsafe.Pointer(&result)) // I think we are safe here because boolean type is of size 1 in both c and go
	return result
}

func Decimal128IsNotZero(a Decimal128) (result bool) {
	C.int128_is_not_zero(unsafe.Pointer(&a), unsafe.Pointer(&result))
	return result
}

func ModDecimal128By10Abs(a Decimal128) (result int64) {
	C.mod_int128_by_10_abs(unsafe.Pointer(&a), unsafe.Pointer(&result))
	return result
}

func AddDecimal128ByInt64(a Decimal128, value int64) (result Decimal128) {
	C.add_int128_int64(unsafe.Pointer(&a), unsafe.Pointer(&value), unsafe.Pointer(&result))
	return result
}

func NegDecimal128(a Decimal128) (result Decimal128) {
	C.neg_int128(unsafe.Pointer(&a), unsafe.Pointer(&result))
	return result
}

func decimalStringPreprocess(s string, precision, scale int32) (result []byte, neg bool, err error) {
	if s == "" {
		return result, neg, errors.New("invalid decimal string")
	}
	parts := strings.Split(s, ".")
	partsNumber := len(parts)
	if partsNumber == 2 { // this means the input string is of the form "123.456"
		part0Bytes := []byte(parts[0])
		part1Bytes := []byte(parts[1])
		if len(parts[0]) > 0 {
			if part0Bytes[0] == '+' {
				part0Bytes = part0Bytes[1:]
			}
			if part0Bytes[0] == '-' {
				neg = true
				part0Bytes = part0Bytes[1:]
			}
		}
		if len(part0Bytes) > int(precision-scale) { // for example, input "123.45" is invalid for Decimal(5, 3)
			return result, neg, errors.New(fmt.Sprintf("input decimal value out of range for Decimal(%d, %d)", precision, scale))
		}
		if len(part1Bytes) > int(scale) {
			part1Bytes = part1Bytes[:scale]
		} else {
			scaleDiff := int(scale) - len(part1Bytes)
			for i := 0; i < scaleDiff; i++ {
				part1Bytes = append(part1Bytes, '0')
			}
		}
		result = append(part0Bytes, part1Bytes...)
		return result, neg, nil
	} else if partsNumber == 1 { // this means the input string is of the form "123",
		part0Bytes := []byte(parts[0])
		if part0Bytes[0] == '+' {
			part0Bytes = part0Bytes[1:]
		}
		if part0Bytes[0] == '-' {
			neg = true
			part0Bytes = part0Bytes[1:]
		}
		if len(part0Bytes) > int(precision-scale) { // for example, input "123" is invalid for Decimal(5, 3)
			return result, neg, errors.New(fmt.Sprintf("input decimal value out of range for Decimal(%d, %d)", precision, scale))
		}
		for i := 0; i < int(scale); i++ {
			part0Bytes = append(part0Bytes, '0')
		}
		result = part0Bytes
		return result, neg, nil
	} else {
		return result, neg, errors.New("invalid decimal string")
	}
}

//todo: use strconv to simplify this code
func ParseStringToDecimal64(s string, precision, scale int32) (result Decimal64, err error) {
	sInBytes, neg, err := decimalStringPreprocess(s, precision, scale)
	if err != nil {
		return result, err
	}
	resultInInt64 := int64(result)
	for _, ch := range sInBytes {
		ch -= '0'
		if ch > 9 {
			return result, errors.New("invalid decimal string")
		}
		digit := int64(ch)
		resultInInt64 = resultInInt64 * 10
		resultInInt64 = resultInInt64 + digit
	}
	if neg {
		resultInInt64 = -resultInInt64
	}
	result = Decimal64(resultInInt64)
	return result, nil
}

func ParseStringToDecimal128WithoutTable(s string) (result Decimal128, scale int32, err error) {
	precision := int32(38)
	parts := strings.Split(s, ".")
	scale = int32(0)
	if len(parts) == 1 || len(parts[1]) == 0 { // this means the input string is of the form "123", "123."
		scale = 0
	} else {
		scale = int32(len(parts[1]))
	}
	result, err = ParseStringToDecimal128(s, precision, scale)
	return result, scale, err
}

func ParseStringToDecimal128(s string, precision, scale int32) (result Decimal128, err error) {
	sInBytes, neg, err := decimalStringPreprocess(s, precision, scale)
	if err != nil {
		return result, err
	}

	for _, ch := range sInBytes {
		ch -= '0'
		if ch > 9 {
			return result, errors.New("invalid decimal string")
		}
		digit := int64(ch)
		result = ScaleDecimal128By10(result)
		result = AddDecimal128ByInt64(result, digit)
	}
	if neg {
		result = NegDecimal128(result)
	}
	return result, nil
}

func (a Decimal64) Decimal64ToString(scale int32) []byte {
	aInInt64 := int64(a)
	if scale == 0 {
		result := strconv.FormatInt(aInInt64, 10)
		return []byte(result)
	}
	if aInInt64 == 0 {
		return []byte("0")
	}
	result := strconv.FormatInt(aInInt64, 10)
	neg := false
	if aInInt64 < 0 {
		neg = true
		result = result[1:]
	}

	length := len(result)
	if length < int(scale) {
		for i := 0; i < int(scale)-length; i++ {
			result = "0" + result
		}
		result = "0." + result
	} else if length == int(scale) {
		result = "0." + result
	} else {
		result = result[:length-int(scale)] + "." + result[length-int(scale):]
	}
	if neg {
		result = "-" + result
	}

	return []byte(result)
}

func (a Decimal128) Decimal128ToString(scale int32) []byte {
	result := ""
	neg := Decimal128IsNegative(a)
	notZero := Decimal128IsNotZero(a)
	if notZero == false {
		return []byte("0")
	}
	tmp := a
	digits := "0123456789"
	digit := int64(0)
	for notZero {
		digit = ModDecimal128By10Abs(tmp)
		result = string(digits[digit]) + result
		tmp = DivideDecimal128By10(tmp)
		notZero = Decimal128IsNotZero(tmp)
	}
	length := len(result)
	if length < int(scale) {
		for i := 0; i < int(scale)-length; i++ {
			result = "0" + result
		}
		result = "0." + result
	} else if length == int(scale) {
		result = "0." + result
	} else {
		if scale != 0 {
			result = result[:length-int(scale)] + "." + result[length-int(scale):]
		}
	}
	if neg {
		result = "-" + result
	}
	return []byte(result)
}

func Decimal64Add(a, b Decimal64, aScale, bScale int32) (result Decimal64) {
	if aScale > bScale {
		scaleDiff := aScale - bScale
		scale := int64(math.Pow10(int(scaleDiff)))
		bScaled := ScaleDecimal64(b, scale)
		result = Decimal64AddAligned(a, bScaled)
	} else if aScale < bScale {
		scaleDiff := bScale - aScale
		scale := int64(math.Pow10(int(scaleDiff)))
		aScaled := ScaleDecimal64(a, scale)
		result = Decimal64AddAligned(aScaled, b)
	} else {
		result = Decimal64AddAligned(a, b)
	}
	return result
}

func Decimal64AddAligned(a, b Decimal64) (result Decimal64) {
	result = Decimal64(int64(a) + int64(b))
	return result
}

func Decimal64Sub(a, b Decimal64, aScale, bScale int32) (result Decimal64) {
	if aScale > bScale {
		scaleDiff := aScale - bScale
		scale := int64(math.Pow10(int(scaleDiff)))
		bScaled := ScaleDecimal64(b, scale)
		result = Decimal64SubAligned(a, bScaled)
	} else if aScale < bScale {
		scaleDiff := bScale - aScale
		scale := int64(math.Pow10(int(scaleDiff)))
		aScaled := ScaleDecimal64(a, scale)
		result = Decimal64SubAligned(aScaled, b)
	} else {
		result = Decimal64SubAligned(a, b)
	}
	return result
}

func Decimal64SubAligned(a, b Decimal64) (result Decimal64) {
	result = Decimal64(int64(a) - int64(b))
	return result
}

func Decimal128Add(a, b Decimal128, aScale, bScale int32) (result Decimal128) {
	if aScale > bScale {
		bScaled := b
		scaleDiff := aScale - bScale
		for i := 0; i < int(scaleDiff); i++ {
			bScaled = ScaleDecimal128By10(bScaled)
		}
		result = Decimal128AddAligned(a, bScaled)
	} else if aScale < bScale {
		aScaled := a
		scaleDiff := bScale - aScale
		for i := 0; i < int(scaleDiff); i++ {
			aScaled = ScaleDecimal128By10(aScaled)
		}
		result = Decimal128AddAligned(aScaled, b)
	} else {
		result = Decimal128AddAligned(a, b)
	}
	return result
}

func Decimal128AddAligned(a, b Decimal128) (result Decimal128) {
	C.add_int128_int128(unsafe.Pointer(&a), unsafe.Pointer(&b), unsafe.Pointer(&result))
	return result
}

func Decimal128Sub(a, b Decimal128, aScale, bScale int32) (result Decimal128) {
	if aScale > bScale {
		bScaled := b
		scaleDiff := aScale - bScale
		for i := 0; i < int(scaleDiff); i++ {
			bScaled = ScaleDecimal128By10(bScaled)
		}
		result = Decimal128SubAligned(a, bScaled)
	} else if aScale < bScale {
		aScaled := a
		scaleDiff := bScale - aScale
		for i := 0; i < int(scaleDiff); i++ {
			aScaled = ScaleDecimal128By10(aScaled)
		}
		result = Decimal128SubAligned(aScaled, b)
	} else {
		result = Decimal128SubAligned(a, b)
	}
	return result
}

func Decimal128SubAligned(a, b Decimal128) (result Decimal128) {
	C.sub_int128_int128(unsafe.Pointer(&a), unsafe.Pointer(&b), unsafe.Pointer(&result))
	return result
}

func Decimal64Decimal64Div(a, b Decimal64, aScale, bScale int32) (result Decimal128) {
	aScaled := InitDecimal128(int64(a))
	for i := 0; i < int(bScale); i++ {
		aScaled = ScaleDecimal128By10(aScaled)
	}
	C.div_int128_int64(unsafe.Pointer(&aScaled), unsafe.Pointer(&b), unsafe.Pointer(&result))
	return result
}

func Decimal128Decimal128Div(a, b Decimal128, aScale, bScale int32) (result Decimal128) {
	aScaled := a
	for i := 0; i < int(bScale); i++ {
		aScaled = ScaleDecimal128By10(aScaled)
	}
	C.div_int128_int128(unsafe.Pointer(&aScaled), unsafe.Pointer(&b), unsafe.Pointer(&result))
	return result
}

func Decimal64ToDecimal128(a Decimal64) (result Decimal128) {
	C.int64_to_int128(unsafe.Pointer(&a), unsafe.Pointer(&result))
	return result
}
