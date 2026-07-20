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

func BoolAscCompare(x, y bool) int {
	if x == y {
		return 0
	}
	if !x && y {
		return -1
	}
	return 1
}

func Decimal64AscCompare(x, y Decimal64) int {
	return x.Compare(y)
}
func Decimal128AscCompare(x, y Decimal128) int {
	return x.Compare(y)
}
func Decimal256AscCompare(x, y Decimal256) int {
	return x.Compare(y)
}

func UuidAscCompare(x, y Uuid) int {
	return x.Compare(y)
}

func TxntsAscCompare(x, y TS) int {
	return x.Compare(&y)
}
func RowidAscCompare(x, y Rowid) int {
	return x.Compare(&y)
}

func BlockidAscCompare(x, y Blockid) int {
	return x.Compare(&y)
}

func GenericAscCompare[T OrderedT](x, y T) int {
	if x == y {
		return 0
	}
	if x < y {
		return -1
	}
	return 1
}

func BoolDescCompare(x, y bool) int {
	if x == y {
		return 0
	}
	if !x && y {
		return 1
	}
	return -1
}

func Decimal64DescCompare(x, y Decimal64) int {
	return -x.Compare(y)
}
func Decimal128DescCompare(x, y Decimal128) int {
	return -x.Compare(y)
}
func Decimal256DescCompare(x, y Decimal256) int {
	return -x.Compare(y)
}

func UuidDescCompare(x, y Uuid) int {
	return -x.Compare(y)
}

func TxntsDescCompare(x, y TS) int {
	return y.Compare(&x)
}
func RowidDescCompare(x, y Rowid) int {
	return y.Compare(&x)
}

func BlockidDescCompare(x, y Blockid) int {
	return y.Compare(&x)
}

func GenericDescCompare[T OrderedT](x, y T) int {
	if x == y {
		return 0
	}
	if x < y {
		return 1
	}
	return -1
}

// Compare returns an integer comparing two arrays/vectors lexicographically.
// TODO: this function might not be correct. we need to compare using tolerance for float values.
// TODO: need to check if we need len(v1)==len(v2) check.
// ArrayElementCompare orders two narrow-typed vectors lexicographically without
// materializing any []float32 bridge — it is called once per comparison in the
// sort/merge/scalar-compare/min-max hot paths, so it must be allocation-free.
// float32/float64/int8/uint8 compare directly in their native type; bf16/f16
// widen one scalar at a time to float32 (direct uint16 comparison would be wrong
// because the sign bit makes bit order disagree with value order).
func ArrayElementCompare[T ArrayElement](v1, v2 []T) int {
	switch a := any(v1).(type) {
	case []float32:
		return arrayOrderedCompare(a, any(v2).([]float32))
	case []float64:
		return arrayOrderedCompare(a, any(v2).([]float64))
	case []int8:
		return arrayOrderedCompare(a, any(v2).([]int8))
	case []uint8:
		return arrayOrderedCompare(a, any(v2).([]uint8))
	case []BF16:
		return arrayFloat16Compare(a, any(v2).([]BF16))
	case []Float16:
		return arrayFloat16Compare(a, any(v2).([]Float16))
	default:
		panic("ArrayElementCompare: unsupported element type")
	}
}

// arrayOrderedCompare compares two slices whose elements support the native `<`
// operator (float32/float64/int8/uint8), lexicographically then by length.
func arrayOrderedCompare[T interface{ ~float32 | ~float64 | ~int8 | ~uint8 }](v1, v2 []T) int {
	minLen := len(v1)
	if len(v2) < minLen {
		minLen = len(v2)
	}
	for i := 0; i < minLen; i++ {
		if v1[i] < v2[i] {
			return -1
		} else if v1[i] > v2[i] {
			return 1
		}
	}
	if len(v1) < len(v2) {
		return -1
	} else if len(v1) > len(v2) {
		return 1
	}
	return 0
}

// arrayFloat16Compare compares two bf16/f16 slices by widening one scalar at a
// time to float32 (the sign bit makes raw uint16 order disagree with value
// order). No slice is allocated.
func arrayFloat16Compare[T interface{ ToFloat32() float32 }](v1, v2 []T) int {
	minLen := len(v1)
	if len(v2) < minLen {
		minLen = len(v2)
	}
	for i := 0; i < minLen; i++ {
		a, b := v1[i].ToFloat32(), v2[i].ToFloat32()
		if a < b {
			return -1
		} else if a > b {
			return 1
		}
	}
	if len(v1) < len(v2) {
		return -1
	} else if len(v1) > len(v2) {
		return 1
	}
	return 0
}

func ArrayCompare[T RealNumbers](v1, v2 []T) int {
	minLen := len(v1)
	if len(v2) < minLen {
		minLen = len(v2)
	}

	for i := 0; i < minLen; i++ {
		if v1[i] < v2[i] {
			return -1
		} else if v1[i] > v2[i] {
			return 1
		}
	}

	if len(v1) < len(v2) {
		return -1
	} else if len(v1) > len(v2) {
		return 1
	}
	return 0
}

func CompareArrayFromBytes[T RealNumbers](_x, _y []byte, desc bool) int {
	x := BytesToArray[T](_x)
	y := BytesToArray[T](_y)

	if desc {
		return ArrayCompare[T](y, x)
	}
	return ArrayCompare[T](x, y)
}

// CompareArrayElementFromBytes is the narrow-type (bf16/f16/int8) counterpart of
// CompareArrayFromBytes; it orders through the float32 bridge.
func CompareArrayElementFromBytes[T ArrayElement](_x, _y []byte, desc bool) int {
	x := BytesToArray[T](_x)
	y := BytesToArray[T](_y)

	if desc {
		return ArrayElementCompare[T](y, x)
	}
	return ArrayElementCompare[T](x, y)
}
