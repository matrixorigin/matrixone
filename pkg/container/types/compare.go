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
