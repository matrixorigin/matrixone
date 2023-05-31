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

package floor

/* floor package provides floor function for all numeric types(uint8, uint16, uint32, uint64, int8, int16, int32, int64, float32, float64).
Floor returns the largest round number that is less than or equal to x.
example:
	floor(12, -1) ----> 10
	floor(12) ----> 12
	floor(-12, -1) ----> -20
	floor(-12, 1) ----> -12
	floor(12.345) ----> 12
	floor(12.345, 1) ----> 12.3
	floor(-12.345, 1) ----> -12.4
	floor(-12.345, -1) ----> -20
	floor(-12.345) ----> -13
floor function takes one or two parameters as its argument, and the second argument must be a constant.
floor(x, N)
floor(x) == floor(x, 0)
N < 0, N zeroes in front of decimal point
N >= 0, floor to the Nth placeholder after decimal point
*/

import (
	"math"
)

var MaxUint64digits = numOfDigits(math.MaxUint64) // 20
var MaxInt64digits = numOfDigits(math.MaxInt64)   // 19

func numOfDigits(value uint64) int64 {
	digits := int64(0)
	for value > 0 {
		value /= 10
		digits++
	}
	return digits
}

// ScaleTable is a lookup array for digits
var ScaleTable = [...]uint64{
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
	10000000000000000000, // 1 followed by 19 zeros, maxUint64 number has 20 digits, so the max scale is 1 followed by 19 zeroes
}
