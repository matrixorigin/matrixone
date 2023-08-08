// Copyright 2023 Matrix Origin
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

package index

import "math"

type signed interface {
	int8 | int16 | int32 | int64
}

type unsigned interface {
	uint8 | uint16 | uint32 | uint64
}

type float interface {
	float32 | float64
}

// addi adds two signed integers of the same type and returns the result and whether
// the result overflowed.
func addi[T signed](a, b T) (int64, bool) {
	s := int64(a) + int64(b)
	if (s > int64(a)) != (b > 0) {
		return s, true
	}
	return s, false
}

// addu adds two unsigned integers of the same type and returns the result and whether
// the result overflowed.
func addu[T unsigned](a, b T) (uint64, bool) {
	s := uint64(a) + uint64(b)
	if s < uint64(a) {
		return s, true
	}
	return s, false
}

// addf adds two floats of the same type and returns the result and whether
// the result overflowed.
func addf[T float](a, b T) (float64, bool) {
	s := float64(a) + float64(b)
	if math.IsInf(s, 0) {
		return s, true
	}
	return s, false
}
