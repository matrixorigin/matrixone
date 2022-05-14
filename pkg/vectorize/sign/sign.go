// Copyright 2022 Matrix Origin
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

package sign

import (
	"golang.org/x/exp/constraints"
)

var (
	SignUint8   func([]uint8, []int8) []int8
	SignUint16  func([]uint16, []int8) []int8
	SignUint32  func([]uint32, []int8) []int8
	SignUint64  func([]uint64, []int8) []int8
	SignInt8    func([]int8, []int8) []int8
	SignInt16   func([]int16, []int8) []int8
	SignInt32   func([]int32, []int8) []int8
	SignInt64   func([]int64, []int8) []int8
	SignFloat32 func([]float32, []int8) []int8
	SignFloat64 func([]float64, []int8) []int8
)

func init() {
	SignUint8 = sign[uint8]
	SignUint16 = sign[uint16]
	SignUint32 = sign[uint32]
	SignUint64 = sign[uint64]
	SignInt8 = sign[int8]
	SignInt16 = sign[int16]
	SignInt32 = sign[int32]
	SignInt64 = sign[int64]
	SignFloat32 = sign[float32]
	SignFloat64 = sign[float64]
}

// Sign function
func sign[T constraints.Integer | constraints.Float](xs []T, rs []int8) []int8 {
	for i := range xs {
		if xs[i] > 0 {
			rs[i] = 1
		} else if xs[i] == 0 {
			rs[i] = 0
		} else {
			rs[i] = -1
		}
	}
	return rs
}
