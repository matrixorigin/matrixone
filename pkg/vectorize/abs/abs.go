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

package abs

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"golang.org/x/exp/constraints"
)

var (
	AbsUint8   func([]uint8, []uint8) []uint8
	AbsUint16  func([]uint16, []uint16) []uint16
	AbsUint32  func([]uint32, []uint32) []uint32
	AbsUint64  func([]uint64, []uint64) []uint64
	AbsInt8    func([]int8, []int8) []int8
	AbsInt16   func([]int16, []int16) []int16
	AbsInt32   func([]int32, []int32) []int32
	AbsInt64   func([]int64, []int64) []int64
	AbsFloat32 func([]float32, []float32) []float32
	AbsFloat64 func([]float64, []float64) []float64
)

func init() {
	AbsUint8 = absUnsigned[uint8]
	AbsUint16 = absUnsigned[uint16]
	AbsUint32 = absUnsigned[uint32]
	AbsUint64 = absUnsigned[uint64]
	AbsInt8 = absSigned[int8]
	AbsInt16 = absSigned[int16]
	AbsInt32 = absSigned[int32]
	AbsInt64 = absSigned[int64]
	AbsFloat32 = absSigned[float32]
	AbsFloat64 = absSigned[float64]
}

// Unsigned simply return
func absUnsigned[T constraints.Unsigned](xs, rs []T) []T {
	return xs
}

// Signed, flip sign and check out of range.
func absSigned[T constraints.Signed | constraints.Float](xs, rs []T) []T {
	for i := range xs {
		if xs[i] < 0 {
			rs[i] = -xs[i]
		} else {
			rs[i] = xs[i]
		}
		if rs[i] < 0 {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "abs int value out of range"))
		}
	}
	return rs
}
