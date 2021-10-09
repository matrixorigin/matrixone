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

package sum

import "matrixone/pkg/vectorize"

var (
	Int8Sum        func([]int8) int64
	Int8SumSels    func([]int8, []int64) int64
	Int16Sum       func([]int16) int64
	Int16SumSels   func([]int16, []int64) int64
	Int32Sum       func([]int32) int64
	Int32SumSels   func([]int32, []int64) int64
	Int64Sum       func([]int64) int64
	Int64SumSels   func([]int64, []int64) int64
	Uint8Sum       func([]uint8) uint64
	Uint8SumSels   func([]uint8, []int64) uint64
	Uint16Sum      func([]uint16) uint64
	Uint16SumSels  func([]uint16, []int64) uint64
	Uint32Sum      func([]uint32) uint64
	Uint32SumSels  func([]uint32, []int64) uint64
	Uint64Sum      func([]uint64) uint64
	Uint64SumSels  func([]uint64, []int64) uint64
	Float32Sum     func([]float32) float32
	Float32SumSels func([]float32, []int64) float32
	Float64Sum     func([]float64) float64
	Float64SumSels func([]float64, []int64) float64
)

func sumSignedGeneric[T vectorize.SignedInt](xs []T) int64 {
	var res int64

	for _, x := range xs {
		res += int64(x)
	}
	return res
}

func sumSignedSelsGeneric[T vectorize.SignedInt](xs []T, sels []int64) int64 {
	var res int64

	for _, sel := range sels {
		res += int64(xs[sel])
	}
	return res
}

func sumUnsignedGeneric[T vectorize.UnsignedInt](xs []T) uint64 {
	var res uint64

	for _, x := range xs {
		res += uint64(x)
	}
	return res
}

func sumUnsignedSelsGeneric[T vectorize.UnsignedInt](xs []T, sels []int64) uint64 {
	var res uint64

	for _, sel := range sels {
		res += uint64(xs[sel])
	}
	return res
}

func sumFloatGeneric[T vectorize.Float](xs []T) T {
	var res T

	for _, x := range xs {
		res += x
	}
	return res
}

func sumFloatSelsGeneric[T vectorize.Float](xs []T, sels []int64) T {
	var res T

	for _, sel := range sels {
		res += xs[sel]
	}
	return res
}
