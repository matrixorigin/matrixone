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

package neg

var (
	Int8Neg    func([]int8, []int8) []int8
	Int16Neg   func([]int16, []int16) []int16
	Int32Neg   func([]int32, []int32) []int32
	Int64Neg   func([]int64, []int64) []int64
	Float32Neg func([]float32, []float32) []float32
	Float64Neg func([]float64, []float64) []float64
)

func int8Neg(xs, rs []int8) []int8 {
	for i, x := range xs {
		rs[i] = -x
	}
	return rs
}

func int16Neg(xs, rs []int16) []int16 {
	for i, x := range xs {
		rs[i] = -x
	}
	return rs
}

func int32Neg(xs, rs []int32) []int32 {
	for i, x := range xs {
		rs[i] = -x
	}
	return rs
}

func int64Neg(xs, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = -x
	}
	return rs
}

func float32Neg(xs, rs []float32) []float32 {
	for i, x := range xs {
		rs[i] = -x
	}
	return rs
}

func float64Neg(xs, rs []float64) []float64 {
	for i, x := range xs {
		rs[i] = -x
	}
	return rs
}
