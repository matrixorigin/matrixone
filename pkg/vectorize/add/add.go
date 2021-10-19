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

package add

import (
	"matrixone/pkg/vectorize"
)

var (
	Int8Add              func([]int8, []int8, []int8) []int8
	Int8AddSels          func([]int8, []int8, []int8, []int64) []int8
	Int8AddScalar        func(int8, []int8, []int8) []int8
	Int8AddScalarSels    func(int8, []int8, []int8, []int64) []int8
	Int16Add             func([]int16, []int16, []int16) []int16
	Int16AddSels         func([]int16, []int16, []int16, []int64) []int16
	Int16AddScalar       func(int16, []int16, []int16) []int16
	Int16AddScalarSels   func(int16, []int16, []int16, []int64) []int16
	Int32Add             func([]int32, []int32, []int32) []int32
	Int32AddSels         func([]int32, []int32, []int32, []int64) []int32
	Int32AddScalar       func(int32, []int32, []int32) []int32
	Int32AddScalarSels   func(int32, []int32, []int32, []int64) []int32
	Int64Add             func([]int64, []int64, []int64) []int64
	Int64AddSels         func([]int64, []int64, []int64, []int64) []int64
	Int64AddScalar       func(int64, []int64, []int64) []int64
	Int64AddScalarSels   func(int64, []int64, []int64, []int64) []int64
	Uint8Add             func([]uint8, []uint8, []uint8) []uint8
	Uint8AddSels         func([]uint8, []uint8, []uint8, []int64) []uint8
	Uint8AddScalar       func(uint8, []uint8, []uint8) []uint8
	Uint8AddScalarSels   func(uint8, []uint8, []uint8, []int64) []uint8
	Uint16Add            func([]uint16, []uint16, []uint16) []uint16
	Uint16AddSels        func([]uint16, []uint16, []uint16, []int64) []uint16
	Uint16AddScalar      func(uint16, []uint16, []uint16) []uint16
	Uint16AddScalarSels  func(uint16, []uint16, []uint16, []int64) []uint16
	Uint32Add            func([]uint32, []uint32, []uint32) []uint32
	Uint32AddSels        func([]uint32, []uint32, []uint32, []int64) []uint32
	Uint32AddScalar      func(uint32, []uint32, []uint32) []uint32
	Uint32AddScalarSels  func(uint32, []uint32, []uint32, []int64) []uint32
	Uint64Add            func([]uint64, []uint64, []uint64) []uint64
	Uint64AddSels        func([]uint64, []uint64, []uint64, []int64) []uint64
	Uint64AddScalar      func(uint64, []uint64, []uint64) []uint64
	Uint64AddScalarSels  func(uint64, []uint64, []uint64, []int64) []uint64
	Float32Add           func([]float32, []float32, []float32) []float32
	Float32AddSels       func([]float32, []float32, []float32, []int64) []float32
	Float32AddScalar     func(float32, []float32, []float32) []float32
	Float32AddScalarSels func(float32, []float32, []float32, []int64) []float32
	Float64Add           func([]float64, []float64, []float64) []float64
	Float64AddSels       func([]float64, []float64, []float64, []int64) []float64
	Float64AddScalar     func(float64, []float64, []float64) []float64
	Float64AddScalarSels func(float64, []float64, []float64, []int64) []float64

	Int8Int16Add		 func([]int16, []int8, []int16) []int16
	Int8Int32Add		 func([]int32, []int8, []int32) []int32
	Int8Int64Add		 func([]int64, []int8, []int64) []int64
	Int16Int32Add		 func([]int32, []int16, []int32) []int32
	Int16Int64Add		 func([]int64, []int16, []int64) []int64
	Int32Int64Add		 func([]int64, []int32, []int64) []int64
	Float32Float64Add 	 func([]float64, []float32, []float64) []float64
	Uint8Uint16Add		 func([]uint16, []uint8, []uint16) []uint16
	Uint8Uint32Add		 func([]uint32, []uint8, []uint32) []uint32
	Uint8Uint64Add		 func([]uint64, []uint8, []uint64) []uint64
	Uint16Uint32Add		 func([]uint32, []uint16, []uint32) []uint32
	Uint16Uint64Add		 func([]uint64, []uint16, []uint64) []uint64
	Uint32Uint64Add		 func([]uint64, []uint32, []uint64) []uint64
	
	Int8Int16AddSels	 func([]int16, []int8, []int16, []int64) []int16
	Int8Int32AddSels	 func([]int32, []int8, []int32, []int64) []int32
	Int8Int64AddSels	 func([]int64, []int8, []int64, []int64) []int64
	Int16Int32AddSels	 func([]int32, []int16, []int32, []int64) []int32
	Int16Int64AddSels	 func([]int64, []int16, []int64, []int64) []int64
	Int32Int64AddSels	 func([]int64, []int32, []int64, []int64) []int64
	Float32Float64AddSels func([]float64, []float32, []float64, []int64) []float64
	Uint8Uint16AddSels		 func([]uint16, []uint8, []uint16, []int64) []uint16
	Uint8Uint32AddSels		 func([]uint32, []uint8, []uint32, []int64) []uint32
	Uint8Uint64AddSels		 func([]uint64, []uint8, []uint64, []int64) []uint64
	Uint16Uint32AddSels		 func([]uint32, []uint16, []uint32, []int64) []uint32
	Uint16Uint64AddSels		 func([]uint64, []uint16, []uint64, []int64) []uint64
	Uint32Uint64AddSels		 func([]uint64, []uint32, []uint64, []int64) []uint64
)

func addGeneric[T vectorize.Numeric](xs, ys, rs []T) []T {
	for i, x := range xs {
		rs[i] = x + ys[i]
	}
	return rs
}

func addSelsGeneric[T vectorize.Numeric](xs, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = xs[sel] + ys[sel]
	}
	return rs
}

func addScalarGeneric[T vectorize.Numeric](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = x + y
	}
	return rs
}

func addScalarSelsGeneric[T vectorize.Numeric](x T, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = x + ys[sel]
	}
	return rs
}

// different type
func addDifferentGeneric[T1 vectorize.Numeric, T2 vectorize.Numeric](xs []T1, ys []T2, rs []T1) []T1 {
	for i, x := range xs {
		rs[i] = x + T1(ys[i])
	}
	return rs
}

func addDifferentSelsGeneric[T1 vectorize.Numeric, T2 vectorize.Numeric](xs []T1, ys []T2, rs[]T1, sels []int64) []T1 {
	for i, sel := range sels {
		rs[i] = xs[sel] + T1(ys[sel])
	}
	return rs
}
