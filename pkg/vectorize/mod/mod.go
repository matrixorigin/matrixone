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

package mod

import "golang.org/x/exp/constraints"

var (
	Int8Mod                = intMod[int8]
	Int8ModSels            = intModSels[int8]
	Int8ModScalar          = intModScalar[int8]
	Int8ModScalarSels      = intModScalarSels[int8]
	Int8ModByScalar        = intModByScalar[int8]
	Int8ModByScalarSels    = intModByScalarSels[int8]
	Int16Mod               = intMod[int16]
	Int16ModSels           = intModSels[int16]
	Int16ModScalar         = intModScalar[int16]
	Int16ModScalarSels     = intModScalarSels[int16]
	Int16ModByScalar       = intModByScalar[int16]
	Int16ModByScalarSels   = intModByScalarSels[int16]
	Int32Mod               = intMod[int32]
	Int32ModSels           = intModSels[int32]
	Int32ModScalar         = intModScalar[int32]
	Int32ModScalarSels     = intModScalarSels[int32]
	Int32ModByScalar       = intModByScalar[int32]
	Int32ModByScalarSels   = intModByScalarSels[int32]
	Int64Mod               = intMod[int64]
	Int64ModSels           = intModSels[int64]
	Int64ModScalar         = intModScalar[int64]
	Int64ModScalarSels     = intModScalarSels[int64]
	Int64ModByScalar       = intModByScalar[int64]
	Int64ModByScalarSels   = intModByScalarSels[int64]
	Uint8Mod               = intMod[uint8]
	Uint8ModSels           = intModSels[uint8]
	Uint8ModScalar         = intModScalar[uint8]
	Uint8ModScalarSels     = intModScalarSels[uint8]
	Uint8ModByScalar       = intModByScalar[uint8]
	Uint8ModByScalarSels   = intModByScalarSels[uint8]
	Uint16Mod              = intMod[uint16]
	Uint16ModSels          = intModSels[uint16]
	Uint16ModScalar        = intModScalar[uint16]
	Uint16ModScalarSels    = intModScalarSels[uint16]
	Uint16ModByScalar      = intModByScalar[uint16]
	Uint16ModByScalarSels  = intModByScalarSels[uint16]
	Uint32Mod              = intMod[uint32]
	Uint32ModSels          = intModSels[uint32]
	Uint32ModScalar        = intModScalar[uint32]
	Uint32ModScalarSels    = intModScalarSels[uint32]
	Uint32ModByScalar      = intModByScalar[uint32]
	Uint32ModByScalarSels  = intModByScalarSels[uint32]
	Uint64Mod              = intMod[uint64]
	Uint64ModSels          = intModSels[uint64]
	Uint64ModScalar        = intModScalar[uint64]
	Uint64ModScalarSels    = intModScalarSels[uint64]
	Uint64ModByScalar      = intModByScalar[uint64]
	Uint64ModByScalarSels  = intModByScalarSels[uint64]
	Float32Mod             = floatMod[float32]
	Float32ModSels         = floatModSels[float32]
	Float32ModScalar       = floatModScalar[float32]
	Float32ModScalarSels   = floatModScalarSels[float32]
	Float32ModByScalar     = floatModByScalar[float32]
	Float32ModByScalarSels = floatModByScalarSels[float32]
	Float64Mod             = floatMod[float64]
	Float64ModSels         = floatModSels[float64]
	Float64ModScalar       = floatModScalar[float64]
	Float64ModScalarSels   = floatModScalarSels[float64]
	Float64ModByScalar     = floatModByScalar[float64]
	Float64ModByScalarSels = floatModByScalarSels[float64]
)

func intMod[T constraints.Integer](xs, ys, rs []T) []T {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func intModSels[T constraints.Integer](xs, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = xs[sel] % ys[sel]
	}
	return rs
}

func intModScalar[T constraints.Integer](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func intModScalarSels[T constraints.Integer](x T, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = x % ys[sel]
	}
	return rs
}

func intModByScalar[T constraints.Integer](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = y % x
	}
	return rs
}

func intModByScalarSels[T constraints.Integer](x T, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = ys[sel] % x
	}
	return rs
}

func floatMod[T constraints.Float](xs, ys, rs []T) []T {
	for i, x := range xs {
		rs[i] = x - x/ys[i]*ys[i]
	}
	return rs
}

func floatModSels[T constraints.Float](xs, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = xs[sel] - xs[sel]/ys[sel]*ys[sel]
	}
	return rs
}

func floatModScalar[T constraints.Float](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = x - x/y*y
	}
	return rs
}

func floatModScalarSels[T constraints.Float](x T, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = x - x/ys[sel]*ys[sel]
	}
	return rs
}

func floatModByScalar[T constraints.Float](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = y - y/x*x
	}
	return rs
}

func floatModByScalarSels[T constraints.Float](x T, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = ys[sel] - ys[sel]/x*x
	}
	return rs
}
