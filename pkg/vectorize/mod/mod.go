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
	Int8Mod                = IntMod[int8]
	Int8ModSels            = IntModSels[int8]
	Int8ModScalar          = IntModScalar[int8]
	Int8ModScalarSels      = IntModScalarSels[int8]
	Int8ModByScalar        = IntModByScalar[int8]
	Int8ModByScalarSels    = IntModByScalarSels[int8]
	Int16Mod               = IntMod[int16]
	Int16ModSels           = IntModSels[int16]
	Int16ModScalar         = IntModScalar[int16]
	Int16ModScalarSels     = IntModScalarSels[int16]
	Int16ModByScalar       = IntModByScalar[int16]
	Int16ModByScalarSels   = IntModByScalarSels[int16]
	Int32Mod               = IntMod[int32]
	Int32ModSels           = IntModSels[int32]
	Int32ModScalar         = IntModScalar[int32]
	Int32ModScalarSels     = IntModScalarSels[int32]
	Int32ModByScalar       = IntModByScalar[int32]
	Int32ModByScalarSels   = IntModByScalarSels[int32]
	Int64Mod               = IntMod[int64]
	Int64ModSels           = IntModSels[int64]
	Int64ModScalar         = IntModScalar[int64]
	Int64ModScalarSels     = IntModScalarSels[int64]
	Int64ModByScalar       = IntModByScalar[int64]
	Int64ModByScalarSels   = IntModByScalarSels[int64]
	Uint8Mod               = IntMod[uint8]
	Uint8ModSels           = IntModSels[uint8]
	Uint8ModScalar         = IntModScalar[uint8]
	Uint8ModScalarSels     = IntModScalarSels[uint8]
	Uint8ModByScalar       = IntModByScalar[uint8]
	Uint8ModByScalarSels   = IntModByScalarSels[uint8]
	Uint16Mod              = IntMod[uint16]
	Uint16ModSels          = IntModSels[uint16]
	Uint16ModScalar        = IntModScalar[uint16]
	Uint16ModScalarSels    = IntModScalarSels[uint16]
	Uint16ModByScalar      = IntModByScalar[uint16]
	Uint16ModByScalarSels  = IntModByScalarSels[uint16]
	Uint32Mod              = IntMod[uint32]
	Uint32ModSels          = IntModSels[uint32]
	Uint32ModScalar        = IntModScalar[uint32]
	Uint32ModScalarSels    = IntModScalarSels[uint32]
	Uint32ModByScalar      = IntModByScalar[uint32]
	Uint32ModByScalarSels  = IntModByScalarSels[uint32]
	Uint64Mod              = IntMod[uint64]
	Uint64ModSels          = IntModSels[uint64]
	Uint64ModScalar        = IntModScalar[uint64]
	Uint64ModScalarSels    = IntModScalarSels[uint64]
	Uint64ModByScalar      = IntModByScalar[uint64]
	Uint64ModByScalarSels  = IntModByScalarSels[uint64]
	Float32Mod             = FloatMod[float32]
	Float32ModSels         = FloatModSels[float32]
	Float32ModScalar       = FloatModScalar[float32]
	Float32ModScalarSels   = FloatModScalarSels[float32]
	Float32ModByScalar     = FloatModByScalar[float32]
	Float32ModByScalarSels = FloatModByScalarSels[float32]
	Float64Mod             = FloatMod[float64]
	Float64ModSels         = FloatModSels[float64]
	Float64ModScalar       = FloatModScalar[float64]
	Float64ModScalarSels   = FloatModScalarSels[float64]
	Float64ModByScalar     = FloatModByScalar[float64]
	Float64ModByScalarSels = FloatModByScalarSels[float64]
)

func IntMod[T constraints.Integer](xs, ys, rs []T) []T {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func IntModSels[T constraints.Integer](xs, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = xs[sel] % ys[sel]
	}
	return rs
}

func IntModScalar[T constraints.Integer](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func IntModScalarSels[T constraints.Integer](x T, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = x % ys[sel]
	}
	return rs
}

func IntModByScalar[T constraints.Integer](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = y % x
	}
	return rs
}

func IntModByScalarSels[T constraints.Integer](x T, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = ys[sel] % x
	}
	return rs
}

func FloatMod[T constraints.Float](xs, ys, rs []T) []T {
	for i, x := range xs {
		rs[i] = x - x/ys[i]*ys[i]
	}
	return rs
}

func FloatModSels[T constraints.Float](xs, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = xs[sel] - xs[sel]/ys[sel]*ys[sel]
	}
	return rs
}

func FloatModScalar[T constraints.Float](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = x - x/y*y
	}
	return rs
}

func FloatModScalarSels[T constraints.Float](x T, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = x - x/ys[sel]*ys[sel]
	}
	return rs
}

func FloatModByScalar[T constraints.Float](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = y - y/x*x
	}
	return rs
}

func FloatModByScalarSels[T constraints.Float](x T, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = ys[sel] - ys[sel]/x*x
	}
	return rs
}
