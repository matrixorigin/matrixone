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

package shuffle

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/vectorize"
)

var (
	Int8Shuffle  func([]int8, []int8, []int64) []int8
	Int16Shuffle func([]int16, []int16, []int64) []int16
	Int32Shuffle func([]int32, []int32, []int64) []int32
	Int64Shuffle func([]int64, []int64, []int64) []int64

	Uint8Shuffle  func([]uint8, []uint8, []int64) []uint8
	Uint16Shuffle func([]uint16, []uint16, []int64) []uint16
	Uint32Shuffle func([]uint32, []uint32, []int64) []uint32
	Uint64Shuffle func([]uint64, []uint64, []int64) []uint64

	Float32Shuffle func([]float32, []float32, []int64) []float32
	Float64Shuffle func([]float64, []float64, []int64) []float64

	DecimalShuffle func([]types.Decimal, []types.Decimal, []int64) []types.Decimal

	DateShuffle     func([]types.Date, []types.Date, []int64) []types.Date
	DatetimeShuffle func([]types.Datetime, []types.Datetime, []int64) []types.Datetime

	TupleShuffle func([][]interface{}, [][]interface{}, []int64) [][]interface{}

	StrShuffle func(*types.Bytes, []uint32, []uint32, []int64) *types.Bytes
)

func init() {
	Int8Shuffle = shuffleGeneric[int8]
	Int16Shuffle = shuffleGeneric[int16]
	Int32Shuffle = shuffleGeneric[int32]
	Int64Shuffle = shuffleGeneric[int64]

	Uint8Shuffle = shuffleGeneric[uint8]
	Uint16Shuffle = shuffleGeneric[uint16]
	Uint32Shuffle = shuffleGeneric[uint32]
	Uint64Shuffle = shuffleGeneric[uint64]

	Float32Shuffle = shuffleGeneric[float32]
	Float64Shuffle = shuffleGeneric[float64]

	DecimalShuffle = shuffleGeneric[types.Decimal]

	DateShuffle = shuffleGeneric[types.Date]
	DatetimeShuffle = shuffleGeneric[types.Datetime]

	TupleShuffle = tupleShuffle

	StrShuffle = strShuffle
}

type primitive interface {
	vectorize.Numeric | types.Decimal
}

func shuffleGeneric[T primitive](vs, ws []T, sels []int64) []T {
	for i, sel := range sels {
		ws[i] = vs[sel]
	}
	copy(vs, ws)
	return vs[:len(sels)]
}

func tupleShuffle(vs, ws [][]interface{}, sels []int64) [][]interface{} {
	for i, sel := range sels {
		ws[i] = vs[sel]
	}
	return ws[:len(sels)]
}

func strShuffle(vs *types.Bytes, os, ns []uint32, sels []int64) *types.Bytes {
	for i, sel := range sels {
		os[i] = vs.Offsets[sel]
		ns[i] = vs.Lengths[sel]
	}
	copy(vs.Offsets, os)
	copy(vs.Lengths, ns)
	vs.Offsets, vs.Lengths = vs.Offsets[:len(sels)], vs.Lengths[:len(sels)]
	return vs
}
