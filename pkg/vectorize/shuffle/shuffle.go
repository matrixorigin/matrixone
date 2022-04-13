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
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var (
	i8Shuffle  func([]int8, []int8, []int64) []int8
	i16Shuffle func([]int16, []int16, []int64) []int16
	i32Shuffle func([]int32, []int32, []int64) []int32
	i64Shuffle func([]int64, []int64, []int64) []int64

	ui8Shuffle  func([]uint8, []uint8, []int64) []uint8
	ui16Shuffle func([]uint16, []uint16, []int64) []uint16
	ui32Shuffle func([]uint32, []uint32, []int64) []uint32
	ui64Shuffle func([]uint64, []uint64, []int64) []uint64

	float32Shuffle func([]float32, []float32, []int64) []float32
	float64Shuffle func([]float64, []float64, []int64) []float64

	dateShuffle     func([]types.Date, []types.Date, []int64) []types.Date
	datetimeShuffle func([]types.Datetime, []types.Datetime, []int64) []types.Datetime

	decimal128Shuffle func([]types.Decimal128, []types.Decimal128, []int64) []types.Decimal128

	tupleShuffle func([][]interface{}, [][]interface{}, []int64) [][]interface{}

	sShuffle func(*types.Bytes, []uint32, []uint32, []int64) *types.Bytes
)

func init() {
	i8Shuffle = i8ShufflePure
	i16Shuffle = i16ShufflePure
	i32Shuffle = i32ShufflePure
	i64Shuffle = i64ShufflePure

	ui8Shuffle = ui8ShufflePure
	ui16Shuffle = ui16ShufflePure
	ui32Shuffle = ui32ShufflePure
	ui64Shuffle = ui64ShufflePure

	float32Shuffle = float32ShufflePure
	float64Shuffle = float64ShufflePure

	decimal128Shuffle = decimal128ShufflePure

	dateShuffle = dateShufflePure
	datetimeShuffle = datetimeShufflePure

	tupleShuffle = tupleShufflePure

	sShuffle = sShufflePure
}

func I8Shuffle(vs, ws []int8, sels []int64) []int8 {
	return i8Shuffle(vs, ws, sels)
}

func I16Shuffle(vs, ws []int16, sels []int64) []int16 {
	return i16Shuffle(vs, ws, sels)
}

func I32Shuffle(vs, ws []int32, sels []int64) []int32 {
	return i32Shuffle(vs, ws, sels)
}

func I64Shuffle(vs, ws []int64, sels []int64) []int64 {
	return i64Shuffle(vs, ws, sels)
}

func Ui8Shuffle(vs, ws []uint8, sels []int64) []uint8 {
	return ui8Shuffle(vs, ws, sels)
}

func Ui16Shuffle(vs, ws []uint16, sels []int64) []uint16 {
	return ui16Shuffle(vs, ws, sels)
}

func Ui32Shuffle(vs, ws []uint32, sels []int64) []uint32 {
	return ui32Shuffle(vs, ws, sels)
}

func Ui64Shuffle(vs, ws []uint64, sels []int64) []uint64 {
	return ui64Shuffle(vs, ws, sels)
}

func Float32Shuffle(vs, ws []float32, sels []int64) []float32 {
	return float32Shuffle(vs, ws, sels)
}

func Float64Shuffle(vs, ws []float64, sels []int64) []float64 {
	return float64Shuffle(vs, ws, sels)
}

func Decimal128Shuffle(vs, ws []types.Decimal128, sels []int64) []types.Decimal128 {
	return decimal128Shuffle(vs, ws, sels)
}

func DateShuffle(vs []types.Date, ws []types.Date, sels []int64) []types.Date {
	return dateShuffle(vs, ws, sels)
}

func DatetimeShuffle(vs []types.Datetime, ws []types.Datetime, sels []int64) []types.Datetime {
	return datetimeShuffle(vs, ws, sels)
}

func TupleShuffle(vs, ws [][]interface{}, sels []int64) [][]interface{} {
	return tupleShuffle(vs, ws, sels)
}

func SShuffle(vs *types.Bytes, os, ns []uint32, sels []int64) *types.Bytes {
	return sShuffle(vs, os, ns, sels)
}

func i8ShufflePure(vs, ws []int8, sels []int64) []int8 {
	for i, sel := range sels {
		ws[i] = vs[sel]
	}
	copy(vs, ws)
	return vs[:len(sels)]
}

func i16ShufflePure(vs, ws []int16, sels []int64) []int16 {
	for i, sel := range sels {
		ws[i] = vs[sel]
	}
	copy(vs, ws)
	return vs[:len(sels)]
}

func i32ShufflePure(vs, ws []int32, sels []int64) []int32 {
	for i, sel := range sels {
		ws[i] = vs[sel]
	}
	copy(vs, ws)
	return vs[:len(sels)]
}

func i64ShufflePure(vs, ws []int64, sels []int64) []int64 {
	for i, sel := range sels {
		ws[i] = vs[sel]
	}
	copy(vs, ws)
	return vs[:len(sels)]
}

func ui8ShufflePure(vs, ws []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		ws[i] = vs[sel]
	}
	copy(vs, ws)
	return vs[:len(sels)]
}

func ui16ShufflePure(vs, ws []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		ws[i] = vs[sel]
	}
	copy(vs, ws)
	return vs[:len(sels)]
}

func ui32ShufflePure(vs, ws []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		ws[i] = vs[sel]
	}
	copy(vs, ws)
	return vs[:len(sels)]
}

func ui64ShufflePure(vs, ws []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		ws[i] = vs[sel]
	}
	copy(vs, ws)
	return vs[:len(sels)]
}

func float32ShufflePure(vs, ws []float32, sels []int64) []float32 {
	for i, sel := range sels {
		ws[i] = vs[sel]
	}
	copy(vs, ws)
	return vs[:len(sels)]
}

func float64ShufflePure(vs, ws []float64, sels []int64) []float64 {
	for i, sel := range sels {
		ws[i] = vs[sel]
	}
	copy(vs, ws)
	return vs[:len(sels)]
}

func decimal128ShufflePure(vs []types.Decimal128, ws []types.Decimal128, sels []int64) []types.Decimal128 {
	for i, sel := range sels {
		ws[i] = vs[sel]
	}
	copy(vs, ws)
	return vs[:len(sels)]
}

func dateShufflePure(vs []types.Date, ws []types.Date, sels []int64) []types.Date {
	for i, sel := range sels {
		ws[i] = vs[sel]
	}
	copy(vs, ws)
	return vs[:len(sels)]
}

func datetimeShufflePure(vs []types.Datetime, ws []types.Datetime, sels []int64) []types.Datetime {
	for i, sel := range sels {
		ws[i] = vs[sel]
	}
	copy(vs, ws)
	return vs[:len(sels)]
}

func tupleShufflePure(vs, ws [][]interface{}, sels []int64) [][]interface{} {
	for i, sel := range sels {
		ws[i] = vs[sel]
	}
	return ws[:len(sels)]
}

func sShufflePure(vs *types.Bytes, os, ns []uint32, sels []int64) *types.Bytes {
	for i, sel := range sels {
		os[i] = vs.Offsets[sel]
		ns[i] = vs.Lengths[sel]
	}
	copy(vs.Offsets, os)
	copy(vs.Lengths, ns)
	vs.Offsets, vs.Lengths = vs.Offsets[:len(sels)], vs.Lengths[:len(sels)]
	return vs
}
