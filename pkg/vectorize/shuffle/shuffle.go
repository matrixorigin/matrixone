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
	int8Shuffle  func([]int8, []int8, []int64) []int8
	int16Shuffle func([]int16, []int16, []int64) []int16
	int32Shuffle func([]int32, []int32, []int64) []int32
	int64Shuffle func([]int64, []int64, []int64) []int64

	uint8Shuffle  func([]uint8, []uint8, []int64) []uint8
	uint16Shuffle func([]uint16, []uint16, []int64) []uint16
	uint32Shuffle func([]uint32, []uint32, []int64) []uint32
	uint64Shuffle func([]uint64, []uint64, []int64) []uint64

	float32Shuffle func([]float32, []float32, []int64) []float32
	float64Shuffle func([]float64, []float64, []int64) []float64

	decimalShuffle func([]types.Decimal, []int64) []types.Decimal

	dateShuffle     func([]types.Date, []types.Date, []int64) []types.Date
	datetimeShuffle func([]types.Datetime, []types.Datetime, []int64) []types.Datetime

	tupleShuffle func([][]interface{}, [][]interface{}, []int64) [][]interface{}

	strShuffle func(*types.Bytes, []uint32, []uint32, []int64) *types.Bytes
)

func init() {
	int8Shuffle = int8ShufflePure
	int16Shuffle = int16ShufflePure
	int32Shuffle = int32ShufflePure
	int64Shuffle = int64ShufflePure

	uint8Shuffle = uint8ShufflePure
	uint16Shuffle = uint16ShufflePure
	uint32Shuffle = uint32ShufflePure
	uint64Shuffle = uint64ShufflePure

	float32Shuffle = float32ShufflePure
	float64Shuffle = float64ShufflePure

	decimalShuffle = decimalShufflePure

	dateShuffle = dateShufflePure
	datetimeShuffle = datetimeShufflePure

	tupleShuffle = tupleShufflePure

	strShuffle = strShufflePure
}

func Int8Shuffle(vs, ws []int8, sels []int64) []int8 {
	return int8Shuffle(vs, ws, sels)
}

func Int16Shuffle(vs, ws []int16, sels []int64) []int16 {
	return int16Shuffle(vs, ws, sels)
}

func Int32Shuffle(vs, ws []int32, sels []int64) []int32 {
	return int32Shuffle(vs, ws, sels)
}

func Int64Shuffle(vs, ws []int64, sels []int64) []int64 {
	return int64Shuffle(vs, ws, sels)
}

func Uint8Shuffle(vs, ws []uint8, sels []int64) []uint8 {
	return uint8Shuffle(vs, ws, sels)
}

func Uint16Shuffle(vs, ws []uint16, sels []int64) []uint16 {
	return uint16Shuffle(vs, ws, sels)
}

func Uint32Shuffle(vs, ws []uint32, sels []int64) []uint32 {
	return uint32Shuffle(vs, ws, sels)
}

func Uint64Shuffle(vs, ws []uint64, sels []int64) []uint64 {
	return uint64Shuffle(vs, ws, sels)
}

func Float32Shuffle(vs, ws []float32, sels []int64) []float32 {
	return float32Shuffle(vs, ws, sels)
}

func Float64Shuffle(vs, ws []float64, sels []int64) []float64 {
	return float64Shuffle(vs, ws, sels)
}

func DecimalShuffle(vs []types.Decimal, sels []int64) []types.Decimal {
	return decimalShuffle(vs, sels)
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

func StrShuffle(vs *types.Bytes, os, ns []uint32, sels []int64) *types.Bytes {
	return strShuffle(vs, os, ns, sels)
}

func int8ShufflePure(vs, ws []int8, sels []int64) []int8 {
	for i, sel := range sels {
		ws[i] = vs[sel]
	}
	copy(vs, ws)
	return vs[:len(sels)]
}

func int16ShufflePure(vs, ws []int16, sels []int64) []int16 {
	for i, sel := range sels {
		ws[i] = vs[sel]
	}
	copy(vs, ws)
	return vs[:len(sels)]
}

func int32ShufflePure(vs, ws []int32, sels []int64) []int32 {
	for i, sel := range sels {
		ws[i] = vs[sel]
	}
	copy(vs, ws)
	return vs[:len(sels)]
}

func int64ShufflePure(vs, ws []int64, sels []int64) []int64 {
	for i, sel := range sels {
		ws[i] = vs[sel]
	}
	copy(vs, ws)
	return vs[:len(sels)]
}

func uint8ShufflePure(vs, ws []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		ws[i] = vs[sel]
	}
	copy(vs, ws)
	return vs[:len(sels)]
}

func uint16ShufflePure(vs, ws []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		ws[i] = vs[sel]
	}
	copy(vs, ws)
	return vs[:len(sels)]
}

func uint32ShufflePure(vs, ws []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		ws[i] = vs[sel]
	}
	copy(vs, ws)
	return vs[:len(sels)]
}

func uint64ShufflePure(vs, ws []uint64, sels []int64) []uint64 {
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

func decimalShufflePure(vs []types.Decimal, sels []int64) []types.Decimal {
	for i, sel := range sels {
		vs[i] = vs[sel]
	}
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

func strShufflePure(vs *types.Bytes, os, ns []uint32, sels []int64) *types.Bytes {
	for i, sel := range sels {
		os[i] = vs.Offsets[sel]
		ns[i] = vs.Lengths[sel]
	}
	copy(vs.Offsets, os)
	copy(vs.Lengths, ns)
	vs.Offsets, vs.Lengths = vs.Offsets[:len(sels)], vs.Lengths[:len(sels)]
	return vs
}
