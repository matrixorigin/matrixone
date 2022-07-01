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

package cot

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"golang.org/x/exp/constraints"
)

var (
	CotUint8   func([]uint8, []float64) []float64
	CotUint16  func([]uint16, []float64) []float64
	CotUint32  func([]uint32, []float64) []float64
	CotUint64  func([]uint64, []float64) []float64
	CotInt8    func([]int8, []float64) []float64
	CotInt16   func([]int16, []float64) []float64
	CotInt32   func([]int32, []float64) []float64
	CotInt64   func([]int64, []float64) []float64
	CotFloat32 func([]float32, []float64) []float64
	CotFloat64 func([]float64, []float64) []float64
)

func init() {
	CotUint8 = Cot[uint8]
	CotUint16 = Cot[uint16]
	CotUint32 = Cot[uint32]
	CotUint64 = Cot[uint64]
	CotInt8 = Cot[int8]
	CotInt16 = Cot[int16]
	CotInt32 = Cot[int32]
	CotInt64 = Cot[int64]
	CotFloat32 = Cot[float32]
	CotFloat64 = Cot[float64]
}

// golang math weirdness, there is no cot function.
func cot(x float64) float64 {
	if x == 0 {
		panic(moerr.NewError(moerr.OUT_OF_RANGE, "cot(0) value out of range"))
	}
	return math.Tan(math.Pi/2.0 - x)
}

func Cot[T constraints.Integer | constraints.Float](inputValues []T, resultValues []float64) []float64 {
	for i, n := range inputValues {
		resultValues[i] = cot(float64(n))
	}
	return resultValues
}
