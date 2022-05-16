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

package sqrt

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"golang.org/x/exp/constraints"
)

var (
	SqrtUint8   func([]uint8, []float64) SqrtResult
	SqrtUint16  func([]uint16, []float64) SqrtResult
	SqrtUint32  func([]uint32, []float64) SqrtResult
	SqrtUint64  func([]uint64, []float64) SqrtResult
	SqrtInt8    func([]int8, []float64) SqrtResult
	SqrtInt16   func([]int16, []float64) SqrtResult
	SqrtInt32   func([]int32, []float64) SqrtResult
	SqrtInt64   func([]int64, []float64) SqrtResult
	SqrtFloat32 func([]float32, []float64) SqrtResult
	SqrtFloat64 func([]float64, []float64) SqrtResult
)

func init() {
	SqrtUint8 = sqrt[uint8]
	SqrtUint16 = sqrt[uint16]
	SqrtUint32 = sqrt[uint32]
	SqrtUint64 = sqrt[uint64]
	SqrtInt8 = sqrt[int8]
	SqrtInt16 = sqrt[int16]
	SqrtInt32 = sqrt[int32]
	SqrtInt64 = sqrt[int64]
	SqrtFloat32 = sqrt[float32]
	SqrtFloat64 = sqrt[float64]
}

type SqrtResult struct {
	Result []float64
	Nsp    *nulls.Nulls
}

func sqrt[T constraints.Integer | constraints.Float](xs []T, rs []float64) SqrtResult {
	result := SqrtResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		result.Result[i] = math.Sqrt(float64(n))
	}
	return result
}
