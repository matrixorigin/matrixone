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
	SqrtUint8 = sqrtUint8
	SqrtUint16 = sqrtUint16
	SqrtUint32 = sqrtUint32
	SqrtUint64 = sqrtUint64
	SqrtInt8 = sqrtInt8
	SqrtInt16 = sqrtInt16
	SqrtInt32 = sqrtInt32
	SqrtInt64 = sqrtInt64
	SqrtFloat32 = sqrtFloat32
	SqrtFloat64 = sqrtFloat64
}

type SqrtResult struct {
	Result []float64
	Nsp    *nulls.Nulls
}

func sqrtUint8(xs []uint8, rs []float64) SqrtResult {
	result := SqrtResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		result.Result[i] = math.Sqrt(float64(n))
	}
	return result
}

func sqrtUint16(xs []uint16, rs []float64) SqrtResult {
	result := SqrtResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		result.Result[i] = math.Sqrt(float64(n))
	}
	return result
}

func sqrtUint32(xs []uint32, rs []float64) SqrtResult {
	result := SqrtResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		result.Result[i] = math.Sqrt(float64(n))
	}
	return result
}

func sqrtUint64(xs []uint64, rs []float64) SqrtResult {
	result := SqrtResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		result.Result[i] = math.Sqrt(float64(n))
	}
	return result
}

func sqrtInt8(xs []int8, rs []float64) SqrtResult {
	result := SqrtResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n < 0 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Sqrt(float64(n))
		}
	}
	return result
}

func sqrtInt16(xs []int16, rs []float64) SqrtResult {
	result := SqrtResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n < 0 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Sqrt(float64(n))
		}
	}
	return result
}

func sqrtInt32(xs []int32, rs []float64) SqrtResult {
	result := SqrtResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n < 0 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Sqrt(float64(n))
		}
	}
	return result
}

func sqrtInt64(xs []int64, rs []float64) SqrtResult {
	result := SqrtResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n < 0 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Sqrt(float64(n))
		}
	}
	return result
}

func sqrtFloat32(xs []float32, rs []float64) SqrtResult {
	result := SqrtResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n < 0 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Sqrt(float64(n))
		}
	}
	return result
}

func sqrtFloat64(xs []float64, rs []float64) SqrtResult {
	result := SqrtResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n < 0 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Sqrt(n)
		}
	}
	return result
}
