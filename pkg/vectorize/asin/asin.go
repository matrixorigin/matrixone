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

package asin

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"golang.org/x/exp/constraints"
)

var (
	AsinUint8   func([]uint8, []float64) AsinResult
	AsinUint16  func([]uint16, []float64) AsinResult
	AsinUint32  func([]uint32, []float64) AsinResult
	AsinUint64  func([]uint64, []float64) AsinResult
	AsinInt8    func([]int8, []float64) AsinResult
	AsinInt16   func([]int16, []float64) AsinResult
	AsinInt32   func([]int32, []float64) AsinResult
	AsinInt64   func([]int64, []float64) AsinResult
	AsinFloat32 func([]float32, []float64) AsinResult
	AsinFloat64 func([]float64, []float64) AsinResult
)

func init() {
	AsinUint8 = asinUint8
	AsinUint16 = asinUint16
	AsinUint32 = asinUint32
	AsinUint64 = asinUint64
	AsinInt8 = asinInt8
	AsinInt16 = asinInt16
	AsinInt32 = asinInt32
	AsinInt64 = asinInt64
	AsinFloat32 = asinFloat32
	AsinFloat64 = asinFloat64
}

type AsinResult struct {
	Result []float64
	Nsp    *nulls.Nulls
}

func asinUint8(xs []uint8, rs []float64) AsinResult {
	result := AsinResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Asin(float64(n))
		}
	}
	return result
}

func asinUint16(xs []uint16, rs []float64) AsinResult {
	result := AsinResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Asin(float64(n))
		}
	}
	return result
}

func asinUint32(xs []uint32, rs []float64) AsinResult {
	result := AsinResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Asin(float64(n))
		}
	}
	return result
}

func asinUint64(xs []uint64, rs []float64) AsinResult {
	result := AsinResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Asin(float64(n))
		}
	}
	return result
}

func asinInt8(xs []int8, rs []float64) AsinResult {
	result := AsinResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n < -1 || n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Asin(float64(n))
		}
	}
	return result
}

func asinInt16(xs []int16, rs []float64) AsinResult {
	result := AsinResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n < -1 || n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Asin(float64(n))
		}
	}
	return result
}

func asinInt32(xs []int32, rs []float64) AsinResult {
	result := AsinResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n < -1 || n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Asin(float64(n))
		}
	}
	return result
}

func asinInt64(xs []int64, rs []float64) AsinResult {
	result := AsinResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n < -1 || n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Asin(float64(n))
		}
	}
	return result
}

func asinFloat32(xs []float32, rs []float64) AsinResult {
	result := AsinResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n < -1 || n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Asin(float64(n))
		}
	}
	return result
}

func asinFloat64(xs []float64, rs []float64) AsinResult {
	result := AsinResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n < -1 || n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Asin(n)
		}
	}
	return result
}

func Asin[T constraints.Integer | constraints.Float](inputValues []T, rs []float64) AsinResult {
	result := AsinResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range inputValues {
		t := float64(n)
		if t < -1 || t > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Asin(t)
		}
	}
	return result
}
