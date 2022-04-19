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

package acos

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
)

var (
	AcosUint8   func([]uint8, []float64) AcosResult
	AcosUint16  func([]uint16, []float64) AcosResult
	AcosUint32  func([]uint32, []float64) AcosResult
	AcosUint64  func([]uint64, []float64) AcosResult
	AcosInt8    func([]int8, []float64) AcosResult
	AcosInt16   func([]int16, []float64) AcosResult
	AcosInt32   func([]int32, []float64) AcosResult
	AcosInt64   func([]int64, []float64) AcosResult
	AcosFloat32 func([]float32, []float64) AcosResult
	AcosFloat64 func([]float64, []float64) AcosResult
)

func init() {
	AcosUint8 = acosUint8
	AcosUint16 = acosUint16
	AcosUint32 = acosUint32
	AcosUint64 = acosUint64
	AcosInt8 = acosInt8
	AcosInt16 = acosInt16
	AcosInt32 = acosInt32
	AcosInt64 = acosInt64
	AcosFloat32 = acosFloat32
	AcosFloat64 = acosFloat64
}

type AcosResult struct {
	Result []float64
	Nsp    *nulls.Nulls
}

func acosUint8(xs []uint8, rs []float64) AcosResult {
	result := AcosResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Acos(float64(n))
		}
	}
	return result
}

func acosUint16(xs []uint16, rs []float64) AcosResult {
	result := AcosResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Acos(float64(n))
		}
	}
	return result
}

func acosUint32(xs []uint32, rs []float64) AcosResult {
	result := AcosResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Acos(float64(n))
		}
	}
	return result
}

func acosUint64(xs []uint64, rs []float64) AcosResult {
	result := AcosResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Acos(float64(n))
		}
	}
	return result
}

func acosInt8(xs []int8, rs []float64) AcosResult {
	result := AcosResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n < -1 || n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Acos(float64(n))
		}
	}
	return result
}

func acosInt16(xs []int16, rs []float64) AcosResult {
	result := AcosResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n < -1 || n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Acos(float64(n))
		}
	}
	return result
}

func acosInt32(xs []int32, rs []float64) AcosResult {
	result := AcosResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n < -1 || n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Acos(float64(n))
		}
	}
	return result
}

func acosInt64(xs []int64, rs []float64) AcosResult {
	result := AcosResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n < -1 || n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Acos(float64(n))
		}
	}
	return result
}

func acosFloat32(xs []float32, rs []float64) AcosResult {
	result := AcosResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n < -1 || n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Acos(float64(n))
		}
	}
	return result
}

func acosFloat64(xs []float64, rs []float64) AcosResult {
	result := AcosResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n < -1 || n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Acos(n)
		}
	}
	return result
}
