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

package ln

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
)

type LnResult struct {
	Result []float64
	Nsp    *nulls.Nulls
}

var (
	LnUint8   func([]uint8, []float64) LnResult
	LnUint16  func([]uint16, []float64) LnResult
	LnUint32  func([]uint32, []float64) LnResult
	LnUint64  func([]uint64, []float64) LnResult
	LnInt8    func([]int8, []float64) LnResult
	LnInt16   func([]int16, []float64) LnResult
	LnInt32   func([]int32, []float64) LnResult
	LnInt64   func([]int64, []float64) LnResult
	LnFloat32 func([]float32, []float64) LnResult
	LnFloat64 func([]float64, []float64) LnResult
)

func init() {
	LnUint8 = lnUint8
	LnUint16 = lnUint16
	LnUint32 = lnUint32
	LnUint64 = lnUint64
	LnInt8 = lnInt8
	LnInt16 = lnInt16
	LnInt32 = lnInt32
	LnInt64 = lnInt64
	LnFloat32 = lnFloat32
	LnFloat64 = lnFloat64
}

func lnUint8(xs []uint8, rs []float64) LnResult {
	result := LnResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		rs[i] = math.Log(float64(n))
	}
	return result
}

func lnUint16(xs []uint16, rs []float64) LnResult {
	result := LnResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		rs[i] = math.Log(float64(n))
	}
	return result
}

func lnUint32(xs []uint32, rs []float64) LnResult {
	result := LnResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		rs[i] = math.Log(float64(n))
	}
	return result
}

func lnUint64(xs []uint64, rs []float64) LnResult {
	result := LnResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		rs[i] = math.Log(float64(n))
	}
	return result
}

func lnInt8(xs []int8, rs []float64) LnResult {
	result := LnResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n <= 0 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			rs[i] = math.Log(float64(n))
		}
	}
	return result
}

func lnInt16(xs []int16, rs []float64) LnResult {
	result := LnResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n <= 0 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			rs[i] = math.Log(float64(n))
		}
	}
	return result
}

func lnInt32(xs []int32, rs []float64) LnResult {
	result := LnResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n <= 0 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			rs[i] = math.Log(float64(n))
		}
	}
	return result
}

func lnInt64(xs []int64, rs []float64) LnResult {
	result := LnResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n <= 0 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			rs[i] = math.Log(float64(n))
		}
	}
	return result
}

func lnFloat32(xs []float32, rs []float64) LnResult {
	result := LnResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n <= 0 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			rs[i] = math.Log(float64(n))
		}
	}
	return result
}

func lnFloat64(xs []float64, rs []float64) LnResult {
	result := LnResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n <= 0 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			rs[i] = math.Log(n)
		}
	}
	return result
}
