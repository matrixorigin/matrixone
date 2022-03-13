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

package exp

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
)

var (
	expUint8   func([]uint8, []float64) ExpResult
	expUint16  func([]uint16, []float64) ExpResult
	expUint32  func([]uint32, []float64) ExpResult
	expUint64  func([]uint64, []float64) ExpResult
	expInt8    func([]int8, []float64) ExpResult
	expInt16   func([]int16, []float64) ExpResult
	expInt32   func([]int32, []float64) ExpResult
	expInt64   func([]int64, []float64) ExpResult
	expFloat32 func([]float32, []float64) ExpResult
	expFloat64 func([]float64, []float64) ExpResult
)

func init() {
	expUint8 = expUint8Pure
	expUint16 = expUint16Pure
	expUint32 = expUint32Pure
	expUint64 = expUint64Pure
	expInt8 = expInt8Pure
	expInt16 = expInt16Pure
	expInt32 = expInt32Pure
	expInt64 = expInt64Pure
	expFloat32 = expFloat32Pure
	expFloat64 = expFloat64Pure
}

type ExpResult struct {
	Result []float64
	Nsp    *nulls.Nulls
}

func ExpUint8(xs []uint8, rs []float64) ExpResult {
	return expUint8(xs, rs)
}

func expUint8Pure(xs []uint8, rs []float64) ExpResult {
	result := ExpResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		result.Result[i] = math.Exp(float64(n))
	}
	return result
}

func ExpUint16(xs []uint16, rs []float64) ExpResult {
	return expUint16(xs, rs)
}

func expUint16Pure(xs []uint16, rs []float64) ExpResult {
	result := ExpResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		result.Result[i] = math.Exp(float64(n))
	}
	return result
}

func ExpUint32(xs []uint32, rs []float64) ExpResult {
	return expUint32(xs, rs)
}

func expUint32Pure(xs []uint32, rs []float64) ExpResult {
	result := ExpResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		result.Result[i] = math.Exp(float64(n))
	}
	return result
}

func ExpUint64(xs []uint64, rs []float64) ExpResult {
	return expUint64(xs, rs)
}

func expUint64Pure(xs []uint64, rs []float64) ExpResult {
	result := ExpResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		result.Result[i] = math.Exp(float64(n))
	}
	return result
}

func ExpInt8(xs []int8, rs []float64) ExpResult {
	return expInt8(xs, rs)
}

func expInt8Pure(xs []int8, rs []float64) ExpResult {
	result := ExpResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		result.Result[i] = math.Exp(float64(n))
	}
	return result
}

func ExpInt16(xs []int16, rs []float64) ExpResult {
	return expInt16(xs, rs)
}

func expInt16Pure(xs []int16, rs []float64) ExpResult {
	result := ExpResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		result.Result[i] = math.Exp(float64(n))
	}
	return result
}

func ExpInt32(xs []int32, rs []float64) ExpResult {
	return expInt32(xs, rs)
}

func expInt32Pure(xs []int32, rs []float64) ExpResult {
	result := ExpResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		result.Result[i] = math.Exp(float64(n))
	}
	return result
}

func ExpInt64(xs []int64, rs []float64) ExpResult {
	return expInt64(xs, rs)
}

func expInt64Pure(xs []int64, rs []float64) ExpResult {
	result := ExpResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		result.Result[i] = math.Exp(float64(n))
	}
	return result
}

func ExpFloat32(xs []float32, rs []float64) ExpResult {
	return expFloat32(xs, rs)
}

func expFloat32Pure(xs []float32, rs []float64) ExpResult {
	result := ExpResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		result.Result[i] = math.Exp(float64(n))
	}
	return result
}

func ExpFloat64(xs []float64, rs []float64) ExpResult {
	return expFloat64(xs, rs)
}

func expFloat64Pure(xs []float64, rs []float64) ExpResult {
	result := ExpResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		result.Result[i] = math.Exp(n)
	}
	return result
}
