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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"math"
)

type LnResult struct {
	Result []float64
	Nsp    *nulls.Nulls
}

var (
	lnUint8   func([]uint8, []float64) LnResult
	lnUint16  func([]uint16, []float64) LnResult
	lnUint32  func([]uint32, []float64) LnResult
	lnUint64  func([]uint64, []float64) LnResult
	lnInt8    func([]int8, []float64) LnResult
	lnInt16   func([]int16, []float64) LnResult
	lnInt32   func([]int32, []float64) LnResult
	lnInt64   func([]int64, []float64) LnResult
	lnFloat32 func([]float32, []float64) LnResult
	lnFloat64 func([]float64, []float64) LnResult
)

func init() {
	lnUint8 = lnUint8Pure
	lnUint16 = lnUint16Pure
	lnUint32 = lnUint32Pure
	lnUint64 = lnUint64Pure
	lnInt8 = lnInt8Pure
	lnInt16 = lnInt16Pure
	lnInt32 = lnInt32Pure
	lnInt64 = lnInt64Pure
	lnFloat32 = lnFloat32Pure
	lnFloat64 = lnFloat64Pure
}

func LnUint8(xs []uint8, rs []float64) LnResult {
	return lnUint8(xs, rs)
}

func lnUint8Pure(xs []uint8, rs []float64) LnResult {
	result := LnResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		rs[i] = math.Log(float64(n)) / math.Log(math.E)
	}
	return result
}

func LnUint16(xs []uint16, rs []float64) LnResult {
	return lnUint16(xs, rs)
}

func lnUint16Pure(xs []uint16, rs []float64) LnResult {
	result := LnResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		rs[i] = math.Log(float64(n)) / math.Log(math.E)
	}
	return result
}

func LnUint32(xs []uint32, rs []float64) LnResult {
	return lnUint32(xs, rs)
}

func lnUint32Pure(xs []uint32, rs []float64) LnResult {
	result := LnResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		rs[i] = math.Log(float64(n)) / math.Log(math.E)
	}
	return result
}

func LnUint64(xs []uint64, rs []float64) LnResult {
	return lnUint64(xs, rs)
}

func lnUint64Pure(xs []uint64, rs []float64) LnResult {
	result := LnResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		rs[i] = math.Log(float64(n)) / math.Log(math.E)
	}
	return result
}

func LnInt8(xs []int8, rs []float64) LnResult {
	return lnInt8(xs, rs)
}

func lnInt8Pure(xs []int8, rs []float64) LnResult {
	result := LnResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n <= 0 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			rs[i] = math.Log(float64(n)) / math.Log(math.E)
		}
	}
	return result
}

func LnInt16(xs []int16, rs []float64) LnResult {
	return lnInt16(xs, rs)
}

func lnInt16Pure(xs []int16, rs []float64) LnResult {
	result := LnResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n <= 0 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			rs[i] = math.Log(float64(n)) / math.Log(math.E)
		}
	}
	return result
}

func LnInt32(xs []int32, rs []float64) LnResult {
	return lnInt32(xs, rs)
}

func lnInt32Pure(xs []int32, rs []float64) LnResult {
	result := LnResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n <= 0 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			rs[i] = math.Log(float64(n)) / math.Log(math.E)
		}
	}
	return result
}

func LnInt64(xs []int64, rs []float64) LnResult {
	return lnInt64(xs, rs)
}

func lnInt64Pure(xs []int64, rs []float64) LnResult {
	result := LnResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n <= 0 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			rs[i] = math.Log(float64(n)) / math.Log(math.E)
		}
	}
	return result
}

func LnFloat32(xs []float32, rs []float64) LnResult {
	return lnFloat32(xs, rs)
}

func lnFloat32Pure(xs []float32, rs []float64) LnResult {
	result := LnResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n <= 0 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			rs[i] = math.Log(float64(n)) / math.Log(math.E)
		}
	}
	return result
}

func LnFloat64(xs []float64, rs []float64) LnResult {
	return lnFloat64(xs, rs)
}

func lnFloat64Pure(xs []float64, rs []float64) LnResult {
	result := LnResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n <= 0 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			rs[i] = math.Log(n) / math.Log(math.E)
		}
	}
	return result
}
