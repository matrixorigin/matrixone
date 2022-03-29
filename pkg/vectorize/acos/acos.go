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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"math"
)

var (
	acosUint8   func([]uint8, []float64) LogResult
	acosUint16  func([]uint16, []float64) LogResult
	acosUint32  func([]uint32, []float64) LogResult
	acosUint64  func([]uint64, []float64) LogResult
	acosInt8    func([]int8, []float64) LogResult
	acosInt16   func([]int16, []float64) LogResult
	acosInt32   func([]int32, []float64) LogResult
	acosInt64   func([]int64, []float64) LogResult
	acosFloat32 func([]float32, []float64) LogResult
	acosFloat64 func([]float64, []float64) LogResult
)

func init() {
	acosUint8 = acosUint8Pure
	acosUint16 = acosUint16Pure
	acosUint32 = acosUint32Pure
	acosUint64 = acosUint64Pure
	acosInt8 = acosInt8Pure
	acosInt16 = acosInt16Pure
	acosInt32 = acosInt32Pure
	acosInt64 = acosInt64Pure
	acosFloat32 = acosFloat32Pure
	acosFloat64 = acosFloat64Pure
}

type LogResult struct {
	Result []float64
	Nsp    *nulls.Nulls
}

func AcosUint8(xs []uint8, rs []float64) LogResult {
	return acosUint8(xs, rs)
}

func acosUint8Pure(xs []uint8, rs []float64) LogResult {
	result := LogResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Acos(float64(n))
		}
	}
	return result
}

func AcosUint16(xs []uint16, rs []float64) LogResult {
	return acosUint16(xs, rs)
}

func acosUint16Pure(xs []uint16, rs []float64) LogResult {
	result := LogResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Acos(float64(n))
		}
	}
	return result
}

func AcosUint32(xs []uint32, rs []float64) LogResult {
	return acosUint32(xs, rs)
}

func acosUint32Pure(xs []uint32, rs []float64) LogResult {
	result := LogResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Acos(float64(n))
		}
	}
	return result
}

func AcosUint64(xs []uint64, rs []float64) LogResult {
	return acosUint64(xs, rs)
}

func acosUint64Pure(xs []uint64, rs []float64) LogResult {
	result := LogResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Acos(float64(n))
		}
	}
	return result
}

func AcosInt8(xs []int8, rs []float64) LogResult {
	return acosInt8(xs, rs)
}

func acosInt8Pure(xs []int8, rs []float64) LogResult {
	result := LogResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n < -1 || n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Acos(float64(n))
		}
	}
	return result
}

func AcosInt16(xs []int16, rs []float64) LogResult {
	return acosInt16(xs, rs)
}

func acosInt16Pure(xs []int16, rs []float64) LogResult {
	result := LogResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n < -1 || n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Acos(float64(n))
		}
	}
	return result
}

func AcosInt32(xs []int32, rs []float64) LogResult {
	return acosInt32(xs, rs)
}

func acosInt32Pure(xs []int32, rs []float64) LogResult {
	result := LogResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n < -1 || n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Acos(float64(n))
		}
	}
	return result
}

func AcosInt64(xs []int64, rs []float64) LogResult {
	return acosInt64(xs, rs)
}

func acosInt64Pure(xs []int64, rs []float64) LogResult {
	result := LogResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n < -1 || n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Acos(float64(n))
		}
	}
	return result
}

func AcosFloat32(xs []float32, rs []float64) LogResult {
	return acosFloat32(xs, rs)
}

func acosFloat32Pure(xs []float32, rs []float64) LogResult {
	result := LogResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n < -1 || n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Acos(float64(n))
		}
	}
	return result
}

func AcosFloat64(xs []float64, rs []float64) LogResult {
	return acosFloat64(xs, rs)
}

func acosFloat64Pure(xs []float64, rs []float64) LogResult {
	result := LogResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n < -1 || n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Acos(n)
		}
	}
	return result
}
