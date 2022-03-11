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

package abs

var (
	absUint8   func([]uint8, []uint8) []uint8
	absUint16  func([]uint16, []uint16) []uint16
	absUint32  func([]uint32, []uint32) []uint32
	absUint64  func([]uint64, []uint64) []uint64
	absInt8    func([]int8, []int8) []int8
	absInt16   func([]int16, []int16) []int16
	absInt32   func([]int32, []int32) []int32
	absInt64   func([]int64, []int64) []int64
	absFloat32 func([]float32, []float32) []float32
	absFloat64 func([]float64, []float64) []float64
)

func init() {
	absUint8 = absUint8Pure
	absUint16 = absUint16Pure
	absUint32 = absUint32Pure
	absUint64 = absUint64Pure
	absInt8 = absInt8Pure
	absInt16 = absInt16Pure
	absInt32 = absInt32Pure
	absInt64 = absInt64Pure
	absFloat32 = absFloat32Pure
	absFloat64 = absFloat64Pure
}

func AbsUint8(xs, rs []uint8) []uint8 {
	return absUint8(xs, rs)
}

// uint8 simply return the input values.
func absUint8Pure(xs, rs []uint8) []uint8 {
	return xs
}

func AbsUint16(xs, rs []uint16) []uint16 {
	return absUint16(xs, rs)
}

// uint16 simply return the input values.
func absUint16Pure(xs, rs []uint16) []uint16 {
	return xs
}

func AbsUint32(xs, rs []uint32) []uint32 {
	return absUint32(xs, rs)
}

// uint32 simply return the input values.
func absUint32Pure(xs, rs []uint32) []uint32 {
	return xs
}

func AbsUint64(xs, rs []uint64) []uint64 {
	return absUint64(xs, rs)
}

// uint64 simply return the input values.
func absUint64Pure(xs, rs []uint64) []uint64 {
	return xs
}

func AbsInt8(xs, rs []int8) []int8 {
	return absInt8(xs, rs)
}

func absInt8Pure(xs, rs []int8) []int8 {
	for i := range xs {
		if xs[i] < 0 {
			rs[i] = -xs[i]
		} else {
			rs[i] = xs[i]
		}
	}
	return rs
}

func AbsInt16(xs, rs []int16) []int16 {
	return absInt16(xs, rs)
}

func absInt16Pure(xs, rs []int16) []int16 {
	for i := range xs {
		if xs[i] < 0 {
			rs[i] = -xs[i]
		} else {
			rs[i] = xs[i]
		}
	}
	return rs
}

func AbsInt32(xs, rs []int32) []int32 {
	return absInt32(xs, rs)
}

func absInt32Pure(xs, rs []int32) []int32 {
	for i := range xs {
		if xs[i] < 0 {
			rs[i] = -xs[i]
		} else {
			rs[i] = xs[i]
		}
	}
	return rs
}

func AbsInt64(xs, rs []int64) []int64 {
	return absInt64(xs, rs)
}

func absInt64Pure(xs, rs []int64) []int64 {
	for i := range xs {
		if xs[i] < 0 {
			rs[i] = -xs[i]
		} else {
			rs[i] = xs[i]
		}
	}
	return rs
}

func AbsFloat32(xs, rs []float32) []float32 {
	return absFloat32(xs, rs)
}

func absFloat32Pure(xs, rs []float32) []float32 {
	for i := range xs {
		if xs[i] < 0 {
			rs[i] = -xs[i]
		} else {
			rs[i] = xs[i]
		}
	}
	return rs
}

func AbsFloat64(xs, rs []float64) []float64 {
	return absFloat64(xs, rs)
}

func absFloat64Pure(xs, rs []float64) []float64 {
	for i := range xs {
		if xs[i] < 0 {
			rs[i] = -xs[i]
		} else {
			rs[i] = xs[i]
		}
	}
	return rs
}
