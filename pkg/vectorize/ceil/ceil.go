// Copyright 2021 Matrix Origin
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

package ceil

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/vectorize/floor"
)

/*ceil(1.23) -----> 2.0
ceil(1.23, 0) ----> 2.0
ceil(1.23, 1) -----> 1.3
ceil(12.34, -1) ---- > 20.0*/

var (
	CeilUint8      func([]uint8, []uint8, int64) []uint8
	CeilUint16     func([]uint16, []uint16, int64) []uint16
	CeilUint32     func([]uint32, []uint32, int64) []uint32
	CeilUint64     func([]uint64, []uint64, int64) []uint64
	CeilInt8       func([]int8, []int8, int64) []int8
	CeilInt16      func([]int16, []int16, int64) []int16
	CeilInt32      func([]int32, []int32, int64) []int32
	CeilInt64      func([]int64, []int64, int64) []int64
	CeilFloat32    func([]float32, []float32, int64) []float32
	CeilFloat64    func([]float64, []float64, int64) []float64
	CeilDecimal64  func([]types.Decimal64, []types.Decimal64, int64, int32) []types.Decimal64
	CeilDecimal128 func([]types.Decimal128, []types.Decimal128, int64, int32) []types.Decimal128
)

func init() {
	CeilUint8 = ceilUint8
	CeilUint16 = ceilUint16
	CeilUint32 = ceilUint32
	CeilUint64 = ceilUint64
	CeilInt8 = ceilInt8
	CeilInt16 = ceilInt16
	CeilInt32 = ceilInt32
	CeilInt64 = ceilInt64
	CeilFloat32 = ceilFloat32
	CeilFloat64 = ceilFloat64
	CeilDecimal64 = ceilDecimal64
	CeilDecimal128 = ceilDecimal128
}

func ceilUint8(xs, rs []uint8, digits int64) []uint8 {
	// maximum uint8 number is 255, so we only need to worry about a few digit cases,
	switch {
	case digits >= 0:
		return xs
	case digits == -1 || digits == -2:
		scale := uint8(floor.ScaleTable[-digits])
		for i := range xs {
			t := xs[i] % scale
			s := xs[i]
			if t != 0 {
				s -= t
				rs[i] = (s + scale) / scale * scale
			} else {
				rs[i] = xs[i]
			}
		}
	case digits <= -floor.MaxUint8digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func ceilUint16(xs, rs []uint16, digits int64) []uint16 {
	switch {
	case digits >= 0:
		return xs
	case digits > -floor.MaxUint16digits:
		scale := uint16(floor.ScaleTable[-digits])
		for i := range xs {
			t := xs[i] % scale
			s := xs[i]
			if t != 0 {
				s -= t
				rs[i] = (s + scale) / scale * scale
			} else {
				rs[i] = xs[i]
			}
		}
	case digits <= -floor.MaxUint16digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func ceilUint32(xs, rs []uint32, digits int64) []uint32 {
	switch {
	case digits >= 0:
		return xs
	case digits > -floor.MaxUint32digits:
		scale := uint32(floor.ScaleTable[-digits])
		for i := range xs {
			t := xs[i] % scale
			s := xs[i]
			if t != 0 {
				s -= t
				rs[i] = (s + scale) / scale * scale
			} else {
				rs[i] = xs[i]
			}
		}
	case digits <= -floor.MaxUint32digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func ceilUint64(xs, rs []uint64, digits int64) []uint64 {
	switch {
	case digits >= 0:
		return xs
	case digits > -floor.MaxUint64digits:
		scale := floor.ScaleTable[-digits]
		for i := range xs {
			t := xs[i] % scale
			s := xs[i]
			if t != 0 {
				s -= t
				rs[i] = (s + scale) / scale * scale
			} else {
				rs[i] = xs[i]
			}
		}
	case digits <= -floor.MaxUint64digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func ceilInt8(xs, rs []int8, digits int64) []int8 {
	switch {
	case digits >= 0:
		return xs
	case digits == -1 || digits == -2:
		scale := int8(floor.ScaleTable[-digits])
		for i := range xs {
			t := xs[i] % scale
			s := xs[i]
			if t != 0 {
				s -= t
				if s >= 0 && xs[i] > 0 {
					rs[i] = (s + scale) / scale * scale
				} else {
					rs[i] = s
				}
			} else {
				rs[i] = xs[i]
			}
		}
	case digits <= -floor.MaxInt8digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func ceilInt16(xs, rs []int16, digits int64) []int16 {
	switch {
	case digits >= 0:
		return xs
	case digits > -floor.MaxInt16digits:
		scale := int16(floor.ScaleTable[-digits])
		for i := range xs {
			t := xs[i] % scale
			s := xs[i]
			if t != 0 {
				s -= t
				if s >= 0 && xs[i] > 0 {
					rs[i] = (s + scale) / scale * scale
				} else {
					rs[i] = s
				}
			} else {
				rs[i] = xs[i]
			}
		}
	case digits <= -floor.MaxInt16digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func ceilInt32(xs, rs []int32, digits int64) []int32 {
	switch {
	case digits >= 0:
		return xs
	case digits > -floor.MaxInt32digits:
		scale := int32(floor.ScaleTable[-digits])
		for i := range xs {
			t := xs[i] % scale
			s := xs[i]
			if t != 0 {
				s -= t
				if s >= 0 && xs[i] > 0 {
					rs[i] = (s + scale) / scale * scale
				} else {
					rs[i] = s
				}
			} else {
				rs[i] = xs[i]
			}
		}
	case digits <= -floor.MaxInt32digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func ceilInt64(xs, rs []int64, digits int64) []int64 {
	switch {
	case digits >= 0:
		return xs
	case digits > -floor.MaxInt64digits:
		scale := int64(floor.ScaleTable[-digits])
		for i := range xs {
			t := xs[i] % scale
			s := xs[i]
			if t != 0 {
				s -= t
				if s >= 0 && xs[i] > 0 {
					rs[i] = (s + scale) / scale * scale
				} else {
					rs[i] = s
				}
			} else {
				rs[i] = xs[i]
			}
		}
	case digits <= -floor.MaxInt64digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func ceilFloat32(xs, rs []float32, digits int64) []float32 {
	if digits == 0 {
		for i := range xs {
			rs[i] = float32(math.Ceil(float64(xs[i])))
		}
	} else {
		scale := float32(math.Pow10(int(digits)))
		for i := range xs {
			value := xs[i] * scale
			rs[i] = float32(math.Ceil(float64(value))) / scale
		}
	}
	return rs
}

func ceilFloat64(xs, rs []float64, digits int64) []float64 {
	if digits == 0 {
		for i := range xs {
			rs[i] = math.Ceil(xs[i])
		}
	} else {
		scale := math.Pow10(int(digits))
		for i := range xs {
			value := xs[i] * scale
			rs[i] = math.Ceil(value) / scale
		}
	}
	return rs
}

func ceilDecimal64(xs, rs []types.Decimal64, digits int64, scale int32) []types.Decimal64 {
	if digits > 19 {
		digits = 19
	}
	if digits < -18 {
		digits = -18
	}
	for i := range xs {
		rs[i] = xs[i].Ceil(scale, int32(digits))
	}
	return rs
}

func ceilDecimal128(xs, rs []types.Decimal128, digits int64, scale int32) []types.Decimal128 {
	if digits > 39 {
		digits = 39
	}
	if digits < -38 {
		digits = -38
	}
	for i := range xs {
		rs[i] = xs[i].Ceil(scale, int32(digits))
	}
	return rs
}
