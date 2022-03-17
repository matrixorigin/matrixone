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

	"github.com/matrixorigin/matrixone/pkg/vectorize/floor"
)

/*ceil(1.23) -----> 2.0
ceil(1.23, 0) ----> 2.0
ceil(1.23, 1) -----> 1.3
ceil(12.34, -1) ---- > 20.0*/

var (
	ceilUint8   func([]uint8, []uint8, int64) []uint8
	ceilUint16  func([]uint16, []uint16, int64) []uint16
	ceilUint32  func([]uint32, []uint32, int64) []uint32
	ceilUint64  func([]uint64, []uint64, int64) []uint64
	ceilInt8    func([]int8, []int8, int64) []int8
	ceilInt16   func([]int16, []int16, int64) []int16
	ceilInt32   func([]int32, []int32, int64) []int32
	ceilInt64   func([]int64, []int64, int64) []int64
	ceilFloat32 func([]float32, []float32, int64) []float32
	ceilFloat64 func([]float64, []float64, int64) []float64
)

func init() {
	ceilUint8 = ceilUint8Pure
	ceilUint16 = ceilUint16Pure
	ceilUint32 = ceilUint32Pure
	ceilUint64 = ceilUint64Pure
	ceilInt8 = ceilInt8Pure
	ceilInt16 = ceilInt16Pure
	ceilInt32 = ceilInt32Pure
	ceilInt64 = ceilInt64Pure
	ceilFloat32 = ceilFloat32Pure
	ceilFloat64 = ceilFloat64Pure
}

func CeilUint8(xs, rs []uint8, digits int64) []uint8 {
	return ceilUint8(xs, rs, digits)
}

func ceilUint8Pure(xs, rs []uint8, digits int64) []uint8 {
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

func CeilUint16(xs, rs []uint16, digits int64) []uint16 {
	return ceilUint16(xs, rs, digits)
}

func ceilUint16Pure(xs, rs []uint16, digits int64) []uint16 {
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
	case digits <= -floor.MaxInt8digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func CeilUint32(xs, rs []uint32, digits int64) []uint32 {
	return ceilUint32(xs, rs, digits)
}

func ceilUint32Pure(xs, rs []uint32, digits int64) []uint32 {
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
	case digits <= -floor.MaxInt8digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func CeilUint64(xs, rs []uint64, digits int64) []uint64 {
	return ceilUint64(xs, rs, digits)
}

func ceilUint64Pure(xs, rs []uint64, digits int64) []uint64 {
	switch {
	case digits >= 0:
		return xs
	case digits > -floor.MaxUint64digits:
		scale := uint64(floor.ScaleTable[-digits])
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

func CeilInt8(xs, rs []int8, digits int64) []int8 {
	return ceilInt8(xs, rs, digits)
}

func ceilInt8Pure(xs, rs []int8, digits int64) []int8 {
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
	case digits <= -floor.MaxUint8digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func CeilInt16(xs, rs []int16, digits int64) []int16 {
	return ceilInt16(xs, rs, digits)
}

func ceilInt16Pure(xs, rs []int16, digits int64) []int16 {
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
	case digits <= -floor.MaxUint8digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func CeilInt32(xs, rs []int32, digits int64) []int32 {
	return ceilInt32(xs, rs, digits)
}

func ceilInt32Pure(xs, rs []int32, digits int64) []int32 {
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
	case digits <= -floor.MaxUint8digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func CeilInt64(xs, rs []int64, digits int64) []int64 {
	return ceilInt64(xs, rs, digits)
}

func ceilInt64Pure(xs, rs []int64, digits int64) []int64 {
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
	case digits <= -floor.MaxUint8digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func CeilFloat32(xs, rs []float32, digits int64) []float32 {
	return ceilFloat32(xs, rs, digits)
}

func ceilFloat32Pure(xs, rs []float32, digits int64) []float32 {
	if digits == 0 {
		for i := range xs {
			rs[i] = float32(math.Ceil(float64(xs[i])))
		}
	} else if digits > 0 {
		scale := float32(floor.ScaleTable[digits])
		for i := range xs {
			value := xs[i] * scale
			rs[i] = float32(math.Ceil(float64(value))) / scale
		}
	} else {
		scale := float32(floor.ScaleTable[-digits])
		for i := range xs {
			value := xs[i] / scale
			rs[i] = float32(math.Ceil(float64(value))) * scale
		}
	}
	return rs
}

func CeilFloat64(xs, rs []float64, digits int64) []float64 {
	return ceilFloat64(xs, rs, digits)
}

func ceilFloat64Pure(xs, rs []float64, digits int64) []float64 {
	if digits == 0 {
		for i := range xs {
			rs[i] = math.Ceil(xs[i])
		}
	} else if digits > 0 {
		scale := float64(floor.ScaleTable[digits])
		for i := range xs {
			value := xs[i] * scale
			rs[i] = math.Ceil(value) / scale
		}
	} else {
		scale := float64(floor.ScaleTable[-digits])
		for i := range xs {
			value := xs[i] / scale
			rs[i] = math.Ceil(value) * scale
		}
	}
	return rs
}
