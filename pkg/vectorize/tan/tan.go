package tan

import (
	"math"
)

var (
	tanUint8   func([]uint8, []float64) []float64
	tanUint16  func([]uint16, []float64) []float64
	tanUint32  func([]uint32, []float64) []float64
	tanUint64  func([]uint64, []float64) []float64
	tanInt8    func([]int8, []float64) []float64
	tanInt16   func([]int16, []float64) []float64
	tanInt32   func([]int32, []float64) []float64
	tanInt64   func([]int64, []float64) []float64
	tanFloat32 func([]float32, []float64) []float64
	tanFloat64 func([]float64, []float64) []float64
)

func init() {
	tanUint8 = tanUint8Pure
	tanUint16 = tanUint16Pure
	tanUint32 = tanUint32Pure
	tanUint64 = tanUint64Pure
	tanInt8 = tanInt8Pure
	tanInt16 = tanInt16Pure
	tanInt32 = tanInt32Pure
	tanInt64 = tanInt64Pure
	tanFloat32 = tanFloat32Pure
	tanFloat64 = tanFloat64Pure
}

func TanUint8(xs []uint8, rs []float64) []float64 {
	return tanUint8(xs, rs)
}

func tanUint8Pure(xs []uint8, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Tan(float64(n))
	}
	return rs
}

func TanUint16(xs []uint16, rs []float64) []float64 {
	return tanUint16(xs, rs)
}

func tanUint16Pure(xs []uint16, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Tan(float64(n))
	}
	return rs
}

func TanUint32(xs []uint32, rs []float64) []float64 {
	return tanUint32(xs, rs)
}

func tanUint32Pure(xs []uint32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Tan(float64(n))
	}
	return rs
}

func TanUint64(xs []uint64, rs []float64) []float64 {
	return tanUint64(xs, rs)
}

func tanUint64Pure(xs []uint64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Tan(float64(n))
	}
	return rs
}

func TanInt8(xs []int8, rs []float64) []float64 {
	return tanInt8(xs, rs)
}

func tanInt8Pure(xs []int8, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Tan(float64(n))
	}
	return rs
}

func TanInt16(xs []int16, rs []float64) []float64 {
	return tanInt16(xs, rs)
}

func tanInt16Pure(xs []int16, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Tan(float64(n))
	}
	return rs
}

func TanInt32(xs []int32, rs []float64) []float64 {
	return tanInt32(xs, rs)
}

func tanInt32Pure(xs []int32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Tan(float64(n))
	}
	return rs
}

func TanInt64(xs []int64, rs []float64) []float64 {
	return tanInt64(xs, rs)
}

func tanInt64Pure(xs []int64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Tan(float64(n))
	}
	return rs
}

func TanFloat32(xs []float32, rs []float64) []float64 {
	return tanFloat32(xs, rs)
}

func tanFloat32Pure(xs []float32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Tan(float64(n))
	}
	return rs
}

func TanFloat64(xs []float64, rs []float64) []float64 {
	return tanFloat64(xs, rs)
}

func tanFloat64Pure(xs []float64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Tan(n)
	}
	return rs
}