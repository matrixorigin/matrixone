package sin

import "math"

var (
	sinUint8   func([]uint8, []float64) []float64
	sinUint16  func([]uint16, []float64) []float64
	sinUint32  func([]uint32, []float64) []float64
	sinUint64  func([]uint64, []float64) []float64
	sinInt8    func([]int8, []float64) []float64
	sinInt16   func([]int16, []float64) []float64
	sinInt32   func([]int32, []float64) []float64
	sinInt64   func([]int64, []float64) []float64
	sinFloat32 func([]float32, []float64) []float64
	sinFloat64 func([]float64, []float64) []float64
)

func init() {
	sinUint8 = sinUint8Pure
	sinUint16 = sinUint16Pure
	sinUint32 = sinUint32Pure
	sinUint64 = sinUint64Pure
	sinInt8 = sinInt8Pure
	sinInt16 = sinInt16Pure
	sinInt32 = sinInt32Pure
	sinInt64 = sinInt64Pure
	sinFloat32 = sinFloat32Pure
	sinFloat64 = sinFloat64Pure
}

func SinFloat32(xs []float32, rs []float64) []float64 {
	return sinFloat32(xs, rs)
}

func sinFloat32Pure(xs []float32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sin(float64(n))
	}
	return rs
}

func SinFloat64(xs, rs []float64) []float64 {
	return sinFloat64(xs, rs)
}

func sinFloat64Pure(xs, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sin(n)
	}
	return rs
}

func SinUint8(xs []uint8, rs []float64) []float64 {
	return sinUint8(xs, rs)
}

func sinUint8Pure(xs []uint8, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sin(float64(n))
	}
	return rs
}

func SinUint16(xs []uint16, rs []float64) []float64 {
	return sinUint16(xs, rs)
}

func sinUint16Pure(xs []uint16, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sin(float64(n))
	}
	return rs
}

func SinUint32(xs []uint32, rs []float64) []float64 {
	return sinUint32(xs, rs)
}

func sinUint32Pure(xs []uint32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sin(float64(n))
	}
	return rs
}

func SinUint64(xs []uint64, rs []float64) []float64 {
	return sinUint64(xs, rs)
}

func sinUint64Pure(xs []uint64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sin(float64(n))
	}
	return rs
}

func SinInt8(xs []int8, rs []float64) []float64 {
	return sinInt8(xs, rs)
}

func sinInt8Pure(xs []int8, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sin(float64(n))
	}
	return rs
}

func SinInt16(xs []int16, rs []float64) []float64 {
	return sinInt16(xs, rs)
}

func sinInt16Pure(xs []int16, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sin(float64(n))
	}
	return rs
}

func SinInt32(xs []int32, rs []float64) []float64 {
	return sinInt32(xs, rs)
}

func sinInt32Pure(xs []int32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sin(float64(n))
	}
	return rs
}

func SinInt64(xs []int64, rs []float64) []float64 {
	return sinInt64(xs, rs)
}

func sinInt64Pure(xs []int64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sin(float64(n))
	}
	return rs
}
