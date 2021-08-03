package mul

var (
	Int8Mul              func([]int8, []int8, []int8) []int8
	Int8MulSels          func([]int8, []int8, []int8, []int64) []int8
	Int8MulScalar        func(int8, []int8, []int8) []int8
	Int8MulScalarSels    func(int8, []int8, []int8, []int64) []int8
	Int16Mul             func([]int16, []int16, []int16) []int16
	Int16MulSels         func([]int16, []int16, []int16, []int64) []int16
	Int16MulScalar       func(int16, []int16, []int16) []int16
	Int16MulScalarSels   func(int16, []int16, []int16, []int64) []int16
	Int32Mul             func([]int32, []int32, []int32) []int32
	Int32MulSels         func([]int32, []int32, []int32, []int64) []int32
	Int32MulScalar       func(int32, []int32, []int32) []int32
	Int32MulScalarSels   func(int32, []int32, []int32, []int64) []int32
	Int64Mul             func([]int64, []int64, []int64) []int64
	Int64MulSels         func([]int64, []int64, []int64, []int64) []int64
	Int64MulScalar       func(int64, []int64, []int64) []int64
	Int64MulScalarSels   func(int64, []int64, []int64, []int64) []int64
	Uint8Mul             func([]uint8, []uint8, []uint8) []uint8
	Uint8MulSels         func([]uint8, []uint8, []uint8, []int64) []uint8
	Uint8MulScalar       func(uint8, []uint8, []uint8) []uint8
	Uint8MulScalarSels   func(uint8, []uint8, []uint8, []int64) []uint8
	Uint16Mul            func([]uint16, []uint16, []uint16) []uint16
	Uint16MulSels        func([]uint16, []uint16, []uint16, []int64) []uint16
	Uint16MulScalar      func(uint16, []uint16, []uint16) []uint16
	Uint16MulScalarSels  func(uint16, []uint16, []uint16, []int64) []uint16
	Uint32Mul            func([]uint32, []uint32, []uint32) []uint32
	Uint32MulSels        func([]uint32, []uint32, []uint32, []int64) []uint32
	Uint32MulScalar      func(uint32, []uint32, []uint32) []uint32
	Uint32MulScalarSels  func(uint32, []uint32, []uint32, []int64) []uint32
	Uint64Mul            func([]uint64, []uint64, []uint64) []uint64
	Uint64MulSels        func([]uint64, []uint64, []uint64, []int64) []uint64
	Uint64MulScalar      func(uint64, []uint64, []uint64) []uint64
	Uint64MulScalarSels  func(uint64, []uint64, []uint64, []int64) []uint64
	Float32Mul           func([]float32, []float32, []float32) []float32
	Float32MulSels       func([]float32, []float32, []float32, []int64) []float32
	Float32MulScalar     func(float32, []float32, []float32) []float32
	Float32MulScalarSels func(float32, []float32, []float32, []int64) []float32
	Float64Mul           func([]float64, []float64, []float64) []float64
	Float64MulSels       func([]float64, []float64, []float64, []int64) []float64
	Float64MulScalar     func(float64, []float64, []float64) []float64
	Float64MulScalarSels func(float64, []float64, []float64, []int64) []float64
)

func init() {
	Int8Mul = int8Mul
	Int8MulSels = int8MulSels
	Int8MulScalar = int8MulScalar
	Int8MulScalarSels = int8MulScalarSels
	Int16Mul = int16Mul
	Int16MulSels = int16MulSels
	Int16MulScalar = int16MulScalar
	Int16MulScalarSels = int16MulScalarSels
	Int32Mul = int32Mul
	Int32MulSels = int32MulSels
	Int32MulScalar = int32MulScalar
	Int32MulScalarSels = int32MulScalarSels
	Int64Mul = int64Mul
	Int64MulSels = int64MulSels
	Int64MulScalar = int64MulScalar
	Int64MulScalarSels = int64MulScalarSels
	Uint8Mul = uint8Mul
	Uint8MulSels = uint8MulSels
	Uint8MulScalar = uint8MulScalar
	Uint8MulScalarSels = uint8MulScalarSels
	Uint16Mul = uint16Mul
	Uint16MulSels = uint16MulSels
	Uint16MulScalar = uint16MulScalar
	Uint16MulScalarSels = uint16MulScalarSels
	Uint32Mul = uint32Mul
	Uint32MulSels = uint32MulSels
	Uint32MulScalar = uint32MulScalar
	Uint32MulScalarSels = uint32MulScalarSels
	Uint64Mul = uint64Mul
	Uint64MulSels = uint64MulSels
	Uint64MulScalar = uint64MulScalar
	Uint64MulScalarSels = uint64MulScalarSels
	Float32Mul = float32Mul
	Float32MulSels = float32MulSels
	Float32MulScalar = float32MulScalar
	Float32MulScalarSels = float32MulScalarSels
	Float64Mul = float64Mul
	Float64MulSels = float64MulSels
	Float64MulScalar = float64MulScalar
	Float64MulScalarSels = float64MulScalarSels
}

func int8Mul(xs, ys, rs []int8) []int8 {
	for i, x := range xs {
		rs[i] = x * ys[i]
	}
	return rs
}

func int8MulSels(xs, ys, rs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		rs[i] = xs[sel] * ys[sel]
	}
	return rs
}

func int8MulScalar(x int8, ys, rs []int8) []int8 {
	for i, y := range ys {
		rs[i] = x * y
	}
	return rs
}

func int8MulScalarSels(x int8, ys, rs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		rs[i] = x * ys[sel]
	}
	return rs
}

func int16Mul(xs, ys, rs []int16) []int16 {
	for i, x := range xs {
		rs[i] = x * ys[i]
	}
	return rs
}

func int16MulSels(xs, ys, rs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		rs[i] = xs[sel] * ys[sel]
	}
	return rs
}

func int16MulScalar(x int16, ys, rs []int16) []int16 {
	for i, y := range ys {
		rs[i] = x * y
	}
	return rs
}

func int16MulScalarSels(x int16, ys, rs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		rs[i] = x * ys[sel]
	}
	return rs
}

func int32Mul(xs, ys, rs []int32) []int32 {
	for i, x := range xs {
		rs[i] = x * ys[i]
	}
	return rs
}

func int32MulSels(xs, ys, rs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		rs[i] = xs[sel] * ys[sel]
	}
	return rs
}

func int32MulScalar(x int32, ys, rs []int32) []int32 {
	for i, y := range ys {
		rs[i] = x * y
	}
	return rs
}

func int32MulScalarSels(x int32, ys, rs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		rs[i] = x * ys[sel]
	}
	return rs
}

func int64Mul(xs, ys, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = x * ys[i]
	}
	return rs
}

func int64MulSels(xs, ys, rs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		rs[i] = xs[sel] * ys[sel]
	}
	return rs
}

func int64MulScalar(x int64, ys, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = x * y
	}
	return rs
}

func int64MulScalarSels(x int64, ys, rs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		rs[i] = x * ys[sel]
	}
	return rs
}

func uint8Mul(xs, ys, rs []uint8) []uint8 {
	for i, x := range xs {
		rs[i] = x * ys[i]
	}
	return rs
}

func uint8MulSels(xs, ys, rs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		rs[i] = xs[sel] * ys[sel]
	}
	return rs
}

func uint8MulScalar(x uint8, ys, rs []uint8) []uint8 {
	for i, y := range ys {
		rs[i] = x * y
	}
	return rs
}

func uint8MulScalarSels(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		rs[i] = x * ys[sel]
	}
	return rs
}

func uint16Mul(xs, ys, rs []uint16) []uint16 {
	for i, x := range xs {
		rs[i] = x * ys[i]
	}
	return rs
}

func uint16MulSels(xs, ys, rs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		rs[i] = xs[sel] * ys[sel]
	}
	return rs
}

func uint16MulScalar(x uint16, ys, rs []uint16) []uint16 {
	for i, y := range ys {
		rs[i] = x * y
	}
	return rs
}

func uint16MulScalarSels(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		rs[i] = x * ys[sel]
	}
	return rs
}

func uint32Mul(xs, ys, rs []uint32) []uint32 {
	for i, x := range xs {
		rs[i] = x * ys[i]
	}
	return rs
}

func uint32MulSels(xs, ys, rs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		rs[i] = xs[sel] * ys[sel]
	}
	return rs
}

func uint32MulScalar(x uint32, ys, rs []uint32) []uint32 {
	for i, y := range ys {
		rs[i] = x * y
	}
	return rs
}

func uint32MulScalarSels(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		rs[i] = x * ys[sel]
	}
	return rs
}

func uint64Mul(xs, ys, rs []uint64) []uint64 {
	for i, x := range xs {
		rs[i] = x * ys[i]
	}
	return rs
}

func uint64MulSels(xs, ys, rs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		rs[i] = xs[sel] * ys[sel]
	}
	return rs
}

func uint64MulScalar(x uint64, ys, rs []uint64) []uint64 {
	for i, y := range ys {
		rs[i] = x * y
	}
	return rs
}

func uint64MulScalarSels(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		rs[i] = x * ys[sel]
	}
	return rs
}

func float32Mul(xs, ys, rs []float32) []float32 {
	for i, x := range xs {
		rs[i] = x * ys[i]
	}
	return rs
}

func float32MulSels(xs, ys, rs []float32, sels []int64) []float32 {
	for i, sel := range sels {
		rs[i] = xs[sel] * ys[sel]
	}
	return rs
}

func float32MulScalar(x float32, ys, rs []float32) []float32 {
	for i, y := range ys {
		rs[i] = x * y
	}
	return rs
}

func float32MulScalarSels(x float32, ys, rs []float32, sels []int64) []float32 {
	for i, sel := range sels {
		rs[i] = x * ys[sel]
	}
	return rs
}

func float64Mul(xs, ys, rs []float64) []float64 {
	for i, x := range xs {
		rs[i] = x * ys[i]
	}
	return rs
}

func float64MulSels(xs, ys, rs []float64, sels []int64) []float64 {
	for i, sel := range sels {
		rs[i] = xs[sel] * ys[sel]
	}
	return rs
}

func float64MulScalar(x float64, ys, rs []float64) []float64 {
	for i, y := range ys {
		rs[i] = x * y
	}
	return rs
}

func float64MulScalarSels(x float64, ys, rs []float64, sels []int64) []float64 {
	for i, sel := range sels {
		rs[i] = x * ys[sel]
	}
	return rs
}
