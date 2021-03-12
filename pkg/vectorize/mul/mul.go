package mul

var (
	int8Mul              func([]int8, []int8, []int8) []int8
	int8MulSels          func([]int8, []int8, []int8, []int64) []int8
	int8MulScalar        func(int8, []int8, []int8) []int8
	int8MulScalarSels    func(int8, []int8, []int8, []int64) []int8
	int16Mul             func([]int16, []int16, []int16) []int16
	int16MulSels         func([]int16, []int16, []int16, []int64) []int16
	int16MulScalar       func(int16, []int16, []int16) []int16
	int16MulScalarSels   func(int16, []int16, []int16, []int64) []int16
	int32Mul             func([]int32, []int32, []int32) []int32
	int32MulSels         func([]int32, []int32, []int32, []int64) []int32
	int32MulScalar       func(int32, []int32, []int32) []int32
	int32MulScalarSels   func(int32, []int32, []int32, []int64) []int32
	int64Mul             func([]int64, []int64, []int64) []int64
	int64MulSels         func([]int64, []int64, []int64, []int64) []int64
	int64MulScalar       func(int64, []int64, []int64) []int64
	int64MulScalarSels   func(int64, []int64, []int64, []int64) []int64
	uint8Mul             func([]uint8, []uint8, []uint8) []uint8
	uint8MulSels         func([]uint8, []uint8, []uint8, []int64) []uint8
	uint8MulScalar       func(uint8, []uint8, []uint8) []uint8
	uint8MulScalarSels   func(uint8, []uint8, []uint8, []int64) []uint8
	uint16Mul            func([]uint16, []uint16, []uint16) []uint16
	uint16MulSels        func([]uint16, []uint16, []uint16, []int64) []uint16
	uint16MulScalar      func(uint16, []uint16, []uint16) []uint16
	uint16MulScalarSels  func(uint16, []uint16, []uint16, []int64) []uint16
	uint32Mul            func([]uint32, []uint32, []uint32) []uint32
	uint32MulSels        func([]uint32, []uint32, []uint32, []int64) []uint32
	uint32MulScalar      func(uint32, []uint32, []uint32) []uint32
	uint32MulScalarSels  func(uint32, []uint32, []uint32, []int64) []uint32
	uint64Mul            func([]uint64, []uint64, []uint64) []uint64
	uint64MulSels        func([]uint64, []uint64, []uint64, []int64) []uint64
	uint64MulScalar      func(uint64, []uint64, []uint64) []uint64
	uint64MulScalarSels  func(uint64, []uint64, []uint64, []int64) []uint64
	float32Mul           func([]float32, []float32, []float32) []float32
	float32MulSels       func([]float32, []float32, []float32, []int64) []float32
	float32MulScalar     func(float32, []float32, []float32) []float32
	float32MulScalarSels func(float32, []float32, []float32, []int64) []float32
	float64Mul           func([]float64, []float64, []float64) []float64
	float64MulSels       func([]float64, []float64, []float64, []int64) []float64
	float64MulScalar     func(float64, []float64, []float64) []float64
	float64MulScalarSels func(float64, []float64, []float64, []int64) []float64
)

func init() {
	int8Mul = int8MulPure
	int8MulSels = int8MulSelsPure
	int8MulScalar = int8MulScalarPure
	int8MulScalarSels = int8MulScalarSelsPure
	int16Mul = int16MulPure
	int16MulSels = int16MulSelsPure
	int16MulScalar = int16MulScalarPure
	int16MulScalarSels = int16MulScalarSelsPure
	int32Mul = int32MulPure
	int32MulSels = int32MulSelsPure
	int32MulScalar = int32MulScalarPure
	int32MulScalarSels = int32MulScalarSelsPure
	int64Mul = int64MulPure
	int64MulSels = int64MulSelsPure
	int64MulScalar = int64MulScalarPure
	int64MulScalarSels = int64MulScalarSelsPure
	uint8Mul = uint8MulPure
	uint8MulSels = uint8MulSelsPure
	uint8MulScalar = uint8MulScalarPure
	uint8MulScalarSels = uint8MulScalarSelsPure
	uint16Mul = uint16MulPure
	uint16MulSels = uint16MulSelsPure
	uint16MulScalar = uint16MulScalarPure
	uint16MulScalarSels = uint16MulScalarSelsPure
	uint32Mul = uint32MulPure
	uint32MulSels = uint32MulSelsPure
	uint32MulScalar = uint32MulScalarPure
	uint32MulScalarSels = uint32MulScalarSelsPure
	uint64Mul = uint64MulPure
	uint64MulSels = uint64MulSelsPure
	uint64MulScalar = uint64MulScalarPure
	uint64MulScalarSels = uint64MulScalarSelsPure
	float32Mul = float32MulPure
	float32MulSels = float32MulSelsPure
	float32MulScalar = float32MulScalarPure
	float32MulScalarSels = float32MulScalarSelsPure
	float64Mul = float64MulPure
	float64MulSels = float64MulSelsPure
	float64MulScalar = float64MulScalarPure
	float64MulScalarSels = float64MulScalarSelsPure
}

func Int8Mul(xs, ys, rs []int8) []int8 {
	return int8Mul(xs, ys, rs)
}

func int8MulPure(xs, ys, rs []int8) []int8 {
	for i, x := range xs {
		rs[i] = x * ys[i]
	}
	return rs
}

func Int8MulSels(xs, ys, rs []int8, sels []int64) []int8 {
	return int8MulSels(xs, ys, rs, sels)
}

func int8MulSelsPure(xs, ys, rs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		rs[i] = xs[sel] * ys[sel]
	}
	return rs
}

func Int8MulScalar(x int8, ys, rs []int8) []int8 {
	return int8MulScalar(x, ys, rs)
}

func int8MulScalarPure(x int8, ys, rs []int8) []int8 {
	for i, y := range ys {
		rs[i] = x * y
	}
	return rs
}

func Int8MulScalarSels(x int8, ys, rs []int8, sels []int64) []int8 {
	return int8MulScalarSels(x, ys, rs, sels)
}

func int8MulScalarSelsPure(x int8, ys, rs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		rs[i] = x * ys[sel]
	}
	return rs
}

func Int16Mul(xs, ys, rs []int16) []int16 {
	return int16Mul(xs, ys, rs)
}

func int16MulPure(xs, ys, rs []int16) []int16 {
	for i, x := range xs {
		rs[i] = x * ys[i]
	}
	return rs
}

func Int16MulSels(xs, ys, rs []int16, sels []int64) []int16 {
	return int16MulSels(xs, ys, rs, sels)
}

func int16MulSelsPure(xs, ys, rs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		rs[i] = xs[sel] * ys[sel]
	}
	return rs
}

func Int16MulScalar(x int16, ys, rs []int16) []int16 {
	return int16MulScalar(x, ys, rs)
}

func int16MulScalarPure(x int16, ys, rs []int16) []int16 {
	for i, y := range ys {
		rs[i] = x * y
	}
	return rs
}

func Int16MulScalarSels(x int16, ys, rs []int16, sels []int64) []int16 {
	return int16MulScalarSels(x, ys, rs, sels)
}

func int16MulScalarSelsPure(x int16, ys, rs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		rs[i] = x * ys[sel]
	}
	return rs
}

func Int32Mul(xs, ys, rs []int32) []int32 {
	return int32Mul(xs, ys, rs)
}

func int32MulPure(xs, ys, rs []int32) []int32 {
	for i, x := range xs {
		rs[i] = x * ys[i]
	}
	return rs
}

func Int32MulSels(xs, ys, rs []int32, sels []int64) []int32 {
	return int32MulSels(xs, ys, rs, sels)
}

func int32MulSelsPure(xs, ys, rs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		rs[i] = xs[sel] * ys[sel]
	}
	return rs
}

func Int32MulScalar(x int32, ys, rs []int32) []int32 {
	return int32MulScalar(x, ys, rs)
}

func int32MulScalarPure(x int32, ys, rs []int32) []int32 {
	for i, y := range ys {
		rs[i] = x * y
	}
	return rs
}

func Int32MulScalarSels(x int32, ys, rs []int32, sels []int64) []int32 {
	return int32MulScalarSels(x, ys, rs, sels)
}

func int32MulScalarSelsPure(x int32, ys, rs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		rs[i] = x * ys[sel]
	}
	return rs
}

func Int64Mul(xs, ys, rs []int64) []int64 {
	return int64Mul(xs, ys, rs)
}

func int64MulPure(xs, ys, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = x * ys[i]
	}
	return rs
}

func Int64MulSels(xs, ys, rs []int64, sels []int64) []int64 {
	return int64MulSels(xs, ys, rs, sels)
}

func int64MulSelsPure(xs, ys, rs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		rs[i] = xs[sel] * ys[sel]
	}
	return rs
}

func Int64MulScalar(x int64, ys, rs []int64) []int64 {
	return int64MulScalar(x, ys, rs)
}

func int64MulScalarPure(x int64, ys, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = x * y
	}
	return rs
}

func Int64MulScalarSels(x int64, ys, rs []int64, sels []int64) []int64 {
	return int64MulScalarSels(x, ys, rs, sels)
}

func int64MulScalarSelsPure(x int64, ys, rs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		rs[i] = x * ys[sel]
	}
	return rs
}

func Uint8Mul(xs, ys, rs []uint8) []uint8 {
	return uint8Mul(xs, ys, rs)
}

func uint8MulPure(xs, ys, rs []uint8) []uint8 {
	for i, x := range xs {
		rs[i] = x * ys[i]
	}
	return rs
}

func Uint8MulSels(xs, ys, rs []uint8, sels []int64) []uint8 {
	return uint8MulSels(xs, ys, rs, sels)
}

func uint8MulSelsPure(xs, ys, rs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		rs[i] = xs[sel] * ys[sel]
	}
	return rs
}

func Uint8MulScalar(x uint8, ys, rs []uint8) []uint8 {
	return uint8MulScalar(x, ys, rs)
}

func uint8MulScalarPure(x uint8, ys, rs []uint8) []uint8 {
	for i, y := range ys {
		rs[i] = x * y
	}
	return rs
}

func Uint8MulScalarSels(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	return uint8MulScalarSels(x, ys, rs, sels)
}

func uint8MulScalarSelsPure(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		rs[i] = x * ys[sel]
	}
	return rs
}

func Uint16Mul(xs, ys, rs []uint16) []uint16 {
	return uint16Mul(xs, ys, rs)
}

func uint16MulPure(xs, ys, rs []uint16) []uint16 {
	for i, x := range xs {
		rs[i] = x * ys[i]
	}
	return rs
}

func Uint16MulSels(xs, ys, rs []uint16, sels []int64) []uint16 {
	return uint16MulSels(xs, ys, rs, sels)
}

func uint16MulSelsPure(xs, ys, rs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		rs[i] = xs[sel] * ys[sel]
	}
	return rs
}

func Uint16MulScalar(x uint16, ys, rs []uint16) []uint16 {
	return uint16MulScalar(x, ys, rs)
}

func uint16MulScalarPure(x uint16, ys, rs []uint16) []uint16 {
	for i, y := range ys {
		rs[i] = x * y
	}
	return rs
}

func Uint16MulScalarSels(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	return uint16MulScalarSels(x, ys, rs, sels)
}

func uint16MulScalarSelsPure(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		rs[i] = x * ys[sel]
	}
	return rs
}

func Uint32Mul(xs, ys, rs []uint32) []uint32 {
	return uint32Mul(xs, ys, rs)
}

func uint32MulPure(xs, ys, rs []uint32) []uint32 {
	for i, x := range xs {
		rs[i] = x * ys[i]
	}
	return rs
}

func Uint32MulSels(xs, ys, rs []uint32, sels []int64) []uint32 {
	return uint32MulSels(xs, ys, rs, sels)
}

func uint32MulSelsPure(xs, ys, rs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		rs[i] = xs[sel] * ys[sel]
	}
	return rs
}

func Uint32MulScalar(x uint32, ys, rs []uint32) []uint32 {
	return uint32MulScalar(x, ys, rs)
}

func uint32MulScalarPure(x uint32, ys, rs []uint32) []uint32 {
	for i, y := range ys {
		rs[i] = x * y
	}
	return rs
}

func Uint32MulScalarSels(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	return uint32MulScalarSels(x, ys, rs, sels)
}

func uint32MulScalarSelsPure(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		rs[i] = x * ys[sel]
	}
	return rs
}

func Uint64Mul(xs, ys, rs []uint64) []uint64 {
	return uint64Mul(xs, ys, rs)
}

func uint64MulPure(xs, ys, rs []uint64) []uint64 {
	for i, x := range xs {
		rs[i] = x * ys[i]
	}
	return rs
}

func Uint64MulSels(xs, ys, rs []uint64, sels []int64) []uint64 {
	return uint64MulSels(xs, ys, rs, sels)
}

func uint64MulSelsPure(xs, ys, rs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		rs[i] = xs[sel] * ys[sel]
	}
	return rs
}

func Uint64MulScalar(x uint64, ys, rs []uint64) []uint64 {
	return uint64MulScalar(x, ys, rs)
}

func uint64MulScalarPure(x uint64, ys, rs []uint64) []uint64 {
	for i, y := range ys {
		rs[i] = x * y
	}
	return rs
}

func Uint64MulScalarSels(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	return uint64MulScalarSels(x, ys, rs, sels)
}

func uint64MulScalarSelsPure(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		rs[i] = x * ys[sel]
	}
	return rs
}

func Float32Mul(xs, ys, rs []float32) []float32 {
	return float32Mul(xs, ys, rs)
}

func float32MulPure(xs, ys, rs []float32) []float32 {
	for i, x := range xs {
		rs[i] = x * ys[i]
	}
	return rs
}

func Float32MulSels(xs, ys, rs []float32, sels []int64) []float32 {
	return float32MulSels(xs, ys, rs, sels)
}

func float32MulSelsPure(xs, ys, rs []float32, sels []int64) []float32 {
	for i, sel := range sels {
		rs[i] = xs[sel] * ys[sel]
	}
	return rs
}

func Float32MulScalar(x float32, ys, rs []float32) []float32 {
	return float32MulScalar(x, ys, rs)
}

func float32MulScalarPure(x float32, ys, rs []float32) []float32 {
	for i, y := range ys {
		rs[i] = x * y
	}
	return rs
}

func Float32MulScalarSels(x float32, ys, rs []float32, sels []int64) []float32 {
	return float32MulScalarSels(x, ys, rs, sels)
}

func float32MulScalarSelsPure(x float32, ys, rs []float32, sels []int64) []float32 {
	for i, sel := range sels {
		rs[i] = x * ys[sel]
	}
	return rs
}

func Float64Mul(xs, ys, rs []float64) []float64 {
	return float64Mul(xs, ys, rs)
}

func float64MulPure(xs, ys, rs []float64) []float64 {
	for i, x := range xs {
		rs[i] = x * ys[i]
	}
	return rs
}

func Float64MulSels(xs, ys, rs []float64, sels []int64) []float64 {
	return float64MulSels(xs, ys, rs, sels)
}

func float64MulSelsPure(xs, ys, rs []float64, sels []int64) []float64 {
	for i, sel := range sels {
		rs[i] = xs[sel] * ys[sel]
	}
	return rs
}

func Float64MulScalar(x float64, ys, rs []float64) []float64 {
	return float64MulScalar(x, ys, rs)
}

func float64MulScalarPure(x float64, ys, rs []float64) []float64 {
	for i, y := range ys {
		rs[i] = x * y
	}
	return rs
}

func Float64MulScalarSels(x float64, ys, rs []float64, sels []int64) []float64 {
	return float64MulScalarSels(x, ys, rs, sels)
}

func float64MulScalarSelsPure(x float64, ys, rs []float64, sels []int64) []float64 {
	for i, sel := range sels {
		rs[i] = x * ys[sel]
	}
	return rs
}
