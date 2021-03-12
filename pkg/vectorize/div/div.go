package div

var (
	int8Div                func([]int8, []int8, []int8) []int8
	int8DivSels            func([]int8, []int8, []int8, []int64) []int8
	int8DivScalar          func(int8, []int8, []int8) []int8
	int8DivScalarSels      func(int8, []int8, []int8, []int64) []int8
	int8DivByScalar        func(int8, []int8, []int8) []int8
	int8DivByScalarSels    func(int8, []int8, []int8, []int64) []int8
	int16Div               func([]int16, []int16, []int16) []int16
	int16DivSels           func([]int16, []int16, []int16, []int64) []int16
	int16DivScalar         func(int16, []int16, []int16) []int16
	int16DivScalarSels     func(int16, []int16, []int16, []int64) []int16
	int16DivByScalar       func(int16, []int16, []int16) []int16
	int16DivByScalarSels   func(int16, []int16, []int16, []int64) []int16
	int32Div               func([]int32, []int32, []int32) []int32
	int32DivSels           func([]int32, []int32, []int32, []int64) []int32
	int32DivScalar         func(int32, []int32, []int32) []int32
	int32DivScalarSels     func(int32, []int32, []int32, []int64) []int32
	int32DivByScalar       func(int32, []int32, []int32) []int32
	int32DivByScalarSels   func(int32, []int32, []int32, []int64) []int32
	int64Div               func([]int64, []int64, []int64) []int64
	int64DivSels           func([]int64, []int64, []int64, []int64) []int64
	int64DivScalar         func(int64, []int64, []int64) []int64
	int64DivScalarSels     func(int64, []int64, []int64, []int64) []int64
	int64DivByScalar       func(int64, []int64, []int64) []int64
	int64DivByScalarSels   func(int64, []int64, []int64, []int64) []int64
	uint8Div               func([]uint8, []uint8, []uint8) []uint8
	uint8DivSels           func([]uint8, []uint8, []uint8, []int64) []uint8
	uint8DivScalar         func(uint8, []uint8, []uint8) []uint8
	uint8DivScalarSels     func(uint8, []uint8, []uint8, []int64) []uint8
	uint8DivByScalar       func(uint8, []uint8, []uint8) []uint8
	uint8DivByScalarSels   func(uint8, []uint8, []uint8, []int64) []uint8
	uint16Div              func([]uint16, []uint16, []uint16) []uint16
	uint16DivSels          func([]uint16, []uint16, []uint16, []int64) []uint16
	uint16DivScalar        func(uint16, []uint16, []uint16) []uint16
	uint16DivScalarSels    func(uint16, []uint16, []uint16, []int64) []uint16
	uint16DivByScalar      func(uint16, []uint16, []uint16) []uint16
	uint16DivByScalarSels  func(uint16, []uint16, []uint16, []int64) []uint16
	uint32Div              func([]uint32, []uint32, []uint32) []uint32
	uint32DivSels          func([]uint32, []uint32, []uint32, []int64) []uint32
	uint32DivScalar        func(uint32, []uint32, []uint32) []uint32
	uint32DivScalarSels    func(uint32, []uint32, []uint32, []int64) []uint32
	uint32DivByScalar      func(uint32, []uint32, []uint32) []uint32
	uint32DivByScalarSels  func(uint32, []uint32, []uint32, []int64) []uint32
	uint64Div              func([]uint64, []uint64, []uint64) []uint64
	uint64DivSels          func([]uint64, []uint64, []uint64, []int64) []uint64
	uint64DivScalar        func(uint64, []uint64, []uint64) []uint64
	uint64DivScalarSels    func(uint64, []uint64, []uint64, []int64) []uint64
	uint64DivByScalar      func(uint64, []uint64, []uint64) []uint64
	uint64DivByScalarSels  func(uint64, []uint64, []uint64, []int64) []uint64
	float32Div             func([]float32, []float32, []float32) []float32
	float32DivSels         func([]float32, []float32, []float32, []int64) []float32
	float32DivScalar       func(float32, []float32, []float32) []float32
	float32DivScalarSels   func(float32, []float32, []float32, []int64) []float32
	float32DivByScalar     func(float32, []float32, []float32) []float32
	float32DivByScalarSels func(float32, []float32, []float32, []int64) []float32
	float64Div             func([]float64, []float64, []float64) []float64
	float64DivSels         func([]float64, []float64, []float64, []int64) []float64
	float64DivScalar       func(float64, []float64, []float64) []float64
	float64DivScalarSels   func(float64, []float64, []float64, []int64) []float64
	float64DivByScalar     func(float64, []float64, []float64) []float64
	float64DivByScalarSels func(float64, []float64, []float64, []int64) []float64
)

func init() {
	int8Div = int8DivPure
	int8DivSels = int8DivSelsPure
	int8DivScalar = int8DivScalarPure
	int8DivScalarSels = int8DivScalarSelsPure
	int8DivByScalar = int8DivByScalarPure
	int8DivByScalarSels = int8DivByScalarSelsPure
	int16Div = int16DivPure
	int16DivSels = int16DivSelsPure
	int16DivScalar = int16DivScalarPure
	int16DivScalarSels = int16DivScalarSelsPure
	int16DivByScalar = int16DivByScalarPure
	int16DivByScalarSels = int16DivByScalarSelsPure
	int32Div = int32DivPure
	int32DivSels = int32DivSelsPure
	int32DivScalar = int32DivScalarPure
	int32DivScalarSels = int32DivScalarSelsPure
	int32DivByScalar = int32DivByScalarPure
	int32DivByScalarSels = int32DivByScalarSelsPure
	int64Div = int64DivPure
	int64DivSels = int64DivSelsPure
	int64DivScalar = int64DivScalarPure
	int64DivScalarSels = int64DivScalarSelsPure
	int64DivByScalar = int64DivByScalarPure
	int64DivByScalarSels = int64DivByScalarSelsPure
	uint8Div = uint8DivPure
	uint8DivSels = uint8DivSelsPure
	uint8DivScalar = uint8DivScalarPure
	uint8DivScalarSels = uint8DivScalarSelsPure
	uint8DivByScalar = uint8DivByScalarPure
	uint8DivByScalarSels = uint8DivByScalarSelsPure
	uint16Div = uint16DivPure
	uint16DivSels = uint16DivSelsPure
	uint16DivScalar = uint16DivScalarPure
	uint16DivScalarSels = uint16DivScalarSelsPure
	uint16DivByScalar = uint16DivByScalarPure
	uint16DivByScalarSels = uint16DivByScalarSelsPure
	uint32Div = uint32DivPure
	uint32DivSels = uint32DivSelsPure
	uint32DivScalar = uint32DivScalarPure
	uint32DivScalarSels = uint32DivScalarSelsPure
	uint32DivByScalar = uint32DivByScalarPure
	uint32DivByScalarSels = uint32DivByScalarSelsPure
	uint64Div = uint64DivPure
	uint64DivSels = uint64DivSelsPure
	uint64DivScalar = uint64DivScalarPure
	uint64DivScalarSels = uint64DivScalarSelsPure
	uint64DivByScalar = uint64DivByScalarPure
	uint64DivByScalarSels = uint64DivByScalarSelsPure
	float32Div = float32DivPure
	float32DivSels = float32DivSelsPure
	float32DivScalar = float32DivScalarPure
	float32DivScalarSels = float32DivScalarSelsPure
	float32DivByScalar = float32DivByScalarPure
	float32DivByScalarSels = float32DivByScalarSelsPure
	float64Div = float64DivPure
	float64DivSels = float64DivSelsPure
	float64DivScalar = float64DivScalarPure
	float64DivScalarSels = float64DivScalarSelsPure
	float64DivByScalar = float64DivByScalarPure
	float64DivByScalarSels = float64DivByScalarSelsPure
}

func Int8Div(xs, ys, rs []int8) []int8 {
	return int8Div(xs, ys, rs)
}

func int8DivPure(xs, ys, rs []int8) []int8 {
	for i, x := range xs {
		rs[i] = x / ys[i]
	}
	return rs
}

func Int8DivSels(xs, ys, rs []int8, sels []int64) []int8 {
	return int8DivSels(xs, ys, rs, sels)
}

func int8DivSelsPure(xs, ys, rs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		rs[i] = xs[sel] / ys[sel]
	}
	return rs
}

func Int8DivScalar(x int8, ys, rs []int8) []int8 {
	return int8DivScalar(x, ys, rs)
}

func int8DivScalarPure(x int8, ys, rs []int8) []int8 {
	for i, y := range ys {
		rs[i] = x / y
	}
	return rs
}

func Int8DivScalarSels(x int8, ys, rs []int8, sels []int64) []int8 {
	return int8DivScalarSels(x, ys, rs, sels)
}

func int8DivScalarSelsPure(x int8, ys, rs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		rs[i] = x / ys[sel]
	}
	return rs
}

func Int8DivByScalar(x int8, ys, rs []int8) []int8 {
	return int8DivByScalar(x, ys, rs)
}

func int8DivByScalarPure(x int8, ys, rs []int8) []int8 {
	for i, y := range ys {
		rs[i] = y / x
	}
	return rs
}

func Int8DivByScalarSels(x int8, ys, rs []int8, sels []int64) []int8 {
	return int8DivByScalarSels(x, ys, rs, sels)
}

func int8DivByScalarSelsPure(x int8, ys, rs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		rs[i] = ys[sel] / x
	}
	return rs
}

func Int16Div(xs, ys, rs []int16) []int16 {
	return int16Div(xs, ys, rs)
}

func int16DivPure(xs, ys, rs []int16) []int16 {
	for i, x := range xs {
		rs[i] = x / ys[i]
	}
	return rs
}

func Int16DivSels(xs, ys, rs []int16, sels []int64) []int16 {
	return int16DivSels(xs, ys, rs, sels)
}

func int16DivSelsPure(xs, ys, rs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		rs[i] = xs[sel] / ys[sel]
	}
	return rs
}

func Int16DivScalar(x int16, ys, rs []int16) []int16 {
	return int16DivScalar(x, ys, rs)
}

func int16DivScalarPure(x int16, ys, rs []int16) []int16 {
	for i, y := range ys {
		rs[i] = x / y
	}
	return rs
}

func Int16DivScalarSels(x int16, ys, rs []int16, sels []int64) []int16 {
	return int16DivScalarSels(x, ys, rs, sels)
}

func int16DivScalarSelsPure(x int16, ys, rs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		rs[i] = x / ys[sel]
	}
	return rs
}

func Int16DivByScalar(x int16, ys, rs []int16) []int16 {
	return int16DivByScalar(x, ys, rs)
}

func int16DivByScalarPure(x int16, ys, rs []int16) []int16 {
	for i, y := range ys {
		rs[i] = y / x
	}
	return rs
}

func Int16DivByScalarSels(x int16, ys, rs []int16, sels []int64) []int16 {
	return int16DivByScalarSels(x, ys, rs, sels)
}

func int16DivByScalarSelsPure(x int16, ys, rs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		rs[i] = ys[sel] / x
	}
	return rs
}

func Int32Div(xs, ys, rs []int32) []int32 {
	return int32Div(xs, ys, rs)
}

func int32DivPure(xs, ys, rs []int32) []int32 {
	for i, x := range xs {
		rs[i] = x / ys[i]
	}
	return rs
}

func Int32DivSels(xs, ys, rs []int32, sels []int64) []int32 {
	return int32DivSels(xs, ys, rs, sels)
}

func int32DivSelsPure(xs, ys, rs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		rs[i] = xs[sel] / ys[sel]
	}
	return rs
}

func Int32DivScalar(x int32, ys, rs []int32) []int32 {
	return int32DivScalar(x, ys, rs)
}

func int32DivScalarPure(x int32, ys, rs []int32) []int32 {
	for i, y := range ys {
		rs[i] = x / y
	}
	return rs
}

func Int32DivScalarSels(x int32, ys, rs []int32, sels []int64) []int32 {
	return int32DivScalarSels(x, ys, rs, sels)
}

func int32DivScalarSelsPure(x int32, ys, rs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		rs[i] = x / ys[sel]
	}
	return rs
}

func Int32DivByScalar(x int32, ys, rs []int32) []int32 {
	return int32DivByScalar(x, ys, rs)
}

func int32DivByScalarPure(x int32, ys, rs []int32) []int32 {
	for i, y := range ys {
		rs[i] = y / x
	}
	return rs
}

func Int32DivByScalarSels(x int32, ys, rs []int32, sels []int64) []int32 {
	return int32DivByScalarSels(x, ys, rs, sels)
}

func int32DivByScalarSelsPure(x int32, ys, rs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		rs[i] = ys[sel] / x
	}
	return rs
}

func Int64Div(xs, ys, rs []int64) []int64 {
	return int64Div(xs, ys, rs)
}

func int64DivPure(xs, ys, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = x / ys[i]
	}
	return rs
}

func Int64DivSels(xs, ys, rs []int64, sels []int64) []int64 {
	return int64DivSels(xs, ys, rs, sels)
}

func int64DivSelsPure(xs, ys, rs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		rs[i] = xs[sel] / ys[sel]
	}
	return rs
}

func Int64DivScalar(x int64, ys, rs []int64) []int64 {
	return int64DivScalar(x, ys, rs)
}

func int64DivScalarPure(x int64, ys, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = x / y
	}
	return rs
}

func Int64DivScalarSels(x int64, ys, rs []int64, sels []int64) []int64 {
	return int64DivScalarSels(x, ys, rs, sels)
}

func int64DivScalarSelsPure(x int64, ys, rs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		rs[i] = x / ys[sel]
	}
	return rs
}

func Int64DivByScalar(x int64, ys, rs []int64) []int64 {
	return int64DivByScalar(x, ys, rs)
}

func int64DivByScalarPure(x int64, ys, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = y / x
	}
	return rs
}

func Int64DivByScalarSels(x int64, ys, rs []int64, sels []int64) []int64 {
	return int64DivByScalarSels(x, ys, rs, sels)
}

func int64DivByScalarSelsPure(x int64, ys, rs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		rs[i] = ys[sel] / x
	}
	return rs
}

func Uint8Div(xs, ys, rs []uint8) []uint8 {
	return uint8Div(xs, ys, rs)
}

func uint8DivPure(xs, ys, rs []uint8) []uint8 {
	for i, x := range xs {
		rs[i] = x / ys[i]
	}
	return rs
}

func Uint8DivSels(xs, ys, rs []uint8, sels []int64) []uint8 {
	return uint8DivSels(xs, ys, rs, sels)
}

func uint8DivSelsPure(xs, ys, rs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		rs[i] = xs[sel] / ys[sel]
	}
	return rs
}

func Uint8DivScalar(x uint8, ys, rs []uint8) []uint8 {
	return uint8DivScalar(x, ys, rs)
}

func uint8DivScalarPure(x uint8, ys, rs []uint8) []uint8 {
	for i, y := range ys {
		rs[i] = x / y
	}
	return rs
}

func Uint8DivScalarSels(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	return uint8DivScalarSels(x, ys, rs, sels)
}

func uint8DivScalarSelsPure(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		rs[i] = x / ys[sel]
	}
	return rs
}

func Uint8DivByScalar(x uint8, ys, rs []uint8) []uint8 {
	return uint8DivByScalar(x, ys, rs)
}

func uint8DivByScalarPure(x uint8, ys, rs []uint8) []uint8 {
	for i, y := range ys {
		rs[i] = y / x
	}
	return rs
}

func Uint8DivByScalarSels(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	return uint8DivByScalarSels(x, ys, rs, sels)
}

func uint8DivByScalarSelsPure(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		rs[i] = ys[sel] / x
	}
	return rs
}

func Uint16Div(xs, ys, rs []uint16) []uint16 {
	return uint16Div(xs, ys, rs)
}

func uint16DivPure(xs, ys, rs []uint16) []uint16 {
	for i, x := range xs {
		rs[i] = x / ys[i]
	}
	return rs
}

func Uint16DivSels(xs, ys, rs []uint16, sels []int64) []uint16 {
	return uint16DivSels(xs, ys, rs, sels)
}

func uint16DivSelsPure(xs, ys, rs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		rs[i] = xs[sel] / ys[sel]
	}
	return rs
}

func Uint16DivScalar(x uint16, ys, rs []uint16) []uint16 {
	return uint16DivScalar(x, ys, rs)
}

func uint16DivScalarPure(x uint16, ys, rs []uint16) []uint16 {
	for i, y := range ys {
		rs[i] = x / y
	}
	return rs
}

func Uint16DivScalarSels(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	return uint16DivScalarSels(x, ys, rs, sels)
}

func uint16DivScalarSelsPure(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		rs[i] = x / ys[sel]
	}
	return rs
}

func Uint16DivByScalar(x uint16, ys, rs []uint16) []uint16 {
	return uint16DivByScalar(x, ys, rs)
}

func uint16DivByScalarPure(x uint16, ys, rs []uint16) []uint16 {
	for i, y := range ys {
		rs[i] = y / x
	}
	return rs
}

func Uint16DivByScalarSels(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	return uint16DivByScalarSels(x, ys, rs, sels)
}

func uint16DivByScalarSelsPure(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		rs[i] = ys[sel] / x
	}
	return rs
}

func Uint32Div(xs, ys, rs []uint32) []uint32 {
	return uint32Div(xs, ys, rs)
}

func uint32DivPure(xs, ys, rs []uint32) []uint32 {
	for i, x := range xs {
		rs[i] = x / ys[i]
	}
	return rs
}

func Uint32DivSels(xs, ys, rs []uint32, sels []int64) []uint32 {
	return uint32DivSels(xs, ys, rs, sels)
}

func uint32DivSelsPure(xs, ys, rs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		rs[i] = xs[sel] / ys[sel]
	}
	return rs
}

func Uint32DivScalar(x uint32, ys, rs []uint32) []uint32 {
	return uint32DivScalar(x, ys, rs)
}

func uint32DivScalarPure(x uint32, ys, rs []uint32) []uint32 {
	for i, y := range ys {
		rs[i] = x / y
	}
	return rs
}

func Uint32DivScalarSels(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	return uint32DivScalarSels(x, ys, rs, sels)
}

func uint32DivScalarSelsPure(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		rs[i] = x / ys[sel]
	}
	return rs
}

func Uint32DivByScalar(x uint32, ys, rs []uint32) []uint32 {
	return uint32DivByScalar(x, ys, rs)
}

func uint32DivByScalarPure(x uint32, ys, rs []uint32) []uint32 {
	for i, y := range ys {
		rs[i] = y / x
	}
	return rs
}

func Uint32DivByScalarSels(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	return uint32DivByScalarSels(x, ys, rs, sels)
}

func uint32DivByScalarSelsPure(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		rs[i] = ys[sel] / x
	}
	return rs
}

func Uint64Div(xs, ys, rs []uint64) []uint64 {
	return uint64Div(xs, ys, rs)
}

func uint64DivPure(xs, ys, rs []uint64) []uint64 {
	for i, x := range xs {
		rs[i] = x / ys[i]
	}
	return rs
}

func Uint64DivSels(xs, ys, rs []uint64, sels []int64) []uint64 {
	return uint64DivSels(xs, ys, rs, sels)
}

func uint64DivSelsPure(xs, ys, rs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		rs[i] = xs[sel] / ys[sel]
	}
	return rs
}

func Uint64DivScalar(x uint64, ys, rs []uint64) []uint64 {
	return uint64DivScalar(x, ys, rs)
}

func uint64DivScalarPure(x uint64, ys, rs []uint64) []uint64 {
	for i, y := range ys {
		rs[i] = x / y
	}
	return rs
}

func Uint64DivScalarSels(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	return uint64DivScalarSels(x, ys, rs, sels)
}

func uint64DivScalarSelsPure(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		rs[i] = x / ys[sel]
	}
	return rs
}

func Uint64DivByScalar(x uint64, ys, rs []uint64) []uint64 {
	return uint64DivByScalar(x, ys, rs)
}

func uint64DivByScalarPure(x uint64, ys, rs []uint64) []uint64 {
	for i, y := range ys {
		rs[i] = y / x
	}
	return rs
}

func Uint64DivByScalarSels(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	return uint64DivByScalarSels(x, ys, rs, sels)
}

func uint64DivByScalarSelsPure(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		rs[i] = ys[sel] / x
	}
	return rs
}

func Float32Div(xs, ys, rs []float32) []float32 {
	return float32Div(xs, ys, rs)
}

func float32DivPure(xs, ys, rs []float32) []float32 {
	for i, x := range xs {
		rs[i] = x / ys[i]
	}
	return rs
}

func Float32DivSels(xs, ys, rs []float32, sels []int64) []float32 {
	return float32DivSels(xs, ys, rs, sels)
}

func float32DivSelsPure(xs, ys, rs []float32, sels []int64) []float32 {
	for i, sel := range sels {
		rs[i] = xs[sel] / ys[sel]
	}
	return rs
}

func Float32DivScalar(x float32, ys, rs []float32) []float32 {
	return float32DivScalar(x, ys, rs)
}

func float32DivScalarPure(x float32, ys, rs []float32) []float32 {
	for i, y := range ys {
		rs[i] = x / y
	}
	return rs
}

func Float32DivScalarSels(x float32, ys, rs []float32, sels []int64) []float32 {
	return float32DivScalarSels(x, ys, rs, sels)
}

func float32DivScalarSelsPure(x float32, ys, rs []float32, sels []int64) []float32 {
	for i, sel := range sels {
		rs[i] = x / ys[sel]
	}
	return rs
}

func Float32DivByScalar(x float32, ys, rs []float32) []float32 {
	return float32DivByScalar(x, ys, rs)
}

func float32DivByScalarPure(x float32, ys, rs []float32) []float32 {
	for i, y := range ys {
		rs[i] = y / x
	}
	return rs
}

func Float32DivByScalarSels(x float32, ys, rs []float32, sels []int64) []float32 {
	return float32DivByScalarSels(x, ys, rs, sels)
}

func float32DivByScalarSelsPure(x float32, ys, rs []float32, sels []int64) []float32 {
	for i, sel := range sels {
		rs[i] = ys[sel] / x
	}
	return rs
}

func Float64Div(xs, ys, rs []float64) []float64 {
	return float64Div(xs, ys, rs)
}

func float64DivPure(xs, ys, rs []float64) []float64 {
	for i, x := range xs {
		rs[i] = x / ys[i]
	}
	return rs
}

func Float64DivSels(xs, ys, rs []float64, sels []int64) []float64 {
	return float64DivSels(xs, ys, rs, sels)
}

func float64DivSelsPure(xs, ys, rs []float64, sels []int64) []float64 {
	for i, sel := range sels {
		rs[i] = xs[sel] / ys[sel]
	}
	return rs
}

func Float64DivScalar(x float64, ys, rs []float64) []float64 {
	return float64DivScalar(x, ys, rs)
}

func float64DivScalarPure(x float64, ys, rs []float64) []float64 {
	for i, y := range ys {
		rs[i] = x / y
	}
	return rs
}

func Float64DivScalarSels(x float64, ys, rs []float64, sels []int64) []float64 {
	return float64DivScalarSels(x, ys, rs, sels)
}

func float64DivScalarSelsPure(x float64, ys, rs []float64, sels []int64) []float64 {
	for i, sel := range sels {
		rs[i] = x / ys[sel]
	}
	return rs
}

func Float64DivByScalar(x float64, ys, rs []float64) []float64 {
	return float64DivByScalar(x, ys, rs)
}

func float64DivByScalarPure(x float64, ys, rs []float64) []float64 {
	for i, y := range ys {
		rs[i] = y / x
	}
	return rs
}

func Float64DivByScalarSels(x float64, ys, rs []float64, sels []int64) []float64 {
	return float64DivByScalarSels(x, ys, rs, sels)
}

func float64DivByScalarSelsPure(x float64, ys, rs []float64, sels []int64) []float64 {
	for i, sel := range sels {
		rs[i] = ys[sel] / x
	}
	return rs
}
