package mod

var (
	int8Mod             func([]int8, []int8, []int8) []int8
	int8ModSels         func([]int8, []int8, []int8, []int64) []int8
	int8ModScalar       func(int8, []int8, []int8) []int8
	int8ModScalarSels   func(int8, []int8, []int8, []int64) []int8
	int16Mod            func([]int16, []int16, []int16) []int16
	int16ModSels        func([]int16, []int16, []int16, []int64) []int16
	int16ModScalar      func(int16, []int16, []int16) []int16
	int16ModScalarSels  func(int16, []int16, []int16, []int64) []int16
	int32Mod            func([]int32, []int32, []int32) []int32
	int32ModSels        func([]int32, []int32, []int32, []int64) []int32
	int32ModScalar      func(int32, []int32, []int32) []int32
	int32ModScalarSels  func(int32, []int32, []int32, []int64) []int32
	int64Mod            func([]int64, []int64, []int64) []int64
	int64ModSels        func([]int64, []int64, []int64, []int64) []int64
	int64ModScalar      func(int64, []int64, []int64) []int64
	int64ModScalarSels  func(int64, []int64, []int64, []int64) []int64
	uint8Mod            func([]uint8, []uint8, []uint8) []uint8
	uint8ModSels        func([]uint8, []uint8, []uint8, []int64) []uint8
	uint8ModScalar      func(uint8, []uint8, []uint8) []uint8
	uint8ModScalarSels  func(uint8, []uint8, []uint8, []int64) []uint8
	uint16Mod           func([]uint16, []uint16, []uint16) []uint16
	uint16ModSels       func([]uint16, []uint16, []uint16, []int64) []uint16
	uint16ModScalar     func(uint16, []uint16, []uint16) []uint16
	uint16ModScalarSels func(uint16, []uint16, []uint16, []int64) []uint16
	uint32Mod           func([]uint32, []uint32, []uint32) []uint32
	uint32ModSels       func([]uint32, []uint32, []uint32, []int64) []uint32
	uint32ModScalar     func(uint32, []uint32, []uint32) []uint32
	uint32ModScalarSels func(uint32, []uint32, []uint32, []int64) []uint32
	uint64Mod           func([]uint64, []uint64, []uint64) []uint64
	uint64ModSels       func([]uint64, []uint64, []uint64, []int64) []uint64
	uint64ModScalar     func(uint64, []uint64, []uint64) []uint64
	uint64ModScalarSels func(uint64, []uint64, []uint64, []int64) []uint64
)

func init() {
	int8Mod = int8ModPure
	int8ModSels = int8ModSelsPure
	int8ModScalar = int8ModScalarPure
	int8ModScalarSels = int8ModScalarSelsPure
	int16Mod = int16ModPure
	int16ModSels = int16ModSelsPure
	int16ModScalar = int16ModScalarPure
	int16ModScalarSels = int16ModScalarSelsPure
	int32Mod = int32ModPure
	int32ModSels = int32ModSelsPure
	int32ModScalar = int32ModScalarPure
	int32ModScalarSels = int32ModScalarSelsPure
	int64Mod = int64ModPure
	int64ModSels = int64ModSelsPure
	int64ModScalar = int64ModScalarPure
	int64ModScalarSels = int64ModScalarSelsPure
	uint8Mod = uint8ModPure
	uint8ModSels = uint8ModSelsPure
	uint8ModScalar = uint8ModScalarPure
	uint8ModScalarSels = uint8ModScalarSelsPure
	uint16Mod = uint16ModPure
	uint16ModSels = uint16ModSelsPure
	uint16ModScalar = uint16ModScalarPure
	uint16ModScalarSels = uint16ModScalarSelsPure
	uint32Mod = uint32ModPure
	uint32ModSels = uint32ModSelsPure
	uint32ModScalar = uint32ModScalarPure
	uint32ModScalarSels = uint32ModScalarSelsPure
	uint64Mod = uint64ModPure
	uint64ModSels = uint64ModSelsPure
	uint64ModScalar = uint64ModScalarPure
	uint64ModScalarSels = uint64ModScalarSelsPure
}

func Int8Mod(xs, ys, rs []int8) []int8 {
	return int8Mod(xs, ys, rs)
}

func int8ModPure(xs, ys, rs []int8) []int8 {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func Int8ModSels(xs, ys, rs []int8, sels []int64) []int8 {
	return int8ModSels(xs, ys, rs, sels)
}

func int8ModSelsPure(xs, ys, rs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		rs[i] = xs[sel] % ys[sel]
	}
	return rs
}

func Int8ModScalar(x int8, ys, rs []int8) []int8 {
	return int8ModScalar(x, ys, rs)
}

func int8ModScalarPure(x int8, ys, rs []int8) []int8 {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func Int8ModScalarSels(x int8, ys, rs []int8, sels []int64) []int8 {
	return int8ModScalarSels(x, ys, rs, sels)
}

func int8ModScalarSelsPure(x int8, ys, rs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		rs[i] = x % ys[sel]
	}
	return rs
}

func Int16Mod(xs, ys, rs []int16) []int16 {
	return int16Mod(xs, ys, rs)
}

func int16ModPure(xs, ys, rs []int16) []int16 {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func Int16ModSels(xs, ys, rs []int16, sels []int64) []int16 {
	return int16ModSels(xs, ys, rs, sels)
}

func int16ModSelsPure(xs, ys, rs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		rs[i] = xs[sel] % ys[sel]
	}
	return rs
}

func Int16ModScalar(x int16, ys, rs []int16) []int16 {
	return int16ModScalar(x, ys, rs)
}

func int16ModScalarPure(x int16, ys, rs []int16) []int16 {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func Int16ModScalarSels(x int16, ys, rs []int16, sels []int64) []int16 {
	return int16ModScalarSels(x, ys, rs, sels)
}

func int16ModScalarSelsPure(x int16, ys, rs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		rs[i] = x % ys[sel]
	}
	return rs
}

func Int32Mod(xs, ys, rs []int32) []int32 {
	return int32Mod(xs, ys, rs)
}

func int32ModPure(xs, ys, rs []int32) []int32 {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func Int32ModSels(xs, ys, rs []int32, sels []int64) []int32 {
	return int32ModSels(xs, ys, rs, sels)
}

func int32ModSelsPure(xs, ys, rs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		rs[i] = xs[sel] % ys[sel]
	}
	return rs
}

func Int32ModScalar(x int32, ys, rs []int32) []int32 {
	return int32ModScalar(x, ys, rs)
}

func int32ModScalarPure(x int32, ys, rs []int32) []int32 {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func Int32ModScalarSels(x int32, ys, rs []int32, sels []int64) []int32 {
	return int32ModScalarSels(x, ys, rs, sels)
}

func int32ModScalarSelsPure(x int32, ys, rs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		rs[i] = x % ys[sel]
	}
	return rs
}

func Int64Mod(xs, ys, rs []int64) []int64 {
	return int64Mod(xs, ys, rs)
}

func int64ModPure(xs, ys, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func Int64ModSels(xs, ys, rs []int64, sels []int64) []int64 {
	return int64ModSels(xs, ys, rs, sels)
}

func int64ModSelsPure(xs, ys, rs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		rs[i] = xs[sel] % ys[sel]
	}
	return rs
}

func Int64ModScalar(x int64, ys, rs []int64) []int64 {
	return int64ModScalar(x, ys, rs)
}

func int64ModScalarPure(x int64, ys, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func Int64ModScalarSels(x int64, ys, rs []int64, sels []int64) []int64 {
	return int64ModScalarSels(x, ys, rs, sels)
}

func int64ModScalarSelsPure(x int64, ys, rs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		rs[i] = x % ys[sel]
	}
	return rs
}

func Uint8Mod(xs, ys, rs []uint8) []uint8 {
	return uint8Mod(xs, ys, rs)
}

func uint8ModPure(xs, ys, rs []uint8) []uint8 {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func Uint8ModSels(xs, ys, rs []uint8, sels []int64) []uint8 {
	return uint8ModSels(xs, ys, rs, sels)
}

func uint8ModSelsPure(xs, ys, rs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		rs[i] = xs[sel] % ys[sel]
	}
	return rs
}

func Uint8ModScalar(x uint8, ys, rs []uint8) []uint8 {
	return uint8ModScalar(x, ys, rs)
}

func uint8ModScalarPure(x uint8, ys, rs []uint8) []uint8 {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func Uint8ModScalarSels(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	return uint8ModScalarSels(x, ys, rs, sels)
}

func uint8ModScalarSelsPure(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		rs[i] = x % ys[sel]
	}
	return rs
}

func Uint16Mod(xs, ys, rs []uint16) []uint16 {
	return uint16Mod(xs, ys, rs)
}

func uint16ModPure(xs, ys, rs []uint16) []uint16 {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func Uint16ModSels(xs, ys, rs []uint16, sels []int64) []uint16 {
	return uint16ModSels(xs, ys, rs, sels)
}

func uint16ModSelsPure(xs, ys, rs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		rs[i] = xs[sel] % ys[sel]
	}
	return rs
}

func Uint16ModScalar(x uint16, ys, rs []uint16) []uint16 {
	return uint16ModScalar(x, ys, rs)
}

func uint16ModScalarPure(x uint16, ys, rs []uint16) []uint16 {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func Uint16ModScalarSels(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	return uint16ModScalarSels(x, ys, rs, sels)
}

func uint16ModScalarSelsPure(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		rs[i] = x % ys[sel]
	}
	return rs
}

func Uint32Mod(xs, ys, rs []uint32) []uint32 {
	return uint32Mod(xs, ys, rs)
}

func uint32ModPure(xs, ys, rs []uint32) []uint32 {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func Uint32ModSels(xs, ys, rs []uint32, sels []int64) []uint32 {
	return uint32ModSels(xs, ys, rs, sels)
}

func uint32ModSelsPure(xs, ys, rs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		rs[i] = xs[sel] % ys[sel]
	}
	return rs
}

func Uint32ModScalar(x uint32, ys, rs []uint32) []uint32 {
	return uint32ModScalar(x, ys, rs)
}

func uint32ModScalarPure(x uint32, ys, rs []uint32) []uint32 {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func Uint32ModScalarSels(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	return uint32ModScalarSels(x, ys, rs, sels)
}

func uint32ModScalarSelsPure(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		rs[i] = x % ys[sel]
	}
	return rs
}

func Uint64Mod(xs, ys, rs []uint64) []uint64 {
	return uint64Mod(xs, ys, rs)
}

func uint64ModPure(xs, ys, rs []uint64) []uint64 {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func Uint64ModSels(xs, ys, rs []uint64, sels []int64) []uint64 {
	return uint64ModSels(xs, ys, rs, sels)
}

func uint64ModSelsPure(xs, ys, rs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		rs[i] = xs[sel] % ys[sel]
	}
	return rs
}

func Uint64ModScalar(x uint64, ys, rs []uint64) []uint64 {
	return uint64ModScalar(x, ys, rs)
}

func uint64ModScalarPure(x uint64, ys, rs []uint64) []uint64 {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func Uint64ModScalarSels(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	return uint64ModScalarSels(x, ys, rs, sels)
}

func uint64ModScalarSelsPure(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		rs[i] = x % ys[sel]
	}
	return rs
}
