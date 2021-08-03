package mod

var (
	Int8Mod             func([]int8, []int8, []int8) []int8
	Int8ModSels         func([]int8, []int8, []int8, []int64) []int8
	Int8ModScalar       func(int8, []int8, []int8) []int8
	Int8ModScalarSels   func(int8, []int8, []int8, []int64) []int8
	Int16Mod            func([]int16, []int16, []int16) []int16
	Int16ModSels        func([]int16, []int16, []int16, []int64) []int16
	Int16ModScalar      func(int16, []int16, []int16) []int16
	Int16ModScalarSels  func(int16, []int16, []int16, []int64) []int16
	Int32Mod            func([]int32, []int32, []int32) []int32
	Int32ModSels        func([]int32, []int32, []int32, []int64) []int32
	Int32ModScalar      func(int32, []int32, []int32) []int32
	Int32ModScalarSels  func(int32, []int32, []int32, []int64) []int32
	Int64Mod            func([]int64, []int64, []int64) []int64
	Int64ModSels        func([]int64, []int64, []int64, []int64) []int64
	Int64ModScalar      func(int64, []int64, []int64) []int64
	Int64ModScalarSels  func(int64, []int64, []int64, []int64) []int64
	Uint8Mod            func([]uint8, []uint8, []uint8) []uint8
	Uint8ModSels        func([]uint8, []uint8, []uint8, []int64) []uint8
	Uint8ModScalar      func(uint8, []uint8, []uint8) []uint8
	Uint8ModScalarSels  func(uint8, []uint8, []uint8, []int64) []uint8
	Uint16Mod           func([]uint16, []uint16, []uint16) []uint16
	Uint16ModSels       func([]uint16, []uint16, []uint16, []int64) []uint16
	Uint16ModScalar     func(uint16, []uint16, []uint16) []uint16
	Uint16ModScalarSels func(uint16, []uint16, []uint16, []int64) []uint16
	Uint32Mod           func([]uint32, []uint32, []uint32) []uint32
	Uint32ModSels       func([]uint32, []uint32, []uint32, []int64) []uint32
	Uint32ModScalar     func(uint32, []uint32, []uint32) []uint32
	Uint32ModScalarSels func(uint32, []uint32, []uint32, []int64) []uint32
	Uint64Mod           func([]uint64, []uint64, []uint64) []uint64
	Uint64ModSels       func([]uint64, []uint64, []uint64, []int64) []uint64
	Uint64ModScalar     func(uint64, []uint64, []uint64) []uint64
	Uint64ModScalarSels func(uint64, []uint64, []uint64, []int64) []uint64
)

func init() {
	Int8Mod = int8Mod
	Int8ModSels = int8ModSels
	Int8ModScalar = int8ModScalar
	Int8ModScalarSels = int8ModScalarSels
	Int16Mod = int16Mod
	Int16ModSels = int16ModSels
	Int16ModScalar = int16ModScalar
	Int16ModScalarSels = int16ModScalarSels
	Int32Mod = int32Mod
	Int32ModSels = int32ModSels
	Int32ModScalar = int32ModScalar
	Int32ModScalarSels = int32ModScalarSels
	Int64Mod = int64Mod
	Int64ModSels = int64ModSels
	Int64ModScalar = int64ModScalar
	Int64ModScalarSels = int64ModScalarSels
	Uint8Mod = uint8Mod
	Uint8ModSels = uint8ModSels
	Uint8ModScalar = uint8ModScalar
	Uint8ModScalarSels = uint8ModScalarSels
	Uint16Mod = uint16Mod
	Uint16ModSels = uint16ModSels
	Uint16ModScalar = uint16ModScalar
	Uint16ModScalarSels = uint16ModScalarSels
	Uint32Mod = uint32Mod
	Uint32ModSels = uint32ModSels
	Uint32ModScalar = uint32ModScalar
	Uint32ModScalarSels = uint32ModScalarSels
	Uint64Mod = uint64Mod
	Uint64ModSels = uint64ModSels
	Uint64ModScalar = uint64ModScalar
	Uint64ModScalarSels = uint64ModScalarSels
}

func int8Mod(xs, ys, rs []int8) []int8 {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func int8ModSels(xs, ys, rs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		rs[i] = xs[sel] % ys[sel]
	}
	return rs
}

func int8ModScalar(x int8, ys, rs []int8) []int8 {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func int8ModScalarSels(x int8, ys, rs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		rs[i] = x % ys[sel]
	}
	return rs
}

func int16Mod(xs, ys, rs []int16) []int16 {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func int16ModSels(xs, ys, rs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		rs[i] = xs[sel] % ys[sel]
	}
	return rs
}

func int16ModScalar(x int16, ys, rs []int16) []int16 {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func int16ModScalarSels(x int16, ys, rs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		rs[i] = x % ys[sel]
	}
	return rs
}

func int32Mod(xs, ys, rs []int32) []int32 {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func int32ModSels(xs, ys, rs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		rs[i] = xs[sel] % ys[sel]
	}
	return rs
}

func int32ModScalar(x int32, ys, rs []int32) []int32 {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func int32ModScalarSels(x int32, ys, rs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		rs[i] = x % ys[sel]
	}
	return rs
}

func int64Mod(xs, ys, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func int64ModSels(xs, ys, rs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		rs[i] = xs[sel] % ys[sel]
	}
	return rs
}

func int64ModScalar(x int64, ys, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func int64ModScalarSels(x int64, ys, rs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		rs[i] = x % ys[sel]
	}
	return rs
}

func uint8Mod(xs, ys, rs []uint8) []uint8 {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func uint8ModSels(xs, ys, rs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		rs[i] = xs[sel] % ys[sel]
	}
	return rs
}

func uint8ModScalar(x uint8, ys, rs []uint8) []uint8 {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func uint8ModScalarSels(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		rs[i] = x % ys[sel]
	}
	return rs
}

func uint16Mod(xs, ys, rs []uint16) []uint16 {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func uint16ModSels(xs, ys, rs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		rs[i] = xs[sel] % ys[sel]
	}
	return rs
}

func uint16ModScalar(x uint16, ys, rs []uint16) []uint16 {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func uint16ModScalarSels(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		rs[i] = x % ys[sel]
	}
	return rs
}

func uint32Mod(xs, ys, rs []uint32) []uint32 {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func uint32ModSels(xs, ys, rs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		rs[i] = xs[sel] % ys[sel]
	}
	return rs
}

func uint32ModScalar(x uint32, ys, rs []uint32) []uint32 {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func uint32ModScalarSels(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		rs[i] = x % ys[sel]
	}
	return rs
}

func uint64Mod(xs, ys, rs []uint64) []uint64 {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func uint64ModSels(xs, ys, rs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		rs[i] = xs[sel] % ys[sel]
	}
	return rs
}

func uint64ModScalar(x uint64, ys, rs []uint64) []uint64 {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func uint64ModScalarSels(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		rs[i] = x % ys[sel]
	}
	return rs
}
