package sub

var (
	Int8Sub                func([]int8, []int8, []int8) []int8
	Int8SubSels            func([]int8, []int8, []int8, []int64) []int8
	Int8SubScalar          func(int8, []int8, []int8) []int8
	Int8SubScalarSels      func(int8, []int8, []int8, []int64) []int8
	Int8SubByScalar        func(int8, []int8, []int8) []int8
	Int8SubByScalarSels    func(int8, []int8, []int8, []int64) []int8
	Int16Sub               func([]int16, []int16, []int16) []int16
	Int16SubSels           func([]int16, []int16, []int16, []int64) []int16
	Int16SubScalar         func(int16, []int16, []int16) []int16
	Int16SubScalarSels     func(int16, []int16, []int16, []int64) []int16
	Int16SubByScalar       func(int16, []int16, []int16) []int16
	Int16SubByScalarSels   func(int16, []int16, []int16, []int64) []int16
	Int32Sub               func([]int32, []int32, []int32) []int32
	Int32SubSels           func([]int32, []int32, []int32, []int64) []int32
	Int32SubScalar         func(int32, []int32, []int32) []int32
	Int32SubScalarSels     func(int32, []int32, []int32, []int64) []int32
	Int32SubByScalar       func(int32, []int32, []int32) []int32
	Int32SubByScalarSels   func(int32, []int32, []int32, []int64) []int32
	Int64Sub               func([]int64, []int64, []int64) []int64
	Int64SubSels           func([]int64, []int64, []int64, []int64) []int64
	Int64SubScalar         func(int64, []int64, []int64) []int64
	Int64SubScalarSels     func(int64, []int64, []int64, []int64) []int64
	Int64SubByScalar       func(int64, []int64, []int64) []int64
	Int64SubByScalarSels   func(int64, []int64, []int64, []int64) []int64
	Uint8Sub               func([]uint8, []uint8, []uint8) []uint8
	Uint8SubSels           func([]uint8, []uint8, []uint8, []int64) []uint8
	Uint8SubScalar         func(uint8, []uint8, []uint8) []uint8
	Uint8SubScalarSels     func(uint8, []uint8, []uint8, []int64) []uint8
	Uint8SubByScalar       func(uint8, []uint8, []uint8) []uint8
	Uint8SubByScalarSels   func(uint8, []uint8, []uint8, []int64) []uint8
	Uint16Sub              func([]uint16, []uint16, []uint16) []uint16
	Uint16SubSels          func([]uint16, []uint16, []uint16, []int64) []uint16
	Uint16SubScalar        func(uint16, []uint16, []uint16) []uint16
	Uint16SubScalarSels    func(uint16, []uint16, []uint16, []int64) []uint16
	Uint16SubByScalar      func(uint16, []uint16, []uint16) []uint16
	Uint16SubByScalarSels  func(uint16, []uint16, []uint16, []int64) []uint16
	Uint32Sub              func([]uint32, []uint32, []uint32) []uint32
	Uint32SubSels          func([]uint32, []uint32, []uint32, []int64) []uint32
	Uint32SubScalar        func(uint32, []uint32, []uint32) []uint32
	Uint32SubScalarSels    func(uint32, []uint32, []uint32, []int64) []uint32
	Uint32SubByScalar      func(uint32, []uint32, []uint32) []uint32
	Uint32SubByScalarSels  func(uint32, []uint32, []uint32, []int64) []uint32
	Uint64Sub              func([]uint64, []uint64, []uint64) []uint64
	Uint64SubSels          func([]uint64, []uint64, []uint64, []int64) []uint64
	Uint64SubScalar        func(uint64, []uint64, []uint64) []uint64
	Uint64SubScalarSels    func(uint64, []uint64, []uint64, []int64) []uint64
	Uint64SubByScalar      func(uint64, []uint64, []uint64) []uint64
	Uint64SubByScalarSels  func(uint64, []uint64, []uint64, []int64) []uint64
	Float32Sub             func([]float32, []float32, []float32) []float32
	Float32SubSels         func([]float32, []float32, []float32, []int64) []float32
	Float32SubScalar       func(float32, []float32, []float32) []float32
	Float32SubScalarSels   func(float32, []float32, []float32, []int64) []float32
	Float32SubByScalar     func(float32, []float32, []float32) []float32
	Float32SubByScalarSels func(float32, []float32, []float32, []int64) []float32
	Float64Sub             func([]float64, []float64, []float64) []float64
	Float64SubSels         func([]float64, []float64, []float64, []int64) []float64
	Float64SubScalar       func(float64, []float64, []float64) []float64
	Float64SubScalarSels   func(float64, []float64, []float64, []int64) []float64
	Float64SubByScalar     func(float64, []float64, []float64) []float64
	Float64SubByScalarSels func(float64, []float64, []float64, []int64) []float64
)

func int8Sub(xs, ys, rs []int8) []int8 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func int8SubSels(xs, ys, rs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func int8SubScalar(x int8, ys, rs []int8) []int8 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func int8SubScalarSels(x int8, ys, rs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func int8SubByScalar(x int8, ys, rs []int8) []int8 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func int8SubByScalarSels(x int8, ys, rs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func int16Sub(xs, ys, rs []int16) []int16 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func int16SubSels(xs, ys, rs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func int16SubScalar(x int16, ys, rs []int16) []int16 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func int16SubScalarSels(x int16, ys, rs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func int16SubByScalar(x int16, ys, rs []int16) []int16 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func int16SubByScalarSels(x int16, ys, rs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func int32Sub(xs, ys, rs []int32) []int32 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func int32SubSels(xs, ys, rs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func int32SubScalar(x int32, ys, rs []int32) []int32 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func int32SubScalarSels(x int32, ys, rs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func int32SubByScalar(x int32, ys, rs []int32) []int32 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func int32SubByScalarSels(x int32, ys, rs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func int64Sub(xs, ys, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func int64SubSels(xs, ys, rs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func int64SubScalar(x int64, ys, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func int64SubScalarSels(x int64, ys, rs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func int64SubByScalar(x int64, ys, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func int64SubByScalarSels(x int64, ys, rs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func uint8Sub(xs, ys, rs []uint8) []uint8 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func uint8SubSels(xs, ys, rs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func uint8SubScalar(x uint8, ys, rs []uint8) []uint8 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func uint8SubScalarSels(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func uint8SubByScalar(x uint8, ys, rs []uint8) []uint8 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func uint8SubByScalarSels(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func uint16Sub(xs, ys, rs []uint16) []uint16 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func uint16SubSels(xs, ys, rs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func uint16SubScalar(x uint16, ys, rs []uint16) []uint16 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func uint16SubScalarSels(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func uint16SubByScalar(x uint16, ys, rs []uint16) []uint16 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func uint16SubByScalarSels(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func uint32Sub(xs, ys, rs []uint32) []uint32 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func uint32SubSels(xs, ys, rs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func uint32SubScalar(x uint32, ys, rs []uint32) []uint32 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func uint32SubScalarSels(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func uint32SubByScalar(x uint32, ys, rs []uint32) []uint32 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func uint32SubByScalarSels(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func uint64Sub(xs, ys, rs []uint64) []uint64 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func uint64SubSels(xs, ys, rs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func uint64SubScalar(x uint64, ys, rs []uint64) []uint64 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func uint64SubScalarSels(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func uint64SubByScalar(x uint64, ys, rs []uint64) []uint64 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func uint64SubByScalarSels(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func float32Sub(xs, ys, rs []float32) []float32 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func float32SubSels(xs, ys, rs []float32, sels []int64) []float32 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func float32SubScalar(x float32, ys, rs []float32) []float32 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func float32SubScalarSels(x float32, ys, rs []float32, sels []int64) []float32 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func float32SubByScalar(x float32, ys, rs []float32) []float32 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func float32SubByScalarSels(x float32, ys, rs []float32, sels []int64) []float32 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func float64Sub(xs, ys, rs []float64) []float64 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func float64SubSels(xs, ys, rs []float64, sels []int64) []float64 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func float64SubScalar(x float64, ys, rs []float64) []float64 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func float64SubScalarSels(x float64, ys, rs []float64, sels []int64) []float64 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func float64SubByScalar(x float64, ys, rs []float64) []float64 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func float64SubByScalarSels(x float64, ys, rs []float64, sels []int64) []float64 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}
