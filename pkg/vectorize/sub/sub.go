package sub

import "golang.org/x/sys/cpu"

var (
	int8Sub                func([]int8, []int8, []int8) []int8
	int8SubSels            func([]int8, []int8, []int8, []int64) []int8
	int8SubScalar          func(int8, []int8, []int8) []int8
	int8SubScalarSels      func(int8, []int8, []int8, []int64) []int8
	int8SubByScalar        func(int8, []int8, []int8) []int8
	int8SubByScalarSels    func(int8, []int8, []int8, []int64) []int8
	int16Sub               func([]int16, []int16, []int16) []int16
	int16SubSels           func([]int16, []int16, []int16, []int64) []int16
	int16SubScalar         func(int16, []int16, []int16) []int16
	int16SubScalarSels     func(int16, []int16, []int16, []int64) []int16
	int16SubByScalar       func(int16, []int16, []int16) []int16
	int16SubByScalarSels   func(int16, []int16, []int16, []int64) []int16
	int32Sub               func([]int32, []int32, []int32) []int32
	int32SubSels           func([]int32, []int32, []int32, []int64) []int32
	int32SubScalar         func(int32, []int32, []int32) []int32
	int32SubScalarSels     func(int32, []int32, []int32, []int64) []int32
	int32SubByScalar       func(int32, []int32, []int32) []int32
	int32SubByScalarSels   func(int32, []int32, []int32, []int64) []int32
	int64Sub               func([]int64, []int64, []int64) []int64
	int64SubSels           func([]int64, []int64, []int64, []int64) []int64
	int64SubScalar         func(int64, []int64, []int64) []int64
	int64SubScalarSels     func(int64, []int64, []int64, []int64) []int64
	int64SubByScalar       func(int64, []int64, []int64) []int64
	int64SubByScalarSels   func(int64, []int64, []int64, []int64) []int64
	uint8Sub               func([]uint8, []uint8, []uint8) []uint8
	uint8SubSels           func([]uint8, []uint8, []uint8, []int64) []uint8
	uint8SubScalar         func(uint8, []uint8, []uint8) []uint8
	uint8SubScalarSels     func(uint8, []uint8, []uint8, []int64) []uint8
	uint8SubByScalar       func(uint8, []uint8, []uint8) []uint8
	uint8SubByScalarSels   func(uint8, []uint8, []uint8, []int64) []uint8
	uint16Sub              func([]uint16, []uint16, []uint16) []uint16
	uint16SubSels          func([]uint16, []uint16, []uint16, []int64) []uint16
	uint16SubScalar        func(uint16, []uint16, []uint16) []uint16
	uint16SubScalarSels    func(uint16, []uint16, []uint16, []int64) []uint16
	uint16SubByScalar      func(uint16, []uint16, []uint16) []uint16
	uint16SubByScalarSels  func(uint16, []uint16, []uint16, []int64) []uint16
	uint32Sub              func([]uint32, []uint32, []uint32) []uint32
	uint32SubSels          func([]uint32, []uint32, []uint32, []int64) []uint32
	uint32SubScalar        func(uint32, []uint32, []uint32) []uint32
	uint32SubScalarSels    func(uint32, []uint32, []uint32, []int64) []uint32
	uint32SubByScalar      func(uint32, []uint32, []uint32) []uint32
	uint32SubByScalarSels  func(uint32, []uint32, []uint32, []int64) []uint32
	uint64Sub              func([]uint64, []uint64, []uint64) []uint64
	uint64SubSels          func([]uint64, []uint64, []uint64, []int64) []uint64
	uint64SubScalar        func(uint64, []uint64, []uint64) []uint64
	uint64SubScalarSels    func(uint64, []uint64, []uint64, []int64) []uint64
	uint64SubByScalar      func(uint64, []uint64, []uint64) []uint64
	uint64SubByScalarSels  func(uint64, []uint64, []uint64, []int64) []uint64
	float32Sub             func([]float32, []float32, []float32) []float32
	float32SubSels         func([]float32, []float32, []float32, []int64) []float32
	float32SubScalar       func(float32, []float32, []float32) []float32
	float32SubScalarSels   func(float32, []float32, []float32, []int64) []float32
	float32SubByScalar     func(float32, []float32, []float32) []float32
	float32SubByScalarSels func(float32, []float32, []float32, []int64) []float32
	float64Sub             func([]float64, []float64, []float64) []float64
	float64SubSels         func([]float64, []float64, []float64, []int64) []float64
	float64SubScalar       func(float64, []float64, []float64) []float64
	float64SubScalarSels   func(float64, []float64, []float64, []int64) []float64
	float64SubByScalar     func(float64, []float64, []float64) []float64
	float64SubByScalarSels func(float64, []float64, []float64, []int64) []float64
)

func init() {
	if cpu.X86.HasAVX512 {
		int8Sub = int8SubAvx512
		int8SubScalar = int8SubScalarAvx512
		int8SubByScalar = int8SubByScalarAvx512
		int16Sub = int16SubAvx512
		int16SubScalar = int16SubScalarAvx512
		int16SubByScalar = int16SubByScalarAvx512
		int32Sub = int32SubAvx512
		int32SubScalar = int32SubScalarAvx512
		int32SubByScalar = int32SubByScalarAvx512
		int64Sub = int64SubAvx512
		int64SubScalar = int64SubScalarAvx512
		int64SubByScalar = int64SubByScalarAvx512
		uint8Sub = uint8SubAvx512
		uint8SubScalar = uint8SubScalarAvx512
		uint8SubByScalar = uint8SubByScalarAvx512
		uint16Sub = uint16SubAvx512
		uint16SubScalar = uint16SubScalarAvx512
		uint16SubByScalar = uint16SubByScalarAvx512
		uint32Sub = uint32SubAvx512
		uint32SubScalar = uint32SubScalarAvx512
		uint32SubByScalar = uint32SubByScalarAvx512
		uint64Sub = uint64SubAvx512
		uint64SubScalar = uint64SubScalarAvx512
		uint64SubByScalar = uint64SubByScalarAvx512
		float32Sub = float32SubAvx512
		float32SubScalar = float32SubScalarAvx512
		float32SubByScalar = float32SubByScalarAvx512
		float64Sub = float64SubAvx512
		float64SubScalar = float64SubScalarAvx512
		float64SubByScalar = float64SubByScalarAvx512
	} else if cpu.X86.HasAVX2 {
		int8Sub = int8SubAvx2
		int8SubScalar = int8SubScalarAvx2
		int8SubByScalar = int8SubByScalarAvx2
		int16Sub = int16SubAvx2
		int16SubScalar = int16SubScalarAvx2
		int16SubByScalar = int16SubByScalarAvx2
		int32Sub = int32SubAvx2
		int32SubScalar = int32SubScalarAvx2
		int32SubByScalar = int32SubByScalarAvx2
		int64Sub = int64SubAvx2
		int64SubScalar = int64SubScalarAvx2
		int64SubByScalar = int64SubByScalarAvx2
		uint8Sub = uint8SubAvx2
		uint8SubScalar = uint8SubScalarAvx2
		uint8SubByScalar = uint8SubByScalarAvx2
		uint16Sub = uint16SubAvx2
		uint16SubScalar = uint16SubScalarAvx2
		uint16SubByScalar = uint16SubByScalarAvx2
		uint32Sub = uint32SubAvx2
		uint32SubScalar = uint32SubScalarAvx2
		uint32SubByScalar = uint32SubByScalarAvx2
		uint64Sub = uint64SubAvx2
		uint64SubScalar = uint64SubScalarAvx2
		uint64SubByScalar = uint64SubByScalarAvx2
		float32Sub = float32SubAvx2
		float32SubScalar = float32SubScalarAvx2
		float32SubByScalar = float32SubByScalarAvx2
		float64Sub = float64SubAvx2
		float64SubScalar = float64SubScalarAvx2
		float64SubByScalar = float64SubByScalarAvx2
	} else {
		int8Sub = int8SubPure
		int8SubScalar = int8SubScalarPure
		int8SubByScalar = int8SubByScalarPure
		int16Sub = int16SubPure
		int16SubScalar = int16SubScalarPure
		int16SubByScalar = int16SubByScalarPure
		int32Sub = int32SubPure
		int32SubScalar = int32SubScalarPure
		int32SubByScalar = int32SubByScalarPure
		int64Sub = int64SubPure
		int64SubScalar = int64SubScalarPure
		int64SubByScalar = int64SubByScalarPure
		uint8Sub = uint8SubPure
		uint8SubScalar = uint8SubScalarPure
		uint8SubByScalar = uint8SubByScalarPure
		uint16Sub = uint16SubPure
		uint16SubScalar = uint16SubScalarPure
		uint16SubByScalar = uint16SubByScalarPure
		uint32Sub = uint32SubPure
		uint32SubScalar = uint32SubScalarPure
		uint32SubByScalar = uint32SubByScalarPure
		uint64Sub = uint64SubPure
		uint64SubScalar = uint64SubScalarPure
		uint64SubByScalar = uint64SubByScalarPure
		float32Sub = float32SubPure
		float32SubScalar = float32SubScalarPure
		float32SubByScalar = float32SubByScalarPure
		float64Sub = float64SubPure
		float64SubScalar = float64SubScalarPure
		float64SubByScalar = float64SubByScalarPure
	}
	int8SubSels = int8SubSelsPure
	int8SubScalarSels = int8SubScalarSelsPure
	int8SubByScalarSels = int8SubByScalarSelsPure
	int16SubSels = int16SubSelsPure
	int16SubScalarSels = int16SubScalarSelsPure
	int16SubByScalarSels = int16SubByScalarSelsPure
	int32SubSels = int32SubSelsPure
	int32SubScalarSels = int32SubScalarSelsPure
	int32SubByScalarSels = int32SubByScalarSelsPure
	int64SubSels = int64SubSelsPure
	int64SubScalarSels = int64SubScalarSelsPure
	int64SubByScalarSels = int64SubByScalarSelsPure
	uint8SubSels = uint8SubSelsPure
	uint8SubScalarSels = uint8SubScalarSelsPure
	uint8SubByScalarSels = uint8SubByScalarSelsPure
	uint16SubSels = uint16SubSelsPure
	uint16SubScalarSels = uint16SubScalarSelsPure
	uint16SubByScalarSels = uint16SubByScalarSelsPure
	uint32SubSels = uint32SubSelsPure
	uint32SubScalarSels = uint32SubScalarSelsPure
	uint32SubByScalarSels = uint32SubByScalarSelsPure
	uint64SubSels = uint64SubSelsPure
	uint64SubScalarSels = uint64SubScalarSelsPure
	uint64SubByScalarSels = uint64SubByScalarSelsPure
	float32SubSels = float32SubSelsPure
	float32SubScalarSels = float32SubScalarSelsPure
	float32SubByScalarSels = float32SubByScalarSelsPure
	float64SubSels = float64SubSelsPure
	float64SubScalarSels = float64SubScalarSelsPure
	float64SubByScalarSels = float64SubByScalarSelsPure
}

func Int8Sub(xs, ys, rs []int8) []int8 {
	return int8Sub(xs, ys, rs)
}

func int8SubPure(xs, ys, rs []int8) []int8 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func int8SubAvx2(xs, ys, rs []int8) []int8 {
	n := len(xs) / 16
	int8SubAvx2Asm(xs[:n*16], ys[:n*16], rs[:n*16])
	for i, j := n*16, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func int8SubAvx512(xs, ys, rs []int8) []int8 {
	n := len(xs) / 16
	int8SubAvx512Asm(xs[:n*16], ys[:n*16], rs[:n*16])
	for i, j := n*16, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func Int8SubSels(xs, ys, rs []int8, sels []int64) []int8 {
	return int8SubSels(xs, ys, rs, sels)
}

func int8SubSelsPure(xs, ys, rs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func Int8SubScalar(x int8, ys, rs []int8) []int8 {
	return int8SubScalar(x, ys, rs)
}

func int8SubScalarPure(x int8, ys, rs []int8) []int8 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func int8SubScalarAvx2(x int8, ys, rs []int8) []int8 {
	n := len(ys) / 16
	int8SubScalarAvx2Asm(x, ys[:n*16], rs[:n*16])
	for i, j := n*16, len(ys); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func int8SubScalarAvx512(x int8, ys, rs []int8) []int8 {
	n := len(ys) / 16
	int8SubScalarAvx512Asm(x, ys[:n*16], rs[:n*16])
	for i, j := n*16, len(ys); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func Int8SubScalarSels(x int8, ys, rs []int8, sels []int64) []int8 {
	return int8SubScalarSels(x, ys, rs, sels)
}

func int8SubScalarSelsPure(x int8, ys, rs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func Int8SubByScalar(x int8, ys, rs []int8) []int8 {
	return int8SubByScalar(x, ys, rs)
}

func int8SubByScalarPure(x int8, ys, rs []int8) []int8 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func int8SubByScalarAvx2(x int8, ys, rs []int8) []int8 {
	n := len(ys) / 16
	int8SubByScalarAvx2Asm(x, ys[:n*16], rs[:n*16])
	for i, j := n*16, len(ys); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func int8SubByScalarAvx512(x int8, ys, rs []int8) []int8 {
	n := len(ys) / 16
	int8SubByScalarAvx512Asm(x, ys[:n*16], rs[:n*16])
	for i, j := n*16, len(ys); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func Int8SubByScalarSels(x int8, ys, rs []int8, sels []int64) []int8 {
	return int8SubByScalarSels(x, ys, rs, sels)
}

func int8SubByScalarSelsPure(x int8, ys, rs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func Int16Sub(xs, ys, rs []int16) []int16 {
	return int16Sub(xs, ys, rs)
}

func int16SubPure(xs, ys, rs []int16) []int16 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func int16SubAvx2(xs, ys, rs []int16) []int16 {
	n := len(xs) / 8
	int16SubAvx2Asm(xs[:n*8], ys[:n*8], rs[:n*8])
	for i, j := n*8, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func int16SubAvx512(xs, ys, rs []int16) []int16 {
	n := len(xs) / 8
	int16SubAvx512Asm(xs[:n*8], ys[:n*8], rs[:n*8])
	for i, j := n*8, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func Int16SubSels(xs, ys, rs []int16, sels []int64) []int16 {
	return int16SubSels(xs, ys, rs, sels)
}

func int16SubSelsPure(xs, ys, rs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func Int16SubScalar(x int16, ys, rs []int16) []int16 {
	return int16SubScalar(x, ys, rs)
}

func int16SubScalarPure(x int16, ys, rs []int16) []int16 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func int16SubScalarAvx2(x int16, ys, rs []int16) []int16 {
	n := len(ys) / 8
	int16SubScalarAvx2Asm(x, ys[:n*8], rs[:n*8])
	for i, j := n*8, len(ys); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func int16SubScalarAvx512(x int16, ys, rs []int16) []int16 {
	n := len(ys) / 8
	int16SubScalarAvx512Asm(x, ys[:n*8], rs[:n*8])
	for i, j := n*8, len(ys); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func Int16SubScalarSels(x int16, ys, rs []int16, sels []int64) []int16 {
	return int16SubScalarSels(x, ys, rs, sels)
}

func int16SubScalarSelsPure(x int16, ys, rs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func Int16SubByScalar(x int16, ys, rs []int16) []int16 {
	return int16SubByScalar(x, ys, rs)
}

func int16SubByScalarPure(x int16, ys, rs []int16) []int16 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func int16SubByScalarAvx2(x int16, ys, rs []int16) []int16 {
	n := len(ys) / 8
	int16SubByScalarAvx2Asm(x, ys[:n*8], rs[:n*8])
	for i, j := n*8, len(ys); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func int16SubByScalarAvx512(x int16, ys, rs []int16) []int16 {
	n := len(ys) / 8
	int16SubByScalarAvx512Asm(x, ys[:n*8], rs[:n*8])
	for i, j := n*8, len(ys); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func Int16SubByScalarSels(x int16, ys, rs []int16, sels []int64) []int16 {
	return int16SubByScalarSels(x, ys, rs, sels)
}

func int16SubByScalarSelsPure(x int16, ys, rs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func Int32Sub(xs, ys, rs []int32) []int32 {
	return int32Sub(xs, ys, rs)
}

func int32SubPure(xs, ys, rs []int32) []int32 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func int32SubAvx2(xs, ys, rs []int32) []int32 {
	n := len(xs) / 4
	int32SubAvx2Asm(xs[:n*4], ys[:n*4], rs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func int32SubAvx512(xs, ys, rs []int32) []int32 {
	n := len(xs) / 4
	int32SubAvx512Asm(xs[:n*4], ys[:n*4], rs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func Int32SubSels(xs, ys, rs []int32, sels []int64) []int32 {
	return int32SubSels(xs, ys, rs, sels)
}

func int32SubSelsPure(xs, ys, rs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func Int32SubScalar(x int32, ys, rs []int32) []int32 {
	return int32SubScalar(x, ys, rs)
}

func int32SubScalarPure(x int32, ys, rs []int32) []int32 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func int32SubScalarAvx2(x int32, ys, rs []int32) []int32 {
	n := len(ys) / 4
	int32SubScalarAvx2Asm(x, ys[:n*4], rs[:n*4])
	for i, j := n*4, len(ys); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func int32SubScalarAvx512(x int32, ys, rs []int32) []int32 {
	n := len(ys) / 4
	int32SubScalarAvx512Asm(x, ys[:n*4], rs[:n*4])
	for i, j := n*4, len(ys); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func Int32SubScalarSels(x int32, ys, rs []int32, sels []int64) []int32 {
	return int32SubScalarSels(x, ys, rs, sels)
}

func int32SubScalarSelsPure(x int32, ys, rs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func Int32SubByScalar(x int32, ys, rs []int32) []int32 {
	return int32SubByScalar(x, ys, rs)
}

func int32SubByScalarPure(x int32, ys, rs []int32) []int32 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func int32SubByScalarAvx2(x int32, ys, rs []int32) []int32 {
	n := len(ys) / 4
	int32SubByScalarAvx2Asm(x, ys[:n*4], rs[:n*4])
	for i, j := n*4, len(ys); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func int32SubByScalarAvx512(x int32, ys, rs []int32) []int32 {
	n := len(ys) / 4
	int32SubByScalarAvx512Asm(x, ys[:n*4], rs[:n*4])
	for i, j := n*4, len(ys); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func Int32SubByScalarSels(x int32, ys, rs []int32, sels []int64) []int32 {
	return int32SubByScalarSels(x, ys, rs, sels)
}

func int32SubByScalarSelsPure(x int32, ys, rs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func Int64Sub(xs, ys, rs []int64) []int64 {
	return int64Sub(xs, ys, rs)
}

func int64SubPure(xs, ys, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func int64SubAvx2(xs, ys, rs []int64) []int64 {
	n := len(xs) / 2
	int64SubAvx2Asm(xs[:n*2], ys[:n*2], rs[:n*2])
	for i, j := n*2, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func int64SubAvx512(xs, ys, rs []int64) []int64 {
	n := len(xs) / 2
	int64SubAvx512Asm(xs[:n*2], ys[:n*2], rs[:n*2])
	for i, j := n*2, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func Int64SubSels(xs, ys, rs []int64, sels []int64) []int64 {
	return int64SubSels(xs, ys, rs, sels)
}

func int64SubSelsPure(xs, ys, rs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func Int64SubScalar(x int64, ys, rs []int64) []int64 {
	return int64SubScalar(x, ys, rs)
}

func int64SubScalarPure(x int64, ys, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func int64SubScalarAvx2(x int64, ys, rs []int64) []int64 {
	n := len(ys) / 2
	int64SubScalarAvx2Asm(x, ys[:n*2], rs[:n*2])
	for i, j := n*2, len(ys); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func int64SubScalarAvx512(x int64, ys, rs []int64) []int64 {
	n := len(ys) / 2
	int64SubScalarAvx512Asm(x, ys[:n*2], rs[:n*2])
	for i, j := n*2, len(ys); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func Int64SubScalarSels(x int64, ys, rs []int64, sels []int64) []int64 {
	return int64SubScalarSels(x, ys, rs, sels)
}

func int64SubScalarSelsPure(x int64, ys, rs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func Int64SubByScalar(x int64, ys, rs []int64) []int64 {
	return int64SubByScalar(x, ys, rs)
}

func int64SubByScalarPure(x int64, ys, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func int64SubByScalarAvx2(x int64, ys, rs []int64) []int64 {
	n := len(ys) / 2
	int64SubByScalarAvx2Asm(x, ys[:n*2], rs[:n*2])
	for i, j := n*2, len(ys); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func int64SubByScalarAvx512(x int64, ys, rs []int64) []int64 {
	n := len(ys) / 2
	int64SubByScalarAvx512Asm(x, ys[:n*2], rs[:n*2])
	for i, j := n*2, len(ys); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func Int64SubByScalarSels(x int64, ys, rs []int64, sels []int64) []int64 {
	return int64SubByScalarSels(x, ys, rs, sels)
}

func int64SubByScalarSelsPure(x int64, ys, rs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func Uint8Sub(xs, ys, rs []uint8) []uint8 {
	return uint8Sub(xs, ys, rs)
}

func uint8SubPure(xs, ys, rs []uint8) []uint8 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func uint8SubAvx2(xs, ys, rs []uint8) []uint8 {
	n := len(xs) / 16
	uint8SubAvx2Asm(xs[:n*16], ys[:n*16], rs[:n*16])
	for i, j := n*16, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func uint8SubAvx512(xs, ys, rs []uint8) []uint8 {
	n := len(xs) / 16
	uint8SubAvx512Asm(xs[:n*16], ys[:n*16], rs[:n*16])
	for i, j := n*16, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func Uint8SubSels(xs, ys, rs []uint8, sels []int64) []uint8 {
	return uint8SubSels(xs, ys, rs, sels)
}

func uint8SubSelsPure(xs, ys, rs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func Uint8SubScalar(x uint8, ys, rs []uint8) []uint8 {
	return uint8SubScalar(x, ys, rs)
}

func uint8SubScalarPure(x uint8, ys, rs []uint8) []uint8 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func uint8SubScalarAvx2(x uint8, ys, rs []uint8) []uint8 {
	n := len(ys) / 16
	uint8SubScalarAvx2Asm(x, ys[:n*16], rs[:n*16])
	for i, j := n*16, len(ys); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func uint8SubScalarAvx512(x uint8, ys, rs []uint8) []uint8 {
	n := len(ys) / 16
	uint8SubScalarAvx512Asm(x, ys[:n*16], rs[:n*16])
	for i, j := n*16, len(ys); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func Uint8SubScalarSels(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	return uint8SubScalarSels(x, ys, rs, sels)
}

func uint8SubScalarSelsPure(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func Uint8SubByScalar(x uint8, ys, rs []uint8) []uint8 {
	return uint8SubByScalar(x, ys, rs)
}

func uint8SubByScalarPure(x uint8, ys, rs []uint8) []uint8 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func uint8SubByScalarAvx2(x uint8, ys, rs []uint8) []uint8 {
	n := len(ys) / 16
	uint8SubByScalarAvx2Asm(x, ys[:n*16], rs[:n*16])
	for i, j := n*16, len(ys); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func uint8SubByScalarAvx512(x uint8, ys, rs []uint8) []uint8 {
	n := len(ys) / 16
	uint8SubByScalarAvx512Asm(x, ys[:n*16], rs[:n*16])
	for i, j := n*16, len(ys); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func Uint8SubByScalarSels(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	return uint8SubByScalarSels(x, ys, rs, sels)
}

func uint8SubByScalarSelsPure(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func Uint16Sub(xs, ys, rs []uint16) []uint16 {
	return uint16Sub(xs, ys, rs)
}

func uint16SubPure(xs, ys, rs []uint16) []uint16 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func uint16SubAvx2(xs, ys, rs []uint16) []uint16 {
	n := len(xs) / 8
	uint16SubAvx2Asm(xs[:n*8], ys[:n*8], rs[:n*8])
	for i, j := n*8, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func uint16SubAvx512(xs, ys, rs []uint16) []uint16 {
	n := len(xs) / 8
	uint16SubAvx512Asm(xs[:n*8], ys[:n*8], rs[:n*8])
	for i, j := n*8, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func Uint16SubSels(xs, ys, rs []uint16, sels []int64) []uint16 {
	return uint16SubSels(xs, ys, rs, sels)
}

func uint16SubSelsPure(xs, ys, rs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func Uint16SubScalar(x uint16, ys, rs []uint16) []uint16 {
	return uint16SubScalar(x, ys, rs)
}

func uint16SubScalarPure(x uint16, ys, rs []uint16) []uint16 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func uint16SubScalarAvx2(x uint16, ys, rs []uint16) []uint16 {
	n := len(ys) / 8
	uint16SubScalarAvx2Asm(x, ys[:n*8], rs[:n*8])
	for i, j := n*8, len(ys); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func uint16SubScalarAvx512(x uint16, ys, rs []uint16) []uint16 {
	n := len(ys) / 8
	uint16SubScalarAvx512Asm(x, ys[:n*8], rs[:n*8])
	for i, j := n*8, len(ys); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func Uint16SubScalarSels(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	return uint16SubScalarSels(x, ys, rs, sels)
}

func uint16SubScalarSelsPure(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func Uint16SubByScalar(x uint16, ys, rs []uint16) []uint16 {
	return uint16SubByScalar(x, ys, rs)
}

func uint16SubByScalarPure(x uint16, ys, rs []uint16) []uint16 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func uint16SubByScalarAvx2(x uint16, ys, rs []uint16) []uint16 {
	n := len(ys) / 8
	uint16SubByScalarAvx2Asm(x, ys[:n*8], rs[:n*8])
	for i, j := n*8, len(ys); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func uint16SubByScalarAvx512(x uint16, ys, rs []uint16) []uint16 {
	n := len(ys) / 8
	uint16SubByScalarAvx512Asm(x, ys[:n*8], rs[:n*8])
	for i, j := n*8, len(ys); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func Uint16SubByScalarSels(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	return uint16SubByScalarSels(x, ys, rs, sels)
}

func uint16SubByScalarSelsPure(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func Uint32Sub(xs, ys, rs []uint32) []uint32 {
	return uint32Sub(xs, ys, rs)
}

func uint32SubPure(xs, ys, rs []uint32) []uint32 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func uint32SubAvx2(xs, ys, rs []uint32) []uint32 {
	n := len(xs) / 4
	uint32SubAvx2Asm(xs[:n*4], ys[:n*4], rs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func uint32SubAvx512(xs, ys, rs []uint32) []uint32 {
	n := len(xs) / 4
	uint32SubAvx512Asm(xs[:n*4], ys[:n*4], rs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func Uint32SubSels(xs, ys, rs []uint32, sels []int64) []uint32 {
	return uint32SubSels(xs, ys, rs, sels)
}

func uint32SubSelsPure(xs, ys, rs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func Uint32SubScalar(x uint32, ys, rs []uint32) []uint32 {
	return uint32SubScalar(x, ys, rs)
}

func uint32SubScalarPure(x uint32, ys, rs []uint32) []uint32 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func uint32SubScalarAvx2(x uint32, ys, rs []uint32) []uint32 {
	n := len(ys) / 4
	uint32SubScalarAvx2Asm(x, ys[:n*4], rs[:n*4])
	for i, j := n*4, len(ys); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func uint32SubScalarAvx512(x uint32, ys, rs []uint32) []uint32 {
	n := len(ys) / 4
	uint32SubScalarAvx512Asm(x, ys[:n*4], rs[:n*4])
	for i, j := n*4, len(ys); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func Uint32SubScalarSels(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	return uint32SubScalarSels(x, ys, rs, sels)
}

func uint32SubScalarSelsPure(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func Uint32SubByScalar(x uint32, ys, rs []uint32) []uint32 {
	return uint32SubByScalar(x, ys, rs)
}

func uint32SubByScalarPure(x uint32, ys, rs []uint32) []uint32 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func uint32SubByScalarAvx2(x uint32, ys, rs []uint32) []uint32 {
	n := len(ys) / 4
	uint32SubByScalarAvx2Asm(x, ys[:n*4], rs[:n*4])
	for i, j := n*4, len(ys); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func uint32SubByScalarAvx512(x uint32, ys, rs []uint32) []uint32 {
	n := len(ys) / 4
	uint32SubByScalarAvx512Asm(x, ys[:n*4], rs[:n*4])
	for i, j := n*4, len(ys); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func Uint32SubByScalarSels(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	return uint32SubByScalarSels(x, ys, rs, sels)
}

func uint32SubByScalarSelsPure(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func Uint64Sub(xs, ys, rs []uint64) []uint64 {
	return uint64Sub(xs, ys, rs)
}

func uint64SubPure(xs, ys, rs []uint64) []uint64 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func uint64SubAvx2(xs, ys, rs []uint64) []uint64 {
	n := len(xs) / 2
	uint64SubAvx2Asm(xs[:n*2], ys[:n*2], rs[:n*2])
	for i, j := n*2, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func uint64SubAvx512(xs, ys, rs []uint64) []uint64 {
	n := len(xs) / 2
	uint64SubAvx512Asm(xs[:n*2], ys[:n*2], rs[:n*2])
	for i, j := n*2, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func Uint64SubSels(xs, ys, rs []uint64, sels []int64) []uint64 {
	return uint64SubSels(xs, ys, rs, sels)
}

func uint64SubSelsPure(xs, ys, rs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func Uint64SubScalar(x uint64, ys, rs []uint64) []uint64 {
	return uint64SubScalar(x, ys, rs)
}

func uint64SubScalarPure(x uint64, ys, rs []uint64) []uint64 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func uint64SubScalarAvx2(x uint64, ys, rs []uint64) []uint64 {
	n := len(ys) / 2
	uint64SubScalarAvx2Asm(x, ys[:n*2], rs[:n*2])
	for i, j := n*2, len(ys); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func uint64SubScalarAvx512(x uint64, ys, rs []uint64) []uint64 {
	n := len(ys) / 2
	uint64SubScalarAvx512Asm(x, ys[:n*2], rs[:n*2])
	for i, j := n*2, len(ys); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func Uint64SubScalarSels(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	return uint64SubScalarSels(x, ys, rs, sels)
}

func uint64SubScalarSelsPure(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func Uint64SubByScalar(x uint64, ys, rs []uint64) []uint64 {
	return uint64SubByScalar(x, ys, rs)
}

func uint64SubByScalarPure(x uint64, ys, rs []uint64) []uint64 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func uint64SubByScalarAvx2(x uint64, ys, rs []uint64) []uint64 {
	n := len(ys) / 2
	uint64SubByScalarAvx2Asm(x, ys[:n*2], rs[:n*2])
	for i, j := n*2, len(ys); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func uint64SubByScalarAvx512(x uint64, ys, rs []uint64) []uint64 {
	n := len(ys) / 2
	uint64SubByScalarAvx512Asm(x, ys[:n*2], rs[:n*2])
	for i, j := n*2, len(ys); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func Uint64SubByScalarSels(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	return uint64SubByScalarSels(x, ys, rs, sels)
}

func uint64SubByScalarSelsPure(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func Float32Sub(xs, ys, rs []float32) []float32 {
	return float32Sub(xs, ys, rs)
}

func float32SubPure(xs, ys, rs []float32) []float32 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func float32SubAvx2(xs, ys, rs []float32) []float32 {
	n := len(xs) / 4
	float32SubAvx2Asm(xs[:n*4], ys[:n*4], rs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func float32SubAvx512(xs, ys, rs []float32) []float32 {
	n := len(xs) / 4
	float32SubAvx512Asm(xs[:n*4], ys[:n*4], rs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func Float32SubSels(xs, ys, rs []float32, sels []int64) []float32 {
	return float32SubSels(xs, ys, rs, sels)
}

func float32SubSelsPure(xs, ys, rs []float32, sels []int64) []float32 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func Float32SubScalar(x float32, ys, rs []float32) []float32 {
	return float32SubScalar(x, ys, rs)
}

func float32SubScalarPure(x float32, ys, rs []float32) []float32 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func float32SubScalarAvx2(x float32, ys, rs []float32) []float32 {
	n := len(ys) / 4
	float32SubScalarAvx2Asm(x, ys[:n*4], rs[:n*4])
	for i, j := n*4, len(ys); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func float32SubScalarAvx512(x float32, ys, rs []float32) []float32 {
	n := len(ys) / 4
	float32SubScalarAvx512Asm(x, ys[:n*4], rs[:n*4])
	for i, j := n*4, len(ys); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func Float32SubScalarSels(x float32, ys, rs []float32, sels []int64) []float32 {
	return float32SubScalarSels(x, ys, rs, sels)
}

func float32SubScalarSelsPure(x float32, ys, rs []float32, sels []int64) []float32 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func Float32SubByScalar(x float32, ys, rs []float32) []float32 {
	return float32SubByScalar(x, ys, rs)
}

func float32SubByScalarPure(x float32, ys, rs []float32) []float32 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func float32SubByScalarAvx2(x float32, ys, rs []float32) []float32 {
	n := len(ys) / 4
	float32SubByScalarAvx2Asm(x, ys[:n*4], rs[:n*4])
	for i, j := n*4, len(ys); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func float32SubByScalarAvx512(x float32, ys, rs []float32) []float32 {
	n := len(ys) / 4
	float32SubByScalarAvx512Asm(x, ys[:n*4], rs[:n*4])
	for i, j := n*4, len(ys); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func Float32SubByScalarSels(x float32, ys, rs []float32, sels []int64) []float32 {
	return float32SubByScalarSels(x, ys, rs, sels)
}

func float32SubByScalarSelsPure(x float32, ys, rs []float32, sels []int64) []float32 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func Float64Sub(xs, ys, rs []float64) []float64 {
	return float64Sub(xs, ys, rs)
}

func float64SubPure(xs, ys, rs []float64) []float64 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func float64SubAvx2(xs, ys, rs []float64) []float64 {
	n := len(xs) / 2
	float64SubAvx2Asm(xs[:n*2], ys[:n*2], rs[:n*2])
	for i, j := n*2, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func float64SubAvx512(xs, ys, rs []float64) []float64 {
	n := len(xs) / 2
	float64SubAvx512Asm(xs[:n*2], ys[:n*2], rs[:n*2])
	for i, j := n*2, len(xs); i < j; i++ {
		rs[i] = xs[i] - ys[i]
	}
	return rs
}

func Float64SubSels(xs, ys, rs []float64, sels []int64) []float64 {
	return float64SubSels(xs, ys, rs, sels)
}

func float64SubSelsPure(xs, ys, rs []float64, sels []int64) []float64 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func Float64SubScalar(x float64, ys, rs []float64) []float64 {
	return float64SubScalar(x, ys, rs)
}

func float64SubScalarPure(x float64, ys, rs []float64) []float64 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func float64SubScalarAvx2(x float64, ys, rs []float64) []float64 {
	n := len(ys) / 2
	float64SubScalarAvx2Asm(x, ys[:n*2], rs[:n*2])
	for i, j := n*2, len(ys); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func float64SubScalarAvx512(x float64, ys, rs []float64) []float64 {
	n := len(ys) / 2
	float64SubScalarAvx512Asm(x, ys[:n*2], rs[:n*2])
	for i, j := n*2, len(ys); i < j; i++ {
		rs[i] = x - ys[i]
	}
	return rs
}

func Float64SubScalarSels(x float64, ys, rs []float64, sels []int64) []float64 {
	return float64SubScalarSels(x, ys, rs, sels)
}

func float64SubScalarSelsPure(x float64, ys, rs []float64, sels []int64) []float64 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func Float64SubByScalar(x float64, ys, rs []float64) []float64 {
	return float64SubByScalar(x, ys, rs)
}

func float64SubByScalarPure(x float64, ys, rs []float64) []float64 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func float64SubByScalarAvx2(x float64, ys, rs []float64) []float64 {
	n := len(ys) / 2
	float64SubByScalarAvx2Asm(x, ys[:n*2], rs[:n*2])
	for i, j := n*2, len(ys); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func float64SubByScalarAvx512(x float64, ys, rs []float64) []float64 {
	n := len(ys) / 2
	float64SubByScalarAvx512Asm(x, ys[:n*2], rs[:n*2])
	for i, j := n*2, len(ys); i < j; i++ {
		rs[i] = ys[i] - x
	}
	return rs
}

func Float64SubByScalarSels(x float64, ys, rs []float64, sels []int64) []float64 {
	return float64SubByScalarSels(x, ys, rs, sels)
}

func float64SubByScalarSelsPure(x float64, ys, rs []float64, sels []int64) []float64 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}
