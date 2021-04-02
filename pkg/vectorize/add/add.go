package add

import "golang.org/x/sys/cpu"

var (
	int8Add              func([]int8, []int8, []int8) []int8
	int8AddSels          func([]int8, []int8, []int8, []int64) []int8
	int8AddScalar        func(int8, []int8, []int8) []int8
	int8AddScalarSels    func(int8, []int8, []int8, []int64) []int8
	int16Add             func([]int16, []int16, []int16) []int16
	int16AddSels         func([]int16, []int16, []int16, []int64) []int16
	int16AddScalar       func(int16, []int16, []int16) []int16
	int16AddScalarSels   func(int16, []int16, []int16, []int64) []int16
	int32Add             func([]int32, []int32, []int32) []int32
	int32AddSels         func([]int32, []int32, []int32, []int64) []int32
	int32AddScalar       func(int32, []int32, []int32) []int32
	int32AddScalarSels   func(int32, []int32, []int32, []int64) []int32
	int64Add             func([]int64, []int64, []int64) []int64
	int64AddSels         func([]int64, []int64, []int64, []int64) []int64
	int64AddScalar       func(int64, []int64, []int64) []int64
	int64AddScalarSels   func(int64, []int64, []int64, []int64) []int64
	uint8Add             func([]uint8, []uint8, []uint8) []uint8
	uint8AddSels         func([]uint8, []uint8, []uint8, []int64) []uint8
	uint8AddScalar       func(uint8, []uint8, []uint8) []uint8
	uint8AddScalarSels   func(uint8, []uint8, []uint8, []int64) []uint8
	uint16Add            func([]uint16, []uint16, []uint16) []uint16
	uint16AddSels        func([]uint16, []uint16, []uint16, []int64) []uint16
	uint16AddScalar      func(uint16, []uint16, []uint16) []uint16
	uint16AddScalarSels  func(uint16, []uint16, []uint16, []int64) []uint16
	uint32Add            func([]uint32, []uint32, []uint32) []uint32
	uint32AddSels        func([]uint32, []uint32, []uint32, []int64) []uint32
	uint32AddScalar      func(uint32, []uint32, []uint32) []uint32
	uint32AddScalarSels  func(uint32, []uint32, []uint32, []int64) []uint32
	uint64Add            func([]uint64, []uint64, []uint64) []uint64
	uint64AddSels        func([]uint64, []uint64, []uint64, []int64) []uint64
	uint64AddScalar      func(uint64, []uint64, []uint64) []uint64
	uint64AddScalarSels  func(uint64, []uint64, []uint64, []int64) []uint64
	float32Add           func([]float32, []float32, []float32) []float32
	float32AddSels       func([]float32, []float32, []float32, []int64) []float32
	float32AddScalar     func(float32, []float32, []float32) []float32
	float32AddScalarSels func(float32, []float32, []float32, []int64) []float32
	float64Add           func([]float64, []float64, []float64) []float64
	float64AddSels       func([]float64, []float64, []float64, []int64) []float64
	float64AddScalar     func(float64, []float64, []float64) []float64
	float64AddScalarSels func(float64, []float64, []float64, []int64) []float64
)

func init() {
	if cpu.X86.HasAVX512 {
		int8Add = int8AddAvx512
		int8AddScalar = int8AddScalarAvx512
		int16Add = int16AddAvx512
		int16AddScalar = int16AddScalarAvx512
		int32Add = int32AddAvx512
		int32AddScalar = int32AddScalarAvx512
		int64Add = int64AddAvx512
		int64AddScalar = int64AddScalarAvx512
		uint8Add = uint8AddAvx512
		uint8AddScalar = uint8AddScalarAvx512
		uint16Add = uint16AddAvx512
		uint16AddScalar = uint16AddScalarAvx512
		uint32Add = uint32AddAvx512
		uint32AddScalar = uint32AddScalarAvx512
		uint64Add = uint64AddAvx512
		uint64AddScalar = uint64AddScalarAvx512
		float32Add = float32AddAvx512
		float32AddScalar = float32AddScalarAvx512
		float64Add = float64AddAvx512
		float64AddScalar = float64AddScalarAvx512
	} else if cpu.X86.HasAVX2 {
		int8Add = int8AddAvx2
		int8AddScalar = int8AddScalarAvx2
		int16Add = int16AddAvx2
		int16AddScalar = int16AddScalarAvx2
		int32Add = int32AddAvx2
		int32AddScalar = int32AddScalarAvx2
		int64Add = int64AddAvx2
		int64AddScalar = int64AddScalarAvx2
		uint8Add = uint8AddAvx2
		uint8AddScalar = uint8AddScalarAvx2
		uint16Add = uint16AddAvx2
		uint16AddScalar = uint16AddScalarAvx2
		uint32Add = uint32AddAvx2
		uint32AddScalar = uint32AddScalarAvx2
		uint64Add = uint64AddAvx2
		uint64AddScalar = uint64AddScalarAvx2
		float32Add = float32AddAvx2
		float32AddScalar = float32AddScalarAvx2
		float64Add = float64AddAvx2
		float64AddScalar = float64AddScalarAvx2
	} else {
		int8Add = int8AddPure
		int8AddScalar = int8AddScalarPure
		int16Add = int16AddPure
		int16AddScalar = int16AddScalarPure
		int32Add = int32AddPure
		int32AddScalar = int32AddScalarPure
		int64Add = int64AddPure
		int64AddScalar = int64AddScalarPure
		uint8Add = uint8AddPure
		uint8AddScalar = uint8AddScalarPure
		uint16Add = uint16AddPure
		uint16AddScalar = uint16AddScalarPure
		uint32Add = uint32AddPure
		uint32AddScalar = uint32AddScalarPure
		uint64Add = uint64AddPure
		uint64AddScalar = uint64AddScalarPure
		float32Add = float32AddPure
		float32AddScalar = float32AddScalarPure
		float64Add = float64AddPure
		float64AddScalar = float64AddScalarPure
	}
	int8AddSels = int8AddSelsPure
	int8AddScalarSels = int8AddScalarSelsPure
	int16AddSels = int16AddSelsPure
	int16AddScalarSels = int16AddScalarSelsPure
	int32AddSels = int32AddSelsPure
	int32AddScalarSels = int32AddScalarSelsPure
	int64AddSels = int64AddSelsPure
	int64AddScalarSels = int64AddScalarSelsPure
	uint8AddSels = uint8AddSelsPure
	uint8AddScalarSels = uint8AddScalarSelsPure
	uint16AddSels = uint16AddSelsPure
	uint16AddScalarSels = uint16AddScalarSelsPure
	uint32AddSels = uint32AddSelsPure
	uint32AddScalarSels = uint32AddScalarSelsPure
	uint64AddSels = uint64AddSelsPure
	uint64AddScalarSels = uint64AddScalarSelsPure
	float32AddSels = float32AddSelsPure
	float32AddScalarSels = float32AddScalarSelsPure
	float64AddSels = float64AddSelsPure
	float64AddScalarSels = float64AddScalarSelsPure
}

func Int8Add(xs, ys, rs []int8) []int8 {
	return int8Add(xs, ys, rs)
}

func int8AddPure(xs, ys, rs []int8) []int8 {
	for i, x := range xs {
		rs[i] = x + ys[i]
	}
	return rs
}

func int8AddAvx2(xs, ys, rs []int8) []int8 {
	n := len(xs) / 16
	int8AddAvx2Asm(xs[:n*16], ys[:n*16], rs[:n*16])
	for i, j := n*16, len(xs); i < j; i++ {
		rs[i] = xs[i] + ys[i]
	}
	return rs
}

func int8AddAvx512(xs, ys, rs []int8) []int8 {
	n := len(xs) / 16
	int8AddAvx512Asm(xs[:n*16], ys[:n*16], rs[:n*16])
	for i, j := n*16, len(xs); i < j; i++ {
		rs[i] = xs[i] + ys[i]
	}
	return rs
}

func Int8AddSels(xs, ys, rs []int8, sels []int64) []int8 {
	return int8AddSels(xs, ys, rs, sels)
}

func int8AddSelsPure(xs, ys, rs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		rs[i] = xs[sel] + ys[sel]
	}
	return rs
}

func Int8AddScalar(x int8, ys, rs []int8) []int8 {
	return int8AddScalar(x, ys, rs)
}

func int8AddScalarPure(x int8, ys, rs []int8) []int8 {
	for i, y := range ys {
		rs[i] = x + y
	}
	return rs
}

func int8AddScalarAvx2(x int8, ys, rs []int8) []int8 {
	n := len(ys) / 16
	int8AddScalarAvx2Asm(x, ys[:n*16], rs[:n*16])
	for i, j := n*16, len(ys); i < j; i++ {
		rs[i] = x + ys[i]
	}
	return rs
}

func int8AddScalarAvx512(x int8, ys, rs []int8) []int8 {
	n := len(ys) / 16
	int8AddScalarAvx512Asm(x, ys[:n*16], rs[:n*16])
	for i, j := n*16, len(ys); i < j; i++ {
		rs[i] = x + ys[i]
	}
	return rs
}

func Int8AddScalarSels(x int8, ys, rs []int8, sels []int64) []int8 {
	return int8AddScalarSels(x, ys, rs, sels)
}

func int8AddScalarSelsPure(x int8, ys, rs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		rs[i] = x + ys[sel]
	}
	return rs
}

func Int16Add(xs, ys, rs []int16) []int16 {
	return int16Add(xs, ys, rs)
}

func int16AddPure(xs, ys, rs []int16) []int16 {
	for i, x := range xs {
		rs[i] = x + ys[i]
	}
	return rs
}

func int16AddAvx2(xs, ys, rs []int16) []int16 {
	n := len(xs) / 8
	int16AddAvx2Asm(xs[:n*8], ys[:n*8], rs[:n*8])
	for i, j := n*8, len(xs); i < j; i++ {
		rs[i] = xs[i] + ys[i]
	}
	return rs
}

func int16AddAvx512(xs, ys, rs []int16) []int16 {
	n := len(xs) / 8
	int16AddAvx512Asm(xs[:n*8], ys[:n*8], rs[:n*8])
	for i, j := n*8, len(xs); i < j; i++ {
		rs[i] = xs[i] + ys[i]
	}
	return rs
}

func Int16AddSels(xs, ys, rs []int16, sels []int64) []int16 {
	return int16AddSels(xs, ys, rs, sels)
}

func int16AddSelsPure(xs, ys, rs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		rs[i] = xs[sel] + ys[sel]
	}
	return rs
}

func Int16AddScalar(x int16, ys, rs []int16) []int16 {
	return int16AddScalar(x, ys, rs)
}

func int16AddScalarPure(x int16, ys, rs []int16) []int16 {
	for i, y := range ys {
		rs[i] = x + y
	}
	return rs
}

func int16AddScalarAvx2(x int16, ys, rs []int16) []int16 {
	n := len(ys) / 8
	int16AddScalarAvx2Asm(x, ys[:n*8], rs[:n*8])
	for i, j := n*8, len(ys); i < j; i++ {
		rs[i] = x + ys[i]
	}
	return rs
}

func int16AddScalarAvx512(x int16, ys, rs []int16) []int16 {
	n := len(ys) / 8
	int16AddScalarAvx512Asm(x, ys[:n*8], rs[:n*8])
	for i, j := n*8, len(ys); i < j; i++ {
		rs[i] = x + ys[i]
	}
	return rs
}

func Int16AddScalarSels(x int16, ys, rs []int16, sels []int64) []int16 {
	return int16AddScalarSels(x, ys, rs, sels)
}

func int16AddScalarSelsPure(x int16, ys, rs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		rs[i] = x + ys[sel]
	}
	return rs
}

func Int32Add(xs, ys, rs []int32) []int32 {
	return int32Add(xs, ys, rs)
}

func int32AddPure(xs, ys, rs []int32) []int32 {
	for i, x := range xs {
		rs[i] = x + ys[i]
	}
	return rs
}

func int32AddAvx2(xs, ys, rs []int32) []int32 {
	n := len(xs) / 4
	int32AddAvx2Asm(xs[:n*4], ys[:n*4], rs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		rs[i] = xs[i] + ys[i]
	}
	return rs
}

func int32AddAvx512(xs, ys, rs []int32) []int32 {
	n := len(xs) / 4
	int32AddAvx512Asm(xs[:n*4], ys[:n*4], rs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		rs[i] = xs[i] + ys[i]
	}
	return rs
}

func Int32AddSels(xs, ys, rs []int32, sels []int64) []int32 {
	return int32AddSels(xs, ys, rs, sels)
}

func int32AddSelsPure(xs, ys, rs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		rs[i] = xs[sel] + ys[sel]
	}
	return rs
}

func Int32AddScalar(x int32, ys, rs []int32) []int32 {
	return int32AddScalar(x, ys, rs)
}

func int32AddScalarPure(x int32, ys, rs []int32) []int32 {
	for i, y := range ys {
		rs[i] = x + y
	}
	return rs
}

func int32AddScalarAvx2(x int32, ys, rs []int32) []int32 {
	n := len(ys) / 4
	int32AddScalarAvx2Asm(x, ys[:n*4], rs[:n*4])
	for i, j := n*4, len(ys); i < j; i++ {
		rs[i] = x + ys[i]
	}
	return rs
}

func int32AddScalarAvx512(x int32, ys, rs []int32) []int32 {
	n := len(ys) / 4
	int32AddScalarAvx512Asm(x, ys[:n*4], rs[:n*4])
	for i, j := n*4, len(ys); i < j; i++ {
		rs[i] = x + ys[i]
	}
	return rs
}

func Int32AddScalarSels(x int32, ys, rs []int32, sels []int64) []int32 {
	return int32AddScalarSels(x, ys, rs, sels)
}

func int32AddScalarSelsPure(x int32, ys, rs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		rs[i] = x + ys[sel]
	}
	return rs
}

func Int64Add(xs, ys, rs []int64) []int64 {
	return int64Add(xs, ys, rs)
}

func int64AddPure(xs, ys, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = x + ys[i]
	}
	return rs
}

func int64AddAvx2(xs, ys, rs []int64) []int64 {
	n := len(xs) / 2
	int64AddAvx2Asm(xs[:n*2], ys[:n*2], rs[:n*2])
	for i, j := n*2, len(xs); i < j; i++ {
		rs[i] = xs[i] + ys[i]
	}
	return rs
}

func int64AddAvx512(xs, ys, rs []int64) []int64 {
	n := len(xs) / 2
	int64AddAvx512Asm(xs[:n*2], ys[:n*2], rs[:n*2])
	for i, j := n*2, len(xs); i < j; i++ {
		rs[i] = xs[i] + ys[i]
	}
	return rs
}

func Int64AddSels(xs, ys, rs []int64, sels []int64) []int64 {
	return int64AddSels(xs, ys, rs, sels)
}

func int64AddSelsPure(xs, ys, rs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		rs[i] = xs[sel] + ys[sel]
	}
	return rs
}

func Int64AddScalar(x int64, ys, rs []int64) []int64 {
	return int64AddScalar(x, ys, rs)
}

func int64AddScalarPure(x int64, ys, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = x + y
	}
	return rs
}

func int64AddScalarAvx2(x int64, ys, rs []int64) []int64 {
	n := len(ys) / 2
	int64AddScalarAvx2Asm(x, ys[:n*2], rs[:n*2])
	for i, j := n*2, len(ys); i < j; i++ {
		rs[i] = x + ys[i]
	}
	return rs
}

func int64AddScalarAvx512(x int64, ys, rs []int64) []int64 {
	n := len(ys) / 2
	int64AddScalarAvx512Asm(x, ys[:n*2], rs[:n*2])
	for i, j := n*2, len(ys); i < j; i++ {
		rs[i] = x + ys[i]
	}
	return rs
}

func Int64AddScalarSels(x int64, ys, rs []int64, sels []int64) []int64 {
	return int64AddScalarSels(x, ys, rs, sels)
}

func int64AddScalarSelsPure(x int64, ys, rs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		rs[i] = x + ys[sel]
	}
	return rs
}

func Uint8Add(xs, ys, rs []uint8) []uint8 {
	return uint8Add(xs, ys, rs)
}

func uint8AddPure(xs, ys, rs []uint8) []uint8 {
	for i, x := range xs {
		rs[i] = x + ys[i]
	}
	return rs
}

func uint8AddAvx2(xs, ys, rs []uint8) []uint8 {
	n := len(xs) / 16
	uint8AddAvx2Asm(xs[:n*16], ys[:n*16], rs[:n*16])
	for i, j := n*16, len(xs); i < j; i++ {
		rs[i] = xs[i] + ys[i]
	}
	return rs
}

func uint8AddAvx512(xs, ys, rs []uint8) []uint8 {
	n := len(xs) / 16
	uint8AddAvx512Asm(xs[:n*16], ys[:n*16], rs[:n*16])
	for i, j := n*16, len(xs); i < j; i++ {
		rs[i] = xs[i] + ys[i]
	}
	return rs
}

func Uint8AddSels(xs, ys, rs []uint8, sels []int64) []uint8 {
	return uint8AddSels(xs, ys, rs, sels)
}

func uint8AddSelsPure(xs, ys, rs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		rs[i] = xs[sel] + ys[sel]
	}
	return rs
}

func Uint8AddScalar(x uint8, ys, rs []uint8) []uint8 {
	return uint8AddScalar(x, ys, rs)
}

func uint8AddScalarPure(x uint8, ys, rs []uint8) []uint8 {
	for i, y := range ys {
		rs[i] = x + y
	}
	return rs
}

func uint8AddScalarAvx2(x uint8, ys, rs []uint8) []uint8 {
	n := len(ys) / 16
	uint8AddScalarAvx2Asm(x, ys[:n*16], rs[:n*16])
	for i, j := n*16, len(ys); i < j; i++ {
		rs[i] = x + ys[i]
	}
	return rs
}

func uint8AddScalarAvx512(x uint8, ys, rs []uint8) []uint8 {
	n := len(ys) / 16
	uint8AddScalarAvx512Asm(x, ys[:n*16], rs[:n*16])
	for i, j := n*16, len(ys); i < j; i++ {
		rs[i] = x + ys[i]
	}
	return rs
}

func Uint8AddScalarSels(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	return uint8AddScalarSels(x, ys, rs, sels)
}

func uint8AddScalarSelsPure(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		rs[i] = x + ys[sel]
	}
	return rs
}

func Uint16Add(xs, ys, rs []uint16) []uint16 {
	return uint16Add(xs, ys, rs)
}

func uint16AddPure(xs, ys, rs []uint16) []uint16 {
	for i, x := range xs {
		rs[i] = x + ys[i]
	}
	return rs
}

func uint16AddAvx2(xs, ys, rs []uint16) []uint16 {
	n := len(xs) / 8
	uint16AddAvx2Asm(xs[:n*8], ys[:n*8], rs[:n*8])
	for i, j := n*8, len(xs); i < j; i++ {
		rs[i] = xs[i] + ys[i]
	}
	return rs
}

func uint16AddAvx512(xs, ys, rs []uint16) []uint16 {
	n := len(xs) / 8
	uint16AddAvx512Asm(xs[:n*8], ys[:n*8], rs[:n*8])
	for i, j := n*8, len(xs); i < j; i++ {
		rs[i] = xs[i] + ys[i]
	}
	return rs
}

func Uint16AddSels(xs, ys, rs []uint16, sels []int64) []uint16 {
	return uint16AddSels(xs, ys, rs, sels)
}

func uint16AddSelsPure(xs, ys, rs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		rs[i] = xs[sel] + ys[sel]
	}
	return rs
}

func Uint16AddScalar(x uint16, ys, rs []uint16) []uint16 {
	return uint16AddScalar(x, ys, rs)
}

func uint16AddScalarPure(x uint16, ys, rs []uint16) []uint16 {
	for i, y := range ys {
		rs[i] = x + y
	}
	return rs
}

func uint16AddScalarAvx2(x uint16, ys, rs []uint16) []uint16 {
	n := len(ys) / 8
	uint16AddScalarAvx2Asm(x, ys[:n*8], rs[:n*8])
	for i, j := n*8, len(ys); i < j; i++ {
		rs[i] = x + ys[i]
	}
	return rs
}

func uint16AddScalarAvx512(x uint16, ys, rs []uint16) []uint16 {
	n := len(ys) / 8
	uint16AddScalarAvx512Asm(x, ys[:n*8], rs[:n*8])
	for i, j := n*8, len(ys); i < j; i++ {
		rs[i] = x + ys[i]
	}
	return rs
}

func Uint16AddScalarSels(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	return uint16AddScalarSels(x, ys, rs, sels)
}

func uint16AddScalarSelsPure(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		rs[i] = x + ys[sel]
	}
	return rs
}

func Uint32Add(xs, ys, rs []uint32) []uint32 {
	return uint32Add(xs, ys, rs)
}

func uint32AddPure(xs, ys, rs []uint32) []uint32 {
	for i, x := range xs {
		rs[i] = x + ys[i]
	}
	return rs
}

func uint32AddAvx2(xs, ys, rs []uint32) []uint32 {
	n := len(xs) / 4
	uint32AddAvx2Asm(xs[:n*4], ys[:n*4], rs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		rs[i] = xs[i] + ys[i]
	}
	return rs
}

func uint32AddAvx512(xs, ys, rs []uint32) []uint32 {
	n := len(xs) / 4
	uint32AddAvx512Asm(xs[:n*4], ys[:n*4], rs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		rs[i] = xs[i] + ys[i]
	}
	return rs
}

func Uint32AddSels(xs, ys, rs []uint32, sels []int64) []uint32 {
	return uint32AddSels(xs, ys, rs, sels)
}

func uint32AddSelsPure(xs, ys, rs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		rs[i] = xs[sel] + ys[sel]
	}
	return rs
}

func Uint32AddScalar(x uint32, ys, rs []uint32) []uint32 {
	return uint32AddScalar(x, ys, rs)
}

func uint32AddScalarPure(x uint32, ys, rs []uint32) []uint32 {
	for i, y := range ys {
		rs[i] = x + y
	}
	return rs
}

func uint32AddScalarAvx2(x uint32, ys, rs []uint32) []uint32 {
	n := len(ys) / 4
	uint32AddScalarAvx2Asm(x, ys[:n*4], rs[:n*4])
	for i, j := n*4, len(ys); i < j; i++ {
		rs[i] = x + ys[i]
	}
	return rs
}

func uint32AddScalarAvx512(x uint32, ys, rs []uint32) []uint32 {
	n := len(ys) / 4
	uint32AddScalarAvx512Asm(x, ys[:n*4], rs[:n*4])
	for i, j := n*4, len(ys); i < j; i++ {
		rs[i] = x + ys[i]
	}
	return rs
}

func Uint32AddScalarSels(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	return uint32AddScalarSels(x, ys, rs, sels)
}

func uint32AddScalarSelsPure(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		rs[i] = x + ys[sel]
	}
	return rs
}

func Uint64Add(xs, ys, rs []uint64) []uint64 {
	return uint64Add(xs, ys, rs)
}

func uint64AddPure(xs, ys, rs []uint64) []uint64 {
	for i, x := range xs {
		rs[i] = x + ys[i]
	}
	return rs
}

func uint64AddAvx2(xs, ys, rs []uint64) []uint64 {
	n := len(xs) / 2
	uint64AddAvx2Asm(xs[:n*2], ys[:n*2], rs[:n*2])
	for i, j := n*2, len(xs); i < j; i++ {
		rs[i] = xs[i] + ys[i]
	}
	return rs
}

func uint64AddAvx512(xs, ys, rs []uint64) []uint64 {
	n := len(xs) / 2
	uint64AddAvx512Asm(xs[:n*2], ys[:n*2], rs[:n*2])
	for i, j := n*2, len(xs); i < j; i++ {
		rs[i] = xs[i] + ys[i]
	}
	return rs
}

func Uint64AddSels(xs, ys, rs []uint64, sels []int64) []uint64 {
	return uint64AddSels(xs, ys, rs, sels)
}

func uint64AddSelsPure(xs, ys, rs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		rs[i] = xs[sel] + ys[sel]
	}
	return rs
}

func Uint64AddScalar(x uint64, ys, rs []uint64) []uint64 {
	return uint64AddScalar(x, ys, rs)
}

func uint64AddScalarPure(x uint64, ys, rs []uint64) []uint64 {
	for i, y := range ys {
		rs[i] = x + y
	}
	return rs
}

func uint64AddScalarAvx2(x uint64, ys, rs []uint64) []uint64 {
	n := len(ys) / 2
	uint64AddScalarAvx2Asm(x, ys[:n*2], rs[:n*2])
	for i, j := n*2, len(ys); i < j; i++ {
		rs[i] = x + ys[i]
	}
	return rs
}

func uint64AddScalarAvx512(x uint64, ys, rs []uint64) []uint64 {
	n := len(ys) / 2
	uint64AddScalarAvx512Asm(x, ys[:n*2], rs[:n*2])
	for i, j := n*2, len(ys); i < j; i++ {
		rs[i] = x + ys[i]
	}
	return rs
}

func Uint64AddScalarSels(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	return uint64AddScalarSels(x, ys, rs, sels)
}

func uint64AddScalarSelsPure(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		rs[i] = x + ys[sel]
	}
	return rs
}

func Float32Add(xs, ys, rs []float32) []float32 {
	return float32Add(xs, ys, rs)
}

func float32AddPure(xs, ys, rs []float32) []float32 {
	for i, x := range xs {
		rs[i] = x + ys[i]
	}
	return rs
}

func float32AddAvx2(xs, ys, rs []float32) []float32 {
	n := len(xs) / 4
	float32AddAvx2Asm(xs[:n*4], ys[:n*4], rs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		rs[i] = xs[i] + ys[i]
	}
	return rs
}

func float32AddAvx512(xs, ys, rs []float32) []float32 {
	n := len(xs) / 4
	float32AddAvx512Asm(xs[:n*4], ys[:n*4], rs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		rs[i] = xs[i] + ys[i]
	}
	return rs
}

func Float32AddSels(xs, ys, rs []float32, sels []int64) []float32 {
	return float32AddSels(xs, ys, rs, sels)
}

func float32AddSelsPure(xs, ys, rs []float32, sels []int64) []float32 {
	for i, sel := range sels {
		rs[i] = xs[sel] + ys[sel]
	}
	return rs
}

func Float32AddScalar(x float32, ys, rs []float32) []float32 {
	return float32AddScalar(x, ys, rs)
}

func float32AddScalarPure(x float32, ys, rs []float32) []float32 {
	for i, y := range ys {
		rs[i] = x + y
	}
	return rs
}

func float32AddScalarAvx2(x float32, ys, rs []float32) []float32 {
	n := len(ys) / 4
	float32AddScalarAvx2Asm(x, ys[:n*4], rs[:n*4])
	for i, j := n*4, len(ys); i < j; i++ {
		rs[i] = x + ys[i]
	}
	return rs
}

func float32AddScalarAvx512(x float32, ys, rs []float32) []float32 {
	n := len(ys) / 4
	float32AddScalarAvx512Asm(x, ys[:n*4], rs[:n*4])
	for i, j := n*4, len(ys); i < j; i++ {
		rs[i] = x + ys[i]
	}
	return rs
}

func Float32AddScalarSels(x float32, ys, rs []float32, sels []int64) []float32 {
	return float32AddScalarSels(x, ys, rs, sels)
}

func float32AddScalarSelsPure(x float32, ys, rs []float32, sels []int64) []float32 {
	for i, sel := range sels {
		rs[i] = x + ys[sel]
	}
	return rs
}

func Float64Add(xs, ys, rs []float64) []float64 {
	return float64Add(xs, ys, rs)
}

func float64AddPure(xs, ys, rs []float64) []float64 {
	for i, x := range xs {
		rs[i] = x + ys[i]
	}
	return rs
}

func float64AddAvx2(xs, ys, rs []float64) []float64 {
	n := len(xs) / 2
	float64AddAvx2Asm(xs[:n*2], ys[:n*2], rs[:n*2])
	for i, j := n*2, len(xs); i < j; i++ {
		rs[i] = xs[i] + ys[i]
	}
	return rs
}

func float64AddAvx512(xs, ys, rs []float64) []float64 {
	n := len(xs) / 2
	float64AddAvx512Asm(xs[:n*2], ys[:n*2], rs[:n*2])
	for i, j := n*2, len(xs); i < j; i++ {
		rs[i] = xs[i] + ys[i]
	}
	return rs
}

func Float64AddSels(xs, ys, rs []float64, sels []int64) []float64 {
	return float64AddSels(xs, ys, rs, sels)
}

func float64AddSelsPure(xs, ys, rs []float64, sels []int64) []float64 {
	for i, sel := range sels {
		rs[i] = xs[sel] + ys[sel]
	}
	return rs
}

func Float64AddScalar(x float64, ys, rs []float64) []float64 {
	return float64AddScalar(x, ys, rs)
}

func float64AddScalarPure(x float64, ys, rs []float64) []float64 {
	for i, y := range ys {
		rs[i] = x + y
	}
	return rs
}

func float64AddScalarAvx2(x float64, ys, rs []float64) []float64 {
	n := len(ys) / 2
	float64AddScalarAvx2Asm(x, ys[:n*2], rs[:n*2])
	for i, j := n*2, len(ys); i < j; i++ {
		rs[i] = x + ys[i]
	}
	return rs
}

func float64AddScalarAvx512(x float64, ys, rs []float64) []float64 {
	n := len(ys) / 2
	float64AddScalarAvx512Asm(x, ys[:n*2], rs[:n*2])
	for i, j := n*2, len(ys); i < j; i++ {
		rs[i] = x + ys[i]
	}
	return rs
}

func Float64AddScalarSels(x float64, ys, rs []float64, sels []int64) []float64 {
	return float64AddScalarSels(x, ys, rs, sels)
}

func float64AddScalarSelsPure(x float64, ys, rs []float64, sels []int64) []float64 {
	for i, sel := range sels {
		rs[i] = x + ys[sel]
	}
	return rs
}
