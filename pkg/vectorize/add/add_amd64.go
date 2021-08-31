//go:build amd64
// +build amd64

package add

import "golang.org/x/sys/cpu"

func int8AddAvx2Asm(x []int8, y []int8, r []int8)
func int8AddScalarAvx2Asm(x int8, y []int8, r []int8)
func int16AddAvx2Asm(x []int16, y []int16, r []int16)
func int16AddScalarAvx2Asm(x int16, y []int16, r []int16)
func int32AddAvx2Asm(x []int32, y []int32, r []int32)
func int32AddScalarAvx2Asm(x int32, y []int32, r []int32)
func int64AddAvx2Asm(x []int64, y []int64, r []int64)
func int64AddScalarAvx2Asm(x int64, y []int64, r []int64)
func uint8AddAvx2Asm(x []uint8, y []uint8, r []uint8)
func uint8AddScalarAvx2Asm(x uint8, y []uint8, r []uint8)
func uint16AddAvx2Asm(x []uint16, y []uint16, r []uint16)
func uint16AddScalarAvx2Asm(x uint16, y []uint16, r []uint16)
func uint32AddAvx2Asm(x []uint32, y []uint32, r []uint32)
func uint32AddScalarAvx2Asm(x uint32, y []uint32, r []uint32)
func uint64AddAvx2Asm(x []uint64, y []uint64, r []uint64)
func uint64AddScalarAvx2Asm(x uint64, y []uint64, r []uint64)
func float32AddAvx2Asm(x []float32, y []float32, r []float32)
func float32AddScalarAvx2Asm(x float32, y []float32, r []float32)
func float64AddAvx2Asm(x []float64, y []float64, r []float64)
func float64AddScalarAvx2Asm(x float64, y []float64, r []float64)

func int8AddAvx512Asm(x []int8, y []int8, r []int8)
func int8AddScalarAvx512Asm(x int8, y []int8, r []int8)
func int16AddAvx512Asm(x []int16, y []int16, r []int16)
func int16AddScalarAvx512Asm(x int16, y []int16, r []int16)
func int32AddAvx512Asm(x []int32, y []int32, r []int32)
func int32AddScalarAvx512Asm(x int32, y []int32, r []int32)
func int64AddAvx512Asm(x []int64, y []int64, r []int64)
func int64AddScalarAvx512Asm(x int64, y []int64, r []int64)
func uint8AddAvx512Asm(x []uint8, y []uint8, r []uint8)
func uint8AddScalarAvx512Asm(x uint8, y []uint8, r []uint8)
func uint16AddAvx512Asm(x []uint16, y []uint16, r []uint16)
func uint16AddScalarAvx512Asm(x uint16, y []uint16, r []uint16)
func uint32AddAvx512Asm(x []uint32, y []uint32, r []uint32)
func uint32AddScalarAvx512Asm(x uint32, y []uint32, r []uint32)
func uint64AddAvx512Asm(x []uint64, y []uint64, r []uint64)
func uint64AddScalarAvx512Asm(x uint64, y []uint64, r []uint64)
func float32AddAvx512Asm(x []float32, y []float32, r []float32)
func float32AddScalarAvx512Asm(x float32, y []float32, r []float32)
func float64AddAvx512Asm(x []float64, y []float64, r []float64)
func float64AddScalarAvx512Asm(x float64, y []float64, r []float64)

func init() {
	if cpu.X86.HasAVX512 {
		Int8Add = int8AddAvx512
		Int8AddScalar = int8AddScalarAvx512
		Int16Add = int16AddAvx512
		Int16AddScalar = int16AddScalarAvx512
		Int32Add = int32AddAvx512
		Int32AddScalar = int32AddScalarAvx512
		Int64Add = int64AddAvx512
		Int64AddScalar = int64AddScalarAvx512
		Uint8Add = uint8AddAvx512
		Uint8AddScalar = uint8AddScalarAvx512
		Uint16Add = uint16AddAvx512
		Uint16AddScalar = uint16AddScalarAvx512
		Uint32Add = uint32AddAvx512
		Uint32AddScalar = uint32AddScalarAvx512
		Uint64Add = uint64AddAvx512
		Uint64AddScalar = uint64AddScalarAvx512
		Float32Add = float32AddAvx512
		Float32AddScalar = float32AddScalarAvx512
		Float64Add = float64AddAvx512
		Float64AddScalar = float64AddScalarAvx512
	} else if cpu.X86.HasAVX2 {
		Int8Add = int8AddAvx2
		Int8AddScalar = int8AddScalarAvx2
		Int16Add = int16AddAvx2
		Int16AddScalar = int16AddScalarAvx2
		Int32Add = int32AddAvx2
		Int32AddScalar = int32AddScalarAvx2
		Int64Add = int64AddAvx2
		Int64AddScalar = int64AddScalarAvx2
		Uint8Add = uint8AddAvx2
		Uint8AddScalar = uint8AddScalarAvx2
		Uint16Add = uint16AddAvx2
		Uint16AddScalar = uint16AddScalarAvx2
		Uint32Add = uint32AddAvx2
		Uint32AddScalar = uint32AddScalarAvx2
		Uint64Add = uint64AddAvx2
		Uint64AddScalar = uint64AddScalarAvx2
		Float32Add = float32AddAvx2
		Float32AddScalar = float32AddScalarAvx2
		Float64Add = float64AddAvx2
		Float64AddScalar = float64AddScalarAvx2
	} else {
		Int8Add = int8Add
		Int8AddScalar = int8AddScalar
		Int16Add = int16Add
		Int16AddScalar = int16AddScalar
		Int32Add = int32Add
		Int32AddScalar = int32AddScalar
		Int64Add = int64Add
		Int64AddScalar = int64AddScalar
		Uint8Add = uint8Add
		Uint8AddScalar = uint8AddScalar
		Uint16Add = uint16Add
		Uint16AddScalar = uint16AddScalar
		Uint32Add = uint32Add
		Uint32AddScalar = uint32AddScalar
		Uint64Add = uint64Add
		Uint64AddScalar = uint64AddScalar
		Float32Add = float32Add
		Float32AddScalar = float32AddScalar
		Float64Add = float64Add
		Float64AddScalar = float64AddScalar
	}

	Int8AddSels = int8AddSels
	Int8AddScalarSels = int8AddScalarSels
	Int16AddSels = int16AddSels
	Int16AddScalarSels = int16AddScalarSels
	Int32AddSels = int32AddSels
	Int32AddScalarSels = int32AddScalarSels
	Int64AddSels = int64AddSels
	Int64AddScalarSels = int64AddScalarSels
	Uint8AddSels = uint8AddSels
	Uint8AddScalarSels = uint8AddScalarSels
	Uint16AddSels = uint16AddSels
	Uint16AddScalarSels = uint16AddScalarSels
	Uint32AddSels = uint32AddSels
	Uint32AddScalarSels = uint32AddScalarSels
	Uint64AddSels = uint64AddSels
	Uint64AddScalarSels = uint64AddScalarSels
	Float32AddSels = float32AddSels
	Float32AddScalarSels = float32AddScalarSels
	Float64AddSels = float64AddSels
	Float64AddScalarSels = float64AddScalarSels
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
