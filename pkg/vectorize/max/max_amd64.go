// +build amd64

package max

import (
	"golang.org/x/sys/cpu"
)

func int8MaxAvx2Asm([]int8, []int8)
func int8MaxAvx512Asm([]int8, []int8)
func int16MaxAvx2Asm([]int16, []int16)
func int16MaxAvx512Asm([]int16, []int16)
func int32MaxAvx2Asm([]int32, []int32)
func int32MaxAvx512Asm([]int32, []int32)
func int64MaxAvx512Asm([]int64, []int64)
func uint8MaxAvx2Asm([]uint8, []uint8)
func uint8MaxAvx512Asm([]uint8, []uint8)
func uint16MaxAvx2Asm([]uint16, []uint16)
func uint16MaxAvx512Asm([]uint16, []uint16)
func uint32MaxAvx2Asm([]uint32, []uint32)
func uint32MaxAvx512Asm([]uint32, []uint32)
func uint64MaxAvx512Asm([]uint64, []uint64)
func float32MaxAvx2Asm([]float32, []float32)
func float32MaxAvx512Asm([]float32, []float32)
func float64MaxAvx2Asm([]float64, []float64)
func float64MaxAvx512Asm([]float64, []float64)

func init() {
	if cpu.X86.HasAVX512 {
		Int8Max = int8MaxAvx512
		Int16Max = int16MaxAvx512
		Int32Max = int32MaxAvx512
		Int64Max = int64MaxAvx512
		Uint8Max = uint8MaxAvx512
		Uint16Max = uint16MaxAvx512
		Uint32Max = uint32MaxAvx512
		Uint64Max = uint64MaxAvx512
		Float32Max = float32MaxAvx512
		Float64Max = float64MaxAvx512
	} else if cpu.X86.HasAVX2 {
		Int8Max = int8MaxAvx2
		Int16Max = int16MaxAvx2
		Int32Max = int32MaxAvx2
		Int64Max = int64Max
		Uint8Max = uint8MaxAvx2
		Uint16Max = uint16MaxAvx2
		Uint32Max = uint32MaxAvx2
		Uint64Max = uint64Max
		Float32Max = float32MaxAvx2
		Float64Max = float64MaxAvx2
	} else {
		Int8Max = int8Max
		Int16Max = int16Max
		Int32Max = int32Max
		Int64Max = int64Max
		Uint8Max = uint8Max
		Uint16Max = uint16Max
		Uint32Max = uint32Max
		Uint64Max = uint64Max
		Float32Max = float32Max
		Float64Max = float64Max
	}

	BoolMax = boolMax
	StrMax = strMax

	BoolMaxSels = boolMaxSels
	Int8MaxSels = int8MaxSels
	Int16MaxSels = int16MaxSels
	Int32MaxSels = int32MaxSels
	Int64MaxSels = int64MaxSels
	Uint8MaxSels = uint8MaxSels
	Uint16MaxSels = uint16MaxSels
	Uint32MaxSels = uint32MaxSels
	Uint64MaxSels = uint64MaxSels
	Float32MaxSels = float32MaxSels
	Float64MaxSels = float64MaxSels
	StrMaxSels = strMaxSels
}

func int8MaxAvx2(xs []int8) int8 {
	n := len(xs) / 16
	var rs [16]int8
	int8MaxAvx2Asm(xs[:n*16], rs[:])
	res := rs[0]
	for i := 1; i < 16; i++ {
		if rs[i] > res {
			res = rs[i]
		}
	}
	for i, j := n*16, len(xs); i < j; i++ {
		if xs[i] > res {
			res = xs[i]
		}
	}
	return res
}

func int8MaxAvx512(xs []int8) int8 {
	n := len(xs) / 16
	var rs [16]int8
	int8MaxAvx512Asm(xs[:n*16], rs[:])
	res := rs[0]
	for i := 1; i < 16; i++ {
		if rs[i] > res {
			res = rs[i]
		}
	}
	for i, j := n*16, len(xs); i < j; i++ {
		if xs[i] > res {
			res = xs[i]
		}
	}
	return res
}

func int16MaxAvx2(xs []int16) int16 {
	n := len(xs) / 8
	var rs [8]int16
	int16MaxAvx2Asm(xs[:n*8], rs[:])
	res := rs[0]
	for i := 1; i < 8; i++ {
		if rs[i] > res {
			res = rs[i]
		}
	}
	for i, j := n*8, len(xs); i < j; i++ {
		if xs[i] > res {
			res = xs[i]
		}
	}
	return res
}

func int16MaxAvx512(xs []int16) int16 {
	n := len(xs) / 8
	var rs [8]int16
	int16MaxAvx512Asm(xs[:n*8], rs[:])
	res := rs[0]
	for i := 1; i < 8; i++ {
		if rs[i] > res {
			res = rs[i]
		}
	}
	for i, j := n*8, len(xs); i < j; i++ {
		if xs[i] > res {
			res = xs[i]
		}
	}
	return res
}

func int32MaxAvx2(xs []int32) int32 {
	n := len(xs) / 4
	var rs [4]int32
	int32MaxAvx2Asm(xs[:n*4], rs[:])
	res := rs[0]
	for i := 1; i < 4; i++ {
		if rs[i] > res {
			res = rs[i]
		}
	}
	for i, j := n*4, len(xs); i < j; i++ {
		if xs[i] > res {
			res = xs[i]
		}
	}
	return res
}

func int32MaxAvx512(xs []int32) int32 {
	n := len(xs) / 4
	var rs [4]int32
	int32MaxAvx512Asm(xs[:n*4], rs[:])
	res := rs[0]
	for i := 1; i < 4; i++ {
		if rs[i] > res {
			res = rs[i]
		}
	}
	for i, j := n*4, len(xs); i < j; i++ {
		if xs[i] > res {
			res = xs[i]
		}
	}
	return res
}

func int64MaxAvx512(xs []int64) int64 {
	n := len(xs) / 2
	var rs [2]int64
	int64MaxAvx512Asm(xs[:n*2], rs[:])
	res := rs[0]
	for i := 1; i < 2; i++ {
		if rs[i] > res {
			res = rs[i]
		}
	}
	for i, j := n*2, len(xs); i < j; i++ {
		if xs[i] > res {
			res = xs[i]
		}
	}
	return res
}

func uint8MaxAvx2(xs []uint8) uint8 {
	n := len(xs) / 16
	var rs [16]uint8
	uint8MaxAvx2Asm(xs[:n*16], rs[:])
	res := rs[0]
	for i := 1; i < 16; i++ {
		if rs[i] > res {
			res = rs[i]
		}
	}
	for i, j := n*16, len(xs); i < j; i++ {
		if xs[i] > res {
			res = xs[i]
		}
	}
	return res
}

func uint8MaxAvx512(xs []uint8) uint8 {
	n := len(xs) / 16
	var rs [16]uint8
	uint8MaxAvx512Asm(xs[:n*16], rs[:])
	res := rs[0]
	for i := 1; i < 16; i++ {
		if rs[i] > res {
			res = rs[i]
		}
	}
	for i, j := n*16, len(xs); i < j; i++ {
		if xs[i] > res {
			res = xs[i]
		}
	}
	return res
}

func uint16MaxAvx2(xs []uint16) uint16 {
	n := len(xs) / 8
	var rs [8]uint16
	uint16MaxAvx2Asm(xs[:n*8], rs[:])
	res := rs[0]
	for i := 1; i < 8; i++ {
		if rs[i] > res {
			res = rs[i]
		}
	}
	for i, j := n*8, len(xs); i < j; i++ {
		if xs[i] > res {
			res = xs[i]
		}
	}
	return res
}

func uint16MaxAvx512(xs []uint16) uint16 {
	n := len(xs) / 8
	var rs [8]uint16
	uint16MaxAvx512Asm(xs[:n*8], rs[:])
	res := rs[0]
	for i := 1; i < 8; i++ {
		if rs[i] > res {
			res = rs[i]
		}
	}
	for i, j := n*8, len(xs); i < j; i++ {
		if xs[i] > res {
			res = xs[i]
		}
	}
	return res
}

func uint32MaxAvx2(xs []uint32) uint32 {
	n := len(xs) / 4
	var rs [4]uint32
	uint32MaxAvx2Asm(xs[:n*4], rs[:])
	res := rs[0]
	for i := 1; i < 4; i++ {
		if rs[i] > res {
			res = rs[i]
		}
	}
	for i, j := n*4, len(xs); i < j; i++ {
		if xs[i] > res {
			res = xs[i]
		}
	}
	return res
}

func uint32MaxAvx512(xs []uint32) uint32 {
	n := len(xs) / 4
	var rs [4]uint32
	uint32MaxAvx512Asm(xs[:n*4], rs[:])
	res := rs[0]
	for i := 1; i < 4; i++ {
		if rs[i] > res {
			res = rs[i]
		}
	}
	for i, j := n*4, len(xs); i < j; i++ {
		if xs[i] > res {
			res = xs[i]
		}
	}
	return res
}

func uint64MaxAvx512(xs []uint64) uint64 {
	n := len(xs) / 2
	var rs [2]uint64
	uint64MaxAvx512Asm(xs[:n*2], rs[:])
	res := rs[0]
	for i := 1; i < 2; i++ {
		if rs[i] > res {
			res = rs[i]
		}
	}
	for i, j := n*2, len(xs); i < j; i++ {
		if xs[i] > res {
			res = xs[i]
		}
	}
	return res
}

func float32MaxAvx2(xs []float32) float32 {
	n := len(xs) / 4
	var rs [4]float32
	float32MaxAvx2Asm(xs[:n*4], rs[:])
	res := rs[0]
	for i := 1; i < 4; i++ {
		if rs[i] > res {
			res = rs[i]
		}
	}
	for i, j := n*4, len(xs); i < j; i++ {
		if xs[i] > res {
			res = xs[i]
		}
	}
	return res
}

func float32MaxAvx512(xs []float32) float32 {
	n := len(xs) / 4
	var rs [4]float32
	float32MaxAvx512Asm(xs[:n*4], rs[:])
	res := rs[0]
	for i := 1; i < 4; i++ {
		if rs[i] > res {
			res = rs[i]
		}
	}
	for i, j := n*4, len(xs); i < j; i++ {
		if xs[i] > res {
			res = xs[i]
		}
	}
	return res
}

func float64MaxAvx2(xs []float64) float64 {
	n := len(xs) / 2
	var rs [2]float64
	float64MaxAvx2Asm(xs[:n*2], rs[:])
	res := rs[0]
	for i := 1; i < 2; i++ {
		if rs[i] > res {
			res = rs[i]
		}
	}
	for i, j := n*2, len(xs); i < j; i++ {
		if xs[i] > res {
			res = xs[i]
		}
	}
	return res
}

func float64MaxAvx512(xs []float64) float64 {
	n := len(xs) / 2
	var rs [2]float64
	float64MaxAvx512Asm(xs[:n*2], rs[:])
	res := rs[0]
	for i := 1; i < 2; i++ {
		if rs[i] > res {
			res = rs[i]
		}
	}
	for i, j := n*2, len(xs); i < j; i++ {
		if xs[i] > res {
			res = xs[i]
		}
	}
	return res
}
