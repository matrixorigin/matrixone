// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build amd64
// +build amd64

package max

import (
	"golang.org/x/sys/cpu"
)

func int8MaxAvx2Asm(x []int8, r []int8)
func int8MaxAvx512Asm(x []int8, r []int8)
func int16MaxAvx2Asm(x []int16, r []int16)
func int16MaxAvx512Asm(x []int16, r []int16)
func int32MaxAvx2Asm(x []int32, r []int32)
func int32MaxAvx512Asm(x []int32, r []int32)
func int64MaxAvx512Asm(x []int64, r []int64)
func uint8MaxAvx2Asm(x []uint8, r []uint8)
func uint8MaxAvx512Asm(x []uint8, r []uint8)
func uint16MaxAvx2Asm(x []uint16, r []uint16)
func uint16MaxAvx512Asm(x []uint16, r []uint16)
func uint32MaxAvx2Asm(x []uint32, r []uint32)
func uint32MaxAvx512Asm(x []uint32, r []uint32)
func uint64MaxAvx512Asm(x []uint64, r []uint64)
func float32MaxAvx2Asm(x []float32, r []float32)
func float32MaxAvx512Asm(x []float32, r []float32)
func float64MaxAvx2Asm(x []float64, r []float64)
func float64MaxAvx512Asm(x []float64, r []float64)

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
		Int64Max = maxGeneric[int64]
		Uint8Max = uint8MaxAvx2
		Uint16Max = uint16MaxAvx2
		Uint32Max = uint32MaxAvx2
		Uint64Max = maxGeneric[uint64]
		Float32Max = float32MaxAvx2
		Float64Max = float64MaxAvx2
	} else {
		Int8Max = maxGeneric[int8]
		Int16Max = maxGeneric[int16]
		Int32Max = maxGeneric[int32]
		Int64Max = maxGeneric[int64]
		Uint8Max = maxGeneric[uint8]
		Uint16Max = maxGeneric[uint16]
		Uint32Max = maxGeneric[uint32]
		Uint64Max = maxGeneric[uint64]
		Float32Max = maxGeneric[float32]
		Float64Max = maxGeneric[float64]
	}

	BoolMax = boolMax
	StrMax = strMax

	BoolMaxSels = boolMaxSels
	Int8MaxSels = maxSelsGeneric[int8]
	Int16MaxSels = maxSelsGeneric[int16]
	Int32MaxSels = maxSelsGeneric[int32]
	Int64MaxSels = maxSelsGeneric[int64]
	Uint8MaxSels = maxSelsGeneric[uint8]
	Uint16MaxSels = maxSelsGeneric[uint16]
	Uint32MaxSels = maxSelsGeneric[uint32]
	Uint64MaxSels = maxSelsGeneric[uint64]
	Float32MaxSels = maxSelsGeneric[float32]
	Float64MaxSels = maxSelsGeneric[float64]
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
