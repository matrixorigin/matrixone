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

package min

import (
	"golang.org/x/sys/cpu"
)

func int8MinAvx2Asm(x []int8, r []int8)
func int8MinAvx512Asm(x []int8, r []int8)
func int16MinAvx2Asm(x []int16, r []int16)
func int16MinAvx512Asm(x []int16, r []int16)
func int32MinAvx2Asm(x []int32, r []int32)
func int32MinAvx512Asm(x []int32, r []int32)
func int64MinAvx512Asm(x []int64, r []int64)
func uint8MinAvx2Asm(x []uint8, r []uint8)
func uint8MinAvx512Asm(x []uint8, r []uint8)
func uint16MinAvx2Asm(x []uint16, r []uint16)
func uint16MinAvx512Asm(x []uint16, r []uint16)
func uint32MinAvx2Asm(x []uint32, r []uint32)
func uint32MinAvx512Asm(x []uint32, r []uint32)
func uint64MinAvx512Asm(x []uint64, r []uint64)
func float32MinAvx2Asm(x []float32, r []float32)
func float32MinAvx512Asm(x []float32, r []float32)
func float64MinAvx2Asm(x []float64, r []float64)
func float64MinAvx512Asm(x []float64, r []float64)

func init() {
	if cpu.X86.HasAVX512 {
		Int8Min = int8MinAvx512
		Int16Min = int16MinAvx512
		Int32Min = int32MinAvx512
		Int64Min = int64MinAvx512
		Uint8Min = uint8MinAvx512
		Uint16Min = uint16MinAvx512
		Uint32Min = uint32MinAvx512
		Uint64Min = uint64MinAvx512
		Float32Min = float32MinAvx512
		Float64Min = float64MinAvx512
	} else if cpu.X86.HasAVX2 {
		Int8Min = int8MinAvx2
		Int16Min = int16MinAvx2
		Int32Min = int32MinAvx2
		Int64Min = int64Min
		Uint8Min = uint8MinAvx2
		Uint16Min = uint16MinAvx2
		Uint32Min = uint32MinAvx2
		Uint64Min = uint64Min
		Float32Min = float32MinAvx2
		Float64Min = float64MinAvx2
	} else {
		Int8Min = int8Min
		Int16Min = int16Min
		Int32Min = int32Min
		Int64Min = int64Min
		Uint8Min = uint8Min
		Uint16Min = uint16Min
		Uint32Min = uint32Min
		Uint64Min = uint64Min
		Float32Min = float32Min
		Float64Min = float64Min
	}

	BoolMin = boolMin
	StrMin = strMin

	BoolMinSels = boolMinSels
	Int8MinSels = int8MinSels
	Int16MinSels = int16MinSels
	Int32MinSels = int32MinSels
	Int64MinSels = int64MinSels
	Uint8MinSels = uint8MinSels
	Uint16MinSels = uint16MinSels
	Uint32MinSels = uint32MinSels
	Uint64MinSels = uint64MinSels
	Float32MinSels = float32MinSels
	Float64MinSels = float64MinSels
	StrMinSels = strMinSels
}

func int8MinAvx2(xs []int8) int8 {
	n := len(xs) / 16
	var rs [16]int8
	int8MinAvx2Asm(xs[:n*16], rs[:])
	res := rs[0]
	for i := 1; i < 16; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*16, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func int8MinAvx512(xs []int8) int8 {
	n := len(xs) / 16
	var rs [16]int8
	int8MinAvx512Asm(xs[:n*16], rs[:])
	res := rs[0]
	for i := 1; i < 16; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*16, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func int16MinAvx2(xs []int16) int16 {
	n := len(xs) / 8
	var rs [8]int16
	int16MinAvx2Asm(xs[:n*8], rs[:])
	res := rs[0]
	for i := 1; i < 8; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*8, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func int16MinAvx512(xs []int16) int16 {
	n := len(xs) / 8
	var rs [8]int16
	int16MinAvx512Asm(xs[:n*8], rs[:])
	res := rs[0]
	for i := 1; i < 8; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*8, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func int32MinAvx2(xs []int32) int32 {
	n := len(xs) / 4
	var rs [4]int32
	int32MinAvx2Asm(xs[:n*4], rs[:])
	res := rs[0]
	for i := 1; i < 4; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*4, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func int32MinAvx512(xs []int32) int32 {
	n := len(xs) / 4
	var rs [4]int32
	int32MinAvx512Asm(xs[:n*4], rs[:])
	res := rs[0]
	for i := 1; i < 4; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*4, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func int64MinAvx512(xs []int64) int64 {
	n := len(xs) / 2
	var rs [2]int64
	int64MinAvx512Asm(xs[:n*2], rs[:])
	res := rs[0]
	for i := 1; i < 2; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*2, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func uint8MinAvx2(xs []uint8) uint8 {
	n := len(xs) / 16
	var rs [16]uint8
	uint8MinAvx2Asm(xs[:n*16], rs[:])
	res := rs[0]
	for i := 1; i < 16; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*16, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func uint8MinAvx512(xs []uint8) uint8 {
	n := len(xs) / 16
	var rs [16]uint8
	uint8MinAvx512Asm(xs[:n*16], rs[:])
	res := rs[0]
	for i := 1; i < 16; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*16, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func uint16MinAvx2(xs []uint16) uint16 {
	n := len(xs) / 8
	var rs [8]uint16
	uint16MinAvx2Asm(xs[:n*8], rs[:])
	res := rs[0]
	for i := 1; i < 8; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*8, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func uint16MinAvx512(xs []uint16) uint16 {
	n := len(xs) / 8
	var rs [8]uint16
	uint16MinAvx512Asm(xs[:n*8], rs[:])
	res := rs[0]
	for i := 1; i < 8; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*8, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func uint32MinAvx2(xs []uint32) uint32 {
	n := len(xs) / 4
	var rs [4]uint32
	uint32MinAvx2Asm(xs[:n*4], rs[:])
	res := rs[0]
	for i := 1; i < 4; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*4, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func uint32MinAvx512(xs []uint32) uint32 {
	n := len(xs) / 4
	var rs [4]uint32
	uint32MinAvx512Asm(xs[:n*4], rs[:])
	res := rs[0]
	for i := 1; i < 4; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*4, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func uint64MinAvx512(xs []uint64) uint64 {
	n := len(xs) / 2
	var rs [2]uint64
	uint64MinAvx512Asm(xs[:n*2], rs[:])
	res := rs[0]
	for i := 1; i < 2; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*2, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func float32MinAvx2(xs []float32) float32 {
	n := len(xs) / 4
	var rs [4]float32
	float32MinAvx2Asm(xs[:n*4], rs[:])
	res := rs[0]
	for i := 1; i < 4; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*4, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func float32MinAvx512(xs []float32) float32 {
	n := len(xs) / 4
	var rs [4]float32
	float32MinAvx512Asm(xs[:n*4], rs[:])
	res := rs[0]
	for i := 1; i < 4; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*4, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func float64MinAvx2(xs []float64) float64 {
	n := len(xs) / 2
	var rs [2]float64
	float64MinAvx2Asm(xs[:n*2], rs[:])
	res := rs[0]
	for i := 1; i < 2; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*2, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func float64MinAvx512(xs []float64) float64 {
	n := len(xs) / 2
	var rs [2]float64
	float64MinAvx512Asm(xs[:n*2], rs[:])
	res := rs[0]
	for i := 1; i < 2; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*2, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}
