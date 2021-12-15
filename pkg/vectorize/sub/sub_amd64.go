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

package sub

import "golang.org/x/sys/cpu"

func int8SubAvx2Asm(x []int8, y []int8, r []int8)
func int8SubScalarAvx2Asm(x int8, y []int8, r []int8)
func int8SubByScalarAvx2Asm(x int8, y []int8, r []int8)
func int16SubAvx2Asm(x []int16, y []int16, r []int16)
func int16SubScalarAvx2Asm(x int16, y []int16, r []int16)
func int16SubByScalarAvx2Asm(x int16, y []int16, r []int16)
func int32SubAvx2Asm(x []int32, y []int32, r []int32)
func int32SubScalarAvx2Asm(x int32, y []int32, r []int32)
func int32SubByScalarAvx2Asm(x int32, y []int32, r []int32)
func int64SubAvx2Asm(x []int64, y []int64, r []int64)
func int64SubScalarAvx2Asm(x int64, y []int64, r []int64)
func int64SubByScalarAvx2Asm(x int64, y []int64, r []int64)
func uint8SubAvx2Asm(x []uint8, y []uint8, r []uint8)
func uint8SubScalarAvx2Asm(x uint8, y []uint8, r []uint8)
func uint8SubByScalarAvx2Asm(x uint8, y []uint8, r []uint8)
func uint16SubAvx2Asm(x []uint16, y []uint16, r []uint16)
func uint16SubScalarAvx2Asm(x uint16, y []uint16, r []uint16)
func uint16SubByScalarAvx2Asm(x uint16, y []uint16, r []uint16)
func uint32SubAvx2Asm(x []uint32, y []uint32, r []uint32)
func uint32SubScalarAvx2Asm(x uint32, y []uint32, r []uint32)
func uint32SubByScalarAvx2Asm(x uint32, y []uint32, r []uint32)
func uint64SubAvx2Asm(x []uint64, y []uint64, r []uint64)
func uint64SubScalarAvx2Asm(x uint64, y []uint64, r []uint64)
func uint64SubByScalarAvx2Asm(x uint64, y []uint64, r []uint64)
func float32SubAvx2Asm(x []float32, y []float32, r []float32)
func float32SubScalarAvx2Asm(x float32, y []float32, r []float32)
func float32SubByScalarAvx2Asm(x float32, y []float32, r []float32)
func float64SubAvx2Asm(x []float64, y []float64, r []float64)
func float64SubScalarAvx2Asm(x float64, y []float64, r []float64)
func float64SubByScalarAvx2Asm(x float64, y []float64, r []float64)

func int8SubAvx512Asm(x []int8, y []int8, r []int8)
func int8SubScalarAvx512Asm(x int8, y []int8, r []int8)
func int8SubByScalarAvx512Asm(x int8, y []int8, r []int8)
func int16SubAvx512Asm(x []int16, y []int16, r []int16)
func int16SubScalarAvx512Asm(x int16, y []int16, r []int16)
func int16SubByScalarAvx512Asm(x int16, y []int16, r []int16)
func int32SubAvx512Asm(x []int32, y []int32, r []int32)
func int32SubScalarAvx512Asm(x int32, y []int32, r []int32)
func int32SubByScalarAvx512Asm(x int32, y []int32, r []int32)
func int64SubAvx512Asm(x []int64, y []int64, r []int64)
func int64SubScalarAvx512Asm(x int64, y []int64, r []int64)
func int64SubByScalarAvx512Asm(x int64, y []int64, r []int64)
func uint8SubAvx512Asm(x []uint8, y []uint8, r []uint8)
func uint8SubScalarAvx512Asm(x uint8, y []uint8, r []uint8)
func uint8SubByScalarAvx512Asm(x uint8, y []uint8, r []uint8)
func uint16SubAvx512Asm(x []uint16, y []uint16, r []uint16)
func uint16SubScalarAvx512Asm(x uint16, y []uint16, r []uint16)
func uint16SubByScalarAvx512Asm(x uint16, y []uint16, r []uint16)
func uint32SubAvx512Asm(x []uint32, y []uint32, r []uint32)
func uint32SubScalarAvx512Asm(x uint32, y []uint32, r []uint32)
func uint32SubByScalarAvx512Asm(x uint32, y []uint32, r []uint32)
func uint64SubAvx512Asm(x []uint64, y []uint64, r []uint64)
func uint64SubScalarAvx512Asm(x uint64, y []uint64, r []uint64)
func uint64SubByScalarAvx512Asm(x uint64, y []uint64, r []uint64)
func float32SubAvx512Asm(x []float32, y []float32, r []float32)
func float32SubScalarAvx512Asm(x float32, y []float32, r []float32)
func float32SubByScalarAvx512Asm(x float32, y []float32, r []float32)
func float64SubAvx512Asm(x []float64, y []float64, r []float64)
func float64SubScalarAvx512Asm(x float64, y []float64, r []float64)
func float64SubByScalarAvx512Asm(x float64, y []float64, r []float64)

func init() {
	if cpu.X86.HasAVX512 {
		Int8Sub = int8SubAvx512
		Int8SubScalar = int8SubScalarAvx512
		Int8SubByScalar = int8SubByScalarAvx512
		Int16Sub = int16SubAvx512
		Int16SubScalar = int16SubScalarAvx512
		Int16SubByScalar = int16SubByScalarAvx512
		Int32Sub = int32SubAvx512
		Int32SubScalar = int32SubScalarAvx512
		Int32SubByScalar = int32SubByScalarAvx512
		Int64Sub = int64SubAvx512
		Int64SubScalar = int64SubScalarAvx512
		Int64SubByScalar = int64SubByScalarAvx512
		Uint8Sub = uint8SubAvx512
		Uint8SubScalar = uint8SubScalarAvx512
		Uint8SubByScalar = uint8SubByScalarAvx512
		Uint16Sub = uint16SubAvx512
		Uint16SubScalar = uint16SubScalarAvx512
		Uint16SubByScalar = uint16SubByScalarAvx512
		Uint32Sub = uint32SubAvx512
		Uint32SubScalar = uint32SubScalarAvx512
		Uint32SubByScalar = uint32SubByScalarAvx512
		Uint64Sub = uint64SubAvx512
		Uint64SubScalar = uint64SubScalarAvx512
		Uint64SubByScalar = uint64SubByScalarAvx512
		Float32Sub = float32SubAvx512
		Float32SubScalar = float32SubScalarAvx512
		Float32SubByScalar = float32SubByScalarAvx512
		Float64Sub = float64SubAvx512
		Float64SubScalar = float64SubScalarAvx512
		Float64SubByScalar = float64SubByScalarAvx512
	} else if cpu.X86.HasAVX2 {
		Int8Sub = int8SubAvx2
		Int8SubScalar = int8SubScalarAvx2
		Int8SubByScalar = int8SubByScalarAvx2
		Int16Sub = int16SubAvx2
		Int16SubScalar = int16SubScalarAvx2
		Int16SubByScalar = int16SubByScalarAvx2
		Int32Sub = int32SubAvx2
		Int32SubScalar = int32SubScalarAvx2
		Int32SubByScalar = int32SubByScalarAvx2
		Int64Sub = int64SubAvx2
		Int64SubScalar = int64SubScalarAvx2
		Int64SubByScalar = int64SubByScalarAvx2
		Uint8Sub = uint8SubAvx2
		Uint8SubScalar = uint8SubScalarAvx2
		Uint8SubByScalar = uint8SubByScalarAvx2
		Uint16Sub = uint16SubAvx2
		Uint16SubScalar = uint16SubScalarAvx2
		Uint16SubByScalar = uint16SubByScalarAvx2
		Uint32Sub = uint32SubAvx2
		Uint32SubScalar = uint32SubScalarAvx2
		Uint32SubByScalar = uint32SubByScalarAvx2
		Uint64Sub = uint64SubAvx2
		Uint64SubScalar = uint64SubScalarAvx2
		Uint64SubByScalar = uint64SubByScalarAvx2
		Float32Sub = float32SubAvx2
		Float32SubScalar = float32SubScalarAvx2
		Float32SubByScalar = float32SubByScalarAvx2
		Float64Sub = float64SubAvx2
		Float64SubScalar = float64SubScalarAvx2
		Float64SubByScalar = float64SubByScalarAvx2
	} else {
		Int8Sub = int8Sub
		Int8SubScalar = int8SubScalar
		Int8SubByScalar = int8SubByScalar
		Int16Sub = int16Sub
		Int16SubScalar = int16SubScalar
		Int16SubByScalar = int16SubByScalar
		Int32Sub = int32Sub
		Int32SubScalar = int32SubScalar
		Int32SubByScalar = int32SubByScalar
		Int64Sub = int64Sub
		Int64SubScalar = int64SubScalar
		Int64SubByScalar = int64SubByScalar
		Uint8Sub = uint8Sub
		Uint8SubScalar = uint8SubScalar
		Uint8SubByScalar = uint8SubByScalar
		Uint16Sub = uint16Sub
		Uint16SubScalar = uint16SubScalar
		Uint16SubByScalar = uint16SubByScalar
		Uint32Sub = uint32Sub
		Uint32SubScalar = uint32SubScalar
		Uint32SubByScalar = uint32SubByScalar
		Uint64Sub = uint64Sub
		Uint64SubScalar = uint64SubScalar
		Uint64SubByScalar = uint64SubByScalar
		Float32Sub = float32Sub
		Float32SubScalar = float32SubScalar
		Float32SubByScalar = float32SubByScalar
		Float64Sub = float64Sub
		Float64SubScalar = float64SubScalar
		Float64SubByScalar = float64SubByScalar
	}

	Int8SubSels = int8SubSels
	Int8SubScalarSels = int8SubScalarSels
	Int8SubByScalarSels = int8SubByScalarSels
	Int16SubSels = int16SubSels
	Int16SubScalarSels = int16SubScalarSels
	Int16SubByScalarSels = int16SubByScalarSels
	Int32SubSels = int32SubSels
	Int32SubScalarSels = int32SubScalarSels
	Int32SubByScalarSels = int32SubByScalarSels
	Int64SubSels = int64SubSels
	Int64SubScalarSels = int64SubScalarSels
	Int64SubByScalarSels = int64SubByScalarSels
	Uint8SubSels = uint8SubSels
	Uint8SubScalarSels = uint8SubScalarSels
	Uint8SubByScalarSels = uint8SubByScalarSels
	Uint16SubSels = uint16SubSels
	Uint16SubScalarSels = uint16SubScalarSels
	Uint16SubByScalarSels = uint16SubByScalarSels
	Uint32SubSels = uint32SubSels
	Uint32SubScalarSels = uint32SubScalarSels
	Uint32SubByScalarSels = uint32SubByScalarSels
	Uint64SubSels = uint64SubSels
	Uint64SubScalarSels = uint64SubScalarSels
	Uint64SubByScalarSels = uint64SubByScalarSels
	Float32SubSels = float32SubSels
	Float32SubScalarSels = float32SubScalarSels
	Float32SubByScalarSels = float32SubByScalarSels
	Float64SubSels = float64SubSels
	Float64SubScalarSels = float64SubScalarSels
	Float64SubByScalarSels = float64SubByScalarSels

	Int32Int64Sub = int32Int64Sub
	Int32Int64SubSels = int32Int64SubSels
	Int16Int64Sub = int16Int64Sub
	Int16Int64SubSels = int16Int64SubSels
	Int8Int64Sub = int8Int64Sub
	Int8Int64SubSels = int8Int64SubSels
	Int16Int32Sub = int16Int32Sub
	Int16Int32SubSels = int16Int32SubSels
	Int8Int32Sub = int8Int32Sub
	Int8Int32SubSels = int8Int32SubSels
	Int8Int16Sub = int8Int16Sub
	Int8Int16SubSels = int8Int16SubSels
	Float32Float64Sub = float32Float64Sub
	Float32Float64SubSels = float32Float64SubSels
	Uint32Uint64Sub = uint32Uint64Sub
	Uint32Uint64SubSels = uint32Uint64SubSels
	Uint16Uint64Sub = uint16Uint64Sub
	Uint16Uint64SubSels = uint16Uint64SubSels
	Uint8Uint64Sub = uint8Uint64Sub
	Uint8Uint64SubSels = uint8Uint64SubSels
	Uint16Uint32Sub = uint16Uint32Sub
	Uint16Uint32SubSels = uint16Uint32SubSels
	Uint8Uint32Sub = uint8Uint32Sub
	Uint8Uint32SubSels = uint8Uint32SubSels
	Uint8Uint16Sub = uint8Uint16Sub
	Uint8Uint16SubSels = uint8Uint16SubSels
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