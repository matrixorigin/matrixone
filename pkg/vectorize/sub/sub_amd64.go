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
		Int8Sub = subGeneric[int8]
		Int8SubScalar = subScalarGeneric[int8]
		Int8SubByScalar = subByScalarGeneric[int8]
		Int16Sub = subGeneric[int16]
		Int16SubScalar = subScalarGeneric[int16]
		Int16SubByScalar = subByScalarGeneric[int16]
		Int32Sub = subGeneric[int32]
		Int32SubScalar = subScalarGeneric[int32]
		Int32SubByScalar = subByScalarGeneric[int32]
		Int64Sub = subGeneric[int64]
		Int64SubScalar = subScalarGeneric[int64]
		Int64SubByScalar = subByScalarGeneric[int64]
		Uint8Sub = subGeneric[uint8]
		Uint8SubScalar = subScalarGeneric[uint8]
		Uint8SubByScalar = subByScalarGeneric[uint8]
		Uint16Sub = subGeneric[uint16]
		Uint16SubScalar = subScalarGeneric[uint16]
		Uint16SubByScalar = subByScalarGeneric[uint16]
		Uint32Sub = subGeneric[uint32]
		Uint32SubScalar = subScalarGeneric[uint32]
		Uint32SubByScalar = subByScalarGeneric[uint32]
		Uint64Sub = subGeneric[uint64]
		Uint64SubScalar = subScalarGeneric[uint64]
		Uint64SubByScalar = subByScalarGeneric[uint64]
		Float32Sub = subGeneric[float32]
		Float32SubScalar = subScalarGeneric[float32]
		Float32SubByScalar = subByScalarGeneric[float32]
		Float64Sub = subGeneric[float64]
		Float64SubScalar = subScalarGeneric[float64]
		Float64SubByScalar = subByScalarGeneric[float64]
	}

	Int8SubSels = subSelsGeneric[int8]
	Int8SubScalarSels = subScalarSelsGeneric[int8]
	Int8SubByScalarSels = subByScalarSelsGeneric[int8]
	Int16SubSels = subSelsGeneric[int16]
	Int16SubScalarSels = subScalarSelsGeneric[int16]
	Int16SubByScalarSels = subByScalarSelsGeneric[int16]
	Int32SubSels = subSelsGeneric[int32]
	Int32SubScalarSels = subScalarSelsGeneric[int32]
	Int32SubByScalarSels = subByScalarSelsGeneric[int32]
	Int64SubSels = subSelsGeneric[int64]
	Int64SubScalarSels = subScalarSelsGeneric[int64]
	Int64SubByScalarSels = subByScalarSelsGeneric[int64]
	Uint8SubSels = subSelsGeneric[uint8]
	Uint8SubScalarSels = subScalarSelsGeneric[uint8]
	Uint8SubByScalarSels = subByScalarSelsGeneric[uint8]
	Uint16SubSels = subSelsGeneric[uint16]
	Uint16SubScalarSels = subScalarSelsGeneric[uint16]
	Uint16SubByScalarSels = subByScalarSelsGeneric[uint16]
	Uint32SubSels = subSelsGeneric[uint32]
	Uint32SubScalarSels = subScalarSelsGeneric[uint32]
	Uint32SubByScalarSels = subByScalarSelsGeneric[uint32]
	Uint64SubSels = subSelsGeneric[uint64]
	Uint64SubScalarSels = subScalarSelsGeneric[uint64]
	Uint64SubByScalarSels = subByScalarSelsGeneric[uint64]
	Float32SubSels = subSelsGeneric[float32]
	Float32SubScalarSels = subScalarSelsGeneric[float32]
	Float32SubByScalarSels = subByScalarSelsGeneric[float32]
	Float64SubSels = subSelsGeneric[float64]
	Float64SubScalarSels = subScalarSelsGeneric[float64]
	Float64SubByScalarSels = subByScalarSelsGeneric[float64]
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
