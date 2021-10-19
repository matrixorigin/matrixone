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
		Int8Add = addGeneric[int8]
		Int8AddScalar = addScalarGeneric[int8]
		Int16Add = addGeneric[int16]
		Int16AddScalar = addScalarGeneric[int16]
		Int32Add = addGeneric[int32]
		Int32AddScalar = addScalarGeneric[int32]
		Int64Add = addGeneric[int64]
		Int64AddScalar = addScalarGeneric[int64]
		Uint8Add = addGeneric[uint8]
		Uint8AddScalar = addScalarGeneric[uint8]
		Uint16Add = addGeneric[uint16]
		Uint16AddScalar = addScalarGeneric[uint16]
		Uint32Add = addGeneric[uint32]
		Uint32AddScalar = addScalarGeneric[uint32]
		Uint64Add = addGeneric[uint64]
		Uint64AddScalar = addScalarGeneric[uint64]
		Float32Add = addGeneric[float32]
		Float32AddScalar = addScalarGeneric[float32]
		Float64Add = addGeneric[float64]
		Float64AddScalar = addScalarGeneric[float64]

		// dt
		Int8Int16Add = addDifferentGeneric[int16, int8]
		Int8Int32Add = addDifferentGeneric[int32, int8]
		Int8Int64Add = addDifferentGeneric[int64, int8]

		Int16Int32Add = addDifferentGeneric[int32, int16]
		Int16Int64Add = addDifferentGeneric[int64, int16]
		Int32Int64Add = addDifferentGeneric[int64, int32]

		Float32Float64Add = addDifferentGeneric[float64, float32]

		Uint8Uint16Add = addDifferentGeneric[uint16, uint8]
		Uint8Uint32Add = addDifferentGeneric[uint32, uint8]
		Uint8Uint64Add = addDifferentGeneric[uint64, uint8]
		Uint16Uint32Add = addDifferentGeneric[uint32, uint16]
		Uint16Uint64Add = addDifferentGeneric[uint64, uint16]
		Uint32Uint64Add = addDifferentGeneric[uint64, uint32]
	}

	Int8AddSels = addSelsGeneric[int8]
	Int8AddScalarSels = addScalarSelsGeneric[int8]
	Int16AddSels = addSelsGeneric[int16]
	Int16AddScalarSels = addScalarSelsGeneric[int16]
	Int32AddSels = addSelsGeneric[int32]
	Int32AddScalarSels = addScalarSelsGeneric[int32]
	Int64AddSels = addSelsGeneric[int64]
	Int64AddScalarSels = addScalarSelsGeneric[int64]
	Uint8AddSels = addSelsGeneric[uint8]
	Uint8AddScalarSels = addScalarSelsGeneric[uint8]
	Uint16AddSels = addSelsGeneric[uint16]
	Uint16AddScalarSels = addScalarSelsGeneric[uint16]
	Uint32AddSels = addSelsGeneric[uint32]
	Uint32AddScalarSels = addScalarSelsGeneric[uint32]
	Uint64AddSels = addSelsGeneric[uint64]
	Uint64AddScalarSels = addScalarSelsGeneric[uint64]
	Float32AddSels = addSelsGeneric[float32]
	Float32AddScalarSels = addScalarSelsGeneric[float32]
	Float64AddSels = addSelsGeneric[float64]
	Float64AddScalarSels = addScalarSelsGeneric[float64]

	//dt
	Int8Int16AddSels = addDifferentSelsGeneric[int16, int8]
	Int8Int32AddSels = addDifferentSelsGeneric[int32, int8]
	Int8Int64AddSels = addDifferentSelsGeneric[int64, int8]
	Int16Int32AddSels = addDifferentSelsGeneric[int32, int16]
	Int16Int64AddSels = addDifferentSelsGeneric[int64, int16]
	Int32Int64AddSels = addDifferentSelsGeneric[int64, int32]

	Float32Float64AddSels = addDifferentSelsGeneric[float64, float32]

	Uint8Uint16AddSels = addDifferentSelsGeneric[uint16, uint8]
	Uint8Uint32AddSels = addDifferentSelsGeneric[uint32, uint8]
	Uint8Uint64AddSels = addDifferentSelsGeneric[uint64, uint8]
	Uint16Uint32AddSels = addDifferentSelsGeneric[uint32, uint16]
	Uint16Uint64AddSels = addDifferentSelsGeneric[uint64, uint16]
	Uint32Uint64AddSels = addDifferentSelsGeneric[uint64, uint32]
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
