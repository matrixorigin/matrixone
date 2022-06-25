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

package sum

import (
	"golang.org/x/sys/cpu"
)

func int8SumAvx2Asm(x []int8) int64
func int8SumAvx512Asm(x []int8) int64
func int16SumAvx2Asm(x []int16) int64
func int16SumAvx512Asm(x []int16) int64
func int32SumAvx2Asm(x []int32) int64
func int32SumAvx512Asm(x []int32) int64
func int64SumAvx2Asm(x []int64) int64
func int64SumAvx512Asm(x []int64) int64
func uint8SumAvx2Asm(x []uint8) uint64
func uint8SumAvx512Asm(x []uint8) uint64
func uint16SumAvx2Asm(x []uint16) uint64
func uint16SumAvx512Asm(x []uint16) uint64
func uint32SumAvx2Asm(x []uint32) uint64
func uint32SumAvx512Asm(x []uint32) uint64
func uint64SumAvx2Asm(x []uint64) uint64
func uint64SumAvx512Asm(x []uint64) uint64
func float32SumAvx2Asm(x []float32) float32
func float32SumAvx512Asm(x []float32) float32
func float64SumAvx2Asm(x []float64) float64
func float64SumAvx512Asm(x []float64) float64

func init() {
	if cpu.X86.HasAVX512 {
		Int8Sum = int8SumAvx512
		Int16Sum = int16SumAvx512
		Int32Sum = int32SumAvx512
		Int64Sum = int64SumAvx512
		Uint8Sum = uint8SumAvx512
		Uint16Sum = uint16SumAvx512
		Uint32Sum = uint32SumAvx512
		Uint64Sum = uint64SumAvx512
		Float32Sum = float32SumAvx512
		Float64Sum = float64SumAvx512
	} else if cpu.X86.HasAVX2 {
		Int8Sum = int8SumAvx2
		Int16Sum = int16SumAvx2
		Int32Sum = int32SumAvx2
		Int64Sum = int64SumAvx2
		Uint8Sum = uint8SumAvx2
		Uint16Sum = uint16SumAvx2
		Uint32Sum = uint32SumAvx2
		Uint64Sum = uint64SumAvx2
		Float32Sum = float32SumAvx2
		Float64Sum = float64SumAvx2
	}
}

func int8SumAvx2(xs []int8) int64 {
	n := len(xs) / 4
	res := int8SumAvx2Asm(xs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		res += int64(xs[i])
	}
	return res
}

func int8SumAvx512(xs []int8) int64 {
	n := len(xs) / 4
	res := int8SumAvx512Asm(xs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		res += int64(xs[i])
	}
	return res
}

func int16SumAvx2(xs []int16) int64 {
	n := len(xs) / 4
	res := int16SumAvx2Asm(xs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		res += int64(xs[i])
	}
	return res
}

func int16SumAvx512(xs []int16) int64 {
	n := len(xs) / 4
	res := int16SumAvx512Asm(xs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		res += int64(xs[i])
	}
	return res
}

func int32SumAvx2(xs []int32) int64 {
	n := len(xs) / 4
	res := int32SumAvx2Asm(xs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		res += int64(xs[i])
	}
	return res
}

func int32SumAvx512(xs []int32) int64 {
	n := len(xs) / 4
	res := int32SumAvx512Asm(xs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		res += int64(xs[i])
	}
	return res
}

func int64SumAvx2(xs []int64) int64 {
	n := len(xs) / 4
	res := int64SumAvx2Asm(xs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		res += xs[i]
	}
	return res
}

func int64SumAvx512(xs []int64) int64 {
	n := len(xs) / 4
	res := int64SumAvx512Asm(xs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		res += xs[i]
	}
	return res
}

func uint8SumAvx2(xs []uint8) uint64 {
	n := len(xs) / 4
	res := uint8SumAvx2Asm(xs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		res += uint64(xs[i])
	}
	return res
}

func uint8SumAvx512(xs []uint8) uint64 {
	n := len(xs) / 4
	res := uint8SumAvx512Asm(xs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		res += uint64(xs[i])
	}
	return res
}

func uint16SumAvx2(xs []uint16) uint64 {
	n := len(xs) / 4
	res := uint16SumAvx2Asm(xs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		res += uint64(xs[i])
	}
	return res
}

func uint16SumAvx512(xs []uint16) uint64 {
	n := len(xs) / 4
	res := uint16SumAvx512Asm(xs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		res += uint64(xs[i])
	}
	return res
}

func uint32SumAvx2(xs []uint32) uint64 {
	n := len(xs) / 4
	res := uint32SumAvx2Asm(xs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		res += uint64(xs[i])
	}
	return res
}

func uint32SumAvx512(xs []uint32) uint64 {
	n := len(xs) / 4
	res := uint32SumAvx512Asm(xs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		res += uint64(xs[i])
	}
	return res
}

func uint64SumAvx2(xs []uint64) uint64 {
	n := len(xs) / 4
	res := uint64SumAvx2Asm(xs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		res += xs[i]
	}
	return res
}

func uint64SumAvx512(xs []uint64) uint64 {
	n := len(xs) / 4
	res := uint64SumAvx512Asm(xs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		res += xs[i]
	}
	return res
}

func float32SumAvx2(xs []float32) float32 {
	n := len(xs) / 4
	res := float32SumAvx2Asm(xs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		res += xs[i]
	}
	return res
}

func float32SumAvx512(xs []float32) float32 {
	n := len(xs) / 4
	res := float32SumAvx512Asm(xs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		res += xs[i]
	}
	return res
}

func float64SumAvx2(xs []float64) float64 {
	n := len(xs) / 4
	res := float64SumAvx2Asm(xs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		res += xs[i]
	}
	return res
}

func float64SumAvx512(xs []float64) float64 {
	n := len(xs) / 4
	res := float64SumAvx512Asm(xs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		res += xs[i]
	}
	return res
}
