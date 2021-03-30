package sum

import (
	"golang.org/x/sys/cpu"
)

var (
	int8Sum        func([]int8) int64
	int8SumSels    func([]int8, []int64) int64
	int16Sum       func([]int16) int64
	int16SumSels   func([]int16, []int64) int64
	int32Sum       func([]int32) int64
	int32SumSels   func([]int32, []int64) int64
	int64Sum       func([]int64) int64
	int64SumSels   func([]int64, []int64) int64
	uint8Sum       func([]uint8) uint64
	uint8SumSels   func([]uint8, []int64) uint64
	uint16Sum      func([]uint16) uint64
	uint16SumSels  func([]uint16, []int64) uint64
	uint32Sum      func([]uint32) uint64
	uint32SumSels  func([]uint32, []int64) uint64
	uint64Sum      func([]uint64) uint64
	uint64SumSels  func([]uint64, []int64) uint64
	float32Sum     func([]float32) float32
	float32SumSels func([]float32, []int64) float32
	float64Sum     func([]float64) float64
	float64SumSels func([]float64, []int64) float64
)

func init() {
	if cpu.X86.HasAVX512 {
		int8Sum = int8SumAvx512
		int16Sum = int16SumAvx512
		int32Sum = int32SumAvx512
		int64Sum = int64SumAvx512
		uint8Sum = uint8SumAvx512
		uint16Sum = uint16SumAvx512
		uint32Sum = uint32SumAvx512
		uint64Sum = uint64SumAvx512
		float32Sum = float32SumAvx512
		float64Sum = float64SumAvx512
	} else if cpu.X86.HasAVX2 {
		int8Sum = int8SumAvx2
		int16Sum = int16SumAvx2
		int32Sum = int32SumAvx2
		int64Sum = int64SumAvx2
		uint8Sum = uint8SumAvx2
		uint16Sum = uint16SumAvx2
		uint32Sum = uint32SumAvx2
		uint64Sum = uint64SumAvx2
		float32Sum = float32SumAvx2
		float64Sum = float64SumAvx2
	} else {
		int8Sum = int8SumPure
		int16Sum = int16SumPure
		int32Sum = int32SumPure
		int64Sum = int64SumPure
		uint8Sum = uint8SumPure
		uint16Sum = uint16SumPure
		uint32Sum = uint32SumPure
		uint64Sum = uint64SumPure
		float32Sum = float32SumPure
		float64Sum = float64SumPure
	}
	int8SumSels = int8SumSelsPure
	int16SumSels = int16SumSelsPure
	int32SumSels = int32SumSelsPure
	int64SumSels = int64SumSelsPure
	uint8SumSels = uint8SumSelsPure
	uint16SumSels = uint16SumSelsPure
	uint32SumSels = uint32SumSelsPure
	uint64SumSels = uint64SumSelsPure
	float32SumSels = float32SumSelsPure
	float64SumSels = float64SumSelsPure
}

func Int8Sum(xs []int8) int64 {
	return int8Sum(xs)
}

func int8SumPure(xs []int8) int64 {
	var res int64

	for _, x := range xs {
		res += int64(x)
	}
	return res
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

func Int8SumSels(xs []int8, sels []int64) int64 {
	return int8SumSels(xs, sels)
}

func int8SumSelsPure(xs []int8, sels []int64) int64 {
	var res int64

	for _, sel := range sels {
		res += int64(xs[sel])
	}
	return res
}

func Int16Sum(xs []int16) int64 {
	return int16Sum(xs)
}

func int16SumPure(xs []int16) int64 {
	var res int64

	for _, x := range xs {
		res += int64(x)
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

func Int16SumSels(xs []int16, sels []int64) int64 {
	return int16SumSels(xs, sels)
}

func int16SumSelsPure(xs []int16, sels []int64) int64 {
	var res int64

	for _, sel := range sels {
		res += int64(xs[sel])
	}
	return res
}

func Int32Sum(xs []int32) int64 {
	return int32Sum(xs)
}

func int32SumPure(xs []int32) int64 {
	var res int64

	for _, x := range xs {
		res += int64(x)
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

func Int32SumSels(xs []int32, sels []int64) int64 {
	return int32SumSels(xs, sels)
}

func int32SumSelsPure(xs []int32, sels []int64) int64 {
	var res int64

	for _, sel := range sels {
		res += int64(xs[sel])
	}
	return res
}

func Int64Sum(xs []int64) int64 {
	return int64Sum(xs)
}

func int64SumPure(xs []int64) int64 {
	var res int64

	for _, x := range xs {
		res += x
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

func Int64SumSels(xs []int64, sels []int64) int64 {
	return int64SumSels(xs, sels)
}

func int64SumSelsPure(xs []int64, sels []int64) int64 {
	var res int64

	for _, sel := range sels {
		res += xs[sel]
	}
	return res
}

func Uint8Sum(xs []uint8) uint64 {
	return uint8Sum(xs)
}

func uint8SumPure(xs []uint8) uint64 {
	var res uint64

	for _, x := range xs {
		res += uint64(x)
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

func Uint8SumSels(xs []uint8, sels []int64) uint64 {
	return uint8SumSels(xs, sels)
}

func uint8SumSelsPure(xs []uint8, sels []int64) uint64 {
	var res uint64

	for _, sel := range sels {
		res += uint64(xs[sel])
	}
	return res
}

func Uint16Sum(xs []uint16) uint64 {
	return uint16Sum(xs)
}

func uint16SumPure(xs []uint16) uint64 {
	var res uint64

	for _, x := range xs {
		res += uint64(x)
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

func Uint16SumSels(xs []uint16, sels []int64) uint64 {
	return uint16SumSels(xs, sels)
}

func uint16SumSelsPure(xs []uint16, sels []int64) uint64 {
	var res uint64

	for _, sel := range sels {
		res += uint64(xs[sel])
	}
	return res
}

func Uint32Sum(xs []uint32) uint64 {
	return uint32Sum(xs)
}

func uint32SumPure(xs []uint32) uint64 {
	var res uint64

	for _, x := range xs {
		res += uint64(x)
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

func Uint32SumSels(xs []uint32, sels []int64) uint64 {
	return uint32SumSels(xs, sels)
}

func uint32SumSelsPure(xs []uint32, sels []int64) uint64 {
	var res uint64

	for _, sel := range sels {
		res += uint64(xs[sel])
	}
	return res
}

func Uint64Sum(xs []uint64) uint64 {
	return uint64Sum(xs)
}

func uint64SumPure(xs []uint64) uint64 {
	var res uint64

	for _, x := range xs {
		res += x
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

func Uint64SumSels(xs []uint64, sels []int64) uint64 {
	return uint64SumSels(xs, sels)
}

func uint64SumSelsPure(xs []uint64, sels []int64) uint64 {
	var res uint64

	for _, sel := range sels {
		res += xs[sel]
	}
	return res
}

func Float32Sum(xs []float32) float32 {
	return float32Sum(xs)
}

func float32SumPure(xs []float32) float32 {
	var res float32

	for _, x := range xs {
		res += x
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

func Float32SumSels(xs []float32, sels []int64) float32 {
	return float32SumSels(xs, sels)
}

func float32SumSelsPure(xs []float32, sels []int64) float32 {
	var res float32

	for _, sel := range sels {
		res += xs[sel]
	}
	return res
}

func Float64Sum(xs []float64) float64 {
	return float64Sum(xs)
}

func float64SumPure(xs []float64) float64 {
	var res float64

	for _, x := range xs {
		res += x
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

func Float64SumSels(xs []float64, sels []int64) float64 {
	return float64SumSels(xs, sels)
}

func float64SumSelsPure(xs []float64, sels []int64) float64 {
	var res float64

	for _, sel := range sels {
		res += xs[sel]
	}
	return res
}
