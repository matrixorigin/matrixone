package sum

import (
	"golang.org/x/sys/cpu"
)

var (
	int8Sum        func([]int8) int8
	int8SumSels    func([]int8, []int64) int8
	int16Sum       func([]int16) int16
	int16SumSels   func([]int16, []int64) int16
	int32Sum       func([]int32) int32
	int32SumSels   func([]int32, []int64) int32
	int64Sum       func([]int64) int64
	int64SumSels   func([]int64, []int64) int64
	uint8Sum       func([]uint8) uint8
	uint8SumSels   func([]uint8, []int64) uint8
	uint16Sum      func([]uint16) uint16
	uint16SumSels  func([]uint16, []int64) uint16
	uint32Sum      func([]uint32) uint32
	uint32SumSels  func([]uint32, []int64) uint32
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
		float32Sum = float32SumAvx512
		float64Sum = float64SumAvx512
	} else if cpu.X86.HasAVX2 {
		int8Sum = int8SumAvx2
		int16Sum = int16SumAvx2
		int32Sum = int32SumAvx2
		int64Sum = int64SumAvx2
		float32Sum = float32SumAvx2
		float64Sum = float64SumAvx2
	} else {
		int8Sum = int8SumPure
		int16Sum = int16SumPure
		int32Sum = int32SumPure
		int64Sum = int64SumPure
		float32Sum = float32SumPure
		float64Sum = float64SumPure
	}
	uint8Sum = uint8SumPure
	uint16Sum = uint16SumPure
	uint32Sum = uint32SumPure
	uint64Sum = uint64SumPure

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

func Int8Sum(xs []int8) int8 {
	return int8Sum(xs)
}

func int8SumPure(xs []int8) int8 {
	var res int8

	for _, x := range xs {
		res += x
	}
	return res
}

func int8SumAvx2(xs []int8) int8 {
	const regItems int = 32 / 1
	n := len(xs) / regItems
	var rs [16]int8
	int8SumAvx2Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 16; i++ {
		res += rs[i]
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
		res += xs[i]
	}
	return res
}

func int8SumAvx512(xs []int8) int8 {
	const regItems int = 64 / 1
	n := len(xs) / regItems
	var rs [16]int8
	int8SumAvx512Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 16; i++ {
		res += rs[i]
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
		res += xs[i]
	}
	return res
}

func Int8SumSels(xs []int8, sels []int64) int8 {
	return int8SumSels(xs, sels)
}

func int8SumSelsPure(xs []int8, sels []int64) int8 {
	var res int8

	for _, sel := range sels {
		res += xs[sel]
	}
	return res
}

func Int16Sum(xs []int16) int16 {
	return int16Sum(xs)
}

func int16SumPure(xs []int16) int16 {
	var res int16

	for _, x := range xs {
		res += x
	}
	return res
}

func int16SumAvx2(xs []int16) int16 {
	const regItems int = 32 / 2
	n := len(xs) / regItems
	var rs [8]int16
	int16SumAvx2Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 8; i++ {
		res += rs[i]
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
		res += xs[i]
	}
	return res
}

func int16SumAvx512(xs []int16) int16 {
	const regItems int = 64 / 2
	n := len(xs) / regItems
	var rs [8]int16
	int16SumAvx512Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 8; i++ {
		res += rs[i]
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
		res += xs[i]
	}
	return res
}

func Int16SumSels(xs []int16, sels []int64) int16 {
	return int16SumSels(xs, sels)
}

func int16SumSelsPure(xs []int16, sels []int64) int16 {
	var res int16

	for _, sel := range sels {
		res += xs[sel]
	}
	return res
}

func Int32Sum(xs []int32) int32 {
	return int32Sum(xs)
}

func int32SumPure(xs []int32) int32 {
	var res int32

	for _, x := range xs {
		res += x
	}
	return res
}

func int32SumAvx2(xs []int32) int32 {
	const regItems int = 32 / 4
	n := len(xs) / regItems
	var rs [4]int32
	int32SumAvx2Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 4; i++ {
		res += rs[i]
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
		res += xs[i]
	}
	return res
}

func int32SumAvx512(xs []int32) int32 {
	const regItems int = 64 / 4
	n := len(xs) / regItems
	var rs [4]int32
	int32SumAvx512Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 4; i++ {
		res += rs[i]
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
		res += xs[i]
	}
	return res
}

func Int32SumSels(xs []int32, sels []int64) int32 {
	return int32SumSels(xs, sels)
}

func int32SumSelsPure(xs []int32, sels []int64) int32 {
	var res int32

	for _, sel := range sels {
		res += xs[sel]
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
	const regItems int = 32 / 8
	n := len(xs) / regItems
	var rs [2]int64
	int64SumAvx2Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 2; i++ {
		res += rs[i]
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
		res += xs[i]
	}
	return res
}

func int64SumAvx512(xs []int64) int64 {
	const regItems int = 64 / 8
	n := len(xs) / regItems
	var rs [2]int64
	int64SumAvx512Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 2; i++ {
		res += rs[i]
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
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

func Uint8Sum(xs []uint8) uint8 {
	return uint8Sum(xs)
}

func uint8SumPure(xs []uint8) uint8 {
	var res uint8

	for _, x := range xs {
		res += x
	}
	return res
}

func Uint8SumSels(xs []uint8, sels []int64) uint8 {
	return uint8SumSels(xs, sels)
}

func uint8SumSelsPure(xs []uint8, sels []int64) uint8 {
	var res uint8

	for _, sel := range sels {
		res += xs[sel]
	}
	return res
}

func Uint16Sum(xs []uint16) uint16 {
	return uint16Sum(xs)
}

func uint16SumPure(xs []uint16) uint16 {
	var res uint16

	for _, x := range xs {
		res += x
	}
	return res
}

func Uint16SumSels(xs []uint16, sels []int64) uint16 {
	return uint16SumSels(xs, sels)
}

func uint16SumSelsPure(xs []uint16, sels []int64) uint16 {
	var res uint16

	for _, sel := range sels {
		res += xs[sel]
	}
	return res
}

func Uint32Sum(xs []uint32) uint32 {
	return uint32Sum(xs)
}

func uint32SumPure(xs []uint32) uint32 {
	var res uint32

	for _, x := range xs {
		res += x
	}
	return res
}

func Uint32SumSels(xs []uint32, sels []int64) uint32 {
	return uint32SumSels(xs, sels)
}

func uint32SumSelsPure(xs []uint32, sels []int64) uint32 {
	var res uint32

	for _, sel := range sels {
		res += xs[sel]
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
	const regItems int = 32 / 4
	n := len(xs) / regItems
	var rs [4]float32
	float32SumAvx2Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 4; i++ {
		res += rs[i]
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
		res += xs[i]
	}
	return res
}

func float32SumAvx512(xs []float32) float32 {
	const regItems int = 64 / 4
	n := len(xs) / regItems
	var rs [4]float32
	float32SumAvx512Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 4; i++ {
		res += rs[i]
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
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
	const regItems int = 32 / 8
	n := len(xs) / regItems
	var rs [2]float64
	float64SumAvx2Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 2; i++ {
		res += rs[i]
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
		res += xs[i]
	}
	return res
}

func float64SumAvx512(xs []float64) float64 {
	const regItems int = 64 / 8
	n := len(xs) / regItems
	var rs [2]float64
	float64SumAvx512Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 2; i++ {
		res += rs[i]
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
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
