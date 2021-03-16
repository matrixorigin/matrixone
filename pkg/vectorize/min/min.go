package min

import (
	"bytes"
	"matrixbase/pkg/container/types"

	"golang.org/x/sys/cpu"
)

var (
	boolMin        func([]bool) bool
	boolMinSels    func([]bool, []int64) bool
	int8Min        func([]int8) int8
	int8MinSels    func([]int8, []int64) int8
	int16Min       func([]int16) int16
	int16MinSels   func([]int16, []int64) int16
	int32Min       func([]int32) int32
	int32MinSels   func([]int32, []int64) int32
	int64Min       func([]int64) int64
	int64MinSels   func([]int64, []int64) int64
	uint8Min       func([]uint8) uint8
	uint8MinSels   func([]uint8, []int64) uint8
	uint16Min      func([]uint16) uint16
	uint16MinSels  func([]uint16, []int64) uint16
	uint32Min      func([]uint32) uint32
	uint32MinSels  func([]uint32, []int64) uint32
	uint64Min      func([]uint64) uint64
	uint64MinSels  func([]uint64, []int64) uint64
	float32Min     func([]float32) float32
	float32MinSels func([]float32, []int64) float32
	float64Min     func([]float64) float64
	float64MinSels func([]float64, []int64) float64
	strMin         func(*types.Bytes) []byte
	strMinSels     func(*types.Bytes, []int64) []byte
)

func init() {
	if cpu.X86.HasAVX512 {
		int8Min = int8MinAvx512
		int16Min = int16MinAvx512
		int32Min = int32MinAvx512
		int64Min = int64MinAvx512
		uint8Min = uint8MinAvx512
		uint16Min = uint16MinAvx512
		uint32Min = uint32MinAvx512
		uint64Min = uint64MinAvx512
		float32Min = float32MinAvx512
		float64Min = float64MinAvx512
	} else if cpu.X86.HasAVX2 {
		int8Min = int8MinAvx2
		int16Min = int16MinAvx2
		int32Min = int32MinAvx2
		int64Min = int64MinPure
		uint8Min = uint8MinAvx2
		uint16Min = uint16MinAvx2
		uint32Min = uint32MinAvx2
		uint64Min = uint64MinPure
		float32Min = float32MinAvx2
		float64Min = float64MinAvx2
	} else {
		int8Min = int8MinPure
		int16Min = int16MinPure
		int32Min = int32MinPure
		int64Min = int64MinPure
		uint8Min = uint8MinPure
		uint16Min = uint16MinPure
		uint32Min = uint32MinPure
		uint64Min = uint64MinPure
		float32Min = float32MinPure
		float64Min = float64MinPure
	}

	boolMin = boolMinPure
	strMin = strMinPure

	boolMinSels = boolMinSelsPure
	int8MinSels = int8MinSelsPure
	int16MinSels = int16MinSelsPure
	int32MinSels = int32MinSelsPure
	int64MinSels = int64MinSelsPure
	uint8MinSels = uint8MinSelsPure
	uint16MinSels = uint16MinSelsPure
	uint32MinSels = uint32MinSelsPure
	uint64MinSels = uint64MinSelsPure
	float32MinSels = float32MinSelsPure
	float64MinSels = float64MinSelsPure
	strMinSels = strMinSelsPure
}

func BoolMin(xs []bool) bool {
	return boolMin(xs)
}

func boolMinPure(xs []bool) bool {
	for _, x := range xs {
		if x == false {
			return false
		}
	}
	return true
}

func BoolMinSels(xs []bool, sels []int64) bool {
	return boolMinSels(xs, sels)
}

func boolMinSelsPure(xs []bool, sels []int64) bool {
	for _, sel := range sels {
		if xs[sel] == false {
			return false
		}
	}
	return true
}

func Int8Min(xs []int8) int8 {
	return int8Min(xs)
}

func int8MinPure(xs []int8) int8 {
	res := xs[0]
	for _, x := range xs {
		if x < res {
			res = x
		}
	}
	return res
}

func int8MinAvx2(xs []int8) int8 {
	const regItems int = 32 / 1
	n := len(xs) / regItems
	var rs [16]int8
	int8MinAvx2Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 16; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func int8MinAvx512(xs []int8) int8 {
	const regItems int = 64 / 1
	n := len(xs) / regItems
	var rs [16]int8
	int8MinAvx512Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 16; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func Int8MinSels(xs []int8, sels []int64) int8 {
	return int8MinSels(xs, sels)
}

func int8MinSelsPure(xs []int8, sels []int64) int8 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x < res {
			res = x
		}
	}
	return res
}

func Int16Min(xs []int16) int16 {
	return int16Min(xs)
}

func int16MinPure(xs []int16) int16 {
	res := xs[0]
	for _, x := range xs {
		if x < res {
			res = x
		}
	}
	return res
}

func int16MinAvx2(xs []int16) int16 {
	const regItems int = 32 / 2
	n := len(xs) / regItems
	var rs [8]int16
	int16MinAvx2Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 8; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func int16MinAvx512(xs []int16) int16 {
	const regItems int = 64 / 2
	n := len(xs) / regItems
	var rs [8]int16
	int16MinAvx512Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 8; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func Int16MinSels(xs []int16, sels []int64) int16 {
	return int16MinSels(xs, sels)
}

func int16MinSelsPure(xs []int16, sels []int64) int16 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x < res {
			res = x
		}
	}
	return res
}

func Int32Min(xs []int32) int32 {
	return int32Min(xs)
}

func int32MinPure(xs []int32) int32 {
	res := xs[0]
	for _, x := range xs {
		if x < res {
			res = x
		}
	}
	return res
}

func int32MinAvx2(xs []int32) int32 {
	const regItems int = 32 / 4
	n := len(xs) / regItems
	var rs [4]int32
	int32MinAvx2Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 4; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func int32MinAvx512(xs []int32) int32 {
	const regItems int = 64 / 4
	n := len(xs) / regItems
	var rs [4]int32
	int32MinAvx512Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 4; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func Int32MinSels(xs []int32, sels []int64) int32 {
	return int32MinSels(xs, sels)
}

func int32MinSelsPure(xs []int32, sels []int64) int32 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x < res {
			res = x
		}
	}
	return res
}

func Int64Min(xs []int64) int64 {
	return int64Min(xs)
}

func int64MinPure(xs []int64) int64 {
	res := xs[0]
	for _, x := range xs {
		if x < res {
			res = x
		}
	}
	return res
}

func int64MinAvx512(xs []int64) int64 {
	const regItems int = 64 / 8
	n := len(xs) / regItems
	var rs [2]int64
	int64MinAvx512Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 2; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func Int64MinSels(xs []int64, sels []int64) int64 {
	return int64MinSels(xs, sels)
}

func int64MinSelsPure(xs []int64, sels []int64) int64 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x < res {
			res = x
		}
	}
	return res
}

func Uint8Min(xs []uint8) uint8 {
	return uint8Min(xs)
}

func uint8MinPure(xs []uint8) uint8 {
	res := xs[0]
	for _, x := range xs {
		if x < res {
			res = x
		}
	}
	return res
}

func uint8MinAvx2(xs []uint8) uint8 {
	const regItems int = 32 / 1
	n := len(xs) / regItems
	var rs [16]uint8
	uint8MinAvx2Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 16; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func uint8MinAvx512(xs []uint8) uint8 {
	const regItems int = 64 / 1
	n := len(xs) / regItems
	var rs [16]uint8
	uint8MinAvx512Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 16; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func Uint8MinSels(xs []uint8, sels []int64) uint8 {
	return uint8MinSels(xs, sels)
}

func uint8MinSelsPure(xs []uint8, sels []int64) uint8 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x < res {
			res = x
		}
	}
	return res
}

func Uint16Min(xs []uint16) uint16 {
	return uint16Min(xs)
}

func uint16MinPure(xs []uint16) uint16 {
	res := xs[0]
	for _, x := range xs {
		if x < res {
			res = x
		}
	}
	return res
}

func uint16MinAvx2(xs []uint16) uint16 {
	const regItems int = 32 / 2
	n := len(xs) / regItems
	var rs [8]uint16
	uint16MinAvx2Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 8; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func uint16MinAvx512(xs []uint16) uint16 {
	const regItems int = 64 / 2
	n := len(xs) / regItems
	var rs [8]uint16
	uint16MinAvx512Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 8; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func Uint16MinSels(xs []uint16, sels []int64) uint16 {
	return uint16MinSels(xs, sels)
}

func uint16MinSelsPure(xs []uint16, sels []int64) uint16 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x < res {
			res = x
		}
	}
	return res
}

func Uint32Min(xs []uint32) uint32 {
	return uint32Min(xs)
}

func uint32MinPure(xs []uint32) uint32 {
	res := xs[0]
	for _, x := range xs {
		if x < res {
			res = x
		}
	}
	return res
}

func uint32MinAvx2(xs []uint32) uint32 {
	const regItems int = 32 / 4
	n := len(xs) / regItems
	var rs [4]uint32
	uint32MinAvx2Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 4; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func uint32MinAvx512(xs []uint32) uint32 {
	const regItems int = 64 / 4
	n := len(xs) / regItems
	var rs [4]uint32
	uint32MinAvx512Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 4; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func Uint32MinSels(xs []uint32, sels []int64) uint32 {
	return uint32MinSels(xs, sels)
}

func uint32MinSelsPure(xs []uint32, sels []int64) uint32 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x < res {
			res = x
		}
	}
	return res
}

func Uint64Min(xs []uint64) uint64 {
	return uint64Min(xs)
}

func uint64MinPure(xs []uint64) uint64 {
	res := xs[0]
	for _, x := range xs {
		if x < res {
			res = x
		}
	}
	return res
}

func uint64MinAvx512(xs []uint64) uint64 {
	const regItems int = 64 / 8
	n := len(xs) / regItems
	var rs [2]uint64
	uint64MinAvx512Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 2; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func Uint64MinSels(xs []uint64, sels []int64) uint64 {
	return uint64MinSels(xs, sels)
}

func uint64MinSelsPure(xs []uint64, sels []int64) uint64 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x < res {
			res = x
		}
	}
	return res
}

func Float32Min(xs []float32) float32 {
	return float32Min(xs)
}

func float32MinPure(xs []float32) float32 {
	res := xs[0]
	for _, x := range xs {
		if x < res {
			res = x
		}
	}
	return res
}

func float32MinAvx2(xs []float32) float32 {
	const regItems int = 32 / 4
	n := len(xs) / regItems
	var rs [4]float32
	float32MinAvx2Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 4; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func float32MinAvx512(xs []float32) float32 {
	const regItems int = 64 / 4
	n := len(xs) / regItems
	var rs [4]float32
	float32MinAvx512Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 4; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func Float32MinSels(xs []float32, sels []int64) float32 {
	return float32MinSels(xs, sels)
}

func float32MinSelsPure(xs []float32, sels []int64) float32 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x < res {
			res = x
		}
	}
	return res
}

func Float64Min(xs []float64) float64 {
	return float64Min(xs)
}

func float64MinPure(xs []float64) float64 {
	res := xs[0]
	for _, x := range xs {
		if x < res {
			res = x
		}
	}
	return res
}

func float64MinAvx2(xs []float64) float64 {
	const regItems int = 32 / 8
	n := len(xs) / regItems
	var rs [2]float64
	float64MinAvx2Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 2; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func float64MinAvx512(xs []float64) float64 {
	const regItems int = 64 / 8
	n := len(xs) / regItems
	var rs [2]float64
	float64MinAvx512Asm(xs[:n*regItems], rs[:])
	res := rs[0]
	for i := 1; i < 2; i++ {
		if rs[i] < res {
			res = rs[i]
		}
	}
	for i, j := n*regItems, len(xs); i < j; i++ {
		if xs[i] < res {
			res = xs[i]
		}
	}
	return res
}

func Float64MinSels(xs []float64, sels []int64) float64 {
	return float64MinSels(xs, sels)
}

func float64MinSelsPure(xs []float64, sels []int64) float64 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x < res {
			res = x
		}
	}
	return res
}

func StrMin(xs *types.Bytes) []byte {
	return strMin(xs)
}

func strMinPure(xs *types.Bytes) []byte {
	res := xs.Get(0)
	for i, n := 0, len(xs.Offsets); i < n; i++ {
		x := xs.Get(i)
		if bytes.Compare(x, res) < 0 {
			res = x
		}
	}
	return res
}

func StrMinSels(xs *types.Bytes, sels []int64) []byte {
	return strMinSels(xs, sels)
}

func strMinSelsPure(xs *types.Bytes, sels []int64) []byte {
	res := xs.Get(int(sels[0]))
	for _, sel := range sels {
		x := xs.Get(int(sel))
		if bytes.Compare(x, res) < 0 {
			res = x
		}
	}
	return res
}
