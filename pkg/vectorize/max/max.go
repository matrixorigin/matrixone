package max

import (
	"bytes"
	"matrixone/pkg/container/types"

	"golang.org/x/sys/cpu"
)

var (
	boolMax        func([]bool) bool
	boolMaxSels    func([]bool, []int64) bool
	int8Max        func([]int8) int8
	int8MaxSels    func([]int8, []int64) int8
	int16Max       func([]int16) int16
	int16MaxSels   func([]int16, []int64) int16
	int32Max       func([]int32) int32
	int32MaxSels   func([]int32, []int64) int32
	int64Max       func([]int64) int64
	int64MaxSels   func([]int64, []int64) int64
	uint8Max       func([]uint8) uint8
	uint8MaxSels   func([]uint8, []int64) uint8
	uint16Max      func([]uint16) uint16
	uint16MaxSels  func([]uint16, []int64) uint16
	uint32Max      func([]uint32) uint32
	uint32MaxSels  func([]uint32, []int64) uint32
	uint64Max      func([]uint64) uint64
	uint64MaxSels  func([]uint64, []int64) uint64
	float32Max     func([]float32) float32
	float32MaxSels func([]float32, []int64) float32
	float64Max     func([]float64) float64
	float64MaxSels func([]float64, []int64) float64
	strMax         func(*types.Bytes) []byte
	strMaxSels     func(*types.Bytes, []int64) []byte
)

func init() {
	if cpu.X86.HasAVX512 {
		int8Max = int8MaxAvx512
		int16Max = int16MaxAvx512
		int32Max = int32MaxAvx512
		int64Max = int64MaxAvx512
		uint8Max = uint8MaxAvx512
		uint16Max = uint16MaxAvx512
		uint32Max = uint32MaxAvx512
		uint64Max = uint64MaxAvx512
		float32Max = float32MaxAvx512
		float64Max = float64MaxAvx512
	} else if cpu.X86.HasAVX2 {
		int8Max = int8MaxAvx2
		int16Max = int16MaxAvx2
		int32Max = int32MaxAvx2
		int64Max = int64MaxPure
		uint8Max = uint8MaxAvx2
		uint16Max = uint16MaxAvx2
		uint32Max = uint32MaxAvx2
		uint64Max = uint64MaxPure
		float32Max = float32MaxAvx2
		float64Max = float64MaxAvx2
	} else {
		int8Max = int8MaxPure
		int16Max = int16MaxPure
		int32Max = int32MaxPure
		int64Max = int64MaxPure
		uint8Max = uint8MaxPure
		uint16Max = uint16MaxPure
		uint32Max = uint32MaxPure
		uint64Max = uint64MaxPure
		float32Max = float32MaxPure
		float64Max = float64MaxPure
	}

	boolMax = boolMaxPure
	strMax = strMaxPure

	boolMaxSels = boolMaxSelsPure
	int8MaxSels = int8MaxSelsPure
	int16MaxSels = int16MaxSelsPure
	int32MaxSels = int32MaxSelsPure
	int64MaxSels = int64MaxSelsPure
	uint8MaxSels = uint8MaxSelsPure
	uint16MaxSels = uint16MaxSelsPure
	uint32MaxSels = uint32MaxSelsPure
	uint64MaxSels = uint64MaxSelsPure
	float32MaxSels = float32MaxSelsPure
	float64MaxSels = float64MaxSelsPure
	strMaxSels = strMaxSelsPure
}

func BoolMax(xs []bool) bool {
	return boolMax(xs)
}

func boolMaxPure(xs []bool) bool {
	for _, x := range xs {
		if x == true {
			return true
		}
	}
	return false
}

func BoolMaxSels(xs []bool, sels []int64) bool {
	return boolMaxSels(xs, sels)
}

func boolMaxSelsPure(xs []bool, sels []int64) bool {
	for _, sel := range sels {
		if xs[sel] == true {
			return true
		}
	}
	return false
}

func Int8Max(xs []int8) int8 {
	return int8Max(xs)
}

func int8MaxPure(xs []int8) int8 {
	res := xs[0]
	for _, x := range xs {
		if x > res {
			res = x
		}
	}
	return res
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

func Int8MaxSels(xs []int8, sels []int64) int8 {
	return int8MaxSels(xs, sels)
}

func int8MaxSelsPure(xs []int8, sels []int64) int8 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x > res {
			res = x
		}
	}
	return res
}

func Int16Max(xs []int16) int16 {
	return int16Max(xs)
}

func int16MaxPure(xs []int16) int16 {
	res := xs[0]
	for _, x := range xs {
		if x > res {
			res = x
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

func Int16MaxSels(xs []int16, sels []int64) int16 {
	return int16MaxSels(xs, sels)
}

func int16MaxSelsPure(xs []int16, sels []int64) int16 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x > res {
			res = x
		}
	}
	return res
}

func Int32Max(xs []int32) int32 {
	return int32Max(xs)
}

func int32MaxPure(xs []int32) int32 {
	res := xs[0]
	for _, x := range xs {
		if x > res {
			res = x
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

func Int32MaxSels(xs []int32, sels []int64) int32 {
	return int32MaxSels(xs, sels)
}

func int32MaxSelsPure(xs []int32, sels []int64) int32 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x > res {
			res = x
		}
	}
	return res
}

func Int64Max(xs []int64) int64 {
	return int64Max(xs)
}

func int64MaxPure(xs []int64) int64 {
	res := xs[0]
	for _, x := range xs {
		if x > res {
			res = x
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

func Int64MaxSels(xs []int64, sels []int64) int64 {
	return int64MaxSels(xs, sels)
}

func int64MaxSelsPure(xs []int64, sels []int64) int64 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x > res {
			res = x
		}
	}
	return res
}

func Uint8Max(xs []uint8) uint8 {
	return uint8Max(xs)
}

func uint8MaxPure(xs []uint8) uint8 {
	res := xs[0]
	for _, x := range xs {
		if x > res {
			res = x
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

func Uint8MaxSels(xs []uint8, sels []int64) uint8 {
	return uint8MaxSels(xs, sels)
}

func uint8MaxSelsPure(xs []uint8, sels []int64) uint8 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x > res {
			res = x
		}
	}
	return res
}

func Uint16Max(xs []uint16) uint16 {
	return uint16Max(xs)
}

func uint16MaxPure(xs []uint16) uint16 {
	res := xs[0]
	for _, x := range xs {
		if x > res {
			res = x
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

func Uint16MaxSels(xs []uint16, sels []int64) uint16 {
	return uint16MaxSels(xs, sels)
}

func uint16MaxSelsPure(xs []uint16, sels []int64) uint16 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x > res {
			res = x
		}
	}
	return res
}

func Uint32Max(xs []uint32) uint32 {
	return uint32Max(xs)
}

func uint32MaxPure(xs []uint32) uint32 {
	res := xs[0]
	for _, x := range xs {
		if x > res {
			res = x
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

func Uint32MaxSels(xs []uint32, sels []int64) uint32 {
	return uint32MaxSels(xs, sels)
}

func uint32MaxSelsPure(xs []uint32, sels []int64) uint32 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x > res {
			res = x
		}
	}
	return res
}

func Uint64Max(xs []uint64) uint64 {
	return uint64Max(xs)
}

func uint64MaxPure(xs []uint64) uint64 {
	res := xs[0]
	for _, x := range xs {
		if x > res {
			res = x
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

func Uint64MaxSels(xs []uint64, sels []int64) uint64 {
	return uint64MaxSels(xs, sels)
}

func uint64MaxSelsPure(xs []uint64, sels []int64) uint64 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x > res {
			res = x
		}
	}
	return res
}

func Float32Max(xs []float32) float32 {
	return float32Max(xs)
}

func float32MaxPure(xs []float32) float32 {
	res := xs[0]
	for _, x := range xs {
		if x > res {
			res = x
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

func Float32MaxSels(xs []float32, sels []int64) float32 {
	return float32MaxSels(xs, sels)
}

func float32MaxSelsPure(xs []float32, sels []int64) float32 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x > res {
			res = x
		}
	}
	return res
}

func Float64Max(xs []float64) float64 {
	return float64Max(xs)
}

func float64MaxPure(xs []float64) float64 {
	res := xs[0]
	for _, x := range xs {
		if x > res {
			res = x
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

func Float64MaxSels(xs []float64, sels []int64) float64 {
	return float64MaxSels(xs, sels)
}

func float64MaxSelsPure(xs []float64, sels []int64) float64 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x > res {
			res = x
		}
	}
	return res
}

func StrMax(xs *types.Bytes) []byte {
	return strMax(xs)
}

func strMaxPure(xs *types.Bytes) []byte {
	res := xs.Get(0)
	for i, n := 0, len(xs.Offsets); i < n; i++ {
		x := xs.Get(i)
		if bytes.Compare(x, res) > 0 {
			res = x
		}
	}
	return res
}

func StrMaxSels(xs *types.Bytes, sels []int64) []byte {
	return strMaxSels(xs, sels)
}

func strMaxSelsPure(xs *types.Bytes, sels []int64) []byte {
	res := xs.Get(int(sels[0]))
	for _, sel := range sels {
		x := xs.Get(int(sel))
		if bytes.Compare(x, res) > 0 {
			res = x
		}
	}
	return res
}
