package min

import (
	"bytes"
	"matrixone/pkg/container/types"
)

var (
	BoolMin func([]bool) bool
	BoolMinSels func([]bool, []int64) bool
	Int8Min func([]int8) int8
	Int8MinSels func([]int8, []int64) int8
	Int16Min func([]int16) int16
	Int16MinSels func([]int16, []int64) int16
	Int32Min func([]int32) int32
	Int32MinSels func([]int32, []int64) int32
	Int64Min func([]int64) int64
	Int64MinSels func([]int64, []int64) int64
	Uint8Min func([]uint8) uint8
	Uint8MinSels func([]uint8, []int64) uint8
	Uint16Min func([]uint16) uint16
	Uint16MinSels func([]uint16, []int64) uint16
	Uint32Min func([]uint32) uint32
	Uint32MinSels func([]uint32, []int64) uint32
	Uint64Min func([]uint64) uint64
	Uint64MinSels func([]uint64, []int64) uint64
	Float32Min func([]float32) float32
	Float32MinSels func([]float32, []int64) float32
	Float64Min func([]float64) float64
	Float64MinSels func([]float64, []int64) float64
	StrMin func(*types.Bytes) []byte
	StrMinSels func(*types.Bytes, []int64) []byte
)

func boolMin(xs []bool) bool {
	for _, x := range xs {
		if x == false {
			return false
		}
	}
	return true
}

func boolMinSels(xs []bool, sels []int64) bool {
	for _, sel := range sels {
		if xs[sel] == false {
			return false
		}
	}
	return true
}

func int8Min(xs []int8) int8 {
	res := xs[0]
	for _, x := range xs {
		if x < res {
			res = x
		}
	}
	return res
}

func int8MinSels(xs []int8, sels []int64) int8 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x < res {
			res = x
		}
	}
	return res
}

func int16Min(xs []int16) int16 {
	res := xs[0]
	for _, x := range xs {
		if x < res {
			res = x
		}
	}
	return res
}

func int16MinSels(xs []int16, sels []int64) int16 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x < res {
			res = x
		}
	}
	return res
}

func int32Min(xs []int32) int32 {
	res := xs[0]
	for _, x := range xs {
		if x < res {
			res = x
		}
	}
	return res
}

func int32MinSels(xs []int32, sels []int64) int32 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x < res {
			res = x
		}
	}
	return res
}

func int64Min(xs []int64) int64 {
	res := xs[0]
	for _, x := range xs {
		if x < res {
			res = x
		}
	}
	return res
}

func int64MinSels(xs []int64, sels []int64) int64 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x < res {
			res = x
		}
	}
	return res
}

func uint8Min(xs []uint8) uint8 {
	res := xs[0]
	for _, x := range xs {
		if x < res {
			res = x
		}
	}
	return res
}

func uint8MinSels(xs []uint8, sels []int64) uint8 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x < res {
			res = x
		}
	}
	return res
}

func uint16Min(xs []uint16) uint16 {
	res := xs[0]
	for _, x := range xs {
		if x < res {
			res = x
		}
	}
	return res
}

func uint16MinSels(xs []uint16, sels []int64) uint16 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x < res {
			res = x
		}
	}
	return res
}

func uint32Min(xs []uint32) uint32 {
	res := xs[0]
	for _, x := range xs {
		if x < res {
			res = x
		}
	}
	return res
}

func uint32MinSels(xs []uint32, sels []int64) uint32 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x < res {
			res = x
		}
	}
	return res
}

func uint64Min(xs []uint64) uint64 {
	res := xs[0]
	for _, x := range xs {
		if x < res {
			res = x
		}
	}
	return res
}

func uint64MinSels(xs []uint64, sels []int64) uint64 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x < res {
			res = x
		}
	}
	return res
}

func float32Min(xs []float32) float32 {
	res := xs[0]
	for _, x := range xs {
		if x < res {
			res = x
		}
	}
	return res
}

func float32MinSels(xs []float32, sels []int64) float32 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x < res {
			res = x
		}
	}
	return res
}

func float64Min(xs []float64) float64 {
	res := xs[0]
	for _, x := range xs {
		if x < res {
			res = x
		}
	}
	return res
}

func float64MinSels(xs []float64, sels []int64) float64 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x < res {
			res = x
		}
	}
	return res
}

func strMin(xs *types.Bytes) []byte {
	res := xs.Get(0)
	for i, n := 0, len(xs.Offsets); i < n; i++ {
		x := xs.Get(int64(i))
		if bytes.Compare(x, res) < 0 {
			res = x
		}
	}
	return res
}

func strMinSels(xs *types.Bytes, sels []int64) []byte {
	res := xs.Get(sels[0])
	for _, sel := range sels {
		x := xs.Get(sel)
		if bytes.Compare(x, res) < 0 {
			res = x
		}
	}
	return res
}
