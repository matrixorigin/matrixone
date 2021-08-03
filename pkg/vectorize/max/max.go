package max

import (
	"bytes"
	"matrixone/pkg/container/types"
)

var (
	BoolMax        func([]bool) bool
	BoolMaxSels    func([]bool, []int64) bool
	Int8Max        func([]int8) int8
	Int8MaxSels    func([]int8, []int64) int8
	Int16Max       func([]int16) int16
	Int16MaxSels   func([]int16, []int64) int16
	Int32Max       func([]int32) int32
	Int32MaxSels   func([]int32, []int64) int32
	Int64Max       func([]int64) int64
	Int64MaxSels   func([]int64, []int64) int64
	Uint8Max       func([]uint8) uint8
	Uint8MaxSels   func([]uint8, []int64) uint8
	Uint16Max      func([]uint16) uint16
	Uint16MaxSels  func([]uint16, []int64) uint16
	Uint32Max      func([]uint32) uint32
	Uint32MaxSels  func([]uint32, []int64) uint32
	Uint64Max      func([]uint64) uint64
	Uint64MaxSels  func([]uint64, []int64) uint64
	Float32Max     func([]float32) float32
	Float32MaxSels func([]float32, []int64) float32
	Float64Max     func([]float64) float64
	Float64MaxSels func([]float64, []int64) float64
	StrMax         func(*types.Bytes) []byte
	StrMaxSels     func(*types.Bytes, []int64) []byte
)

func boolMax(xs []bool) bool {
	for _, x := range xs {
		if x == true {
			return true
		}
	}
	return false
}

func boolMaxSels(xs []bool, sels []int64) bool {
	for _, sel := range sels {
		if xs[sel] == true {
			return true
		}
	}
	return false
}

func int8Max(xs []int8) int8 {
	res := xs[0]
	for _, x := range xs {
		if x > res {
			res = x
		}
	}
	return res
}

func int8MaxSels(xs []int8, sels []int64) int8 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x > res {
			res = x
		}
	}
	return res
}

func int16Max(xs []int16) int16 {
	res := xs[0]
	for _, x := range xs {
		if x > res {
			res = x
		}
	}
	return res
}

func int16MaxSels(xs []int16, sels []int64) int16 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x > res {
			res = x
		}
	}
	return res
}

func int32Max(xs []int32) int32 {
	res := xs[0]
	for _, x := range xs {
		if x > res {
			res = x
		}
	}
	return res
}

func int32MaxSels(xs []int32, sels []int64) int32 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x > res {
			res = x
		}
	}
	return res
}

func int64Max(xs []int64) int64 {
	res := xs[0]
	for _, x := range xs {
		if x > res {
			res = x
		}
	}
	return res
}

func int64MaxSels(xs []int64, sels []int64) int64 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x > res {
			res = x
		}
	}
	return res
}

func uint8Max(xs []uint8) uint8 {
	res := xs[0]
	for _, x := range xs {
		if x > res {
			res = x
		}
	}
	return res
}

func uint8MaxSels(xs []uint8, sels []int64) uint8 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x > res {
			res = x
		}
	}
	return res
}

func uint16Max(xs []uint16) uint16 {
	res := xs[0]
	for _, x := range xs {
		if x > res {
			res = x
		}
	}
	return res
}

func uint16MaxSels(xs []uint16, sels []int64) uint16 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x > res {
			res = x
		}
	}
	return res
}

func uint32Max(xs []uint32) uint32 {
	res := xs[0]
	for _, x := range xs {
		if x > res {
			res = x
		}
	}
	return res
}

func uint32MaxSels(xs []uint32, sels []int64) uint32 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x > res {
			res = x
		}
	}
	return res
}

func uint64Max(xs []uint64) uint64 {
	res := xs[0]
	for _, x := range xs {
		if x > res {
			res = x
		}
	}
	return res
}

func uint64MaxSels(xs []uint64, sels []int64) uint64 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x > res {
			res = x
		}
	}
	return res
}

func float32Max(xs []float32) float32 {
	res := xs[0]
	for _, x := range xs {
		if x > res {
			res = x
		}
	}
	return res
}

func float32MaxSels(xs []float32, sels []int64) float32 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x > res {
			res = x
		}
	}
	return res
}

func float64Max(xs []float64) float64 {
	res := xs[0]
	for _, x := range xs {
		if x > res {
			res = x
		}
	}
	return res
}

func float64MaxSels(xs []float64, sels []int64) float64 {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x > res {
			res = x
		}
	}
	return res
}

func strMax(xs *types.Bytes) []byte {
	res := xs.Get(0)
	for i, n := 0, len(xs.Offsets); i < n; i++ {
		x := xs.Get(int64(i))
		if bytes.Compare(x, res) > 0 {
			res = x
		}
	}
	return res
}

func strMaxSels(xs *types.Bytes, sels []int64) []byte {
	res := xs.Get(sels[0])
	for _, sel := range sels {
		x := xs.Get(sel)
		if bytes.Compare(x, res) > 0 {
			res = x
		}
	}
	return res
}
