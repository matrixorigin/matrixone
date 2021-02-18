package max

import (
	"bytes"
	"matrixbase/pkg/container/vector"
)

var (
	boolMax      func([]bool) bool
	i64Max       func([]int64) int64
	f64Max       func([]float64) float64
	bytesMax     func(*vector.Bytes) []byte
	boolMaxSels  func([]bool, []int64) bool
	i64MaxSels   func([]int64, []int64) int64
	f64MaxSels   func([]float64, []int64) float64
	bytesMaxSels func(*vector.Bytes, []int64) []byte
)

func init() {
	i64Max = i64MaxPure
	f64Max = f64MaxPure
	boolMax = boolMaxPure
	bytesMax = bytesMaxPure
	i64MaxSels = i64MaxSelsPure
	f64MaxSels = f64MaxSelsPure
	boolMaxSels = boolMaxSelsPure
	bytesMaxSels = bytesMaxSelsPure
}

func BoolMax(xs []bool) bool {
	return boolMaxPure(xs)
}

func I64Max(xs []int64) int64 {
	return i64Max(xs)
}

func F64Max(xs []float64) float64 {
	return f64Max(xs)
}

func BytesMax(xs *vector.Bytes) []byte {
	return bytesMax(xs)
}

func BoolMaxSels(xs []bool, sels []int64) bool {
	return boolMaxSelsPure(xs, sels)
}

func I64MaxSels(xs []int64, sels []int64) int64 {
	return i64MaxSels(xs, sels)
}

func F64MaxSels(xs []float64, sels []int64) float64 {
	return f64MaxSels(xs, sels)
}

func BytesMaxSels(xs *vector.Bytes, sels []int64) []byte {
	return bytesMaxSels(xs, sels)
}

func boolMaxPure(xs []bool) bool {
	for _, x := range xs {
		if x {
			return true
		}
	}
	return false
}

func i64MaxPure(xs []int64) int64 {
	max := xs[0]
	for _, x := range xs {
		if x > max {
			max = x
		}
	}
	return max
}

func f64MaxPure(xs []float64) float64 {
	max := xs[0]
	for _, x := range xs {
		if x > max {
			max = x
		}
	}
	return max
}

func bytesMaxPure(xs *vector.Bytes) []byte {
	var tm []byte
	var max []byte

	for i, o := range xs.Os {
		if tm = xs.Data[o : o+xs.Ns[i]]; bytes.Compare(tm, max) > 0 {
			max = tm
		}
	}
	return max
}

func boolMaxSelsPure(xs []bool, sels []int64) bool {
	for _, sel := range sels {
		if xs[sel] {
			return true
		}
	}
	return false
}

func i64MaxSelsPure(xs []int64, sels []int64) int64 {
	max := xs[sels[0]]
	for _, sel := range sels {
		if x := xs[sel]; x > max {
			max = x
		}
	}
	return max
}

func f64MaxSelsPure(xs []float64, sels []int64) float64 {
	max := xs[sels[0]]
	for _, sel := range sels {
		if x := xs[sel]; x > max {
			max = x
		}
	}
	return max
}

func bytesMaxSelsPure(xs *vector.Bytes, sels []int64) []byte {
	var tm []byte
	var max []byte

	for _, sel := range sels {
		if tm = xs.Data[xs.Os[sel] : xs.Os[sel]+xs.Ns[sel]]; bytes.Compare(tm, max) > 0 {
			max = tm
		}
	}
	return max
}
