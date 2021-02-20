package min

import (
	"bytes"
	"matrixbase/pkg/container/vector"
)

var (
	boolMin      func([]bool) bool
	i64Min       func([]int64) int64
	f64Min       func([]float64) float64
	bytesMin     func(*vector.Bytes) []byte
	boolMinSels  func([]bool, []int64) bool
	i64MinSels   func([]int64, []int64) int64
	f64MinSels   func([]float64, []int64) float64
	bytesMinSels func(*vector.Bytes, []int64) []byte
)

func init() {
	i64Min = i64MinPure
	f64Min = f64MinPure
	boolMin = boolMinPure
	bytesMin = bytesMinPure
	i64MinSels = i64MinSelsPure
	f64MinSels = f64MinSelsPure
	boolMinSels = boolMinSelsPure
	bytesMinSels = bytesMinSelsPure
}

func BoolMin(xs []bool) bool {
	return boolMinPure(xs)
}

func I64Min(xs []int64) int64 {
	return i64Min(xs)
}

func F64Min(xs []float64) float64 {
	return f64Min(xs)
}

func BytesMin(xs *vector.Bytes) []byte {
	return bytesMin(xs)
}

func BoolMinSels(xs []bool, sels []int64) bool {
	return boolMinSelsPure(xs, sels)
}

func I64MinSels(xs []int64, sels []int64) int64 {
	return i64MinSels(xs, sels)
}

func F64MinSels(xs []float64, sels []int64) float64 {
	return f64MinSels(xs, sels)
}

func BytesMinSels(xs *vector.Bytes, sels []int64) []byte {
	return bytesMinSels(xs, sels)
}

func boolMinPure(xs []bool) bool {
	for _, x := range xs {
		if !x {
			return false
		}
	}
	return true
}

func i64MinPure(xs []int64) int64 {
	min := xs[0]
	for _, x := range xs {
		if x < min {
			min = x
		}
	}
	return min
}

func f64MinPure(xs []float64) float64 {
	min := xs[0]
	for _, x := range xs {
		if x < min {
			min = x
		}
	}
	return min
}

func bytesMinPure(xs *vector.Bytes) []byte {
	var tm []byte

	min := xs.Data[xs.Os[0] : xs.Os[0]+xs.Ns[0]]
	for i, o := range xs.Os {
		if tm = xs.Data[o : o+xs.Ns[i]]; bytes.Compare(tm, min) < 0 {
			min = tm
		}
	}
	return min
}

func boolMinSelsPure(xs []bool, sels []int64) bool {
	for _, sel := range sels {
		if !xs[sel] {
			return false
		}
	}
	return true
}

func i64MinSelsPure(xs []int64, sels []int64) int64 {
	min := xs[sels[0]]
	for _, sel := range sels {
		if x := xs[sel]; x < min {
			min = x
		}
	}
	return min
}

func f64MinSelsPure(xs []float64, sels []int64) float64 {
	min := xs[sels[0]]
	for _, sel := range sels {
		if x := xs[sel]; x < min {
			min = x
		}
	}
	return min
}

func bytesMinSelsPure(xs *vector.Bytes, sels []int64) []byte {
	var tm []byte

	min := xs.Data[xs.Os[sels[0]] : xs.Os[sels[0]]+xs.Ns[sels[0]]]
	for _, sel := range sels {
		if tm = xs.Data[xs.Os[sel] : xs.Os[sel]+xs.Ns[sel]]; bytes.Compare(tm, min) < 0 {
			min = tm
		}
	}
	return min
}
