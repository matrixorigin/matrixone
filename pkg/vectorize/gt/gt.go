package gt

import (
	"bytes"
	"matrixone/pkg/container/types"
)

var (
	int8Gt              func([]int8, []int8, []int64) []int64
	int8GtSels          func([]int8, []int8, []int64, []int64) []int64
	int8GtScalar        func(int8, []int8, []int64) []int64
	int8GtScalarSels    func(int8, []int8, []int64, []int64) []int64
	int16Gt             func([]int16, []int16, []int64) []int64
	int16GtSels         func([]int16, []int16, []int64, []int64) []int64
	int16GtScalar       func(int16, []int16, []int64) []int64
	int16GtScalarSels   func(int16, []int16, []int64, []int64) []int64
	int32Gt             func([]int32, []int32, []int64) []int64
	int32GtSels         func([]int32, []int32, []int64, []int64) []int64
	int32GtScalar       func(int32, []int32, []int64) []int64
	int32GtScalarSels   func(int32, []int32, []int64, []int64) []int64
	int64Gt             func([]int64, []int64, []int64) []int64
	int64GtSels         func([]int64, []int64, []int64, []int64) []int64
	int64GtScalar       func(int64, []int64, []int64) []int64
	int64GtScalarSels   func(int64, []int64, []int64, []int64) []int64
	uint8Gt             func([]uint8, []uint8, []int64) []int64
	uint8GtSels         func([]uint8, []uint8, []int64, []int64) []int64
	uint8GtScalar       func(uint8, []uint8, []int64) []int64
	uint8GtScalarSels   func(uint8, []uint8, []int64, []int64) []int64
	uint16Gt            func([]uint16, []uint16, []int64) []int64
	uint16GtSels        func([]uint16, []uint16, []int64, []int64) []int64
	uint16GtScalar      func(uint16, []uint16, []int64) []int64
	uint16GtScalarSels  func(uint16, []uint16, []int64, []int64) []int64
	uint32Gt            func([]uint32, []uint32, []int64) []int64
	uint32GtSels        func([]uint32, []uint32, []int64, []int64) []int64
	uint32GtScalar      func(uint32, []uint32, []int64) []int64
	uint32GtScalarSels  func(uint32, []uint32, []int64, []int64) []int64
	uint64Gt            func([]uint64, []uint64, []int64) []int64
	uint64GtSels        func([]uint64, []uint64, []int64, []int64) []int64
	uint64GtScalar      func(uint64, []uint64, []int64) []int64
	uint64GtScalarSels  func(uint64, []uint64, []int64, []int64) []int64
	float32Gt           func([]float32, []float32, []int64) []int64
	float32GtSels       func([]float32, []float32, []int64, []int64) []int64
	float32GtScalar     func(float32, []float32, []int64) []int64
	float32GtScalarSels func(float32, []float32, []int64, []int64) []int64
	float64Gt           func([]float64, []float64, []int64) []int64
	float64GtSels       func([]float64, []float64, []int64, []int64) []int64
	float64GtScalar     func(float64, []float64, []int64) []int64
	float64GtScalarSels func(float64, []float64, []int64, []int64) []int64
	strGt               func(*types.Bytes, *types.Bytes, []int64) []int64
	strGtSels           func(*types.Bytes, *types.Bytes, []int64, []int64) []int64
	strGtScalar         func([]byte, *types.Bytes, []int64) []int64
	strGtScalarSels     func([]byte, *types.Bytes, []int64, []int64) []int64
)

func init() {
	int8Gt = int8GtPure
	int8GtSels = int8GtSelsPure
	int8GtScalar = int8GtScalarPure
	int8GtScalarSels = int8GtScalarSelsPure
	int16Gt = int16GtPure
	int16GtSels = int16GtSelsPure
	int16GtScalar = int16GtScalarPure
	int16GtScalarSels = int16GtScalarSelsPure
	int32Gt = int32GtPure
	int32GtSels = int32GtSelsPure
	int32GtScalar = int32GtScalarPure
	int32GtScalarSels = int32GtScalarSelsPure
	int64Gt = int64GtPure
	int64GtSels = int64GtSelsPure
	int64GtScalar = int64GtScalarPure
	int64GtScalarSels = int64GtScalarSelsPure
	uint8Gt = uint8GtPure
	uint8GtSels = uint8GtSelsPure
	uint8GtScalar = uint8GtScalarPure
	uint8GtScalarSels = uint8GtScalarSelsPure
	uint16Gt = uint16GtPure
	uint16GtSels = uint16GtSelsPure
	uint16GtScalar = uint16GtScalarPure
	uint16GtScalarSels = uint16GtScalarSelsPure
	uint32Gt = uint32GtPure
	uint32GtSels = uint32GtSelsPure
	uint32GtScalar = uint32GtScalarPure
	uint32GtScalarSels = uint32GtScalarSelsPure
	uint64Gt = uint64GtPure
	uint64GtSels = uint64GtSelsPure
	uint64GtScalar = uint64GtScalarPure
	uint64GtScalarSels = uint64GtScalarSelsPure
	float32Gt = float32GtPure
	float32GtSels = float32GtSelsPure
	float32GtScalar = float32GtScalarPure
	float32GtScalarSels = float32GtScalarSelsPure
	float64Gt = float64GtPure
	float64GtSels = float64GtSelsPure
	float64GtScalar = float64GtScalarPure
	float64GtScalarSels = float64GtScalarSelsPure
	strGt = strGtPure
	strGtSels = strGtSelsPure
	strGtScalar = strGtScalarPure
	strGtScalarSels = strGtScalarSelsPure
}

func Int8Gt(xs, ys []int8, rs []int64) []int64 {
	return int8Gt(xs, ys, rs)
}

func int8GtPure(xs, ys []int8, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x > ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Int8GtSels(xs, ys []int8, rs, sels []int64) []int64 {
	return int8GtSels(xs, ys, rs, sels)
}

func int8GtSelsPure(xs, ys []int8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Int8GtScalar(x int8, ys []int8, rs []int64) []int64 {
	return int8GtScalar(x, ys, rs)
}

func int8GtScalarPure(x int8, ys []int8, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x > y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Int8GtScalarSels(x int8, ys []int8, rs, sels []int64) []int64 {
	return int8GtScalarSels(x, ys, rs, sels)
}

func int8GtScalarSelsPure(x int8, ys []int8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Int16Gt(xs, ys []int16, rs []int64) []int64 {
	return int16Gt(xs, ys, rs)
}

func int16GtPure(xs, ys []int16, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x > ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Int16GtSels(xs, ys []int16, rs, sels []int64) []int64 {
	return int16GtSels(xs, ys, rs, sels)
}

func int16GtSelsPure(xs, ys []int16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Int16GtScalar(x int16, ys []int16, rs []int64) []int64 {
	return int16GtScalar(x, ys, rs)
}

func int16GtScalarPure(x int16, ys []int16, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x > y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Int16GtScalarSels(x int16, ys []int16, rs, sels []int64) []int64 {
	return int16GtScalarSels(x, ys, rs, sels)
}

func int16GtScalarSelsPure(x int16, ys []int16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Int32Gt(xs, ys []int32, rs []int64) []int64 {
	return int32Gt(xs, ys, rs)
}

func int32GtPure(xs, ys []int32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x > ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Int32GtSels(xs, ys []int32, rs, sels []int64) []int64 {
	return int32GtSels(xs, ys, rs, sels)
}

func int32GtSelsPure(xs, ys []int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Int32GtScalar(x int32, ys []int32, rs []int64) []int64 {
	return int32GtScalar(x, ys, rs)
}

func int32GtScalarPure(x int32, ys []int32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x > y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Int32GtScalarSels(x int32, ys []int32, rs, sels []int64) []int64 {
	return int32GtScalarSels(x, ys, rs, sels)
}

func int32GtScalarSelsPure(x int32, ys []int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Int64Gt(xs, ys []int64, rs []int64) []int64 {
	return int64Gt(xs, ys, rs)
}

func int64GtPure(xs, ys []int64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x > ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Int64GtSels(xs, ys []int64, rs, sels []int64) []int64 {
	return int64GtSels(xs, ys, rs, sels)
}

func int64GtSelsPure(xs, ys []int64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Int64GtScalar(x int64, ys []int64, rs []int64) []int64 {
	return int64GtScalar(x, ys, rs)
}

func int64GtScalarPure(x int64, ys []int64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x > y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Int64GtScalarSels(x int64, ys []int64, rs, sels []int64) []int64 {
	return int64GtScalarSels(x, ys, rs, sels)
}

func int64GtScalarSelsPure(x int64, ys []int64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint8Gt(xs, ys []uint8, rs []int64) []int64 {
	return uint8Gt(xs, ys, rs)
}

func uint8GtPure(xs, ys []uint8, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x > ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint8GtSels(xs, ys []uint8, rs, sels []int64) []int64 {
	return uint8GtSels(xs, ys, rs, sels)
}

func uint8GtSelsPure(xs, ys []uint8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint8GtScalar(x uint8, ys []uint8, rs []int64) []int64 {
	return uint8GtScalar(x, ys, rs)
}

func uint8GtScalarPure(x uint8, ys []uint8, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x > y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint8GtScalarSels(x uint8, ys []uint8, rs, sels []int64) []int64 {
	return uint8GtScalarSels(x, ys, rs, sels)
}

func uint8GtScalarSelsPure(x uint8, ys []uint8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint16Gt(xs, ys []uint16, rs []int64) []int64 {
	return uint16Gt(xs, ys, rs)
}

func uint16GtPure(xs, ys []uint16, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x > ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint16GtSels(xs, ys []uint16, rs, sels []int64) []int64 {
	return uint16GtSels(xs, ys, rs, sels)
}

func uint16GtSelsPure(xs, ys []uint16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint16GtScalar(x uint16, ys []uint16, rs []int64) []int64 {
	return uint16GtScalar(x, ys, rs)
}

func uint16GtScalarPure(x uint16, ys []uint16, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x > y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint16GtScalarSels(x uint16, ys []uint16, rs, sels []int64) []int64 {
	return uint16GtScalarSels(x, ys, rs, sels)
}

func uint16GtScalarSelsPure(x uint16, ys []uint16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint32Gt(xs, ys []uint32, rs []int64) []int64 {
	return uint32Gt(xs, ys, rs)
}

func uint32GtPure(xs, ys []uint32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x > ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint32GtSels(xs, ys []uint32, rs, sels []int64) []int64 {
	return uint32GtSels(xs, ys, rs, sels)
}

func uint32GtSelsPure(xs, ys []uint32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint32GtScalar(x uint32, ys []uint32, rs []int64) []int64 {
	return uint32GtScalar(x, ys, rs)
}

func uint32GtScalarPure(x uint32, ys []uint32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x > y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint32GtScalarSels(x uint32, ys []uint32, rs, sels []int64) []int64 {
	return uint32GtScalarSels(x, ys, rs, sels)
}

func uint32GtScalarSelsPure(x uint32, ys []uint32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint64Gt(xs, ys []uint64, rs []int64) []int64 {
	return uint64Gt(xs, ys, rs)
}

func uint64GtPure(xs, ys []uint64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x > ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint64GtSels(xs, ys []uint64, rs, sels []int64) []int64 {
	return uint64GtSels(xs, ys, rs, sels)
}

func uint64GtSelsPure(xs, ys []uint64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint64GtScalar(x uint64, ys []uint64, rs []int64) []int64 {
	return uint64GtScalar(x, ys, rs)
}

func uint64GtScalarPure(x uint64, ys []uint64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x > y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint64GtScalarSels(x uint64, ys []uint64, rs, sels []int64) []int64 {
	return uint64GtScalarSels(x, ys, rs, sels)
}

func uint64GtScalarSelsPure(x uint64, ys []uint64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Float32Gt(xs, ys []float32, rs []int64) []int64 {
	return float32Gt(xs, ys, rs)
}

func float32GtPure(xs, ys []float32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x > ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Float32GtSels(xs, ys []float32, rs, sels []int64) []int64 {
	return float32GtSels(xs, ys, rs, sels)
}

func float32GtSelsPure(xs, ys []float32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Float32GtScalar(x float32, ys []float32, rs []int64) []int64 {
	return float32GtScalar(x, ys, rs)
}

func float32GtScalarPure(x float32, ys []float32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x > y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Float32GtScalarSels(x float32, ys []float32, rs, sels []int64) []int64 {
	return float32GtScalarSels(x, ys, rs, sels)
}

func float32GtScalarSelsPure(x float32, ys []float32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Float64Gt(xs, ys []float64, rs []int64) []int64 {
	return float64Gt(xs, ys, rs)
}

func float64GtPure(xs, ys []float64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x > ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Float64GtSels(xs, ys []float64, rs, sels []int64) []int64 {
	return float64GtSels(xs, ys, rs, sels)
}

func float64GtSelsPure(xs, ys []float64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Float64GtScalar(x float64, ys []float64, rs []int64) []int64 {
	return float64GtScalar(x, ys, rs)
}

func float64GtScalarPure(x float64, ys []float64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x > y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Float64GtScalarSels(x float64, ys []float64, rs, sels []int64) []int64 {
	return float64GtScalarSels(x, ys, rs, sels)
}

func float64GtScalarSelsPure(x float64, ys []float64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func StrGt(xs, ys *types.Bytes, rs []int64) []int64 {
	return strGt(xs, ys, rs)
}

func strGtPure(xs, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(xs.Offsets); i < n; i++ {
		if bytes.Compare(xs.Get(int64(i)), ys.Get(int64(i))) > 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func StrGtSels(xs, ys *types.Bytes, rs, sels []int64) []int64 {
	return strGtSels(xs, ys, rs, sels)
}

func strGtSelsPure(xs, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(xs.Get(sel), ys.Get(sel)) > 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func StrGtScalar(x []byte, ys *types.Bytes, rs []int64) []int64 {
	return strGtScalar(x, ys, rs)
}

func strGtScalarPure(x []byte, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(ys.Offsets); i < n; i++ {
		if bytes.Compare(x, ys.Get(int64(i))) > 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func StrGtScalarSels(x []byte, ys *types.Bytes, rs, sels []int64) []int64 {
	return strGtScalarSels(x, ys, rs, sels)
}

func strGtScalarSelsPure(x []byte, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(x, ys.Get(sel)) > 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}
