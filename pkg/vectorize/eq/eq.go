package eq

import (
	"bytes"
	"matrixbase/pkg/container/types"
)

var (
    int8Eq func([]int8, []int8, []int64) []int64
    int8EqSels func([]int8, []int8, []int64, []int64) []int64
    int8EqScalar func(int8, []int8, []int64) []int64
    int8EqScalarSels func(int8, []int8, []int64, []int64) []int64
    int16Eq func([]int16, []int16, []int64) []int64
    int16EqSels func([]int16, []int16, []int64, []int64) []int64
    int16EqScalar func(int16, []int16, []int64) []int64
    int16EqScalarSels func(int16, []int16, []int64, []int64) []int64
    int32Eq func([]int32, []int32, []int64) []int64
    int32EqSels func([]int32, []int32, []int64, []int64) []int64
    int32EqScalar func(int32, []int32, []int64) []int64
    int32EqScalarSels func(int32, []int32, []int64, []int64) []int64
    int64Eq func([]int64, []int64, []int64) []int64
    int64EqSels func([]int64, []int64, []int64, []int64) []int64
    int64EqScalar func(int64, []int64, []int64) []int64
    int64EqScalarSels func(int64, []int64, []int64, []int64) []int64
    uint8Eq func([]uint8, []uint8, []int64) []int64
    uint8EqSels func([]uint8, []uint8, []int64, []int64) []int64
    uint8EqScalar func(uint8, []uint8, []int64) []int64
    uint8EqScalarSels func(uint8, []uint8, []int64, []int64) []int64
    uint16Eq func([]uint16, []uint16, []int64) []int64
    uint16EqSels func([]uint16, []uint16, []int64, []int64) []int64
    uint16EqScalar func(uint16, []uint16, []int64) []int64
    uint16EqScalarSels func(uint16, []uint16, []int64, []int64) []int64
    uint32Eq func([]uint32, []uint32, []int64) []int64
    uint32EqSels func([]uint32, []uint32, []int64, []int64) []int64
    uint32EqScalar func(uint32, []uint32, []int64) []int64
    uint32EqScalarSels func(uint32, []uint32, []int64, []int64) []int64
    uint64Eq func([]uint64, []uint64, []int64) []int64
    uint64EqSels func([]uint64, []uint64, []int64, []int64) []int64
    uint64EqScalar func(uint64, []uint64, []int64) []int64
    uint64EqScalarSels func(uint64, []uint64, []int64, []int64) []int64
    float32Eq func([]float32, []float32, []int64) []int64
    float32EqSels func([]float32, []float32, []int64, []int64) []int64
    float32EqScalar func(float32, []float32, []int64) []int64
    float32EqScalarSels func(float32, []float32, []int64, []int64) []int64
    float64Eq func([]float64, []float64, []int64) []int64
    float64EqSels func([]float64, []float64, []int64, []int64) []int64
    float64EqScalar func(float64, []float64, []int64) []int64
    float64EqScalarSels func(float64, []float64, []int64, []int64) []int64
    strEq func(*types.Bytes, *types.Bytes, []int64) []int64
    strEqSels func(*types.Bytes, *types.Bytes, []int64, []int64) []int64
    strEqScalar func([]byte, *types.Bytes, []int64) []int64
    strEqScalarSels func([]byte, *types.Bytes, []int64, []int64) []int64
)

func init() {
	int8Eq = int8EqPure
	int8EqSels = int8EqSelsPure
	int8EqScalar = int8EqScalarPure
	int8EqScalarSels = int8EqScalarSelsPure
	int16Eq = int16EqPure
	int16EqSels = int16EqSelsPure
	int16EqScalar = int16EqScalarPure
	int16EqScalarSels = int16EqScalarSelsPure
	int32Eq = int32EqPure
	int32EqSels = int32EqSelsPure
	int32EqScalar = int32EqScalarPure
	int32EqScalarSels = int32EqScalarSelsPure
	int64Eq = int64EqPure
	int64EqSels = int64EqSelsPure
	int64EqScalar = int64EqScalarPure
	int64EqScalarSels = int64EqScalarSelsPure
	uint8Eq = uint8EqPure
	uint8EqSels = uint8EqSelsPure
	uint8EqScalar = uint8EqScalarPure
	uint8EqScalarSels = uint8EqScalarSelsPure
	uint16Eq = uint16EqPure
	uint16EqSels = uint16EqSelsPure
	uint16EqScalar = uint16EqScalarPure
	uint16EqScalarSels = uint16EqScalarSelsPure
	uint32Eq = uint32EqPure
	uint32EqSels = uint32EqSelsPure
	uint32EqScalar = uint32EqScalarPure
	uint32EqScalarSels = uint32EqScalarSelsPure
	uint64Eq = uint64EqPure
	uint64EqSels = uint64EqSelsPure
	uint64EqScalar = uint64EqScalarPure
	uint64EqScalarSels = uint64EqScalarSelsPure
	float32Eq = float32EqPure
	float32EqSels = float32EqSelsPure
	float32EqScalar = float32EqScalarPure
	float32EqScalarSels = float32EqScalarSelsPure
	float64Eq = float64EqPure
	float64EqSels = float64EqSelsPure
	float64EqScalar = float64EqScalarPure
	float64EqScalarSels = float64EqScalarSelsPure
	strEq = strEqPure
	strEqSels = strEqSelsPure
	strEqScalar = strEqScalarPure
	strEqScalarSels = strEqScalarSelsPure
}

func Int8Eq(xs, ys []int8, rs []int64) []int64 {
	return int8Eq(xs, ys, rs)
}

func int8EqPure(xs, ys []int8, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Int8EqSels(xs, ys []int8, rs, sels []int64) []int64 {
	return int8EqSels(xs, ys, rs, sels)
}

func int8EqSelsPure(xs, ys []int8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Int8EqScalar(x int8, ys []int8, rs []int64) []int64 {
	return int8EqScalar(x, ys, rs)
}

func int8EqScalarPure(x int8, ys []int8, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Int8EqScalarSels(x int8, ys []int8, rs, sels []int64) []int64 {
	return int8EqScalarSels(x, ys, rs, sels)
}

func int8EqScalarSelsPure(x int8, ys []int8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Int16Eq(xs, ys []int16, rs []int64) []int64 {
	return int16Eq(xs, ys, rs)
}

func int16EqPure(xs, ys []int16, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Int16EqSels(xs, ys []int16, rs, sels []int64) []int64 {
	return int16EqSels(xs, ys, rs, sels)
}

func int16EqSelsPure(xs, ys []int16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Int16EqScalar(x int16, ys []int16, rs []int64) []int64 {
	return int16EqScalar(x, ys, rs)
}

func int16EqScalarPure(x int16, ys []int16, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Int16EqScalarSels(x int16, ys []int16, rs, sels []int64) []int64 {
	return int16EqScalarSels(x, ys, rs, sels)
}

func int16EqScalarSelsPure(x int16, ys []int16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Int32Eq(xs, ys []int32, rs []int64) []int64 {
	return int32Eq(xs, ys, rs)
}

func int32EqPure(xs, ys []int32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Int32EqSels(xs, ys []int32, rs, sels []int64) []int64 {
	return int32EqSels(xs, ys, rs, sels)
}

func int32EqSelsPure(xs, ys []int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Int32EqScalar(x int32, ys []int32, rs []int64) []int64 {
	return int32EqScalar(x, ys, rs)
}

func int32EqScalarPure(x int32, ys []int32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Int32EqScalarSels(x int32, ys []int32, rs, sels []int64) []int64 {
	return int32EqScalarSels(x, ys, rs, sels)
}

func int32EqScalarSelsPure(x int32, ys []int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Int64Eq(xs, ys []int64, rs []int64) []int64 {
	return int64Eq(xs, ys, rs)
}

func int64EqPure(xs, ys []int64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Int64EqSels(xs, ys []int64, rs, sels []int64) []int64 {
	return int64EqSels(xs, ys, rs, sels)
}

func int64EqSelsPure(xs, ys []int64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Int64EqScalar(x int64, ys []int64, rs []int64) []int64 {
	return int64EqScalar(x, ys, rs)
}

func int64EqScalarPure(x int64, ys []int64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Int64EqScalarSels(x int64, ys []int64, rs, sels []int64) []int64 {
	return int64EqScalarSels(x, ys, rs, sels)
}

func int64EqScalarSelsPure(x int64, ys []int64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Uint8Eq(xs, ys []uint8, rs []int64) []int64 {
	return uint8Eq(xs, ys, rs)
}

func uint8EqPure(xs, ys []uint8, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Uint8EqSels(xs, ys []uint8, rs, sels []int64) []int64 {
	return uint8EqSels(xs, ys, rs, sels)
}

func uint8EqSelsPure(xs, ys []uint8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Uint8EqScalar(x uint8, ys []uint8, rs []int64) []int64 {
	return uint8EqScalar(x, ys, rs)
}

func uint8EqScalarPure(x uint8, ys []uint8, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Uint8EqScalarSels(x uint8, ys []uint8, rs, sels []int64) []int64 {
	return uint8EqScalarSels(x, ys, rs, sels)
}

func uint8EqScalarSelsPure(x uint8, ys []uint8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Uint16Eq(xs, ys []uint16, rs []int64) []int64 {
	return uint16Eq(xs, ys, rs)
}

func uint16EqPure(xs, ys []uint16, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Uint16EqSels(xs, ys []uint16, rs, sels []int64) []int64 {
	return uint16EqSels(xs, ys, rs, sels)
}

func uint16EqSelsPure(xs, ys []uint16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Uint16EqScalar(x uint16, ys []uint16, rs []int64) []int64 {
	return uint16EqScalar(x, ys, rs)
}

func uint16EqScalarPure(x uint16, ys []uint16, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Uint16EqScalarSels(x uint16, ys []uint16, rs, sels []int64) []int64 {
	return uint16EqScalarSels(x, ys, rs, sels)
}

func uint16EqScalarSelsPure(x uint16, ys []uint16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Uint32Eq(xs, ys []uint32, rs []int64) []int64 {
	return uint32Eq(xs, ys, rs)
}

func uint32EqPure(xs, ys []uint32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Uint32EqSels(xs, ys []uint32, rs, sels []int64) []int64 {
	return uint32EqSels(xs, ys, rs, sels)
}

func uint32EqSelsPure(xs, ys []uint32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Uint32EqScalar(x uint32, ys []uint32, rs []int64) []int64 {
	return uint32EqScalar(x, ys, rs)
}

func uint32EqScalarPure(x uint32, ys []uint32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Uint32EqScalarSels(x uint32, ys []uint32, rs, sels []int64) []int64 {
	return uint32EqScalarSels(x, ys, rs, sels)
}

func uint32EqScalarSelsPure(x uint32, ys []uint32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Uint64Eq(xs, ys []uint64, rs []int64) []int64 {
	return uint64Eq(xs, ys, rs)
}

func uint64EqPure(xs, ys []uint64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Uint64EqSels(xs, ys []uint64, rs, sels []int64) []int64 {
	return uint64EqSels(xs, ys, rs, sels)
}

func uint64EqSelsPure(xs, ys []uint64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Uint64EqScalar(x uint64, ys []uint64, rs []int64) []int64 {
	return uint64EqScalar(x, ys, rs)
}

func uint64EqScalarPure(x uint64, ys []uint64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Uint64EqScalarSels(x uint64, ys []uint64, rs, sels []int64) []int64 {
	return uint64EqScalarSels(x, ys, rs, sels)
}

func uint64EqScalarSelsPure(x uint64, ys []uint64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Float32Eq(xs, ys []float32, rs []int64) []int64 {
	return float32Eq(xs, ys, rs)
}

func float32EqPure(xs, ys []float32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Float32EqSels(xs, ys []float32, rs, sels []int64) []int64 {
	return float32EqSels(xs, ys, rs, sels)
}

func float32EqSelsPure(xs, ys []float32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Float32EqScalar(x float32, ys []float32, rs []int64) []int64 {
	return float32EqScalar(x, ys, rs)
}

func float32EqScalarPure(x float32, ys []float32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Float32EqScalarSels(x float32, ys []float32, rs, sels []int64) []int64 {
	return float32EqScalarSels(x, ys, rs, sels)
}

func float32EqScalarSelsPure(x float32, ys []float32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Float64Eq(xs, ys []float64, rs []int64) []int64 {
	return float64Eq(xs, ys, rs)
}

func float64EqPure(xs, ys []float64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Float64EqSels(xs, ys []float64, rs, sels []int64) []int64 {
	return float64EqSels(xs, ys, rs, sels)
}

func float64EqSelsPure(xs, ys []float64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Float64EqScalar(x float64, ys []float64, rs []int64) []int64 {
	return float64EqScalar(x, ys, rs)
}

func float64EqScalarPure(x float64, ys []float64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Float64EqScalarSels(x float64, ys []float64, rs, sels []int64) []int64 {
	return float64EqScalarSels(x, ys, rs, sels)
}

func float64EqScalarSelsPure(x float64, ys []float64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func StrEq(xs, ys *types.Bytes, rs []int64) []int64 {
	return strEq(xs, ys, rs)
}

func strEqPure(xs, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(xs.Offsets); i < n; i++ {
		if bytes.Compare(xs.Get(i), ys.Get(i)) == 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func StrEqSels(xs, ys *types.Bytes, rs, sels []int64) []int64 {
	return strEqSels(xs, ys, rs, sels)
}

func strEqSelsPure(xs, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(xs.Get(int(sel)), ys.Get(int(sel))) == 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func StrEqScalar(x []byte, ys *types.Bytes, rs []int64) []int64 {
	return strEqScalar(x, ys, rs)
}

func strEqScalarPure(x []byte, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(ys.Offsets); i < n; i++ {
		if bytes.Compare(x, ys.Get(i)) == 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func StrEqScalarSels(x []byte, ys *types.Bytes, rs, sels []int64) []int64 {
	return strEqScalarSels(x, ys, rs, sels)
}

func strEqScalarSelsPure(x []byte, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(x, ys.Get(int(sel))) == 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}
