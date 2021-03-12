package ge

import (
	"bytes"
	"matrixbase/pkg/container/types"
)

var (
    int8Ge func([]int8, []int8, []int64) []int64
    int8GeSels func([]int8, []int8, []int64, []int64) []int64
    int8GeScalar func(int8, []int8, []int64) []int64
    int8GeScalarSels func(int8, []int8, []int64, []int64) []int64
    int16Ge func([]int16, []int16, []int64) []int64
    int16GeSels func([]int16, []int16, []int64, []int64) []int64
    int16GeScalar func(int16, []int16, []int64) []int64
    int16GeScalarSels func(int16, []int16, []int64, []int64) []int64
    int32Ge func([]int32, []int32, []int64) []int64
    int32GeSels func([]int32, []int32, []int64, []int64) []int64
    int32GeScalar func(int32, []int32, []int64) []int64
    int32GeScalarSels func(int32, []int32, []int64, []int64) []int64
    int64Ge func([]int64, []int64, []int64) []int64
    int64GeSels func([]int64, []int64, []int64, []int64) []int64
    int64GeScalar func(int64, []int64, []int64) []int64
    int64GeScalarSels func(int64, []int64, []int64, []int64) []int64
    uint8Ge func([]uint8, []uint8, []int64) []int64
    uint8GeSels func([]uint8, []uint8, []int64, []int64) []int64
    uint8GeScalar func(uint8, []uint8, []int64) []int64
    uint8GeScalarSels func(uint8, []uint8, []int64, []int64) []int64
    uint16Ge func([]uint16, []uint16, []int64) []int64
    uint16GeSels func([]uint16, []uint16, []int64, []int64) []int64
    uint16GeScalar func(uint16, []uint16, []int64) []int64
    uint16GeScalarSels func(uint16, []uint16, []int64, []int64) []int64
    uint32Ge func([]uint32, []uint32, []int64) []int64
    uint32GeSels func([]uint32, []uint32, []int64, []int64) []int64
    uint32GeScalar func(uint32, []uint32, []int64) []int64
    uint32GeScalarSels func(uint32, []uint32, []int64, []int64) []int64
    uint64Ge func([]uint64, []uint64, []int64) []int64
    uint64GeSels func([]uint64, []uint64, []int64, []int64) []int64
    uint64GeScalar func(uint64, []uint64, []int64) []int64
    uint64GeScalarSels func(uint64, []uint64, []int64, []int64) []int64
    float32Ge func([]float32, []float32, []int64) []int64
    float32GeSels func([]float32, []float32, []int64, []int64) []int64
    float32GeScalar func(float32, []float32, []int64) []int64
    float32GeScalarSels func(float32, []float32, []int64, []int64) []int64
    float64Ge func([]float64, []float64, []int64) []int64
    float64GeSels func([]float64, []float64, []int64, []int64) []int64
    float64GeScalar func(float64, []float64, []int64) []int64
    float64GeScalarSels func(float64, []float64, []int64, []int64) []int64
    strGe func(*types.Bytes, *types.Bytes, []int64) []int64
    strGeSels func(*types.Bytes, *types.Bytes, []int64, []int64) []int64
    strGeScalar func([]byte, *types.Bytes, []int64) []int64
    strGeScalarSels func([]byte, *types.Bytes, []int64, []int64) []int64
)

func init() {
	int8Ge = int8GePure
	int8GeSels = int8GeSelsPure
	int8GeScalar = int8GeScalarPure
	int8GeScalarSels = int8GeScalarSelsPure
	int16Ge = int16GePure
	int16GeSels = int16GeSelsPure
	int16GeScalar = int16GeScalarPure
	int16GeScalarSels = int16GeScalarSelsPure
	int32Ge = int32GePure
	int32GeSels = int32GeSelsPure
	int32GeScalar = int32GeScalarPure
	int32GeScalarSels = int32GeScalarSelsPure
	int64Ge = int64GePure
	int64GeSels = int64GeSelsPure
	int64GeScalar = int64GeScalarPure
	int64GeScalarSels = int64GeScalarSelsPure
	uint8Ge = uint8GePure
	uint8GeSels = uint8GeSelsPure
	uint8GeScalar = uint8GeScalarPure
	uint8GeScalarSels = uint8GeScalarSelsPure
	uint16Ge = uint16GePure
	uint16GeSels = uint16GeSelsPure
	uint16GeScalar = uint16GeScalarPure
	uint16GeScalarSels = uint16GeScalarSelsPure
	uint32Ge = uint32GePure
	uint32GeSels = uint32GeSelsPure
	uint32GeScalar = uint32GeScalarPure
	uint32GeScalarSels = uint32GeScalarSelsPure
	uint64Ge = uint64GePure
	uint64GeSels = uint64GeSelsPure
	uint64GeScalar = uint64GeScalarPure
	uint64GeScalarSels = uint64GeScalarSelsPure
	float32Ge = float32GePure
	float32GeSels = float32GeSelsPure
	float32GeScalar = float32GeScalarPure
	float32GeScalarSels = float32GeScalarSelsPure
	float64Ge = float64GePure
	float64GeSels = float64GeSelsPure
	float64GeScalar = float64GeScalarPure
	float64GeScalarSels = float64GeScalarSelsPure
	strGe = strGePure
	strGeSels = strGeSelsPure
	strGeScalar = strGeScalarPure
	strGeScalarSels = strGeScalarSelsPure
}

func Int8Ge(xs, ys []int8, rs []int64) []int64 {
	return int8Ge(xs, ys, rs)
}

func int8GePure(xs, ys []int8, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Int8GeSels(xs, ys []int8, rs, sels []int64) []int64 {
	return int8GeSels(xs, ys, rs, sels)
}

func int8GeSelsPure(xs, ys []int8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Int8GeScalar(x int8, ys []int8, rs []int64) []int64 {
	return int8GeScalar(x, ys, rs)
}

func int8GeScalarPure(x int8, ys []int8, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Int8GeScalarSels(x int8, ys []int8, rs, sels []int64) []int64 {
	return int8GeScalarSels(x, ys, rs, sels)
}

func int8GeScalarSelsPure(x int8, ys []int8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Int16Ge(xs, ys []int16, rs []int64) []int64 {
	return int16Ge(xs, ys, rs)
}

func int16GePure(xs, ys []int16, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Int16GeSels(xs, ys []int16, rs, sels []int64) []int64 {
	return int16GeSels(xs, ys, rs, sels)
}

func int16GeSelsPure(xs, ys []int16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Int16GeScalar(x int16, ys []int16, rs []int64) []int64 {
	return int16GeScalar(x, ys, rs)
}

func int16GeScalarPure(x int16, ys []int16, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Int16GeScalarSels(x int16, ys []int16, rs, sels []int64) []int64 {
	return int16GeScalarSels(x, ys, rs, sels)
}

func int16GeScalarSelsPure(x int16, ys []int16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Int32Ge(xs, ys []int32, rs []int64) []int64 {
	return int32Ge(xs, ys, rs)
}

func int32GePure(xs, ys []int32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Int32GeSels(xs, ys []int32, rs, sels []int64) []int64 {
	return int32GeSels(xs, ys, rs, sels)
}

func int32GeSelsPure(xs, ys []int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Int32GeScalar(x int32, ys []int32, rs []int64) []int64 {
	return int32GeScalar(x, ys, rs)
}

func int32GeScalarPure(x int32, ys []int32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Int32GeScalarSels(x int32, ys []int32, rs, sels []int64) []int64 {
	return int32GeScalarSels(x, ys, rs, sels)
}

func int32GeScalarSelsPure(x int32, ys []int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Int64Ge(xs, ys []int64, rs []int64) []int64 {
	return int64Ge(xs, ys, rs)
}

func int64GePure(xs, ys []int64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Int64GeSels(xs, ys []int64, rs, sels []int64) []int64 {
	return int64GeSels(xs, ys, rs, sels)
}

func int64GeSelsPure(xs, ys []int64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Int64GeScalar(x int64, ys []int64, rs []int64) []int64 {
	return int64GeScalar(x, ys, rs)
}

func int64GeScalarPure(x int64, ys []int64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Int64GeScalarSels(x int64, ys []int64, rs, sels []int64) []int64 {
	return int64GeScalarSels(x, ys, rs, sels)
}

func int64GeScalarSelsPure(x int64, ys []int64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Uint8Ge(xs, ys []uint8, rs []int64) []int64 {
	return uint8Ge(xs, ys, rs)
}

func uint8GePure(xs, ys []uint8, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Uint8GeSels(xs, ys []uint8, rs, sels []int64) []int64 {
	return uint8GeSels(xs, ys, rs, sels)
}

func uint8GeSelsPure(xs, ys []uint8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Uint8GeScalar(x uint8, ys []uint8, rs []int64) []int64 {
	return uint8GeScalar(x, ys, rs)
}

func uint8GeScalarPure(x uint8, ys []uint8, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Uint8GeScalarSels(x uint8, ys []uint8, rs, sels []int64) []int64 {
	return uint8GeScalarSels(x, ys, rs, sels)
}

func uint8GeScalarSelsPure(x uint8, ys []uint8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Uint16Ge(xs, ys []uint16, rs []int64) []int64 {
	return uint16Ge(xs, ys, rs)
}

func uint16GePure(xs, ys []uint16, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Uint16GeSels(xs, ys []uint16, rs, sels []int64) []int64 {
	return uint16GeSels(xs, ys, rs, sels)
}

func uint16GeSelsPure(xs, ys []uint16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Uint16GeScalar(x uint16, ys []uint16, rs []int64) []int64 {
	return uint16GeScalar(x, ys, rs)
}

func uint16GeScalarPure(x uint16, ys []uint16, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Uint16GeScalarSels(x uint16, ys []uint16, rs, sels []int64) []int64 {
	return uint16GeScalarSels(x, ys, rs, sels)
}

func uint16GeScalarSelsPure(x uint16, ys []uint16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Uint32Ge(xs, ys []uint32, rs []int64) []int64 {
	return uint32Ge(xs, ys, rs)
}

func uint32GePure(xs, ys []uint32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Uint32GeSels(xs, ys []uint32, rs, sels []int64) []int64 {
	return uint32GeSels(xs, ys, rs, sels)
}

func uint32GeSelsPure(xs, ys []uint32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Uint32GeScalar(x uint32, ys []uint32, rs []int64) []int64 {
	return uint32GeScalar(x, ys, rs)
}

func uint32GeScalarPure(x uint32, ys []uint32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Uint32GeScalarSels(x uint32, ys []uint32, rs, sels []int64) []int64 {
	return uint32GeScalarSels(x, ys, rs, sels)
}

func uint32GeScalarSelsPure(x uint32, ys []uint32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Uint64Ge(xs, ys []uint64, rs []int64) []int64 {
	return uint64Ge(xs, ys, rs)
}

func uint64GePure(xs, ys []uint64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Uint64GeSels(xs, ys []uint64, rs, sels []int64) []int64 {
	return uint64GeSels(xs, ys, rs, sels)
}

func uint64GeSelsPure(xs, ys []uint64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Uint64GeScalar(x uint64, ys []uint64, rs []int64) []int64 {
	return uint64GeScalar(x, ys, rs)
}

func uint64GeScalarPure(x uint64, ys []uint64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Uint64GeScalarSels(x uint64, ys []uint64, rs, sels []int64) []int64 {
	return uint64GeScalarSels(x, ys, rs, sels)
}

func uint64GeScalarSelsPure(x uint64, ys []uint64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Float32Ge(xs, ys []float32, rs []int64) []int64 {
	return float32Ge(xs, ys, rs)
}

func float32GePure(xs, ys []float32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Float32GeSels(xs, ys []float32, rs, sels []int64) []int64 {
	return float32GeSels(xs, ys, rs, sels)
}

func float32GeSelsPure(xs, ys []float32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Float32GeScalar(x float32, ys []float32, rs []int64) []int64 {
	return float32GeScalar(x, ys, rs)
}

func float32GeScalarPure(x float32, ys []float32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Float32GeScalarSels(x float32, ys []float32, rs, sels []int64) []int64 {
	return float32GeScalarSels(x, ys, rs, sels)
}

func float32GeScalarSelsPure(x float32, ys []float32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Float64Ge(xs, ys []float64, rs []int64) []int64 {
	return float64Ge(xs, ys, rs)
}

func float64GePure(xs, ys []float64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Float64GeSels(xs, ys []float64, rs, sels []int64) []int64 {
	return float64GeSels(xs, ys, rs, sels)
}

func float64GeSelsPure(xs, ys []float64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Float64GeScalar(x float64, ys []float64, rs []int64) []int64 {
	return float64GeScalar(x, ys, rs)
}

func float64GeScalarPure(x float64, ys []float64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Float64GeScalarSels(x float64, ys []float64, rs, sels []int64) []int64 {
	return float64GeScalarSels(x, ys, rs, sels)
}

func float64GeScalarSelsPure(x float64, ys []float64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func StrGe(xs, ys *types.Bytes, rs []int64) []int64 {
	return strGe(xs, ys, rs)
}

func strGePure(xs, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(xs.Offsets); i < n; i++ {
		if bytes.Compare(xs.Get(i), ys.Get(i)) >= 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func StrGeSels(xs, ys *types.Bytes, rs, sels []int64) []int64 {
	return strGeSels(xs, ys, rs, sels)
}

func strGeSelsPure(xs, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(xs.Get(int(sel)), ys.Get(int(sel))) >= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func StrGeScalar(x []byte, ys *types.Bytes, rs []int64) []int64 {
	return strGeScalar(x, ys, rs)
}

func strGeScalarPure(x []byte, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(ys.Offsets); i < n; i++ {
		if bytes.Compare(x, ys.Get(i)) >= 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func StrGeScalarSels(x []byte, ys *types.Bytes, rs, sels []int64) []int64 {
	return strGeScalarSels(x, ys, rs, sels)
}

func strGeScalarSelsPure(x []byte, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(x, ys.Get(int(sel))) >= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}
