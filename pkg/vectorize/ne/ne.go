package ne

import (
	"bytes"
	"matrixone/pkg/container/types"
)

var (
    int8Ne func([]int8, []int8, []int64) []int64
    int8NeSels func([]int8, []int8, []int64, []int64) []int64
    int8NeScalar func(int8, []int8, []int64) []int64
    int8NeScalarSels func(int8, []int8, []int64, []int64) []int64
    int16Ne func([]int16, []int16, []int64) []int64
    int16NeSels func([]int16, []int16, []int64, []int64) []int64
    int16NeScalar func(int16, []int16, []int64) []int64
    int16NeScalarSels func(int16, []int16, []int64, []int64) []int64
    int32Ne func([]int32, []int32, []int64) []int64
    int32NeSels func([]int32, []int32, []int64, []int64) []int64
    int32NeScalar func(int32, []int32, []int64) []int64
    int32NeScalarSels func(int32, []int32, []int64, []int64) []int64
    int64Ne func([]int64, []int64, []int64) []int64
    int64NeSels func([]int64, []int64, []int64, []int64) []int64
    int64NeScalar func(int64, []int64, []int64) []int64
    int64NeScalarSels func(int64, []int64, []int64, []int64) []int64
    uint8Ne func([]uint8, []uint8, []int64) []int64
    uint8NeSels func([]uint8, []uint8, []int64, []int64) []int64
    uint8NeScalar func(uint8, []uint8, []int64) []int64
    uint8NeScalarSels func(uint8, []uint8, []int64, []int64) []int64
    uint16Ne func([]uint16, []uint16, []int64) []int64
    uint16NeSels func([]uint16, []uint16, []int64, []int64) []int64
    uint16NeScalar func(uint16, []uint16, []int64) []int64
    uint16NeScalarSels func(uint16, []uint16, []int64, []int64) []int64
    uint32Ne func([]uint32, []uint32, []int64) []int64
    uint32NeSels func([]uint32, []uint32, []int64, []int64) []int64
    uint32NeScalar func(uint32, []uint32, []int64) []int64
    uint32NeScalarSels func(uint32, []uint32, []int64, []int64) []int64
    uint64Ne func([]uint64, []uint64, []int64) []int64
    uint64NeSels func([]uint64, []uint64, []int64, []int64) []int64
    uint64NeScalar func(uint64, []uint64, []int64) []int64
    uint64NeScalarSels func(uint64, []uint64, []int64, []int64) []int64
    float32Ne func([]float32, []float32, []int64) []int64
    float32NeSels func([]float32, []float32, []int64, []int64) []int64
    float32NeScalar func(float32, []float32, []int64) []int64
    float32NeScalarSels func(float32, []float32, []int64, []int64) []int64
    float64Ne func([]float64, []float64, []int64) []int64
    float64NeSels func([]float64, []float64, []int64, []int64) []int64
    float64NeScalar func(float64, []float64, []int64) []int64
    float64NeScalarSels func(float64, []float64, []int64, []int64) []int64
    strNe func(*types.Bytes, *types.Bytes, []int64) []int64
    strNeSels func(*types.Bytes, *types.Bytes, []int64, []int64) []int64
    strNeScalar func([]byte, *types.Bytes, []int64) []int64
    strNeScalarSels func([]byte, *types.Bytes, []int64, []int64) []int64
)

func init() {
	int8Ne = int8NePure
	int8NeSels = int8NeSelsPure
	int8NeScalar = int8NeScalarPure
	int8NeScalarSels = int8NeScalarSelsPure
	int16Ne = int16NePure
	int16NeSels = int16NeSelsPure
	int16NeScalar = int16NeScalarPure
	int16NeScalarSels = int16NeScalarSelsPure
	int32Ne = int32NePure
	int32NeSels = int32NeSelsPure
	int32NeScalar = int32NeScalarPure
	int32NeScalarSels = int32NeScalarSelsPure
	int64Ne = int64NePure
	int64NeSels = int64NeSelsPure
	int64NeScalar = int64NeScalarPure
	int64NeScalarSels = int64NeScalarSelsPure
	uint8Ne = uint8NePure
	uint8NeSels = uint8NeSelsPure
	uint8NeScalar = uint8NeScalarPure
	uint8NeScalarSels = uint8NeScalarSelsPure
	uint16Ne = uint16NePure
	uint16NeSels = uint16NeSelsPure
	uint16NeScalar = uint16NeScalarPure
	uint16NeScalarSels = uint16NeScalarSelsPure
	uint32Ne = uint32NePure
	uint32NeSels = uint32NeSelsPure
	uint32NeScalar = uint32NeScalarPure
	uint32NeScalarSels = uint32NeScalarSelsPure
	uint64Ne = uint64NePure
	uint64NeSels = uint64NeSelsPure
	uint64NeScalar = uint64NeScalarPure
	uint64NeScalarSels = uint64NeScalarSelsPure
	float32Ne = float32NePure
	float32NeSels = float32NeSelsPure
	float32NeScalar = float32NeScalarPure
	float32NeScalarSels = float32NeScalarSelsPure
	float64Ne = float64NePure
	float64NeSels = float64NeSelsPure
	float64NeScalar = float64NeScalarPure
	float64NeScalarSels = float64NeScalarSelsPure
	strNe = strNePure
	strNeSels = strNeSelsPure
	strNeScalar = strNeScalarPure
	strNeScalarSels = strNeScalarSelsPure
}

func Int8Ne(xs, ys []int8, rs []int64) []int64 {
	return int8Ne(xs, ys, rs)
}

func int8NePure(xs, ys []int8, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x != ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Int8NeSels(xs, ys []int8, rs, sels []int64) []int64 {
	return int8NeSels(xs, ys, rs, sels)
}

func int8NeSelsPure(xs, ys []int8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Int8NeScalar(x int8, ys []int8, rs []int64) []int64 {
	return int8NeScalar(x, ys, rs)
}

func int8NeScalarPure(x int8, ys []int8, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x != y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Int8NeScalarSels(x int8, ys []int8, rs, sels []int64) []int64 {
	return int8NeScalarSels(x, ys, rs, sels)
}

func int8NeScalarSelsPure(x int8, ys []int8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Int16Ne(xs, ys []int16, rs []int64) []int64 {
	return int16Ne(xs, ys, rs)
}

func int16NePure(xs, ys []int16, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x != ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Int16NeSels(xs, ys []int16, rs, sels []int64) []int64 {
	return int16NeSels(xs, ys, rs, sels)
}

func int16NeSelsPure(xs, ys []int16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Int16NeScalar(x int16, ys []int16, rs []int64) []int64 {
	return int16NeScalar(x, ys, rs)
}

func int16NeScalarPure(x int16, ys []int16, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x != y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Int16NeScalarSels(x int16, ys []int16, rs, sels []int64) []int64 {
	return int16NeScalarSels(x, ys, rs, sels)
}

func int16NeScalarSelsPure(x int16, ys []int16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Int32Ne(xs, ys []int32, rs []int64) []int64 {
	return int32Ne(xs, ys, rs)
}

func int32NePure(xs, ys []int32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x != ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Int32NeSels(xs, ys []int32, rs, sels []int64) []int64 {
	return int32NeSels(xs, ys, rs, sels)
}

func int32NeSelsPure(xs, ys []int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Int32NeScalar(x int32, ys []int32, rs []int64) []int64 {
	return int32NeScalar(x, ys, rs)
}

func int32NeScalarPure(x int32, ys []int32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x != y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Int32NeScalarSels(x int32, ys []int32, rs, sels []int64) []int64 {
	return int32NeScalarSels(x, ys, rs, sels)
}

func int32NeScalarSelsPure(x int32, ys []int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Int64Ne(xs, ys []int64, rs []int64) []int64 {
	return int64Ne(xs, ys, rs)
}

func int64NePure(xs, ys []int64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x != ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Int64NeSels(xs, ys []int64, rs, sels []int64) []int64 {
	return int64NeSels(xs, ys, rs, sels)
}

func int64NeSelsPure(xs, ys []int64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Int64NeScalar(x int64, ys []int64, rs []int64) []int64 {
	return int64NeScalar(x, ys, rs)
}

func int64NeScalarPure(x int64, ys []int64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x != y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Int64NeScalarSels(x int64, ys []int64, rs, sels []int64) []int64 {
	return int64NeScalarSels(x, ys, rs, sels)
}

func int64NeScalarSelsPure(x int64, ys []int64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Uint8Ne(xs, ys []uint8, rs []int64) []int64 {
	return uint8Ne(xs, ys, rs)
}

func uint8NePure(xs, ys []uint8, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x != ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Uint8NeSels(xs, ys []uint8, rs, sels []int64) []int64 {
	return uint8NeSels(xs, ys, rs, sels)
}

func uint8NeSelsPure(xs, ys []uint8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Uint8NeScalar(x uint8, ys []uint8, rs []int64) []int64 {
	return uint8NeScalar(x, ys, rs)
}

func uint8NeScalarPure(x uint8, ys []uint8, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x != y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Uint8NeScalarSels(x uint8, ys []uint8, rs, sels []int64) []int64 {
	return uint8NeScalarSels(x, ys, rs, sels)
}

func uint8NeScalarSelsPure(x uint8, ys []uint8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Uint16Ne(xs, ys []uint16, rs []int64) []int64 {
	return uint16Ne(xs, ys, rs)
}

func uint16NePure(xs, ys []uint16, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x != ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Uint16NeSels(xs, ys []uint16, rs, sels []int64) []int64 {
	return uint16NeSels(xs, ys, rs, sels)
}

func uint16NeSelsPure(xs, ys []uint16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Uint16NeScalar(x uint16, ys []uint16, rs []int64) []int64 {
	return uint16NeScalar(x, ys, rs)
}

func uint16NeScalarPure(x uint16, ys []uint16, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x != y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Uint16NeScalarSels(x uint16, ys []uint16, rs, sels []int64) []int64 {
	return uint16NeScalarSels(x, ys, rs, sels)
}

func uint16NeScalarSelsPure(x uint16, ys []uint16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Uint32Ne(xs, ys []uint32, rs []int64) []int64 {
	return uint32Ne(xs, ys, rs)
}

func uint32NePure(xs, ys []uint32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x != ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Uint32NeSels(xs, ys []uint32, rs, sels []int64) []int64 {
	return uint32NeSels(xs, ys, rs, sels)
}

func uint32NeSelsPure(xs, ys []uint32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Uint32NeScalar(x uint32, ys []uint32, rs []int64) []int64 {
	return uint32NeScalar(x, ys, rs)
}

func uint32NeScalarPure(x uint32, ys []uint32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x != y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Uint32NeScalarSels(x uint32, ys []uint32, rs, sels []int64) []int64 {
	return uint32NeScalarSels(x, ys, rs, sels)
}

func uint32NeScalarSelsPure(x uint32, ys []uint32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Uint64Ne(xs, ys []uint64, rs []int64) []int64 {
	return uint64Ne(xs, ys, rs)
}

func uint64NePure(xs, ys []uint64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x != ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Uint64NeSels(xs, ys []uint64, rs, sels []int64) []int64 {
	return uint64NeSels(xs, ys, rs, sels)
}

func uint64NeSelsPure(xs, ys []uint64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Uint64NeScalar(x uint64, ys []uint64, rs []int64) []int64 {
	return uint64NeScalar(x, ys, rs)
}

func uint64NeScalarPure(x uint64, ys []uint64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x != y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Uint64NeScalarSels(x uint64, ys []uint64, rs, sels []int64) []int64 {
	return uint64NeScalarSels(x, ys, rs, sels)
}

func uint64NeScalarSelsPure(x uint64, ys []uint64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Float32Ne(xs, ys []float32, rs []int64) []int64 {
	return float32Ne(xs, ys, rs)
}

func float32NePure(xs, ys []float32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x != ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Float32NeSels(xs, ys []float32, rs, sels []int64) []int64 {
	return float32NeSels(xs, ys, rs, sels)
}

func float32NeSelsPure(xs, ys []float32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Float32NeScalar(x float32, ys []float32, rs []int64) []int64 {
	return float32NeScalar(x, ys, rs)
}

func float32NeScalarPure(x float32, ys []float32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x != y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Float32NeScalarSels(x float32, ys []float32, rs, sels []int64) []int64 {
	return float32NeScalarSels(x, ys, rs, sels)
}

func float32NeScalarSelsPure(x float32, ys []float32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Float64Ne(xs, ys []float64, rs []int64) []int64 {
	return float64Ne(xs, ys, rs)
}

func float64NePure(xs, ys []float64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x != ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Float64NeSels(xs, ys []float64, rs, sels []int64) []int64 {
	return float64NeSels(xs, ys, rs, sels)
}

func float64NeSelsPure(xs, ys []float64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func Float64NeScalar(x float64, ys []float64, rs []int64) []int64 {
	return float64NeScalar(x, ys, rs)
}

func float64NeScalarPure(x float64, ys []float64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x != y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func Float64NeScalarSels(x float64, ys []float64, rs, sels []int64) []int64 {
	return float64NeScalarSels(x, ys, rs, sels)
}

func float64NeScalarSelsPure(x float64, ys []float64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func StrNe(xs, ys *types.Bytes, rs []int64) []int64 {
	return strNe(xs, ys, rs)
}

func strNePure(xs, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(xs.Offsets); i < n; i++ {
		if bytes.Compare(xs.Get(i), ys.Get(i)) != 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func StrNeSels(xs, ys *types.Bytes, rs, sels []int64) []int64 {
	return strNeSels(xs, ys, rs, sels)
}

func strNeSelsPure(xs, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(xs.Get(int(sel)), ys.Get(int(sel))) != 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}

func StrNeScalar(x []byte, ys *types.Bytes, rs []int64) []int64 {
	return strNeScalar(x, ys, rs)
}

func strNeScalarPure(x []byte, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(ys.Offsets); i < n; i++ {
		if bytes.Compare(x, ys.Get(i)) != 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs
}

func StrNeScalarSels(x []byte, ys *types.Bytes, rs, sels []int64) []int64 {
	return strNeScalarSels(x, ys, rs, sels)
}

func strNeScalarSelsPure(x []byte, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(x, ys.Get(int(sel))) != 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs
}
