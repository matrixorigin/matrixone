package lt

import (
	"bytes"
	"matrixone/pkg/container/types"
)

var (
	int8Lt              func([]int8, []int8, []int64) []int64
	int8LtSels          func([]int8, []int8, []int64, []int64) []int64
	int8LtScalar        func(int8, []int8, []int64) []int64
	int8LtScalarSels    func(int8, []int8, []int64, []int64) []int64
	int16Lt             func([]int16, []int16, []int64) []int64
	int16LtSels         func([]int16, []int16, []int64, []int64) []int64
	int16LtScalar       func(int16, []int16, []int64) []int64
	int16LtScalarSels   func(int16, []int16, []int64, []int64) []int64
	int32Lt             func([]int32, []int32, []int64) []int64
	int32LtSels         func([]int32, []int32, []int64, []int64) []int64
	int32LtScalar       func(int32, []int32, []int64) []int64
	int32LtScalarSels   func(int32, []int32, []int64, []int64) []int64
	int64Lt             func([]int64, []int64, []int64) []int64
	int64LtSels         func([]int64, []int64, []int64, []int64) []int64
	int64LtScalar       func(int64, []int64, []int64) []int64
	int64LtScalarSels   func(int64, []int64, []int64, []int64) []int64
	uint8Lt             func([]uint8, []uint8, []int64) []int64
	uint8LtSels         func([]uint8, []uint8, []int64, []int64) []int64
	uint8LtScalar       func(uint8, []uint8, []int64) []int64
	uint8LtScalarSels   func(uint8, []uint8, []int64, []int64) []int64
	uint16Lt            func([]uint16, []uint16, []int64) []int64
	uint16LtSels        func([]uint16, []uint16, []int64, []int64) []int64
	uint16LtScalar      func(uint16, []uint16, []int64) []int64
	uint16LtScalarSels  func(uint16, []uint16, []int64, []int64) []int64
	uint32Lt            func([]uint32, []uint32, []int64) []int64
	uint32LtSels        func([]uint32, []uint32, []int64, []int64) []int64
	uint32LtScalar      func(uint32, []uint32, []int64) []int64
	uint32LtScalarSels  func(uint32, []uint32, []int64, []int64) []int64
	uint64Lt            func([]uint64, []uint64, []int64) []int64
	uint64LtSels        func([]uint64, []uint64, []int64, []int64) []int64
	uint64LtScalar      func(uint64, []uint64, []int64) []int64
	uint64LtScalarSels  func(uint64, []uint64, []int64, []int64) []int64
	float32Lt           func([]float32, []float32, []int64) []int64
	float32LtSels       func([]float32, []float32, []int64, []int64) []int64
	float32LtScalar     func(float32, []float32, []int64) []int64
	float32LtScalarSels func(float32, []float32, []int64, []int64) []int64
	float64Lt           func([]float64, []float64, []int64) []int64
	float64LtSels       func([]float64, []float64, []int64, []int64) []int64
	float64LtScalar     func(float64, []float64, []int64) []int64
	float64LtScalarSels func(float64, []float64, []int64, []int64) []int64
	strLt               func(*types.Bytes, *types.Bytes, []int64) []int64
	strLtSels           func(*types.Bytes, *types.Bytes, []int64, []int64) []int64
	strLtScalar         func([]byte, *types.Bytes, []int64) []int64
	strLtScalarSels     func([]byte, *types.Bytes, []int64, []int64) []int64
)

func init() {
	int8Lt = int8LtPure
	int8LtSels = int8LtSelsPure
	int8LtScalar = int8LtScalarPure
	int8LtScalarSels = int8LtScalarSelsPure
	int16Lt = int16LtPure
	int16LtSels = int16LtSelsPure
	int16LtScalar = int16LtScalarPure
	int16LtScalarSels = int16LtScalarSelsPure
	int32Lt = int32LtPure
	int32LtSels = int32LtSelsPure
	int32LtScalar = int32LtScalarPure
	int32LtScalarSels = int32LtScalarSelsPure
	int64Lt = int64LtPure
	int64LtSels = int64LtSelsPure
	int64LtScalar = int64LtScalarPure
	int64LtScalarSels = int64LtScalarSelsPure
	uint8Lt = uint8LtPure
	uint8LtSels = uint8LtSelsPure
	uint8LtScalar = uint8LtScalarPure
	uint8LtScalarSels = uint8LtScalarSelsPure
	uint16Lt = uint16LtPure
	uint16LtSels = uint16LtSelsPure
	uint16LtScalar = uint16LtScalarPure
	uint16LtScalarSels = uint16LtScalarSelsPure
	uint32Lt = uint32LtPure
	uint32LtSels = uint32LtSelsPure
	uint32LtScalar = uint32LtScalarPure
	uint32LtScalarSels = uint32LtScalarSelsPure
	uint64Lt = uint64LtPure
	uint64LtSels = uint64LtSelsPure
	uint64LtScalar = uint64LtScalarPure
	uint64LtScalarSels = uint64LtScalarSelsPure
	float32Lt = float32LtPure
	float32LtSels = float32LtSelsPure
	float32LtScalar = float32LtScalarPure
	float32LtScalarSels = float32LtScalarSelsPure
	float64Lt = float64LtPure
	float64LtSels = float64LtSelsPure
	float64LtScalar = float64LtScalarPure
	float64LtScalarSels = float64LtScalarSelsPure
	strLt = strLtPure
	strLtSels = strLtSelsPure
	strLtScalar = strLtScalarPure
	strLtScalarSels = strLtScalarSelsPure
}

func Int8Lt(xs, ys []int8, rs []int64) []int64 {
	return int8Lt(xs, ys, rs)
}

func int8LtPure(xs, ys []int8, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Int8LtSels(xs, ys []int8, rs, sels []int64) []int64 {
	return int8LtSels(xs, ys, rs, sels)
}

func int8LtSelsPure(xs, ys []int8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Int8LtScalar(x int8, ys []int8, rs []int64) []int64 {
	return int8LtScalar(x, ys, rs)
}

func int8LtScalarPure(x int8, ys []int8, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Int8LtScalarSels(x int8, ys []int8, rs, sels []int64) []int64 {
	return int8LtScalarSels(x, ys, rs, sels)
}

func int8LtScalarSelsPure(x int8, ys []int8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Int16Lt(xs, ys []int16, rs []int64) []int64 {
	return int16Lt(xs, ys, rs)
}

func int16LtPure(xs, ys []int16, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Int16LtSels(xs, ys []int16, rs, sels []int64) []int64 {
	return int16LtSels(xs, ys, rs, sels)
}

func int16LtSelsPure(xs, ys []int16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Int16LtScalar(x int16, ys []int16, rs []int64) []int64 {
	return int16LtScalar(x, ys, rs)
}

func int16LtScalarPure(x int16, ys []int16, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Int16LtScalarSels(x int16, ys []int16, rs, sels []int64) []int64 {
	return int16LtScalarSels(x, ys, rs, sels)
}

func int16LtScalarSelsPure(x int16, ys []int16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Int32Lt(xs, ys []int32, rs []int64) []int64 {
	return int32Lt(xs, ys, rs)
}

func int32LtPure(xs, ys []int32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Int32LtSels(xs, ys []int32, rs, sels []int64) []int64 {
	return int32LtSels(xs, ys, rs, sels)
}

func int32LtSelsPure(xs, ys []int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Int32LtScalar(x int32, ys []int32, rs []int64) []int64 {
	return int32LtScalar(x, ys, rs)
}

func int32LtScalarPure(x int32, ys []int32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Int32LtScalarSels(x int32, ys []int32, rs, sels []int64) []int64 {
	return int32LtScalarSels(x, ys, rs, sels)
}

func int32LtScalarSelsPure(x int32, ys []int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Int64Lt(xs, ys []int64, rs []int64) []int64 {
	return int64Lt(xs, ys, rs)
}

func int64LtPure(xs, ys []int64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Int64LtSels(xs, ys []int64, rs, sels []int64) []int64 {
	return int64LtSels(xs, ys, rs, sels)
}

func int64LtSelsPure(xs, ys []int64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Int64LtScalar(x int64, ys []int64, rs []int64) []int64 {
	return int64LtScalar(x, ys, rs)
}

func int64LtScalarPure(x int64, ys []int64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Int64LtScalarSels(x int64, ys []int64, rs, sels []int64) []int64 {
	return int64LtScalarSels(x, ys, rs, sels)
}

func int64LtScalarSelsPure(x int64, ys []int64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint8Lt(xs, ys []uint8, rs []int64) []int64 {
	return uint8Lt(xs, ys, rs)
}

func uint8LtPure(xs, ys []uint8, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint8LtSels(xs, ys []uint8, rs, sels []int64) []int64 {
	return uint8LtSels(xs, ys, rs, sels)
}

func uint8LtSelsPure(xs, ys []uint8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint8LtScalar(x uint8, ys []uint8, rs []int64) []int64 {
	return uint8LtScalar(x, ys, rs)
}

func uint8LtScalarPure(x uint8, ys []uint8, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint8LtScalarSels(x uint8, ys []uint8, rs, sels []int64) []int64 {
	return uint8LtScalarSels(x, ys, rs, sels)
}

func uint8LtScalarSelsPure(x uint8, ys []uint8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint16Lt(xs, ys []uint16, rs []int64) []int64 {
	return uint16Lt(xs, ys, rs)
}

func uint16LtPure(xs, ys []uint16, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint16LtSels(xs, ys []uint16, rs, sels []int64) []int64 {
	return uint16LtSels(xs, ys, rs, sels)
}

func uint16LtSelsPure(xs, ys []uint16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint16LtScalar(x uint16, ys []uint16, rs []int64) []int64 {
	return uint16LtScalar(x, ys, rs)
}

func uint16LtScalarPure(x uint16, ys []uint16, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint16LtScalarSels(x uint16, ys []uint16, rs, sels []int64) []int64 {
	return uint16LtScalarSels(x, ys, rs, sels)
}

func uint16LtScalarSelsPure(x uint16, ys []uint16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint32Lt(xs, ys []uint32, rs []int64) []int64 {
	return uint32Lt(xs, ys, rs)
}

func uint32LtPure(xs, ys []uint32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint32LtSels(xs, ys []uint32, rs, sels []int64) []int64 {
	return uint32LtSels(xs, ys, rs, sels)
}

func uint32LtSelsPure(xs, ys []uint32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint32LtScalar(x uint32, ys []uint32, rs []int64) []int64 {
	return uint32LtScalar(x, ys, rs)
}

func uint32LtScalarPure(x uint32, ys []uint32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint32LtScalarSels(x uint32, ys []uint32, rs, sels []int64) []int64 {
	return uint32LtScalarSels(x, ys, rs, sels)
}

func uint32LtScalarSelsPure(x uint32, ys []uint32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint64Lt(xs, ys []uint64, rs []int64) []int64 {
	return uint64Lt(xs, ys, rs)
}

func uint64LtPure(xs, ys []uint64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint64LtSels(xs, ys []uint64, rs, sels []int64) []int64 {
	return uint64LtSels(xs, ys, rs, sels)
}

func uint64LtSelsPure(xs, ys []uint64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint64LtScalar(x uint64, ys []uint64, rs []int64) []int64 {
	return uint64LtScalar(x, ys, rs)
}

func uint64LtScalarPure(x uint64, ys []uint64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint64LtScalarSels(x uint64, ys []uint64, rs, sels []int64) []int64 {
	return uint64LtScalarSels(x, ys, rs, sels)
}

func uint64LtScalarSelsPure(x uint64, ys []uint64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Float32Lt(xs, ys []float32, rs []int64) []int64 {
	return float32Lt(xs, ys, rs)
}

func float32LtPure(xs, ys []float32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Float32LtSels(xs, ys []float32, rs, sels []int64) []int64 {
	return float32LtSels(xs, ys, rs, sels)
}

func float32LtSelsPure(xs, ys []float32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Float32LtScalar(x float32, ys []float32, rs []int64) []int64 {
	return float32LtScalar(x, ys, rs)
}

func float32LtScalarPure(x float32, ys []float32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Float32LtScalarSels(x float32, ys []float32, rs, sels []int64) []int64 {
	return float32LtScalarSels(x, ys, rs, sels)
}

func float32LtScalarSelsPure(x float32, ys []float32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Float64Lt(xs, ys []float64, rs []int64) []int64 {
	return float64Lt(xs, ys, rs)
}

func float64LtPure(xs, ys []float64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Float64LtSels(xs, ys []float64, rs, sels []int64) []int64 {
	return float64LtSels(xs, ys, rs, sels)
}

func float64LtSelsPure(xs, ys []float64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Float64LtScalar(x float64, ys []float64, rs []int64) []int64 {
	return float64LtScalar(x, ys, rs)
}

func float64LtScalarPure(x float64, ys []float64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Float64LtScalarSels(x float64, ys []float64, rs, sels []int64) []int64 {
	return float64LtScalarSels(x, ys, rs, sels)
}

func float64LtScalarSelsPure(x float64, ys []float64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func StrLt(xs, ys *types.Bytes, rs []int64) []int64 {
	return strLt(xs, ys, rs)
}

func strLtPure(xs, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(xs.Offsets); i < n; i++ {
		if bytes.Compare(xs.Get(int64(i)), ys.Get(int64(i))) < 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func StrLtSels(xs, ys *types.Bytes, rs, sels []int64) []int64 {
	return strLtSels(xs, ys, rs, sels)
}

func strLtSelsPure(xs, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(xs.Get(sel), ys.Get(sel)) < 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func StrLtScalar(x []byte, ys *types.Bytes, rs []int64) []int64 {
	return strLtScalar(x, ys, rs)
}

func strLtScalarPure(x []byte, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(ys.Offsets); i < n; i++ {
		if bytes.Compare(x, ys.Get(int64(i))) < 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func StrLtScalarSels(x []byte, ys *types.Bytes, rs, sels []int64) []int64 {
	return strLtScalarSels(x, ys, rs, sels)
}

func strLtScalarSelsPure(x []byte, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(x, ys.Get(sel)) < 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}
