package le

import (
	"bytes"
	"matrixone/pkg/container/types"
)

var (
	int8Le              func([]int8, []int8, []int64) []int64
	int8LeSels          func([]int8, []int8, []int64, []int64) []int64
	int8LeScalar        func(int8, []int8, []int64) []int64
	int8LeScalarSels    func(int8, []int8, []int64, []int64) []int64
	int16Le             func([]int16, []int16, []int64) []int64
	int16LeSels         func([]int16, []int16, []int64, []int64) []int64
	int16LeScalar       func(int16, []int16, []int64) []int64
	int16LeScalarSels   func(int16, []int16, []int64, []int64) []int64
	int32Le             func([]int32, []int32, []int64) []int64
	int32LeSels         func([]int32, []int32, []int64, []int64) []int64
	int32LeScalar       func(int32, []int32, []int64) []int64
	int32LeScalarSels   func(int32, []int32, []int64, []int64) []int64
	int64Le             func([]int64, []int64, []int64) []int64
	int64LeSels         func([]int64, []int64, []int64, []int64) []int64
	int64LeScalar       func(int64, []int64, []int64) []int64
	int64LeScalarSels   func(int64, []int64, []int64, []int64) []int64
	uint8Le             func([]uint8, []uint8, []int64) []int64
	uint8LeSels         func([]uint8, []uint8, []int64, []int64) []int64
	uint8LeScalar       func(uint8, []uint8, []int64) []int64
	uint8LeScalarSels   func(uint8, []uint8, []int64, []int64) []int64
	uint16Le            func([]uint16, []uint16, []int64) []int64
	uint16LeSels        func([]uint16, []uint16, []int64, []int64) []int64
	uint16LeScalar      func(uint16, []uint16, []int64) []int64
	uint16LeScalarSels  func(uint16, []uint16, []int64, []int64) []int64
	uint32Le            func([]uint32, []uint32, []int64) []int64
	uint32LeSels        func([]uint32, []uint32, []int64, []int64) []int64
	uint32LeScalar      func(uint32, []uint32, []int64) []int64
	uint32LeScalarSels  func(uint32, []uint32, []int64, []int64) []int64
	uint64Le            func([]uint64, []uint64, []int64) []int64
	uint64LeSels        func([]uint64, []uint64, []int64, []int64) []int64
	uint64LeScalar      func(uint64, []uint64, []int64) []int64
	uint64LeScalarSels  func(uint64, []uint64, []int64, []int64) []int64
	float32Le           func([]float32, []float32, []int64) []int64
	float32LeSels       func([]float32, []float32, []int64, []int64) []int64
	float32LeScalar     func(float32, []float32, []int64) []int64
	float32LeScalarSels func(float32, []float32, []int64, []int64) []int64
	float64Le           func([]float64, []float64, []int64) []int64
	float64LeSels       func([]float64, []float64, []int64, []int64) []int64
	float64LeScalar     func(float64, []float64, []int64) []int64
	float64LeScalarSels func(float64, []float64, []int64, []int64) []int64
	strLe               func(*types.Bytes, *types.Bytes, []int64) []int64
	strLeSels           func(*types.Bytes, *types.Bytes, []int64, []int64) []int64
	strLeScalar         func([]byte, *types.Bytes, []int64) []int64
	strLeScalarSels     func([]byte, *types.Bytes, []int64, []int64) []int64
)

func init() {
	int8Le = int8LePure
	int8LeSels = int8LeSelsPure
	int8LeScalar = int8LeScalarPure
	int8LeScalarSels = int8LeScalarSelsPure
	int16Le = int16LePure
	int16LeSels = int16LeSelsPure
	int16LeScalar = int16LeScalarPure
	int16LeScalarSels = int16LeScalarSelsPure
	int32Le = int32LePure
	int32LeSels = int32LeSelsPure
	int32LeScalar = int32LeScalarPure
	int32LeScalarSels = int32LeScalarSelsPure
	int64Le = int64LePure
	int64LeSels = int64LeSelsPure
	int64LeScalar = int64LeScalarPure
	int64LeScalarSels = int64LeScalarSelsPure
	uint8Le = uint8LePure
	uint8LeSels = uint8LeSelsPure
	uint8LeScalar = uint8LeScalarPure
	uint8LeScalarSels = uint8LeScalarSelsPure
	uint16Le = uint16LePure
	uint16LeSels = uint16LeSelsPure
	uint16LeScalar = uint16LeScalarPure
	uint16LeScalarSels = uint16LeScalarSelsPure
	uint32Le = uint32LePure
	uint32LeSels = uint32LeSelsPure
	uint32LeScalar = uint32LeScalarPure
	uint32LeScalarSels = uint32LeScalarSelsPure
	uint64Le = uint64LePure
	uint64LeSels = uint64LeSelsPure
	uint64LeScalar = uint64LeScalarPure
	uint64LeScalarSels = uint64LeScalarSelsPure
	float32Le = float32LePure
	float32LeSels = float32LeSelsPure
	float32LeScalar = float32LeScalarPure
	float32LeScalarSels = float32LeScalarSelsPure
	float64Le = float64LePure
	float64LeSels = float64LeSelsPure
	float64LeScalar = float64LeScalarPure
	float64LeScalarSels = float64LeScalarSelsPure
	strLe = strLePure
	strLeSels = strLeSelsPure
	strLeScalar = strLeScalarPure
	strLeScalarSels = strLeScalarSelsPure
}

func Int8Le(xs, ys []int8, rs []int64) []int64 {
	return int8Le(xs, ys, rs)
}

func int8LePure(xs, ys []int8, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Int8LeSels(xs, ys []int8, rs, sels []int64) []int64 {
	return int8LeSels(xs, ys, rs, sels)
}

func int8LeSelsPure(xs, ys []int8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Int8LeScalar(x int8, ys []int8, rs []int64) []int64 {
	return int8LeScalar(x, ys, rs)
}

func int8LeScalarPure(x int8, ys []int8, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Int8LeScalarSels(x int8, ys []int8, rs, sels []int64) []int64 {
	return int8LeScalarSels(x, ys, rs, sels)
}

func int8LeScalarSelsPure(x int8, ys []int8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Int16Le(xs, ys []int16, rs []int64) []int64 {
	return int16Le(xs, ys, rs)
}

func int16LePure(xs, ys []int16, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Int16LeSels(xs, ys []int16, rs, sels []int64) []int64 {
	return int16LeSels(xs, ys, rs, sels)
}

func int16LeSelsPure(xs, ys []int16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Int16LeScalar(x int16, ys []int16, rs []int64) []int64 {
	return int16LeScalar(x, ys, rs)
}

func int16LeScalarPure(x int16, ys []int16, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Int16LeScalarSels(x int16, ys []int16, rs, sels []int64) []int64 {
	return int16LeScalarSels(x, ys, rs, sels)
}

func int16LeScalarSelsPure(x int16, ys []int16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Int32Le(xs, ys []int32, rs []int64) []int64 {
	return int32Le(xs, ys, rs)
}

func int32LePure(xs, ys []int32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Int32LeSels(xs, ys []int32, rs, sels []int64) []int64 {
	return int32LeSels(xs, ys, rs, sels)
}

func int32LeSelsPure(xs, ys []int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Int32LeScalar(x int32, ys []int32, rs []int64) []int64 {
	return int32LeScalar(x, ys, rs)
}

func int32LeScalarPure(x int32, ys []int32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Int32LeScalarSels(x int32, ys []int32, rs, sels []int64) []int64 {
	return int32LeScalarSels(x, ys, rs, sels)
}

func int32LeScalarSelsPure(x int32, ys []int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Int64Le(xs, ys []int64, rs []int64) []int64 {
	return int64Le(xs, ys, rs)
}

func int64LePure(xs, ys []int64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Int64LeSels(xs, ys []int64, rs, sels []int64) []int64 {
	return int64LeSels(xs, ys, rs, sels)
}

func int64LeSelsPure(xs, ys []int64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Int64LeScalar(x int64, ys []int64, rs []int64) []int64 {
	return int64LeScalar(x, ys, rs)
}

func int64LeScalarPure(x int64, ys []int64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Int64LeScalarSels(x int64, ys []int64, rs, sels []int64) []int64 {
	return int64LeScalarSels(x, ys, rs, sels)
}

func int64LeScalarSelsPure(x int64, ys []int64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint8Le(xs, ys []uint8, rs []int64) []int64 {
	return uint8Le(xs, ys, rs)
}

func uint8LePure(xs, ys []uint8, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint8LeSels(xs, ys []uint8, rs, sels []int64) []int64 {
	return uint8LeSels(xs, ys, rs, sels)
}

func uint8LeSelsPure(xs, ys []uint8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint8LeScalar(x uint8, ys []uint8, rs []int64) []int64 {
	return uint8LeScalar(x, ys, rs)
}

func uint8LeScalarPure(x uint8, ys []uint8, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint8LeScalarSels(x uint8, ys []uint8, rs, sels []int64) []int64 {
	return uint8LeScalarSels(x, ys, rs, sels)
}

func uint8LeScalarSelsPure(x uint8, ys []uint8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint16Le(xs, ys []uint16, rs []int64) []int64 {
	return uint16Le(xs, ys, rs)
}

func uint16LePure(xs, ys []uint16, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint16LeSels(xs, ys []uint16, rs, sels []int64) []int64 {
	return uint16LeSels(xs, ys, rs, sels)
}

func uint16LeSelsPure(xs, ys []uint16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint16LeScalar(x uint16, ys []uint16, rs []int64) []int64 {
	return uint16LeScalar(x, ys, rs)
}

func uint16LeScalarPure(x uint16, ys []uint16, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint16LeScalarSels(x uint16, ys []uint16, rs, sels []int64) []int64 {
	return uint16LeScalarSels(x, ys, rs, sels)
}

func uint16LeScalarSelsPure(x uint16, ys []uint16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint32Le(xs, ys []uint32, rs []int64) []int64 {
	return uint32Le(xs, ys, rs)
}

func uint32LePure(xs, ys []uint32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint32LeSels(xs, ys []uint32, rs, sels []int64) []int64 {
	return uint32LeSels(xs, ys, rs, sels)
}

func uint32LeSelsPure(xs, ys []uint32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint32LeScalar(x uint32, ys []uint32, rs []int64) []int64 {
	return uint32LeScalar(x, ys, rs)
}

func uint32LeScalarPure(x uint32, ys []uint32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint32LeScalarSels(x uint32, ys []uint32, rs, sels []int64) []int64 {
	return uint32LeScalarSels(x, ys, rs, sels)
}

func uint32LeScalarSelsPure(x uint32, ys []uint32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint64Le(xs, ys []uint64, rs []int64) []int64 {
	return uint64Le(xs, ys, rs)
}

func uint64LePure(xs, ys []uint64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint64LeSels(xs, ys []uint64, rs, sels []int64) []int64 {
	return uint64LeSels(xs, ys, rs, sels)
}

func uint64LeSelsPure(xs, ys []uint64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint64LeScalar(x uint64, ys []uint64, rs []int64) []int64 {
	return uint64LeScalar(x, ys, rs)
}

func uint64LeScalarPure(x uint64, ys []uint64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Uint64LeScalarSels(x uint64, ys []uint64, rs, sels []int64) []int64 {
	return uint64LeScalarSels(x, ys, rs, sels)
}

func uint64LeScalarSelsPure(x uint64, ys []uint64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Float32Le(xs, ys []float32, rs []int64) []int64 {
	return float32Le(xs, ys, rs)
}

func float32LePure(xs, ys []float32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Float32LeSels(xs, ys []float32, rs, sels []int64) []int64 {
	return float32LeSels(xs, ys, rs, sels)
}

func float32LeSelsPure(xs, ys []float32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Float32LeScalar(x float32, ys []float32, rs []int64) []int64 {
	return float32LeScalar(x, ys, rs)
}

func float32LeScalarPure(x float32, ys []float32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Float32LeScalarSels(x float32, ys []float32, rs, sels []int64) []int64 {
	return float32LeScalarSels(x, ys, rs, sels)
}

func float32LeScalarSelsPure(x float32, ys []float32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Float64Le(xs, ys []float64, rs []int64) []int64 {
	return float64Le(xs, ys, rs)
}

func float64LePure(xs, ys []float64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Float64LeSels(xs, ys []float64, rs, sels []int64) []int64 {
	return float64LeSels(xs, ys, rs, sels)
}

func float64LeSelsPure(xs, ys []float64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func Float64LeScalar(x float64, ys []float64, rs []int64) []int64 {
	return float64LeScalar(x, ys, rs)
}

func float64LeScalarPure(x float64, ys []float64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func Float64LeScalarSels(x float64, ys []float64, rs, sels []int64) []int64 {
	return float64LeScalarSels(x, ys, rs, sels)
}

func float64LeScalarSelsPure(x float64, ys []float64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func StrLe(xs, ys *types.Bytes, rs []int64) []int64 {
	return strLe(xs, ys, rs)
}

func strLePure(xs, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(xs.Offsets); i < n; i++ {
		if bytes.Compare(xs.Get(int64(i)), ys.Get(int64(i))) <= 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func StrLeSels(xs, ys *types.Bytes, rs, sels []int64) []int64 {
	return strLeSels(xs, ys, rs, sels)
}

func strLeSelsPure(xs, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(xs.Get(sel), ys.Get(sel)) <= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func StrLeScalar(x []byte, ys *types.Bytes, rs []int64) []int64 {
	return strLeScalar(x, ys, rs)
}

func strLeScalarPure(x []byte, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(ys.Offsets); i < n; i++ {
		if bytes.Compare(x, ys.Get(int64(i))) <= 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func StrLeScalarSels(x []byte, ys *types.Bytes, rs, sels []int64) []int64 {
	return strLeScalarSels(x, ys, rs, sels)
}

func strLeScalarSelsPure(x []byte, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(x, ys.Get(sel)) <= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}
