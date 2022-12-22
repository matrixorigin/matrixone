package ascii

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"math"
)

var (
	ints   = []int64{1e16, 1e8, 1e4, 1e2, 1e1}
	uints  = []uint64{1e16, 1e8, 1e4, 1e2, 1e1}
	floats = []float64{1e256, 1e128, 1e64, 1e32, 1e16, 1e8, 1e4, 1e2, 1e1}
)

func IntBatch[T types.Ints](xs []T, rs []uint8, nsp *nulls.Nulls) {
	for i, x := range xs {
		if nsp.Contains(uint64(i)) {
			continue
		}
		rs[i] = IntSingle(int64(x))
	}
}

func IntSingle[T types.Ints](val T) uint8 {
	if val < 0 {
		return '-'
	}
	i64Val := int64(val)
	for _, v := range ints {
		if i64Val >= v {
			i64Val /= v
		}
	}
	return uint8(i64Val) + '0'
}

func UintBatch[T types.UInts](xs []T, rs []uint8, nsp *nulls.Nulls) {
	for i, x := range xs {
		if nsp.Contains(uint64(i)) {
			continue
		}
		rs[i] = UintSingle(x)
	}
}

func UintSingle[T types.UInts](val T) uint8 {
	u64Val := uint64(val)
	for _, v := range uints {
		if u64Val >= v {
			u64Val /= v
		}
	}
	return uint8(u64Val) + '0'
}

func FloatBatch[T types.Floats](xs []T, rs []uint8, nsp *nulls.Nulls) {
	for i, x := range xs {
		if nsp.Contains(uint64(i)) {
			continue
		}
		rs[i] = FloatSingle(x)
	}
}

func FloatSingle[T types.Floats](val T) uint8 {
	if val < 0 {
		return '-'
	}
	f64Val := float64(val)
	for _, v := range floats {
		if f64Val >= v {
			f64Val /= v
		}
	}
	tmp := uint8(math.Floor(f64Val))
	return tmp + '0'
}

func StringBatch(xs [][]byte, rs []uint8, nsp *nulls.Nulls) {
	for i, x := range xs {
		if nsp.Contains(uint64(i)) {
			continue
		}
		rs[i] = StringSingle(x)
	}
}

func StringSingle(val []byte) uint8 {
	if len(val) == 0 {
		return 0
	}
	return val[0]
}
