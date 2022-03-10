package log

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"math"
)

var (
	logUint8   func([]uint8, []float64) LogResult
	logUint16  func([]uint16, []float64) LogResult
	logUint32  func([]uint32, []float64) LogResult
	logUint64  func([]uint64, []float64) LogResult
	logInt8    func([]int8, []float64) LogResult
	logInt16   func([]int16, []float64) LogResult
	logInt32   func([]int32, []float64) LogResult
	logInt64   func([]int64, []float64) LogResult
	logFloat32 func([]float32, []float64) LogResult
	logFloat64 func([]float64, []float64) LogResult
)

func init() {
	logUint8 = logUint8Pure
	logUint16 = logUint16Pure
	logUint32 = logUint32Pure
	logUint64 = logUint64Pure
	logInt8 = logInt8Pure
	logInt16 = logInt16Pure
	logInt32 = logInt32Pure
	logInt64 = logInt64Pure
	logFloat32 = logFloat32Pure
	logFloat64 = logFloat64Pure
}

type LogResult struct {
	Result []float64
	Nsp    *nulls.Nulls
}

func LogUint8(xs []uint8, rs []float64) LogResult {
	return logUint8(xs, rs)
}

func logUint8Pure(xs []uint8, rs []float64) LogResult {
	result := LogResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		result.Result[i] = math.Log(float64(n))
	}
	return result
}

func LogUint16(xs []uint16, rs []float64) LogResult {
	return logUint16(xs, rs)
}

func logUint16Pure(xs []uint16, rs []float64) LogResult {
	result := LogResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		result.Result[i] = math.Log(float64(n))
	}
	return result
}

func LogUint32(xs []uint32, rs []float64) LogResult {
	return logUint32(xs, rs)
}

func logUint32Pure(xs []uint32, rs []float64) LogResult {
	result := LogResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		result.Result[i] = math.Log(float64(n))
	}
	return result
}

func LogUint64(xs []uint64, rs []float64) LogResult {
	return logUint64(xs, rs)
}

func logUint64Pure(xs []uint64, rs []float64) LogResult {
	result := LogResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		result.Result[i] = math.Log(float64(n))
	}
	return result
}

func LogInt8(xs []int8, rs []float64) LogResult {
	return logInt8(xs, rs)
}

func logInt8Pure(xs []int8, rs []float64) LogResult {
	result := LogResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n <= 0 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Log(float64(n))
		}
	}
	return result
}

func LogInt16(xs []int16, rs []float64) LogResult {
	return logInt16(xs, rs)
}

func logInt16Pure(xs []int16, rs []float64) LogResult {
	result := LogResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n <= 0 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Log(float64(n))
		}
	}
	return result
}

func LogInt32(xs []int32, rs []float64) LogResult {
	return logInt32(xs, rs)
}

func logInt32Pure(xs []int32, rs []float64) LogResult {
	result := LogResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n <= 0 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Log(float64(n))
		}
	}
	return result
}

func LogInt64(xs []int64, rs []float64) LogResult {
	return logInt64(xs, rs)
}

func logInt64Pure(xs []int64, rs []float64) LogResult {
	result := LogResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n <= 0 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Log(float64(n))
		}
	}
	return result
}

func LogFloat32(xs []float32, rs []float64) LogResult {
	return logFloat32(xs, rs)
}

func logFloat32Pure(xs []float32, rs []float64) LogResult {
	result := LogResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n <= 0 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Log(float64(n))
		}
	}
	return result
}

func LogFloat64(xs []float64, rs []float64) LogResult {
	return logFloat64(xs, rs)
}

func logFloat64Pure(xs []float64, rs []float64) LogResult {
	result := LogResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range xs {
		if n <= 0 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Log(n)
		}
	}
	return result
}
