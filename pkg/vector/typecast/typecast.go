package typecast

import (
	"matrixbase/pkg/container/nulls"
	"matrixbase/pkg/container/vector"
	"reflect"
	"strconv"
	"unsafe"
)

var (
	bToI64    func([]bool, []int64) []int64
	bToF64    func([]bool, []float64) []float64
	bToString func([]bool, []string) []string

	i64ToB      func([]int64, []bool) []bool
	i64ToF64    func([]int64, []float64) []float64
	i64ToString func([]int64, []string) []string

	f64ToB      func([]float64, []bool) []bool
	f64ToI64    func([]float64, []int64) []int64
	f64ToString func([]float64, []string) []string

	bytesToB   func(*vector.Bytes, []bool, *nulls.Nulls) []bool
	bytesToI64 func(*vector.Bytes, []int64, *nulls.Nulls) []int64
	bytesToF64 func(*vector.Bytes, []float64, *nulls.Nulls) []float64
)

var Bools = [2]string{"false", "true"}

func init() {
	bToI64 = bToI64Pure
	bToF64 = bToF64Pure
	bToString = bToStringPure

	i64ToB = i64ToBPure
	i64ToF64 = i64ToF64Pure
	i64ToString = i64ToStringPure

	f64ToB = f64ToBPure
	f64ToI64 = f64ToI64Pure
	f64ToString = f64ToStringPure

	bytesToB = bytesToBPure
	bytesToI64 = bytesToI64Pure
	bytesToF64 = bytesToF64Pure
}

func BToI64(xs []bool, rs []int64) []int64 {
	return bToI64(xs, rs)
}

func BToF64(xs []bool, rs []float64) []float64 {
	return bToF64(xs, rs)
}

func BToString(xs []bool, rs []string) []string {
	return bToString(xs, rs)
}

func I64ToB(xs []int64, rs []bool) []bool {
	return i64ToB(xs, rs)
}

func I64ToF64(xs []int64, rs []float64) []float64 {
	return i64ToF64(xs, rs)
}

func I64ToString(xs []int64, rs []string) []string {
	return i64ToString(xs, rs)
}

func F64ToB(xs []float64, rs []bool) []bool {
	return f64ToB(xs, rs)
}

func F64ToI64(xs []float64, rs []int64) []int64 {
	return f64ToI64(xs, rs)
}

func F64ToString(xs []float64, rs []string) []string {
	return f64ToString(xs, rs)
}

func BytesToB(xs *vector.Bytes, rs []bool, nsp *nulls.Nulls) []bool {
	return bytesToB(xs, rs, nsp)
}

func BytesToI64(xs *vector.Bytes, rs []int64, nsp *nulls.Nulls) []int64 {
	return bytesToI64(xs, rs, nsp)
}

func BytesToF64(xs *vector.Bytes, rs []float64, nsp *nulls.Nulls) []float64 {
	return bytesToF64(xs, rs, nsp)
}

func bToI64Pure(xs []bool, rs []int64) []int64 {
	var xp []uint8

	{
		hp := *(*reflect.SliceHeader)(unsafe.Pointer(&xs))
		xp = *(*[]uint8)(unsafe.Pointer(&hp))
	}
	for i, x := range xp {
		rs[i] = int64(x)
	}
	return rs
}

func bToF64Pure(xs []bool, rs []float64) []float64 {
	var xp []uint8

	{
		hp := *(*reflect.SliceHeader)(unsafe.Pointer(&xs))
		xp = *(*[]uint8)(unsafe.Pointer(&hp))
	}
	for i, x := range xp {
		rs[i] = float64(x)
	}
	return rs
}

func bToStringPure(xs []bool, rs []string) []string {
	var xp []uint8

	{
		hp := *(*reflect.SliceHeader)(unsafe.Pointer(&xs))
		xp = *(*[]uint8)(unsafe.Pointer(&hp))
	}
	for i, x := range xp {
		rs[i] = Bools[x]
	}
	return rs
}

func i64ToBPure(xs []int64, rs []bool) []bool {
	var rp []uint8

	{
		hp := *(*reflect.SliceHeader)(unsafe.Pointer(&rs))
		rp = *(*[]uint8)(unsafe.Pointer(&hp))
	}
	for i, x := range xs {
		rp[i] = uint8(x)
	}
	return rs
}

func i64ToF64Pure(xs []int64, rs []float64) []float64 {
	for i, x := range xs {
		rs[i] = float64(x)
	}
	return rs
}

func i64ToStringPure(xs []int64, rs []string) []string {
	for i, x := range xs {
		rs[i] = strconv.FormatInt(x, 10)
	}
	return rs
}

func f64ToBPure(xs []float64, rs []bool) []bool {
	var rp []uint8

	{
		hp := *(*reflect.SliceHeader)(unsafe.Pointer(&rs))
		rp = *(*[]uint8)(unsafe.Pointer(&hp))
	}
	for i, x := range xs {
		rp[i] = uint8(x)
	}
	return rs
}

func f64ToI64Pure(xs []float64, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = int64(x)
	}
	return rs
}

func f64ToStringPure(xs []float64, rs []string) []string {
	for i, x := range xs {
		rs[i] = strconv.FormatFloat(x, 'g', -1, 64)
	}
	return rs
}

func bytesToBPure(xs *vector.Bytes, rs []bool, nsp *nulls.Nulls) []bool {
	var err error
	var tm []byte

	for i, o := range xs.Os {
		tm = xs.Data[o : o+xs.Ns[i]]
		if rs[i], err = strconv.ParseBool(*(*string)(unsafe.Pointer(&tm))); err != nil {
			nsp.Add(uint64(i))
		}
	}
	return rs
}

func bytesToI64Pure(xs *vector.Bytes, rs []int64, nsp *nulls.Nulls) []int64 {
	var err error
	var tm []byte

	for i, o := range xs.Os {
		tm = xs.Data[o : o+xs.Ns[i]]
		if rs[i], err = strconv.ParseInt(*(*string)(unsafe.Pointer(&tm)), 0, 64); err != nil {
			nsp.Add(uint64(i))
		}
	}
	return rs
}

func bytesToF64Pure(xs *vector.Bytes, rs []float64, nsp *nulls.Nulls) []float64 {
	var err error
	var tm []byte

	for i, o := range xs.Os {
		tm = xs.Data[o : o+xs.Ns[i]]
		if rs[i], err = strconv.ParseFloat(*(*string)(unsafe.Pointer(&tm)), 64); err != nil {
			nsp.Add(uint64(i))
		}
	}
	return rs
}
