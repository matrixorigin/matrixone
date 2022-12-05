// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sum

/*
#include "mo.h"
#cgo CFLAGS: -I../../../cgo
#cgo LDFLAGS: -L../../../cgo -lmo -lm
*/
import "C"

import (
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"golang.org/x/exp/constraints"
)

var (
	Int8Sum      = signedSum[int8]
	Int16Sum     = signedSum[int16]
	Int32Sum     = signedSum[int32]
	Int64Sum     = signedSum[int64]
	Int8SumSels  = signedSumSels[int8]
	Int16SumSels = signedSumSels[int16]
	Int32SumSels = signedSumSels[int32]
	Int64SumSels = signedSumSels[int64]

	Uint8Sum      = unsignedSum[uint8]
	Uint16Sum     = unsignedSum[uint16]
	Uint32Sum     = unsignedSum[uint32]
	Uint64Sum     = unsignedSum[uint64]
	Uint8SumSels  = unsignedSumSels[uint8]
	Uint16SumSels = unsignedSumSels[uint16]
	Uint32SumSels = unsignedSumSels[uint32]
	Uint64SumSels = unsignedSumSels[uint64]

	Float32Sum     = floatSum[float32]
	Float64Sum     = floatSum[float64]
	Float32SumSels = floatSumSels[float32]
	Float64SumSels = floatSumSels[float64]
)

func signedSum[T constraints.Signed](xs []T) int64 {
	var res int64

	for _, x := range xs {
		res += int64(x)
	}
	return res
}

func signedSumSels[T constraints.Signed](xs []T, sels []int64) int64 {
	var res int64

	for _, sel := range sels {
		res += int64(xs[sel])
	}
	return res
}

func unsignedSum[T constraints.Unsigned](xs []T) uint64 {
	var res uint64

	for _, x := range xs {
		res += uint64(x)
	}
	return res
}

func unsignedSumSels[T constraints.Unsigned](xs []T, sels []int64) uint64 {
	var res uint64

	for _, sel := range sels {
		res += uint64(xs[sel])
	}
	return res
}

func floatSum[T constraints.Float](xs []T) T {
	var res T

	for _, x := range xs {
		res += x
	}
	return res
}

func floatSumSels[T constraints.Float](xs []T, sels []int64) T {
	var res T

	for _, sel := range sels {
		res += xs[sel]
	}
	return res
}

func Decimal64Sum(rs, vs []types.Decimal64, start int64, count int64, vps []uint64, zs []int64, nsp *nulls.Nulls) error {
	rc := C.Decimal64_VecSum((*C.int64_t)(unsafe.Pointer(&rs[0])), (*C.int64_t)(unsafe.Pointer(&vs[0])),
		(C.int64_t)(start), (C.int64_t)(count), (*C.uint64_t)(&vps[0]), (*C.int64_t)(&zs[0]), (*C.uint64_t)(nulls.Ptr(nsp)))
	if rc != 0 {
		return moerr.NewOutOfRangeNoCtx("decimal64", "decimal SUM")
	}
	return nil
}

func Decimal64Sum128(rs []types.Decimal128, vs []types.Decimal64, start int64, count int64, vps []uint64, zs []int64, nsp *nulls.Nulls) error {
	rc := C.Decimal64_VecSumToDecimal128((*C.int64_t)(unsafe.Pointer(&rs[0])), (*C.int64_t)(unsafe.Pointer(&vs[0])),
		(C.int64_t)(start), (C.int64_t)(count), (*C.uint64_t)(&vps[0]), (*C.int64_t)(&zs[0]), (*C.uint64_t)(nulls.Ptr(nsp)))
	if rc != 0 {
		return moerr.NewOutOfRangeNoCtx("decimal128", "decimal SUM")
	}
	return nil
}

func Decimal128Sum(rs, vs []types.Decimal128, start int64, count int64, vps []uint64, zs []int64, nsp *nulls.Nulls) error {
	rc := C.Decimal128_VecSum((*C.int64_t)(unsafe.Pointer(&rs[0])), (*C.int64_t)(unsafe.Pointer(&vs[0])),
		(C.int64_t)(start), (C.int64_t)(count), (*C.uint64_t)(&vps[0]), (*C.int64_t)(&zs[0]), (*C.uint64_t)(nulls.Ptr(nsp)))
	if rc != 0 {
		return moerr.NewOutOfRangeNoCtx("decimal128", "decimal SUM")
	}
	return nil
}

/*
func VecSum(rs, vs []uint64, start int64, count int64, vps []uint64, zs []int64, nulls bitmap) {
	for i := int64(0); i < count; i++ {
		if vps[i] == 0 {
			continue
		}
		if nulls.Contains(i + start) {
			continue
		}
		rs[vps[i]-1] += vs[i+start] * zs[i+start]
	}
}

func VecSumDecimal64(rs, vs []types.Decimal64, start int64, count int64, vps []uint64, zs []int64, nulls bitmap)

func VecSumDecimal64ToDecimal128(rs []types.Decimal128, vs []types.Decimal64, start int64, count int64, vps []uint64, zs []int64, nulls bitmap)

func VecSumDecimal128(rs, vs []types.Decimal128, start int64, count int64, vps []uint64, zs []int64, nulls bitmap)
*/
