// Copyright 2022 Matrix Origin
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

package compare

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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"golang.org/x/exp/constraints"
)

const (
	LEFT_IS_SCALAR  = 1
	RIGHT_IS_SCALAR = 2
)

func dec64PtrToC(p *types.Decimal64) *C.int64_t {
	return (*C.int64_t)(unsafe.Pointer(p))
}
func dec128PtrToC(p *types.Decimal128) *C.int64_t {
	return (*C.int64_t)(unsafe.Pointer(p))
}

func GetScalarFlag(xs, ys, rs *vector.Vector) int {
	flag := 0
	if xs.IsScalar() {
		flag |= LEFT_IS_SCALAR
	}
	if ys.IsScalar() {
		flag |= RIGHT_IS_SCALAR
	}
	return flag
}

func NumericEqual[T constraints.Integer | constraints.Float | bool](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustTCols[T](xs), vector.MustTCols[T](ys), vector.MustTCols[bool](rs)
	flag := GetScalarFlag(xs, ys, rs)
	rc := C.Numeric_VecEq(unsafe.Pointer(&rt[0]), unsafe.Pointer(&xt[0]), unsafe.Pointer(&yt[0]), C.uint64_t(len(rt)),
		(*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag), C.int32_t(int32(xs.Typ.Oid)))
	if rc != 0 {
		return moerr.NewInvalidArgNoCtx("numeric equal", "")
	}
	return nil
}

func NumericNotEqual[T constraints.Integer | constraints.Float | bool](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustTCols[T](xs), vector.MustTCols[T](ys), vector.MustTCols[bool](rs)
	flag := GetScalarFlag(xs, ys, rs)
	rc := C.Numeric_VecNe(unsafe.Pointer(&rt[0]), unsafe.Pointer(&xt[0]), unsafe.Pointer(&yt[0]), C.uint64_t(len(rt)),
		(*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag), C.int32_t(int32(xs.Typ.Oid)))
	if rc != 0 {
		return moerr.NewInvalidArgNoCtx("numeric not equal", "")
	}
	return nil
}

func NumericGreatThan[T constraints.Integer | constraints.Float | bool](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustTCols[T](xs), vector.MustTCols[T](ys), vector.MustTCols[bool](rs)
	flag := GetScalarFlag(xs, ys, rs)
	rc := C.Numeric_VecGt(unsafe.Pointer(&rt[0]), unsafe.Pointer(&xt[0]), unsafe.Pointer(&yt[0]), C.uint64_t(len(rt)),
		(*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag), C.int32_t(int32(xs.Typ.Oid)))
	if rc != 0 {
		return moerr.NewInvalidArgNoCtx("numeric greater than", "")
	}
	return nil
}

func NumericGreatEqual[T constraints.Integer | constraints.Float | bool](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustTCols[T](xs), vector.MustTCols[T](ys), vector.MustTCols[bool](rs)
	flag := GetScalarFlag(xs, ys, rs)
	rc := C.Numeric_VecGe(unsafe.Pointer(&rt[0]), unsafe.Pointer(&xt[0]), unsafe.Pointer(&yt[0]), C.uint64_t(len(rt)),
		(*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag), C.int32_t(int32(xs.Typ.Oid)))
	if rc != 0 {
		return moerr.NewInvalidArgNoCtx("numeric greater equal", "")
	}
	return nil
}

func NumericLessThan[T constraints.Integer | constraints.Float | bool](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustTCols[T](xs), vector.MustTCols[T](ys), vector.MustTCols[bool](rs)
	flag := GetScalarFlag(xs, ys, rs)
	rc := C.Numeric_VecLt(unsafe.Pointer(&rt[0]), unsafe.Pointer(&xt[0]), unsafe.Pointer(&yt[0]), C.uint64_t(len(rt)),
		(*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag), C.int32_t(int32(xs.Typ.Oid)))
	if rc != 0 {
		return moerr.NewInvalidArgNoCtx("numeric less than", "")
	}
	return nil
}

func NumericLessEqual[T constraints.Integer | constraints.Float | bool](xs, ys, rs *vector.Vector) error {
	xt, yt, rt := vector.MustTCols[T](xs), vector.MustTCols[T](ys), vector.MustTCols[bool](rs)
	flag := GetScalarFlag(xs, ys, rs)
	rc := C.Numeric_VecLe(unsafe.Pointer(&rt[0]), unsafe.Pointer(&xt[0]), unsafe.Pointer(&yt[0]), C.uint64_t(len(rt)),
		(*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag), C.int32_t(int32(xs.Typ.Oid)))
	if rc != 0 {
		return moerr.NewInvalidArgNoCtx("numeric less equal", "")
	}
	return nil
}

func Decimal64VecEq(xs, ys, rs *vector.Vector) error {
	xt := vector.MustTCols[types.Decimal64](xs)
	yt := vector.MustTCols[types.Decimal64](ys)
	rt := vector.MustTCols[bool](rs)
	flag := GetScalarFlag(xs, ys, rs)
	rc := C.Decimal64_VecEQ((*C.bool)(&rt[0]), dec64PtrToC(&xt[0]), dec64PtrToC(&yt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag))
	if rc != 0 {
		return moerr.NewInvalidArgNoCtx("decimal64 equal", "")
	}
	return nil
}

func Decimal128VecEq(xs, ys, rs *vector.Vector) error {
	xt := vector.MustTCols[types.Decimal128](xs)
	yt := vector.MustTCols[types.Decimal128](ys)
	rt := vector.MustTCols[bool](rs)
	flag := GetScalarFlag(xs, ys, rs)
	rc := C.Decimal128_VecEQ((*C.bool)(&rt[0]), dec128PtrToC(&xt[0]), dec128PtrToC(&yt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag))
	if rc != 0 {
		return moerr.NewInvalidArgNoCtx("decimal128 equal", "")
	}
	return nil
}

func Decimal64VecNe(xs, ys, rs *vector.Vector) error {
	xt := vector.MustTCols[types.Decimal64](xs)
	yt := vector.MustTCols[types.Decimal64](ys)
	rt := vector.MustTCols[bool](rs)
	flag := GetScalarFlag(xs, ys, rs)
	rc := C.Decimal64_VecNE((*C.bool)(&rt[0]), dec64PtrToC(&xt[0]), dec64PtrToC(&yt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag))
	if rc != 0 {
		return moerr.NewInvalidArgNoCtx("decimal64 not equal", "")
	}
	return nil
}

func Decimal128VecNe(xs, ys, rs *vector.Vector) error {
	xt := vector.MustTCols[types.Decimal128](xs)
	yt := vector.MustTCols[types.Decimal128](ys)
	rt := vector.MustTCols[bool](rs)
	flag := GetScalarFlag(xs, ys, rs)
	rc := C.Decimal128_VecNE((*C.bool)(&rt[0]), dec128PtrToC(&xt[0]), dec128PtrToC(&yt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag))
	if rc != 0 {
		return moerr.NewInvalidArgNoCtx("decimal128 not equal", "")
	}
	return nil
}

func Decimal64VecGt(xs, ys, rs *vector.Vector) error {
	xt := vector.MustTCols[types.Decimal64](xs)
	yt := vector.MustTCols[types.Decimal64](ys)
	rt := vector.MustTCols[bool](rs)
	flag := GetScalarFlag(xs, ys, rs)
	rc := C.Decimal64_VecGT((*C.bool)(&rt[0]), dec64PtrToC(&xt[0]), dec64PtrToC(&yt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag))
	if rc != 0 {
		return moerr.NewInvalidArgNoCtx("decimal64 greater than", "")
	}
	return nil
}

func Decimal128VecGt(xs, ys, rs *vector.Vector) error {
	xt := vector.MustTCols[types.Decimal128](xs)
	yt := vector.MustTCols[types.Decimal128](ys)
	rt := vector.MustTCols[bool](rs)
	flag := GetScalarFlag(xs, ys, rs)
	rc := C.Decimal128_VecGT((*C.bool)(&rt[0]), dec128PtrToC(&xt[0]), dec128PtrToC(&yt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag))
	if rc != 0 {
		return moerr.NewInvalidArgNoCtx("decimal128 greater than", "")
	}
	return nil
}

func Decimal64VecGe(xs, ys, rs *vector.Vector) error {
	xt := vector.MustTCols[types.Decimal64](xs)
	yt := vector.MustTCols[types.Decimal64](ys)
	rt := vector.MustTCols[bool](rs)
	flag := GetScalarFlag(xs, ys, rs)
	rc := C.Decimal64_VecGE((*C.bool)(&rt[0]), dec64PtrToC(&xt[0]), dec64PtrToC(&yt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag))
	if rc != 0 {
		return moerr.NewInvalidArgNoCtx("decimal64 greater equal", "")
	}
	return nil
}

func Decimal128VecGe(xs, ys, rs *vector.Vector) error {
	xt := vector.MustTCols[types.Decimal128](xs)
	yt := vector.MustTCols[types.Decimal128](ys)
	rt := vector.MustTCols[bool](rs)
	flag := GetScalarFlag(xs, ys, rs)
	rc := C.Decimal128_VecGE((*C.bool)(&rt[0]), dec128PtrToC(&xt[0]), dec128PtrToC(&yt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag))
	if rc != 0 {
		return moerr.NewInvalidArgNoCtx("decimal128 greater equal", "")
	}
	return nil
}

func Decimal64VecLt(xs, ys, rs *vector.Vector) error {
	xt := vector.MustTCols[types.Decimal64](xs)
	yt := vector.MustTCols[types.Decimal64](ys)
	rt := vector.MustTCols[bool](rs)
	flag := GetScalarFlag(xs, ys, rs)
	rc := C.Decimal64_VecLT((*C.bool)(&rt[0]), dec64PtrToC(&xt[0]), dec64PtrToC(&yt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag))
	if rc != 0 {
		return moerr.NewInvalidArgNoCtx("decimal64 less than", "")
	}
	return nil
}

func Decimal128VecLt(xs, ys, rs *vector.Vector) error {
	xt := vector.MustTCols[types.Decimal128](xs)
	yt := vector.MustTCols[types.Decimal128](ys)
	rt := vector.MustTCols[bool](rs)
	flag := GetScalarFlag(xs, ys, rs)
	rc := C.Decimal128_VecLT((*C.bool)(&rt[0]), dec128PtrToC(&xt[0]), dec128PtrToC(&yt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag))
	if rc != 0 {
		return moerr.NewInvalidArgNoCtx("decimal128 less than", "")
	}
	return nil
}

func Decimal64VecLe(xs, ys, rs *vector.Vector) error {
	xt := vector.MustTCols[types.Decimal64](xs)
	yt := vector.MustTCols[types.Decimal64](ys)
	rt := vector.MustTCols[bool](rs)
	flag := GetScalarFlag(xs, ys, rs)
	rc := C.Decimal64_VecLE((*C.bool)(&rt[0]), dec64PtrToC(&xt[0]), dec64PtrToC(&yt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag))
	if rc != 0 {
		return moerr.NewInvalidArgNoCtx("decimal64 less equal", "")
	}
	return nil
}

func Decimal128VecLe(xs, ys, rs *vector.Vector) error {
	xt := vector.MustTCols[types.Decimal128](xs)
	yt := vector.MustTCols[types.Decimal128](ys)
	rt := vector.MustTCols[bool](rs)
	flag := GetScalarFlag(xs, ys, rs)
	rc := C.Decimal128_VecLE((*C.bool)(&rt[0]), dec128PtrToC(&xt[0]), dec128PtrToC(&yt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.Nsp)), C.int32_t(flag))
	if rc != 0 {
		return moerr.NewInvalidArgNoCtx("decimal128 less equal", "")
	}
	return nil
}
