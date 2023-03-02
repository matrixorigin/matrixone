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

package sub

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
	"github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func dec64PtrToC(p *types.Decimal64) *C.int64_t {
	return (*C.int64_t)(unsafe.Pointer(p))
}
func dec128PtrToC(p *types.Decimal128) *C.int64_t {
	return (*C.int64_t)(unsafe.Pointer(p))
}

func Decimal64VecSub(xs, ys, rs *vector.Vector) error {
	xt := vector.MustFixedCol[types.Decimal64](xs)
	yt := vector.MustFixedCol[types.Decimal64](ys)
	rt := vector.MustFixedCol[types.Decimal64](rs)

	flag := 0
	if xs.IsConst() {
		flag |= LEFT_IS_SCALAR
	}
	if ys.IsConst() {
		flag |= RIGHT_IS_SCALAR
	}

	rc := C.Decimal64_VecSub(dec64PtrToC(&rt[0]), dec64PtrToC(&xt[0]), dec64PtrToC(&yt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.GetNulls())), C.int32_t(flag))
	if rc != 0 {
		return moerr.NewOutOfRangeNoCtx("decimal64", "decimal SUB")
	}
	return nil
}

func Decimal128VecSub(xs, ys, rs *vector.Vector) error {
	xt := vector.MustFixedCol[types.Decimal128](xs)
	yt := vector.MustFixedCol[types.Decimal128](ys)
	rt := vector.MustFixedCol[types.Decimal128](rs)

	flag := 0
	if xs.IsConst() {
		flag |= LEFT_IS_SCALAR
	}
	if ys.IsConst() {
		flag |= RIGHT_IS_SCALAR
	}
	rc := C.Decimal128_VecSub(dec128PtrToC(&rt[0]), dec128PtrToC(&xt[0]), dec128PtrToC(&yt[0]),
		C.uint64_t(len(rt)), (*C.uint64_t)(nulls.Ptr(rs.GetNulls())), C.int32_t(flag))
	if rc != 0 {
		return moerr.NewOutOfRangeNoCtx("decimal128", "decimal SUB")
	}
	return nil
}

func DatetimeSub(xs, ys, rs *vector.Vector) error {
	xt := vector.MustFixedCol[types.Datetime](xs)
	yt := vector.MustFixedCol[types.Datetime](ys)
	rt := vector.MustFixedCol[int64](rs)

	if len(xt) == 1 && len(yt) == 1 {
		res := xt[0].DatetimeMinusWithSecond(yt[0])
		rt[0] = res
	} else if len(xt) == 1 {
		for i := range yt {
			res := xt[0].DatetimeMinusWithSecond(yt[i])
			rt[i] = res
		}
	} else if len(yt) == 1 {
		for i := range xt {
			res := xt[i].DatetimeMinusWithSecond(yt[0])
			rt[i] = res
		}
	} else {
		for i := range xt {
			res := xt[i].DatetimeMinusWithSecond(yt[i])
			rt[i] = res
		}
	}
	return nil
}
